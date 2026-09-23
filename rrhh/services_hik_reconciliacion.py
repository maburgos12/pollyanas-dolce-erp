from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4

from django.db import transaction
from django.utils import timezone

from core.models import AuditLog

from .models import (
    AsistenciaEmpleado,
    Empleado,
    EventoHikCloud,
    HoraExtra,
    IncidenciaAsistencia,
)
from .services import usuario_jefe_directo_de_empleado
from .services_extra_bloqueos import bloquear_jornadas_extra
from .services_extra_conciliacion import NOTA_EXTRA_AUTOMATICA
from .services_hik_ingesta import _run_post_projection_effects, project_receipt


class ReconciliacionHikError(ValueError):
    pass


@dataclass(frozen=True)
class PlanReconciliacionHik:
    codigo_afectado: str
    codigo_origen: str
    empleado_origen_id: int
    empleado_destino_id: int
    desde: date
    hasta: date
    recibo_ids: tuple[int, ...]
    asistencia_ids: tuple[int, ...]
    hora_extra_ids: tuple[int, ...]
    incidencia_ids: tuple[int, ...]
    recibos_rechazados_ids: tuple[int, ...]

    def resumen(self, *, modo: str) -> dict:
        return {
            "modo": modo,
            "codigo_afectado": self.codigo_afectado,
            "codigo_origen": self.codigo_origen,
            "empleado_origen_id": self.empleado_origen_id,
            "empleado_destino_id": self.empleado_destino_id,
            "desde": self.desde.isoformat(),
            "hasta": self.hasta.isoformat(),
            "conteos": {
                "recibos": len(self.recibo_ids),
                "asistencias": len(self.asistencia_ids),
                "horas_extra": len(self.hora_extra_ids),
                "incidencias": len(self.incidencia_ids),
                "recibos_rechazados": len(self.recibos_rechazados_ids),
            },
            "ids": {
                "recibos": list(self.recibo_ids),
                "asistencias": list(self.asistencia_ids),
                "horas_extra": list(self.hora_extra_ids),
                "incidencias": list(self.incidencia_ids),
            },
        }


def _limites(desde: date, hasta: date) -> tuple[datetime, datetime]:
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(desde, time.min), tz)
    fin = timezone.make_aware(datetime.combine(hasta + timedelta(days=1), time.min), tz)
    return inicio, fin


def preparar_reconciliacion(
    *,
    codigo_afectado: str,
    codigo_origen: str,
    empleado_origen_id: int,
    empleado_destino_id: int,
    desde: date,
    hasta: date,
) -> PlanReconciliacionHik:
    codigo_afectado = (codigo_afectado or "").strip()
    codigo_origen = (codigo_origen or "").strip()
    if not codigo_afectado or not codigo_origen or codigo_afectado == codigo_origen:
        raise ReconciliacionHikError("Los dos códigos son obligatorios y deben ser distintos.")
    if desde > hasta:
        raise ReconciliacionHikError("El rango de fechas es inválido.")
    if (hasta - desde).days > 31:
        raise ReconciliacionHikError("El rango no puede exceder 32 días.")
    if empleado_origen_id == empleado_destino_id:
        raise ReconciliacionHikError("Origen y destino deben ser personas distintas.")

    try:
        origen = Empleado.objects.get(pk=empleado_origen_id)
        destino = Empleado.objects.get(pk=empleado_destino_id)
    except Empleado.DoesNotExist as exc:
        raise ReconciliacionHikError("No existe uno de los empleados indicados.") from exc

    baja = origen.bajas_rrhh.order_by("-fecha_baja", "-pk").first()
    if origen.activo or not baja or desde <= baja.fecha_baja:
        raise ReconciliacionHikError(
            "El origen debe estar inactivo y todo el rango debe ser posterior a su baja."
        )
    if not destino.activo:
        raise ReconciliacionHikError("El destino debe permanecer activo.")
    if origen.codigo != codigo_afectado or destino.codigo != codigo_origen:
        raise ReconciliacionHikError(
            "Los códigos actuales no coinciden con el intercambio solicitado."
        )

    inicio, fin = _limites(desde, hasta)
    historia_afectado = EventoHikCloud.objects.filter(
        codigo_externo__iexact=codigo_afectado,
        empleado_id=destino.id,
        ocurrido_en__lt=inicio,
    ).exists()
    if not historia_afectado:
        raise ReconciliacionHikError(
            "El código afectado no tiene historia previa comprobable en el destino."
        )
    historia_origen = EventoHikCloud.objects.filter(
        codigo_externo__iexact=codigo_origen,
        empleado_id=origen.id,
        ocurrido_en__lt=inicio,
    ).exists()
    if not historia_origen:
        raise ReconciliacionHikError(
            "El código que se restaurará no tiene historia previa comprobable en el origen."
        )

    recibos_rango = EventoHikCloud.objects.filter(
        codigo_externo__iexact=codigo_afectado,
        ocurrido_en__gte=inicio,
        ocurrido_en__lt=fin,
    ).order_by("pk")
    recibos = list(recibos_rango)
    if any(item.empleado_id != origen.id for item in recibos):
        raise ReconciliacionHikError(
            "Hay recibos del código afectado ligados a una tercera identidad en el rango."
        )
    if not recibos:
        raise ReconciliacionHikError("No hay recibos Hik para reconciliar en el rango.")
    estados_permitidos = {
        EventoHikCloud.ESTADO_ACEPTADO,
        EventoHikCloud.ESTADO_RECHAZADO,
    }
    if any(item.estado not in estados_permitidos for item in recibos):
        raise ReconciliacionHikError(
            "Hay recibos no terminales en el rango; deben resolverse antes de reconciliar."
        )

    asistencias = list(
        AsistenciaEmpleado.objects.filter(
            empleado_id=origen.id,
            fecha__gte=desde,
            fecha__lte=hasta,
        ).order_by("pk")
    )
    fuentes_hik = {
        AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        AsistenciaEmpleado.FUENTE_HIKCONNECT_EXCEL,
    }
    if any(item.fuente not in fuentes_hik for item in asistencias):
        raise ReconciliacionHikError("Una asistencia no proviene de Hik; se requiere revisión humana.")
    fechas = {item.fecha for item in asistencias}
    fechas_recibos = {timezone.localtime(item.ocurrido_en).date() for item in recibos}
    if not fechas.issubset(fechas_recibos):
        raise ReconciliacionHikError(
            "Una asistencia del origen no está respaldada por recibos del código afectado."
        )
    fechas_aceptadas = {
        timezone.localtime(item.ocurrido_en).date()
        for item in recibos
        if item.estado == EventoHikCloud.ESTADO_ACEPTADO
    }
    if not fechas_aceptadas.issubset(fechas):
        raise ReconciliacionHikError(
            "Un recibo aceptado no tiene asistencia Hik proyectada en el origen."
        )
    if fechas and AsistenciaEmpleado.objects.filter(
        empleado_id=destino.id,
        fecha__in=fechas,
    ).exists():
        raise ReconciliacionHikError("Ya existe asistencia del destino en una fecha afectada.")

    extras = list(
        HoraExtra.objects.filter(
            empleado_id=origen.id,
            fecha__in=fechas,
        ).order_by("pk")
    )
    for extra in extras:
        if (
            extra.estado != HoraExtra.ESTADO_PENDIENTE
            or extra.ajuste_autorizacion
            or not (extra.notas or "").startswith(NOTA_EXTRA_AUTOMATICA)
            or extra.asistencia_id not in {item.id for item in asistencias}
        ):
            raise ReconciliacionHikError(
                f"La hora extra protegida {extra.id} requiere revisión humana y bloquea la corrección."
            )

    incidencias = list(
        IncidenciaAsistencia.objects.filter(
            asistencia_id__in=[item.id for item in asistencias]
        ).order_by("pk")
    )
    return PlanReconciliacionHik(
        codigo_afectado=codigo_afectado,
        codigo_origen=codigo_origen,
        empleado_origen_id=origen.id,
        empleado_destino_id=destino.id,
        desde=desde,
        hasta=hasta,
        recibo_ids=tuple(item.id for item in recibos),
        asistencia_ids=tuple(item.id for item in asistencias),
        hora_extra_ids=tuple(item.id for item in extras),
        incidencia_ids=tuple(item.id for item in incidencias),
        recibos_rechazados_ids=tuple(
            item.id for item in recibos
            if item.estado == EventoHikCloud.ESTADO_RECHAZADO
        ),
    )


def _snapshot(plan: PlanReconciliacionHik) -> dict:
    campos_empleado = ["id", "codigo", "nombre", "activo"]
    return {
        "creado_en": timezone.now().isoformat(),
        "plan": {**asdict(plan), "desde": plan.desde.isoformat(), "hasta": plan.hasta.isoformat()},
        "conteos": plan.resumen(modo="backup")["conteos"],
        "empleados": list(
            Empleado.objects.filter(
                pk__in=[plan.empleado_origen_id, plan.empleado_destino_id]
            ).order_by("pk").values(*campos_empleado)
        ),
        "recibos": list(EventoHikCloud.objects.filter(pk__in=plan.recibo_ids).order_by("pk").values()),
        "asistencias": list(
            AsistenciaEmpleado.objects.filter(pk__in=plan.asistencia_ids).order_by("pk").values()
        ),
        "horas_extra": list(HoraExtra.objects.filter(pk__in=plan.hora_extra_ids).order_by("pk").values()),
        "incidencias": list(
            IncidenciaAsistencia.objects.filter(pk__in=plan.incidencia_ids).order_by("pk").values()
        ),
    }


def escribir_respaldo(plan: PlanReconciliacionHik, backup_dir: str | Path) -> tuple[Path, str]:
    directory = Path(backup_dir).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_dir():
        raise ReconciliacionHikError("El destino de respaldo no es un directorio.")
    payload = json.dumps(
        _snapshot(plan), ensure_ascii=False, indent=2, sort_keys=True, default=str
    ).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    stamp = timezone.now().strftime("%Y%m%dT%H%M%S")
    path = directory / f"rrhh-hik-{plan.codigo_afectado}-{stamp}-{uuid4().hex[:8]}.json"
    with path.open("xb") as handle:
        handle.write(payload)
    return path, digest


def aplicar_reconciliacion(
    plan: PlanReconciliacionHik,
    *,
    backup_dir: str | Path,
) -> dict:
    backup_path, backup_sha256 = escribir_respaldo(plan, backup_dir)
    efectos_ids: list[int] = []
    with transaction.atomic():
        list(
            Empleado.objects.select_for_update().filter(
                pk__in=[plan.empleado_origen_id, plan.empleado_destino_id]
            ).order_by("pk")
        )
        jornadas = {
            (empleado_id, fecha)
            for empleado_id in [plan.empleado_origen_id, plan.empleado_destino_id]
            for fecha in (
                plan.desde + timedelta(days=offset)
                for offset in range((plan.hasta - plan.desde).days + 1)
            )
        }
        bloquear_jornadas_extra(jornadas)
        list(EventoHikCloud.objects.select_for_update().filter(pk__in=plan.recibo_ids).order_by("pk"))
        list(AsistenciaEmpleado.objects.select_for_update().filter(pk__in=plan.asistencia_ids).order_by("pk"))
        list(HoraExtra.objects.select_for_update().filter(pk__in=plan.hora_extra_ids).order_by("pk"))
        list(IncidenciaAsistencia.objects.select_for_update().filter(pk__in=plan.incidencia_ids).order_by("pk"))

        vigente = preparar_reconciliacion(
            codigo_afectado=plan.codigo_afectado,
            codigo_origen=plan.codigo_origen,
            empleado_origen_id=plan.empleado_origen_id,
            empleado_destino_id=plan.empleado_destino_id,
            desde=plan.desde,
            hasta=plan.hasta,
        )
        if vigente != plan:
            raise ReconciliacionHikError(
                "Los datos cambiaron después del preview; no se aplicó la corrección."
            )

        temporal = f"HIK-TEMP-{uuid4().hex[:12]}"
        Empleado.objects.filter(pk=plan.empleado_origen_id).update(codigo=temporal)
        Empleado.objects.filter(pk=plan.empleado_destino_id).update(codigo=plan.codigo_afectado)
        Empleado.objects.filter(pk=plan.empleado_origen_id).update(codigo=plan.codigo_origen)

        EventoHikCloud.objects.filter(pk__in=plan.recibo_ids).update(
            empleado_id=plan.empleado_destino_id
        )
        if plan.recibos_rechazados_ids:
            EventoHikCloud.objects.filter(pk__in=plan.recibos_rechazados_ids).update(
                estado=EventoHikCloud.ESTADO_RECIBIDO,
                reason_code="",
                retryable=False,
                projection_status="pending",
                effects_status="pending",
                procesado_en=None,
                ultimo_error="",
            )
        AsistenciaEmpleado.objects.filter(pk__in=plan.asistencia_ids).update(
            empleado_id=plan.empleado_destino_id
        )
        jefe_id = getattr(usuario_jefe_directo_de_empleado(
            Empleado.objects.get(pk=plan.empleado_destino_id)
        ), "id", None)
        HoraExtra.objects.filter(pk__in=plan.hora_extra_ids).update(
            empleado_id=plan.empleado_destino_id,
            jefe_directo_id=jefe_id,
        )
        IncidenciaAsistencia.objects.filter(pk__in=plan.incidencia_ids).update(
            empleado_id=plan.empleado_destino_id
        )

        for receipt_id in plan.recibos_rechazados_ids:
            projected = project_receipt(receipt_id, empleado_id=plan.empleado_destino_id)
            if projected.projection_status == "applied":
                efectos_ids.append(receipt_id)

        payload = {
            **plan.resumen(modo="apply"),
            "respaldo": str(backup_path),
            "respaldo_sha256": backup_sha256,
            "motivo": "Restaurar identidad Hik histórica sin borrar evidencia",
        }
        audit = AuditLog.objects.create(
            action="RECONCILE",
            model="rrhh.IdentidadHik",
            object_id=plan.codigo_afectado,
            payload=payload,
        )

    for receipt_id in efectos_ids:
        _run_post_projection_effects(receipt_id)
    return {
        **plan.resumen(modo="apply"),
        "respaldo": str(backup_path),
        "respaldo_sha256": backup_sha256,
        "audit_log_id": audit.id,
    }
