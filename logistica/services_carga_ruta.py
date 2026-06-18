from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Prefetch, Q
from django.utils import timezone

from core.access import can_manage_submodule
from core.models import Notificacion, Sucursal
from core.notificaciones import crear_notificaciones
from pos_bridge.models import PointTransferLine
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.services.open_transfer_sync_service import OpenTransferSyncService, resolve_requesting_erp_branch
from pos_bridge.utils.helpers import normalize_text
from recetas.models import SolicitudReabastoCedis, SolicitudReabastoCedisLinea

from .models import (
    EventoRuta,
    ParadaEntregaEvidencia,
    ParadaRuta,
    PuntoLogistico,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
)


@dataclass(frozen=True)
class ChecklistCargaResumen:
    checklist: RutaCargaChecklist
    creadas: int = 0
    actualizadas: int = 0
    omitidas: int = 0


@dataclass(frozen=True)
class RecepcionPointResumen:
    ruta: RutaEntrega
    evidencias_creadas: int = 0
    evidencias_existentes: int = 0
    paradas_actualizadas: int = 0
    lineas_recibidas: int = 0
    lineas_pendientes_point: int = 0


def _cantidad_esperada(line: PointTransferLine) -> Decimal:
    return Decimal(str(line.sent_quantity or 0))


def _paradas_por_sucursal(ruta: RutaEntrega) -> dict[int, ParadaRuta]:
    paradas = ruta.paradas.select_related("punto", "punto__sucursal").order_by("orden", "id")
    result = {}
    sucursales = list(Sucursal.objects.filter(activa=True).only("id", "nombre"))
    for parada in paradas:
        if parada.punto.sucursal_id and parada.punto.sucursal_id not in result:
            result[parada.punto.sucursal_id] = parada
            normalized = normalize_text(parada.punto.sucursal.nombre)
            aliases = [sucursal for sucursal in sucursales if sucursal.id != parada.punto.sucursal_id and normalize_text(sucursal.nombre) in normalized]
            if len(aliases) == 1:
                result.setdefault(aliases[0].id, parada)
    return result


def _cantidad_referencia_entrega(linea: RutaCargaChecklistLinea) -> Decimal:
    if linea.cantidad_cargada is not None:
        return Decimal(str(linea.cantidad_cargada or 0))
    return Decimal(str(linea.cantidad_enviada_esperada or 0))


def obtener_checklist_carga(ruta: RutaEntrega) -> RutaCargaChecklist:
    checklist, _ = RutaCargaChecklist.objects.get_or_create(
        ruta=ruta,
        defaults={"estatus": RutaCargaChecklist.ESTATUS_PENDIENTE},
    )
    return checklist


def obtener_checklist_carga_detallado(ruta: RutaEntrega) -> RutaCargaChecklist:
    checklist = obtener_checklist_carga(ruta)
    lineas_qs = RutaCargaChecklistLinea.objects.select_related(
        "parada",
        "point_transfer_line",
        "validado_por",
        "validado_por__empleado_rrhh",
    ).order_by("parada__orden", "item_name", "id")
    return (
        RutaCargaChecklist.objects.select_related("ruta")
        .prefetch_related(Prefetch("lineas", queryset=lineas_qs))
        .get(pk=checklist.pk)
    )


def checklist_tiene_recepcion_point_pendiente(ruta: RutaEntrega) -> bool:
    checklist = getattr(ruta, "checklist_carga", None)
    if not checklist:
        return False
    return checklist.lineas.filter(point_transfer_line__isnull=False, point_transfer_line__is_received=False).exists()


def _sincronizar_lineas_point_para_ruta(*, ruta: RutaEntrega, checklist: RutaCargaChecklist, solo_abiertas: bool = False) -> tuple[int, int, int]:
    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        return 0, 0, 0

    branch_ids = set(paradas_by_branch)
    candidates = (
        PointTransferLine.objects.select_related("erp_origin_branch", "erp_destination_branch", "origin_branch", "destination_branch")
        .filter(is_cancelled=False, registered_at__date__in=[ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta])
        .filter(Q(erp_origin_branch_id__in=branch_ids) | Q(erp_destination_branch_id__in=branch_ids))
        .order_by("transfer_external_id", "detail_external_id", "id")
    )
    if solo_abiertas:
        candidates = candidates.filter(is_open=True)

    creadas = 0
    actualizadas = 0
    omitidas = 0
    for line in candidates:
        branch = resolve_requesting_erp_branch(line)
        if branch is None or branch.id not in paradas_by_branch:
            omitidas += 1
            continue
        if (
            RutaCargaChecklistLinea.objects.filter(source_hash=line.source_hash)
            .exclude(checklist=checklist)
            .exists()
        ):
            omitidas += 1
            continue
        cantidad_esperada = _cantidad_esperada(line)
        if cantidad_esperada <= 0:
            RutaCargaChecklistLinea.objects.filter(checklist=checklist, source_hash=line.source_hash).delete()
            omitidas += 1
            continue
        parada = paradas_by_branch[branch.id]
        defaults = {
            "parada": parada,
            "point_transfer_line": line,
            "transfer_external_id": line.transfer_external_id,
            "detail_external_id": line.detail_external_id,
            "item_code": line.item_code,
            "item_name": line.item_name,
            "unit": line.unit,
            "erp_origin_branch": line.erp_origin_branch,
            "erp_destination_branch": line.erp_destination_branch,
            "cantidad_solicitada": line.requested_quantity,
            "cantidad_enviada_esperada": cantidad_esperada,
        }
        _, created = RutaCargaChecklistLinea.objects.update_or_create(
            checklist=checklist,
            source_hash=line.source_hash,
            defaults=defaults,
        )
        if created:
            creadas += 1
        else:
            actualizadas += 1
    return creadas, actualizadas, omitidas


def _sincronizar_lineas_consolidado_para_ruta(*, ruta: RutaEntrega, checklist: RutaCargaChecklist) -> tuple[int, int, int]:
    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        return 0, 0, 0

    lineas = (
        SolicitudReabastoCedisLinea.objects.select_related("solicitud", "solicitud__sucursal", "receta")
        .filter(
            solicitud__fecha_operacion=ruta.fecha_ruta,
            solicitud__sucursal_id__in=set(paradas_by_branch),
            solicitud__estado__in=[
                SolicitudReabastoCedis.ESTADO_ENVIADA,
                SolicitudReabastoCedis.ESTADO_ATENDIDA,
            ],
        )
        .order_by("solicitud__sucursal__codigo", "receta__nombre", "id")
    )
    creadas = 0
    actualizadas = 0
    omitidas = 0
    for linea in lineas:
        cantidad = Decimal(str(linea.solicitado or 0))
        source_hash = f"cedis-reabasto-{ruta.fecha_ruta:%Y%m%d}-{linea.solicitud.sucursal_id}-{linea.receta_id}"
        if cantidad <= 0:
            RutaCargaChecklistLinea.objects.filter(checklist=checklist, source_hash=source_hash).delete()
            omitidas += 1
            continue
        if RutaCargaChecklistLinea.objects.filter(source_hash=source_hash).exclude(checklist=checklist).exists():
            omitidas += 1
            continue
        receta = linea.receta
        _, created = RutaCargaChecklistLinea.objects.update_or_create(
            checklist=checklist,
            source_hash=source_hash,
            defaults={
                "parada": paradas_by_branch[linea.solicitud.sucursal_id],
                "point_transfer_line": None,
                "transfer_external_id": linea.solicitud.folio,
                "detail_external_id": str(linea.id),
                "item_code": receta.codigo_point or "",
                "item_name": receta.nombre,
                "unit": "",
                "erp_origin_branch": None,
                "erp_destination_branch": linea.solicitud.sucursal,
                "cantidad_solicitada": cantidad,
                "cantidad_enviada_esperada": cantidad,
            },
        )
        if created:
            creadas += 1
        else:
            actualizadas += 1
    return creadas, actualizadas, omitidas


def _producto_key(value: str) -> str:
    return (value or "").strip().lower()


def _linea_producto_key(linea: RutaCargaChecklistLinea) -> str:
    return _producto_key(linea.item_code or linea.item_name)


def _point_producto_key(line: PointTransferLine) -> str:
    return _producto_key(line.item_code or line.item_name)


def _point_recibidas_por_ruta(ruta: RutaEntrega) -> dict[tuple[int, str], list[PointTransferLine]]:
    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        return {}
    branch_ids = set(paradas_by_branch)
    result: dict[tuple[int, str], list[PointTransferLine]] = {}
    lines = (
        PointTransferLine.objects.select_related("erp_origin_branch", "erp_destination_branch", "origin_branch", "destination_branch")
        .filter(
            is_cancelled=False,
            is_received=True,
            registered_at__date__in=[ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta],
        )
        .filter(Q(erp_origin_branch_id__in=branch_ids) | Q(erp_destination_branch_id__in=branch_ids))
        .order_by("received_at", "id")
    )
    for line in lines:
        branch = resolve_requesting_erp_branch(line)
        key = _point_producto_key(line)
        if branch is None or branch.id not in branch_ids or not key:
            continue
        result.setdefault((branch.id, key), []).append(line)
    return result


def _evidencia_point(linea: RutaCargaChecklistLinea) -> ParadaEntregaEvidencia | None:
    return (
        ParadaEntregaEvidencia.objects.filter(linea_carga=linea, client_event_id__startswith="point-recepcion-")
        .order_by("-capturado_en", "-id")
        .first()
    )


def _cantidad_recibida_linea(linea: RutaCargaChecklistLinea) -> Decimal | None:
    evidencia = _evidencia_point(linea)
    if evidencia is not None:
        return Decimal(str(evidencia.cantidad_entregada or 0))
    if linea.point_transfer_line_id and linea.point_transfer_line.is_received:
        return Decimal(str(linea.point_transfer_line.received_quantity or 0))
    return None


def _confirmacion_completa_parada(parada: ParadaRuta) -> ParadaEntregaEvidencia | None:
    return (
        parada.evidencias_entrega.filter(linea_carga__isnull=True, tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION)
        .order_by("-capturado_en", "-id")
        .first()
    )


def _cantidad_recibida_con_respaldo_pwa(
    linea: RutaCargaChecklistLinea,
    confirmacion: ParadaEntregaEvidencia | None,
) -> Decimal | None:
    recibida = _cantidad_recibida_linea(linea)
    if recibida is not None:
        return recibida
    if confirmacion is not None and not linea.point_transfer_line_id:
        return _cantidad_referencia_entrega(linea)
    return None


def _elegir_recepcion_point(linea: RutaCargaChecklistLinea, candidates: list[PointTransferLine]) -> list[PointTransferLine]:
    if len(candidates) <= 1:
        return candidates
    esperado = _cantidad_referencia_entrega(linea)
    exactas = [line for line in candidates if Decimal(str(line.received_quantity or 0)) == esperado]
    if not exactas:
        return []
    return [max(exactas, key=lambda line: (line.received_at or line.updated_at, line.id))]


@transaction.atomic
def sincronizar_checklist_carga_desde_point(*, ruta: RutaEntrega, user=None, ejecutar_sync: bool = True) -> ChecklistCargaResumen:
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    if ruta.estatus != RutaEntrega.ESTATUS_PLANEADA:
        raise ValidationError("La carga solo se puede sincronizar mientras la ruta está planeada.")

    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        raise ValidationError("La ruta no tiene paradas ligadas a sucursales para relacionar transferencias Point.")
    checklist = RutaCargaChecklist.objects.select_for_update().filter(ruta=ruta).first()
    if checklist and checklist.lineas.exclude(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists():
        return ChecklistCargaResumen(checklist=checklist)
    checklist = checklist or obtener_checklist_carga(ruta)
    checklist.sincronizado_en = timezone.now()
    if checklist.estatus == RutaCargaChecklist.ESTATUS_PENDIENTE:
        checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
    checklist.save(update_fields=["point_sync_job", "sincronizado_en", "estatus", "actualizado_en"])

    creadas, actualizadas, omitidas = _sincronizar_lineas_consolidado_para_ruta(ruta=ruta, checklist=checklist)
    if creadas or actualizadas:
        checklist.lineas.filter(point_transfer_line__isnull=False, estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).delete()
        checklist.notas = "Carga esperada generada desde consolidado CEDIS."
        checklist.save(update_fields=["notas", "actualizado_en"])
    if not checklist.lineas.exists():
        sync_job = None
        if ejecutar_sync:
            service = OpenTransferSyncService()
            for fecha in [ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta]:
                sync_job = service.sync_open_transfers(fecha=fecha, triggered_by=user)
                if sync_job.status != sync_job.STATUS_SUCCESS:
                    raise ValidationError("No se pudo sincronizar Point para generar la carga esperada.")
            checklist.point_sync_job = sync_job or checklist.point_sync_job
            checklist.save(update_fields=["point_sync_job", "actualizado_en"])
        checklist.lineas.filter(point_transfer_line__is_open=False).delete()
        creadas, actualizadas, omitidas = _sincronizar_lineas_point_para_ruta(ruta=ruta, checklist=checklist, solo_abiertas=True)

    if checklist.lineas.exists():
        if checklist.estatus == RutaCargaChecklist.ESTATUS_BLOQUEADA:
            checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
            checklist.notas = ""
            checklist.save(update_fields=["estatus", "notas", "actualizado_en"])
    else:
        checklist.estatus = RutaCargaChecklist.ESTATUS_BLOQUEADA
        checklist.notas = "No se encontraron transferencias abiertas de Point para las sucursales de esta ruta."
        checklist.save(update_fields=["estatus", "notas", "actualizado_en"])

    return ChecklistCargaResumen(checklist=checklist, creadas=creadas, actualizadas=actualizadas, omitidas=omitidas)


def validar_usuario_puede_operar_checklist(*, user, ruta: RutaEntrega, repartidor) -> None:
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La ruta no permite confirmar carga en este estatus.")
    if not repartidor or ruta.repartidor_id != repartidor.id:
        raise PermissionDenied("No tienes permiso para confirmar carga de esta ruta.")


@transaction.atomic
def validar_linea_carga(
    *,
    user,
    ruta: RutaEntrega,
    repartidor,
    linea_id: int,
    cantidad_cargada,
    motivo_diferencia: str = "",
    notas: str = "",
    client_event_id: str = "",
) -> RutaCargaChecklistLinea:
    validar_usuario_puede_operar_checklist(user=user, ruta=ruta, repartidor=repartidor)
    checklist = obtener_checklist_carga(ruta)
    linea = RutaCargaChecklistLinea.objects.select_for_update().select_related("checklist", "parada").get(
        pk=linea_id,
        checklist=checklist,
    )

    client_event_id = (client_event_id or "").strip()
    if client_event_id and linea.client_event_id == client_event_id and linea.estatus != RutaCargaChecklistLinea.ESTATUS_PENDIENTE:
        return linea

    cantidad = Decimal(str(cantidad_cargada))
    if cantidad < 0:
        raise ValidationError("La cantidad cargada no puede ser negativa.")

    esperada = Decimal(str(linea.cantidad_enviada_esperada or 0))
    if cantidad == esperada:
        estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        motivo_diferencia = ""
    elif cantidad == 0:
        estatus = RutaCargaChecklistLinea.ESTATUS_FALTANTE
    elif cantidad < esperada:
        estatus = RutaCargaChecklistLinea.ESTATUS_PARCIAL
    else:
        estatus = RutaCargaChecklistLinea.ESTATUS_SOBRANTE

    if estatus != RutaCargaChecklistLinea.ESTATUS_CARGADA and not motivo_diferencia:
        raise ValidationError("Selecciona el motivo de la diferencia antes de guardar.")

    linea.cantidad_cargada = cantidad
    linea.estatus = estatus
    linea.motivo_diferencia = motivo_diferencia
    linea.notas = notas or ""
    linea.client_event_id = client_event_id
    linea.validado_por = user
    linea.validado_en = timezone.now()
    linea.save(
        update_fields=[
            "cantidad_cargada",
            "estatus",
            "motivo_diferencia",
            "notas",
            "client_event_id",
            "validado_por",
            "validado_en",
            "actualizado_en",
        ]
    )

    pendientes = checklist.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists()
    diferencias = checklist.lineas.exclude(
        estatus__in=[RutaCargaChecklistLinea.ESTATUS_CARGADA, RutaCargaChecklistLinea.ESTATUS_NO_APLICA]
    ).exists()
    if pendientes:
        checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
    elif diferencias:
        checklist.estatus = RutaCargaChecklist.ESTATUS_CON_INCIDENCIA
    else:
        checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
        checklist.confirmado_por = user
        checklist.confirmado_en = timezone.now()
    checklist.save(update_fields=["estatus", "confirmado_por", "confirmado_en", "actualizado_en"])
    return linea


def checklist_bloquea_salida(ruta: RutaEntrega) -> str | None:
    checklist = getattr(ruta, "checklist_carga", None)
    if not checklist or not checklist.lineas.exists():
        return None
    if checklist.estatus == RutaCargaChecklist.ESTATUS_CONFIRMADA:
        return None
    if checklist.estatus == RutaCargaChecklist.ESTATUS_CON_INCIDENCIA and checklist.motivo_override:
        return None
    if checklist.lineas.exclude(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists():
        return None
    return "confirma al menos una línea de carga antes de liberar la ruta"


@transaction.atomic
def registrar_recarga_cedis(*, ruta: RutaEntrega, user, notas: str = "") -> EventoRuta:
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La recarga CEDIS solo aplica a rutas planeadas o en ruta.")
    checklist = RutaCargaChecklist.objects.select_for_update().filter(ruta=ruta).first()
    if not checklist or not checklist.lineas.exists():
        raise ValidationError("La ruta no tiene carga esperada para registrar recarga CEDIS.")

    pendientes = checklist.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).count()
    diferencias = checklist.lineas.exclude(
        estatus__in=[RutaCargaChecklistLinea.ESTATUS_CARGADA, RutaCargaChecklistLinea.ESTATUS_NO_APLICA]
    ).count()
    if ruta.estatus == RutaEntrega.ESTATUS_PLANEADA:
        if pendientes:
            raise ValidationError("Primero valida cada línea como cargada, parcial o faltante.")
        if checklist.estatus != RutaCargaChecklist.ESTATUS_CON_INCIDENCIA:
            raise ValidationError("La salida parcial solo aplica cuando hay faltantes o parciales.")
        checklist.motivo_override = notas or "Salida parcial autorizada con recarga CEDIS programada."
        checklist.save(update_fields=["motivo_override", "actualizado_en"])

    numero = (
        EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL, metadata__tipo="recarga_cedis").count()
        + 1
    )
    return EventoRuta.objects.create(
        ruta=ruta,
        tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
        severidad=EventoRuta.SEVERIDAD_INFO,
        descripcion=f"Recarga CEDIS {numero} registrada por logística.",
        metadata={
            "tipo": "recarga_cedis",
            "numero": numero,
            "pendientes": pendientes,
            "diferencias": diferencias,
            "notas": notas,
        },
        creado_por=user,
    )


def _usuarios_logistica_rutas():
    User = get_user_model()
    usuarios = User.objects.filter(is_active=True).prefetch_related("groups", "module_access")
    return [usuario for usuario in usuarios if can_manage_submodule(usuario, "logistica", "rutas")]


def paradas_con_entrega_requerida(ruta: RutaEntrega):
    return ruta.paradas.select_related("punto").exclude(punto__tipo=PuntoLogistico.TIPO_CEDIS)


def ruta_tiene_entregas_pendientes(ruta: RutaEntrega) -> bool:
    return paradas_con_entrega_requerida(ruta).filter(entrega_estado=ParadaRuta.ENTREGA_PENDIENTE).exists()


def ruta_tiene_diferencias_entrega(ruta: RutaEntrega) -> bool:
    return paradas_con_entrega_requerida(ruta).filter(
        entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA]
    ).exists()


def _resumen_diferencias_cierre(ruta: RutaEntrega) -> list[dict]:
    checklist = getattr(ruta, "checklist_carga", None)
    resumen = []
    for parada in paradas_con_entrega_requerida(ruta).filter(
        entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA]
    ).order_by("orden", "id"):
        item = {
            "parada_id": parada.id,
            "orden": parada.orden,
            "punto": parada.punto_nombre_snapshot,
            "entrega_estado": parada.entrega_estado,
            "productos": [],
        }
        if checklist:
            for linea in checklist.lineas.filter(parada=parada).select_related("point_transfer_line").order_by("item_name", "id"):
                esperado = _cantidad_referencia_entrega(linea)
                recibido = _cantidad_recibida_linea(linea)
                if recibido != esperado:
                    item["productos"].append(
                        {
                            "codigo": linea.item_code,
                            "producto": linea.item_name,
                            "esperado": str(esperado),
                            "recibido": str(recibido) if recibido is not None else None,
                        }
                    )
        resumen.append(item)
    return resumen


@transaction.atomic
def cerrar_ruta_con_diferencia_autorizada(*, ruta: RutaEntrega, user, notas: str = "") -> EventoRuta:
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
        raise ValidationError("Solo puedes cerrar con diferencia una ruta en seguimiento.")
    if ruta.paradas.filter(estado=ParadaRuta.ESTADO_PENDIENTE).exists():
        raise ValidationError("No se puede cerrar: hay paradas pendientes por visitar u omitir.")
    if ruta_tiene_entregas_pendientes(ruta):
        raise ValidationError("No se puede cerrar: hay paradas sin entrega confirmada.")
    if not ruta_tiene_diferencias_entrega(ruta):
        raise ValidationError("Esta ruta no tiene diferencias; usa el cierre normal.")

    diferencias = _resumen_diferencias_cierre(ruta)
    ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
    ruta.hora_cierre_real = ruta.hora_cierre_real or timezone.now()
    ruta.save(update_fields=["estatus", "hora_cierre_real", "updated_at"])
    evento = EventoRuta.objects.create(
        ruta=ruta,
        tipo=EventoRuta.TIPO_CIERRE,
        severidad=EventoRuta.SEVERIDAD_ALERTA,
        descripcion="Ruta cerrada con diferencia autorizada para revisión de Logística.",
        metadata={
            "tipo": "cierre_con_diferencia_autorizada",
            "diferencias": diferencias,
            "notas": notas,
        },
        creado_por=user,
    )
    crear_notificaciones(
        _usuarios_logistica_rutas(),
        titulo=f"Ruta con diferencia: {ruta.folio}",
        mensaje="Cerrada operativamente con diferencia autorizada. Revisar evidencia y recepción Point.",
        url=f"/logistica/rutas/{ruta.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=user,
        objeto_tipo="logistica.RutaEntrega",
        objeto_id=ruta.id,
        excluir=user,
    )
    return evento


@transaction.atomic
def confirmar_checklist_carga_manual(*, ruta: RutaEntrega, user, notas: str = "") -> int:
    if ruta.estatus != RutaEntrega.ESTATUS_PLANEADA:
        raise ValidationError("Solo puedes confirmar carga manual en una ruta planeada.")
    checklist = RutaCargaChecklist.objects.select_for_update().filter(ruta=ruta).first()
    if not checklist or not checklist.lineas.exists():
        raise ValidationError("La ruta no tiene carga Point para confirmar.")
    now = timezone.now()
    lineas = list(checklist.lineas.select_for_update().filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE))
    for linea in lineas:
        linea.cantidad_cargada = linea.cantidad_enviada_esperada
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.motivo_diferencia = ""
        linea.notas = notas or "Carga confirmada manualmente por logística."
        linea.client_event_id = f"manual-carga-{ruta.id}-{linea.id}"
        linea.validado_por = user
        linea.validado_en = now
        linea.save(
            update_fields=[
                "cantidad_cargada",
                "estatus",
                "motivo_diferencia",
                "notas",
                "client_event_id",
                "validado_por",
                "validado_en",
                "actualizado_en",
            ]
        )
    diferencias = checklist.lineas.exclude(
        estatus__in=[RutaCargaChecklistLinea.ESTATUS_CARGADA, RutaCargaChecklistLinea.ESTATUS_NO_APLICA]
    ).exists()
    checklist.estatus = RutaCargaChecklist.ESTATUS_CON_INCIDENCIA if diferencias else RutaCargaChecklist.ESTATUS_CONFIRMADA
    checklist.confirmado_por = None if diferencias else user
    checklist.confirmado_en = None if diferencias else now
    checklist.notas = notas or "Carga confirmada manualmente por logística."
    checklist.save(update_fields=["estatus", "confirmado_por", "confirmado_en", "notas", "actualizado_en"])
    if not diferencias:
        registrar_evento_checklist_confirmado(ruta=ruta, user=user)
    return len(lineas)


def registrar_evento_checklist_confirmado(*, ruta: RutaEntrega, user) -> None:
    if EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL, metadata__tipo="checklist_carga").exists():
        return
    EventoRuta.objects.create(
        ruta=ruta,
        tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
        severidad=EventoRuta.SEVERIDAD_INFO,
        descripcion="Checklist de carga confirmado antes de salida.",
        metadata={"tipo": "checklist_carga"},
        creado_por=user,
    )


@transaction.atomic
def sincronizar_recepcion_desde_point(*, ruta: RutaEntrega, user=None, ejecutar_sync: bool = True) -> RecepcionPointResumen:
    if ruta.estatus not in {RutaEntrega.ESTATUS_EN_RUTA, RutaEntrega.ESTATUS_COMPLETADA}:
        raise ValidationError("La recepción Point solo aplica a rutas en seguimiento o completadas.")

    checklist = obtener_checklist_carga(ruta)

    if ejecutar_sync:
        sync_job = PointMovementSyncService().run_transfer_sync(
            start_date=ruta.fecha_ruta,
            end_date=ruta.fecha_ruta,
            triggered_by=user,
        )
        if sync_job.status != sync_job.STATUS_SUCCESS:
            raise ValidationError("No se pudo sincronizar Point para confirmar recepción de transferencias.")

    if not checklist.lineas.exists():
        return RecepcionPointResumen(ruta=ruta)

    source_hashes = list(checklist.lineas.exclude(source_hash="").values_list("source_hash", flat=True))
    point_lines = {
        line.source_hash: line
        for line in PointTransferLine.objects.select_related("sync_job").filter(
            source_hash__in=source_hashes,
            is_cancelled=False,
        )
    }
    point_recibidas = _point_recibidas_por_ruta(ruta)

    evidencias_creadas = 0
    evidencias_existentes = 0
    paradas_actualizadas = 0
    lineas_recibidas = 0
    lineas_pendientes_point = 0
    touched_paradas: set[int] = set()

    for linea in checklist.lineas.select_related("parada").order_by("parada__orden", "id"):
        point_line = point_lines.get(linea.source_hash)
        if point_line is not None and linea.point_transfer_line_id != point_line.id:
            linea.point_transfer_line = point_line
            linea.save(update_fields=["point_transfer_line", "actualizado_en"])
        received_lines = []
        if point_line is not None and point_line.is_received:
            received_lines = [point_line]
        elif linea.parada.punto.sucursal_id:
            candidates = point_recibidas.get((linea.parada.punto.sucursal_id, _linea_producto_key(linea)), [])
            received_lines = _elegir_recepcion_point(linea, candidates)
            if received_lines and linea.point_transfer_line_id != received_lines[0].id:
                linea.point_transfer_line = received_lines[0]
                linea.save(update_fields=["point_transfer_line", "actualizado_en"])
        if not received_lines:
            lineas_pendientes_point += 1
            continue

        lineas_recibidas += 1
        received_quantity = sum((Decimal(str(line.received_quantity or 0)) for line in received_lines), Decimal("0"))
        received_at_values = [line.received_at for line in received_lines if line.received_at]
        received_at = max(received_at_values) if received_at_values else timezone.now()
        client_event_id = f"point-recepcion-{linea.source_hash}"
        metadata = {
            "origen": "point_transfer",
            "transfer_external_id": received_lines[0].transfer_external_id,
            "detail_external_id": received_lines[0].detail_external_id,
            "received_by": ", ".join(sorted({line.received_by for line in received_lines if line.received_by})),
            "is_finalized": all(line.is_finalized for line in received_lines),
            "source_hashes": [line.source_hash for line in received_lines],
        }
        evidencia, created = ParadaEntregaEvidencia.objects.get_or_create(
            ruta=ruta,
            parada=linea.parada,
            linea_carga=linea,
            client_event_id=client_event_id,
            defaults={
                "tipo": ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                "cantidad_entregada": received_quantity,
                "comentario": "Recepción confirmada desde Point.",
                "capturado_por": user,
                "capturado_en": received_at,
                "metadata": metadata,
            },
        )
        if created:
            evidencias_creadas += 1
        else:
            evidencias_existentes += 1
            evidencia.cantidad_entregada = received_quantity
            evidencia.capturado_por = user
            evidencia.capturado_en = received_at
            evidencia.metadata = metadata
            evidencia.save(update_fields=["cantidad_entregada", "capturado_por", "capturado_en", "metadata"])
        touched_paradas.add(linea.parada_id)

    for parada in ruta.paradas.filter(id__in=touched_paradas).order_by("orden", "id"):
        lineas = list(checklist.lineas.filter(parada=parada).select_related("point_transfer_line"))
        if not lineas:
            continue
        confirmacion_pwa = _confirmacion_completa_parada(parada)
        recibidas = [linea for linea in lineas if _cantidad_recibida_con_respaldo_pwa(linea, confirmacion_pwa) is not None]
        if not recibidas:
            continue

        todas_recibidas = len(recibidas) == len(lineas)
        recibido_total = sum(
            (_cantidad_recibida_con_respaldo_pwa(linea, confirmacion_pwa) or Decimal("0") for linea in recibidas),
            Decimal("0"),
        )
        esperado_total = sum((_cantidad_referencia_entrega(linea) for linea in lineas), Decimal("0"))
        cantidades_cuadran = todas_recibidas and all(
            _cantidad_recibida_con_respaldo_pwa(linea, confirmacion_pwa) == _cantidad_referencia_entrega(linea)
            for linea in lineas
        )
        if not todas_recibidas:
            entrega_estado = ParadaRuta.ENTREGA_PENDIENTE
        elif recibido_total == 0:
            entrega_estado = ParadaRuta.ENTREGA_NO_ENTREGADA
        elif cantidades_cuadran:
            entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        else:
            entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA

        received_at_values = [linea.point_transfer_line.received_at for linea in recibidas if linea.point_transfer_line and linea.point_transfer_line.received_at]
        entrega_confirmada_en = max(received_at_values) if received_at_values else (confirmacion_pwa.capturado_en if confirmacion_pwa else timezone.now())
        update_fields = [
            "estado",
            "hora_llegada_real",
            "hora_salida_real",
            "entrega_estado",
            "entrega_confirmada_en",
            "entrega_confirmada_por",
            "entrega_notas",
            "actualizado_en",
        ]
        parada.estado = ParadaRuta.ESTADO_VISITADA
        parada.hora_llegada_real = parada.hora_llegada_real or entrega_confirmada_en
        parada.hora_salida_real = parada.hora_salida_real or entrega_confirmada_en
        parada.entrega_estado = entrega_estado
        parada.entrega_confirmada_en = entrega_confirmada_en
        parada.entrega_confirmada_por = user
        parada.entrega_notas = (
            f"Recepción Point: {lineas_recibidas} línea(s) recibidas. "
            f"Esperado/cargado {esperado_total}, recibido {recibido_total}."
            + ("" if todas_recibidas else " Pendiente de sincronizar recepción completa.")
        )
        parada.save(update_fields=update_fields)
        paradas_actualizadas += 1

    if paradas_actualizadas:
        ruta.recompute_route_control()
        ruta.save(update_fields=["cumplimiento_porcentaje", "updated_at"])

    return RecepcionPointResumen(
        ruta=ruta,
        evidencias_creadas=evidencias_creadas,
        evidencias_existentes=evidencias_existentes,
        paradas_actualizadas=paradas_actualizadas,
        lineas_recibidas=lineas_recibidas,
        lineas_pendientes_point=lineas_pendientes_point,
    )
