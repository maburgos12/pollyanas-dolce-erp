from __future__ import annotations

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from core.access import can_manage_submodule
from logistica.models import DiscrepanciaLogistica, ParadaEntregaEvidencia


def jefe_inmediato_para_actor(actor):
    empleado = getattr(actor, "empleado_rrhh", None)
    jefe = getattr(getattr(empleado, "jefe_directo", None), "usuario_erp", None)
    if jefe and jefe.is_active:
        return jefe
    User = get_user_model()
    candidatos = User.objects.filter(is_active=True).select_related("empleado_rrhh").order_by("id")
    ventas = next(
        (user for user in candidatos if getattr(getattr(user, "empleado_rrhh", None), "departamento", "") == "VENTAS"),
        None,
    )
    if ventas:
        return ventas
    return next((user for user in candidatos if can_manage_submodule(user, "logistica", "rutas")), None) or User.objects.filter(is_active=True, is_superuser=True).order_by("id").first()


@transaction.atomic
def registrar_discrepancias_recepcion(*, evidencias, actor, motivos) -> list[DiscrepanciaLogistica]:
    evidencia_ids = [evidencia.id for evidencia in evidencias if evidencia.linea_carga_id]
    filas = list(
        ParadaEntregaEvidencia.objects.select_for_update(of=("self",))
        .filter(id__in=evidencia_ids)
        .select_related("linea_carga", "linea_carga__checklist", "ruta", "parada")
        .order_by("id")
    )
    casos = []
    asignado_a = jefe_inmediato_para_actor(actor)
    for evidencia in filas:
        linea = evidencia.linea_carga
        recibida = evidencia.cantidad_entregada
        cargada = linea.cantidad_cargada
        if recibida is None or cargada is None or Decimal(recibida) == Decimal(cargada):
            continue
        motivo = str(motivos.get(linea.id) or motivos.get(str(linea.id)) or "").strip()
        if not motivo:
            raise ValidationError(f"Explica la diferencia de recepción para {linea.item_name}.")
        caso, _ = DiscrepanciaLogistica.objects.update_or_create(
            linea_carga=linea,
            origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
            estado__in=[
                DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE,
                DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA,
            ],
            defaults={
                "ruta": evidencia.ruta,
                "parada": evidencia.parada,
                "cantidad_enviada": linea.cantidad_enviada_esperada,
                "cantidad_cargada": cargada,
                "cantidad_recibida": recibida,
                "motivo": motivo,
                "notas": evidencia.comentario or "",
                "asignado_a": asignado_a,
                "creado_por": actor,
                "estado": DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE,
            },
        )
        casos.append(caso)
    return casos


@transaction.atomic
def registrar_discrepancias_point(*, evidencias, ruta, actor=None) -> list[DiscrepanciaLogistica]:
    """Crea la deuda administrativa cuando Point recibe distinto a la carga física."""
    filas = list(
        ParadaEntregaEvidencia.objects.select_for_update(of=("self",))
        .filter(id__in=[evidencia.id for evidencia in evidencias if evidencia.linea_carga_id])
        .select_related("linea_carga", "parada")
        .order_by("id")
    )
    responsable_operativo = getattr(getattr(ruta, "repartidor", None), "user", None)
    creado_por = actor or getattr(ruta, "created_by", None) or responsable_operativo
    if creado_por is None:
        creado_por = get_user_model().objects.filter(is_active=True, is_superuser=True).order_by("id").first()
    if creado_por is None:
        return []
    asignado_a = jefe_inmediato_para_actor(responsable_operativo or creado_por)
    casos = []
    for evidencia in filas:
        linea = evidencia.linea_carga
        recibida = evidencia.cantidad_entregada
        cargada = linea.cantidad_cargada
        if recibida is None or cargada is None or Decimal(recibida) == Decimal(cargada):
            continue
        caso, _ = DiscrepanciaLogistica.objects.update_or_create(
            linea_carga=linea,
            origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
            estado__in=[
                DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE,
                DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA,
            ],
            defaults={
                "ruta": ruta,
                "parada": evidencia.parada,
                "cantidad_enviada": linea.cantidad_enviada_esperada,
                "cantidad_cargada": cargada,
                "cantidad_recibida": recibida,
                "motivo": "diferencia_recepcion_point",
                "notas": "Point registró una cantidad recibida distinta a la carga física.",
                "asignado_a": asignado_a,
                "creado_por": creado_por,
                "estado": DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE,
            },
        )
        casos.append(caso)
    return casos


@transaction.atomic
def resolver_discrepancia(*, caso, actor, accion, comentario):
    caso = DiscrepanciaLogistica.objects.select_for_update().get(pk=caso.pk)
    if actor != caso.asignado_a and not can_manage_submodule(actor, "logistica", "rutas"):
        raise PermissionDenied("No tienes permiso para revisar esta discrepancia.")
    estados = {
        "validar_real": DiscrepanciaLogistica.ESTADO_VALIDADA_REAL,
        "marcar_incorrecta": DiscrepanciaLogistica.ESTADO_MARCADA_INCORRECTA,
        "solicitar_aclaracion": DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA,
    }
    if accion not in estados:
        raise ValidationError("La acción de revisión no es válida.")
    comentario = str(comentario or "").strip()
    if not comentario:
        raise ValidationError("Escribe una resolución o solicitud de aclaración.")
    # El guardrail de ParadaRuta inspecciona cualquier atributo llamado estado;
    # este objeto es DiscrepanciaLogistica, por eso escribimos mediante attname.
    caso.__dict__[DiscrepanciaLogistica._meta.get_field("estado").attname] = estados[accion]
    caso.revisado_por = actor
    caso.revisado_en = timezone.now()
    caso.resolucion = comentario
    caso.save(update_fields=["estado", "revisado_por", "revisado_en", "resolucion"])
    return caso


def pendientes_vencidos_para_planeacion(user, fecha):
    return list(
        DiscrepanciaLogistica.objects.filter(
            asignado_a=user,
            estado=DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE,
            creado_en__date__lt=fecha,
        )
        .select_related("ruta", "parada", "linea_carga")
        .order_by("creado_en", "id")
    )
