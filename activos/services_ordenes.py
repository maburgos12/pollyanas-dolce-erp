"""Transiciones de órdenes compartidas por ERP y mantenimiento móvil."""
from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from .models import BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento


TRANSICIONES_ORDEN = {
    OrdenMantenimiento.ESTATUS_PENDIENTE: (
        OrdenMantenimiento.ESTATUS_EN_PROCESO,
        OrdenMantenimiento.ESTATUS_CERRADA,
        OrdenMantenimiento.ESTATUS_CANCELADA,
    ),
    OrdenMantenimiento.ESTATUS_EN_PROCESO: (
        OrdenMantenimiento.ESTATUS_CERRADA,
        OrdenMantenimiento.ESTATUS_CANCELADA,
    ),
}


class TransicionOrdenInvalida(ValueError):
    pass


@transaction.atomic
def cambiar_estatus_orden(orden_id, estatus, user, *, source=None):
    # Nunca decidir con la instancia que el consumidor leyó antes del bloqueo.
    orden = OrdenMantenimiento.objects.select_for_update().get(pk=orden_id)
    anterior = orden.estatus
    destino = estatus or anterior
    if destino == anterior:
        return orden, anterior, False, None
    if destino not in TRANSICIONES_ORDEN.get(anterior, ()):
        raise TransicionOrdenInvalida(f"Transición inválida: {anterior} -> {destino}.")

    today = timezone.localdate()
    orden.estatus = destino
    campos = ["estatus", "actualizado_en"]
    if destino == OrdenMantenimiento.ESTATUS_EN_PROCESO and not orden.fecha_inicio:
        orden.fecha_inicio = today
        campos.append("fecha_inicio")
    if destino == OrdenMantenimiento.ESTATUS_CERRADA:
        orden.fecha_cierre = today
        campos.append("fecha_cierre")
        if orden.plan_ref_id:
            plan = PlanMantenimiento.objects.select_for_update().get(pk=orden.plan_ref_id)
            plan.ultima_ejecucion = today
            plan.recompute_next_date()
            plan.save(update_fields=["ultima_ejecucion", "proxima_ejecucion", "actualizado_en"])
    orden.save(update_fields=campos)
    bitacora = BitacoraMantenimiento.objects.create(
        orden=orden, accion="ESTATUS", comentario=f"{anterior} -> {destino}", usuario=user,
    )
    payload = {"from": anterior, "to": destino, "folio": orden.folio}
    if source:
        payload["source"] = source
    log_event(user, "UPDATE", "activos.OrdenMantenimiento", orden.id, payload)
    return orden, anterior, True, bitacora
