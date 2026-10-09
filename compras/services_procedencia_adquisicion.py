"""Sólo relaciona fuentes existentes y registra su confirmación documental."""

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Q
from activos.models import Activo
from core.access import can_manage_inventario, can_view_inventario
from core.models import AuditLog
from mantenimiento.services_vinculos_proveedores import actor_actual
from .access_departamentales import (
    _areas_lectura_solicitudes,
    _puede_ver_solicitud,
    puede_gestionar_compras_departamentales,
)
from .models import (
    ProcedenciaAdquisicion,
    RecepcionItemDepartamental,
    LineaOrdenCompraDepartamental,
    IntentoCompraDepartamental,
    ItemCompraDepartamental,
)


def procedencias_visibles(actor, *, recepcion_id=None, activo_id=None):
    relaciones = ProcedenciaAdquisicion.objects.select_related(
        "recepcion", "linea", "intento", "item__solicitud__area", "activo", "autor"
    ).filter(item__isnull=False, activo__isnull=False)
    if not can_view_inventario(actor):
        return []
    areas = _areas_lectura_solicitudes(actor)
    if areas is not None:
        relaciones = relaciones.filter(item__solicitud__area_id__in=areas)
    if recepcion_id is not None:
        relaciones = relaciones.filter(recepcion_original_id=recepcion_id)
    if activo_id is not None:
        relaciones = relaciones.filter(activo_id=activo_id)
    return list(relaciones)


@transaction.atomic
def confirmar_procedencia(
    *,
    user,
    recepcion_id,
    activo_id,
    version,
    referencia_unidad,
    motivo,
    evidencia,
    confirmado,
):
    actor = actor_actual(user)
    if not puede_gestionar_compras_departamentales(actor) or not can_manage_inventario(
        actor
    ):
        raise PermissionDenied("Necesitas gestionar recepciones de Compras y equipos.")
    referencia = " ".join(str(referencia_unidad or "").split())
    motivo, evidencia = str(motivo or "").strip(), str(evidencia or "").strip()
    if (
        confirmado is not True
        or not referencia
        or len(referencia) > 200
        or not motivo
        or not evidencia
    ):
        raise ValidationError(
            "Revisa el origen actual e indica unidad física, motivo y evidencia documental."
        )
    try:
        recepcion_id, activo_id, version = (
            int(recepcion_id),
            int(activo_id),
            int(version),
        )
    except (ValueError, TypeError):
        raise ValidationError("Selecciona una recepción, equipo y versión existentes.")
    pista = (
        RecepcionItemDepartamental.objects.select_related("linea_orden")
        .filter(pk=recepcion_id)
        .first()
    )
    if pista is None:
        raise ValidationError(
            "La recepción original ya no está disponible.", code="conflict"
        )
    # Mismo orden que Recepcion.save: el bloqueo del artículo serializa el origen.
    item = (
        ItemCompraDepartamental.objects.select_for_update(of=("self",))
        .select_related("solicitud")
        .filter(pk=pista.linea_orden.item_id)
        .first()
    )
    if item is None:
        raise ValidationError("El artículo original fue eliminado.", code="conflict")
    if not _puede_ver_solicitud(actor, item.solicitud):
        raise PermissionDenied("No puedes gestionar esta solicitud de origen.")
    intento = (
        IntentoCompraDepartamental.objects.select_for_update()
        .filter(pk=pista.linea_orden.intento_id)
        .first()
    )
    linea = (
        LineaOrdenCompraDepartamental.objects.select_for_update()
        .filter(pk=pista.linea_orden_id)
        .first()
    )
    recepcion = (
        RecepcionItemDepartamental.objects.select_for_update()
        .filter(pk=recepcion_id)
        .first()
    )
    if (
        not recepcion
        or not linea
        or not intento
        or recepcion.linea_orden_id != linea.pk
        or linea.item_id != item.pk
        or linea.intento_id != intento.pk
        or intento.item_id != item.pk
    ):
        raise ValidationError(
            "El origen cambió. Vuelve a revisar la recepción.", code="conflict"
        )
    if recepcion.cantidad_recibida <= 0:
        raise ValidationError(
            "La recepción no acredita una cantidad positiva.", code="conflict"
        )
    if version != intento.version:
        raise ValidationError(
            "La versión de origen cambió. Revisa la recepción actual antes de confirmar.",
            code="conflict",
        )
    activo = Activo.objects.select_for_update().filter(pk=activo_id).first()
    if activo is None:
        raise ValidationError("Selecciona un equipo existente.")
    # Un identificador reutilizado no revive una fuente documental eliminada,
    # aunque se intente otra unidad o destino.
    tombstones = Q()
    for fuente, identidad in (
        ("recepcion", recepcion.pk),
        ("linea", linea.pk),
        ("intento", intento.pk),
        ("item", item.pk),
        ("activo", activo.pk),
    ):
        tombstones |= Q(
            **{f"{fuente}_original_id": identidad, f"{fuente}__isnull": True}
        )
    if ProcedenciaAdquisicion.objects.filter(tombstones).exists():
        raise ValidationError(
            "Una fuente original fue eliminada. No se restaura su identidad.",
            code="conflict",
        )
    normalizada = referencia.casefold()
    if len(normalizada) > 200:
        raise ValidationError(
            "La referencia normalizada debe tener máximo 200 caracteres."
        )
    existente = (
        ProcedenciaAdquisicion.objects.filter(recepcion_original_id=recepcion.pk)
        .filter(Q(unidad_normalizada=normalizada) | Q(activo_original_id=activo.pk))
        .first()
    )
    if existente:
        exacto = (
            existente.recepcion_id == recepcion.pk
            and existente.linea_id == linea.pk
            and existente.intento_id == intento.pk
            and existente.item_id == item.pk
            and existente.activo_id == activo.pk
            and existente.unidad_normalizada == normalizada
            and existente.motivo == motivo
            and existente.evidencia == evidencia
            and existente.version_confirmada == version
        )
        if not exacto:
            raise ValidationError(
                "La unidad u origen ya tiene una confirmación diferente. Se conserva la original.",
                code="conflict",
            )
        return existente, False
    vinculo = ProcedenciaAdquisicion.objects.create(
        recepcion=recepcion,
        linea=linea,
        intento=intento,
        item=item,
        activo=activo,
        recepcion_original_id=recepcion.pk,
        linea_original_id=linea.pk,
        intento_original_id=intento.pk,
        item_original_id=item.pk,
        activo_original_id=activo.pk,
        version_confirmada=version,
        referencia_unidad=referencia,
        unidad_normalizada=normalizada,
        autor=actor,
        autor_original_id=actor.pk,
        motivo=motivo,
        evidencia=evidencia,
    )
    AuditLog.objects.create(
        user=actor,
        action="CREATE",
        model="compras.ProcedenciaAdquisicion",
        object_id=str(vinculo.pk),
        payload={
            "recepcion_id": recepcion.pk,
            "linea_id": linea.pk,
            "intento_id": intento.pk,
            "item_id": item.pk,
            "activo_id": activo.pk,
            "version_confirmada": version,
            "referencia_unidad": referencia,
            "motivo": motivo,
            "evidencia": evidencia,
        },
    )
    return vinculo, True
