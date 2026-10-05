"""Soporte documental de trabajos, sin reconocimiento ni conciliación financiera."""
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Q

from core.access import can_manage_module, can_view_reportes, can_view_submodule, is_admin_or_dg
from core.models import AuditLog
from reportes.models import GastoOperativoMensual, ObligacionGasto
from reportes.services_gastos_compromisos import usuario_puede_capturar_area
from reportes.views_presupuesto_real import _areas_resumen_permitidas
from sat_client.models import CfdiDescargado
from syncfy_client.models import MovimientoBancario
from .models import DocumentoFinancieroTrabajo
from .services_access import authorized_fallas, authorized_orders, can_access_mantenimiento, can_write_mantenimiento, can_view_costs
from .services_vinculos_proveedores import actor_actual, _limite_explicito_mantenimiento

TIPOS = (("obligacion", "Obligación y gasto espejo"), ("gasto", "Gasto operativo"),
         ("cfdi", "CFDI"), ("movimiento", "Movimiento bancario"))
MODELOS = {"obligacion": ObligacionGasto, "gasto": GastoOperativoMensual,
           "cfdi": CfdiDescargado, "movimiento": MovimientoBancario}


def trabajos_autorizados(actor, tipo):
    if _limite_explicito_mantenimiento(actor) == "none" or not can_access_mantenimiento(actor):
        raise PermissionDenied("Necesitas consultar Mantenimiento.")
    if tipo == "orden":
        return authorized_orders(actor)
    if tipo == "falla":
        return authorized_fallas(actor)
    raise ValidationError("Selecciona una orden o una incidencia existente.")


def fuentes_autorizadas(actor, tipo):
    """Toda restricción se aplica en SQL antes de paginar o materializar."""
    if tipo not in MODELOS:
        raise ValidationError("Selecciona un tipo documental válido.")
    qs = MODELOS[tipo].objects.all()
    if tipo == "obligacion":
        try:
            areas = _areas_resumen_permitidas(actor)
        except PermissionDenied:
            return qs.none()
        if areas is not None:
            qs = qs.filter(area__codigo__in=areas)
        if not can_view_reportes(actor):
            qs = qs.filter(gasto_operativo__isnull=True)
        return qs.select_related("area", "gasto_operativo").prefetch_related("pagos", "parcialidades")
    if tipo == "gasto":
        if not can_view_reportes(actor):
            return qs.none()
        # Los espejos sólo se consultan si también se permite leer su obligación.
        return qs.filter(Q(obligacion_gasto__isnull=True) | Q(obligacion_gasto__in=fuentes_autorizadas(actor, "obligacion"))).select_related("centro_costo", "categoria_gasto", "obligacion_gasto__area")
    if tipo == "cfdi":
        return qs if can_view_submodule(actor, "conciliacion", "fiscal") else qs.none()
    return qs if is_admin_or_dg(actor) else qs.none()


def puede_confirmar(actor, tipo, fuente=None):
    if _limite_explicito_mantenimiento(actor) in {"none", "view"} or not can_write_mantenimiento(actor):
        return False
    if tipo == "obligacion":
        return fuente is not None and usuario_puede_capturar_area(actor, fuente.area)
    if tipo == "gasto":
        if fuente is not None:
            espejo = getattr(fuente, "obligacion_gasto", None)
            if espejo:
                return usuario_puede_capturar_area(actor, espejo.area)
        return can_manage_module(actor, "reportes")
    if tipo == "cfdi":
        return can_view_submodule(actor, "conciliacion", "fiscal")
    return tipo == "movimiento" and is_admin_or_dg(actor)


def documentos_visibles_lote(actor, orden_ids, falla_ids):
    actor = actor_actual(actor)
    qs = DocumentoFinancieroTrabajo.objects.filter(
        Q(tipo_trabajo="orden", orden__in=trabajos_autorizados(actor, "orden").filter(pk__in=orden_ids))
        | Q(tipo_trabajo="falla", falla__in=trabajos_autorizados(actor, "falla").filter(pk__in=falla_ids)))
    for doc_tipo, _ in TIPOS:
        # Cada origen retenido exige su alcance actual, aunque la fuente
        # principal haya cambiado de espejo después de la confirmación.
        permitido = Q(**{f"{doc_tipo}_original_id__isnull": True}) | Q(
            **{f"{doc_tipo}__in": fuentes_autorizadas(actor, doc_tipo)})
        puede_tombstone = (can_manage_module(actor, "reportes") if doc_tipo in {"obligacion", "gasto"}
            else can_view_submodule(actor, "conciliacion", "fiscal") if doc_tipo == "cfdi" else is_admin_or_dg(actor))
        if puede_tombstone:
            permitido |= Q(**{f"{doc_tipo}__isnull": True})
        qs = qs.filter(permitido)
    return qs.select_related("obligacion__gasto_operativo", "gasto__centro_costo", "gasto__categoria_gasto", "cfdi", "movimiento", "autor").prefetch_related("obligacion__pagos", "obligacion__parcialidades")


def documentos_visibles(actor, tipo, trabajo_id):
    return documentos_visibles_lote(actor, [trabajo_id] if tipo == "orden" else [], [trabajo_id] if tipo == "falla" else [])


def detalle_fuente(tipo, fuente, actor):
    if fuente is None:
        return {"titulo": "Fuente eliminada · revisión pendiente", "estado": "Eliminada", "importe": None, "naturaleza": "Procedencia conservada", "moneda": ""}
    importe_visible = can_view_costs(actor)
    if tipo == "obligacion":
        return {"titulo": fuente.concepto, "estado": fuente.get_estado_display(), "importe": fuente.monto_reconocido if importe_visible else None,
                "naturaleza": "Gasto reconocido · compromiso de pago", "moneda": "MXN",
                "espejo_id": fuente.gasto_operativo_id,
                "etapas": [
                    {"tipo": "Parcialidad", "id": p.pk, "fecha": p.fecha_vencimiento, "referencia": str(p.numero), "importe": p.monto if importe_visible else None}
                    for p in fuente.parcialidades.all()
                ] + [
                    {"tipo": "Pago registrado", "id": p.pk, "fecha": p.fecha_pago, "referencia": p.referencia, "importe": p.monto if importe_visible else None}
                    for p in fuente.pagos.all()
                ]}
    if tipo == "gasto":
        return {"titulo": str(fuente), "estado": fuente.get_tipo_dato_display(), "importe": fuente.monto if importe_visible else None,
                "naturaleza": "Estimado" if fuente.es_estimado else fuente.get_tipo_dato_display(), "moneda": "MXN"}
    if tipo == "cfdi":
        return {"titulo": fuente.uuid, "estado": fuente.estatus, "importe": fuente.total if importe_visible else None,
                "naturaleza": "Complemento de pago (REP) · soporte fiscal" if fuente.tipo_comprobante == "P" else "Soporte fiscal", "moneda": fuente.moneda}
    return {"titulo": fuente.descripcion, "estado": fuente.get_tipo_conciliacion_display() or "Sin conciliación",
            "importe": fuente.monto if importe_visible else None, "naturaleza": fuente.get_tipo_display() + " · soporte bancario", "moneda": fuente.moneda,
            "cfdi_relacionado_id": fuente.cfdi_relacionado_id if can_view_submodule(actor, "conciliacion", "fiscal") else None,
            "movimiento_relacionado_id": fuente.movimiento_relacionado_id}


@transaction.atomic
def confirmar_documento(*, user, tipo_trabajo, trabajo_id, tipo_documento, documento_id, motivo, evidencia, confirmado):
    actor = actor_actual(user)
    trabajos = trabajos_autorizados(actor, tipo_trabajo)
    if _limite_explicito_mantenimiento(actor) in {"none", "view"} or not can_write_mantenimiento(actor):
        raise PermissionDenied("Necesitas gestionar Mantenimiento.")
    motivo, evidencia = str(motivo or "").strip(), str(evidencia or "").strip()
    if confirmado is not True or not motivo or not evidencia or len(motivo) > 2000 or len(evidencia) > 4000:
        raise ValidationError("Revisa el documento e indica motivo (máximo 2000) y evidencia (máximo 4000).")
    try:
        trabajo_id, documento_id = int(trabajo_id), int(documento_id)
    except (TypeError, ValueError):
        raise ValidationError("Selecciona IDs existentes.")
    # Todos los envíos del mismo trabajo se serializan. NO KEY UPDATE mantiene
    # compatibilidad con las FK de los escritores vigentes.
    trabajo = trabajos.select_for_update(of=("self",), no_key=True).filter(pk=trabajo_id).first()
    if not trabajo:
        raise PermissionDenied("El trabajo no existe o ya no está en tu alcance.")
    fuente_qs = fuentes_autorizadas(actor, tipo_documento)
    fuente = fuente_qs.filter(pk=documento_id).first()
    if not fuente:
        raise PermissionDenied("El documento no existe o ya no está en tu alcance.")
    obligacion = fuente if tipo_documento == "obligacion" else None
    if tipo_documento == "gasto":
        obligacion = fuentes_autorizadas(actor, "obligacion").filter(gasto_operativo_id=fuente.pk).first()
    # Mismo orden que el escritor económico: obligación antes de gasto espejo.
    if obligacion:
        obligacion = fuentes_autorizadas(actor, "obligacion").select_for_update(of=("self",), no_key=True).filter(pk=obligacion.pk).first()
        if obligacion is None:
            raise PermissionDenied("La obligación ya no está disponible en tu alcance actual.")
        gasto_id = obligacion.gasto_operativo_id
        if tipo_documento == "gasto" and gasto_id != documento_id:
            raise ValidationError("La relación entre obligación y gasto cambió. Revisa la fuente actual.", code="conflict")
        tipo_canonico, canonico_id = "obligacion", obligacion.pk
    else:
        gasto_id = fuente.pk if tipo_documento == "gasto" else None
        tipo_canonico, canonico_id = tipo_documento, fuente.pk
    fuente = fuente_qs.select_for_update(of=("self",), no_key=True).filter(pk=documento_id).first()
    if not fuente:
        raise PermissionDenied("El documento ya no está disponible.")
    gasto = None
    if gasto_id:
        gasto = fuentes_autorizadas(actor, "gasto").select_for_update(of=("self",), no_key=True).filter(pk=gasto_id).first()
        if not gasto:
            raise PermissionDenied("No puedes consultar el gasto espejo actual.")
    if tipo_documento == "gasto":
        espejo_actual = ObligacionGasto.objects.filter(gasto_operativo_id=documento_id).values_list("pk", flat=True).first()
        if espejo_actual != (obligacion.pk if obligacion else None):
            raise ValidationError("La fuente cambió durante la revisión. Revisa la obligación actual.", code="conflict")
    if not puede_confirmar(actor, tipo_canonico, obligacion or fuente):
        raise PermissionDenied("Necesitas la capacidad vigente para confirmar el documento de origen.")
    relaciones = DocumentoFinancieroTrabajo.objects.all()
    campos = {tipo_trabajo: trabajo, tipo_canonico: obligacion or fuente}
    if gasto:
        campos["gasto"] = gasto
    # Un PK reutilizado nunca revive una procedencia eliminada, aun en otro trabajo.
    for campo, obj in campos.items():
        if relaciones.filter(**{f"{campo}_original_id": obj.pk, f"{campo}__isnull": True}).exists():
            raise ValidationError("Una fuente original fue eliminada. Su identidad requiere revisión.", code="conflict")
    mismo_hecho = Q(tipo_documento=tipo_canonico, documento_original_id=canonico_id)
    if gasto:
        mismo_hecho |= Q(gasto_original_id=gasto.pk)
    if obligacion:
        mismo_hecho |= Q(obligacion_original_id=obligacion.pk)
    existente = relaciones.filter(tipo_trabajo=tipo_trabajo, trabajo_original_id=trabajo_id).filter(mismo_hecho).first()
    if existente:
        exacto = (existente.tipo_documento == tipo_canonico and existente.documento_original_id == canonico_id) or (gasto is not None and existente.tipo_documento == "gasto" and existente.gasto_id == gasto.pk)
        exacto &= existente.motivo == motivo and existente.evidencia == evidencia
        exacto &= all(getattr(existente, f"{campo}_id") == obj.pk for campo, obj in campos.items() if getattr(existente, f"{campo}_original_id") is not None)
        if not exacto:
            raise ValidationError("Este trabajo y documento ya tienen otra confirmación. Se conserva la original.", code="conflict")
        return existente, False
    valores = {**campos, **{f"{campo}_original_id": obj.pk for campo, obj in campos.items()}}
    vinculo = DocumentoFinancieroTrabajo.objects.create(tipo_trabajo=tipo_trabajo, trabajo_original_id=trabajo_id,
        tipo_documento=tipo_canonico, documento_original_id=canonico_id, **valores,
        motivo=motivo, evidencia=evidencia, autor=actor, autor_original_id=actor.pk)
    AuditLog.objects.create(user=actor, action="CREATE", model="mantenimiento.DocumentoFinancieroTrabajo",
        object_id=str(vinculo.pk), payload={"tipo_trabajo": tipo_trabajo, "trabajo_id": trabajo_id,
        "tipo_documento": tipo_canonico, "documento_id": canonico_id, "gasto_id": gasto_id,
        "motivo": motivo, "evidencia": evidencia})
    return vinculo, True
