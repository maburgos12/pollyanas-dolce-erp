import logging
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from core.access import is_admin_or_dg
from maestros.models import CostoInsumo, Insumo
from mermas.models import MermaInsumo, MermaInsumoEvento, OrdenAjustePoint
from pos_bridge.models import PointBranch, PointTransferLine
from pos_bridge.services.live_inventory_lookup_service import (
    PointLiveInventoryLookupError,
    PointLiveInventoryLookupService,
)
from pos_bridge.utils.exceptions import AuthenticationError, ExtractionError
from rrhh.services_identidad import empleado_vinculado_usuario


logger = logging.getLogger(__name__)


def _es_error_transitorio_point(exc):
    cause = exc.__cause__
    if isinstance(cause, (ExtractionError, OSError, TimeoutError)):
        return True
    if isinstance(cause, AuthenticationError):
        message = str(cause).casefold()
        return any(
            marker in message
            for marker in ("429", "500", "502", "503", "504", "timeout", "temporal", "connection")
        )
    return False


@dataclass(frozen=True)
class InsumoElegiblePoint:
    codigo_point: str
    nombre_point: str
    unidad_point: str
    existencia: Decimal
    snapshot_capturado_en: object
    ultimo_movimiento_en: object


@dataclass(frozen=True)
class InsumoRecibidoPoint:
    codigo_point: str
    nombre_point: str
    unidad_point: str
    existencia: Decimal | None = None
    snapshot_capturado_en: object | None = None


def insumos_recibidos_para_sucursal(sucursal):
    """Catálogo recibido por la sucursal; la existencia se consulta en vivo al elegir."""
    transfers = (
        PointTransferLine.objects.filter(
            erp_destination_branch=sucursal,
            is_current_snapshot=True,
            is_received=True,
            is_cancelled=False,
            is_insumo=True,
            received_quantity__gt=0,
        )
        .exclude(item_code="")
        .values("item_code", "item_name", "unit", "received_at", "registered_at")
        .order_by("item_code", "-received_at", "-registered_at", "-id")
    )
    by_code = {}
    for line in transfers:
        code = (line["item_code"] or "").strip()
        unit = (line["unit"] or "").strip().upper()
        if not code or not unit:
            continue
        row = by_code.setdefault(code, {"name": line["item_name"], "units": set()})
        row["units"].add(unit)
    result = [
        InsumoRecibidoPoint(
            codigo_point=code,
            nombre_point=data["name"],
            unidad_point=next(iter(data["units"])),
        )
        for code, data in by_code.items()
        if len(data["units"]) == 1
    ]
    return sorted(result, key=lambda item: item.nombre_point.casefold())


def consultar_existencia_insumo_point(
    sucursal,
    codigo_point,
    *,
    live_service=None,
    insumo_recibido=None,
):
    """Resuelve un insumo recibido y consulta su existencia vigente directamente en Point."""
    code = (codigo_point or "").strip()
    item = insumo_recibido
    if item is not None and item.codigo_point != code:
        item = None
    if item is None:
        received = {
            row.codigo_point: row
            for row in insumos_recibidos_para_sucursal(sucursal)
        }
        item = received.get(code)
    if item is None:
        raise ValidationError("El insumo no fue recibido por esta sucursal.")
    point_branch = (
        PointBranch.objects.filter(erp_branch=sucursal)
        .order_by("-last_seen_at", "-id")
        .first()
    )
    if point_branch is None:
        raise PointLiveInventoryLookupError(
            "La sucursal no tiene una relación vigente con Point."
        )
    service = live_service or PointLiveInventoryLookupService()
    live_result = None
    for attempt in range(3):
        try:
            live_result = service.get_stock(
                product_codes=[code],
                sucursal=sucursal,
                point_branch=point_branch,
            )
            break
        except PointLiveInventoryLookupError as exc:
            if attempt == 2 or not _es_error_transitorio_point(exc):
                raise
            logger.warning(
                "Reintentando existencia Point de insumo codigo=%s sucursal=%s intento=%s: %s",
                code,
                sucursal.pk,
                attempt + 1,
                exc,
            )
    if live_result is None:
        raise PointLiveInventoryLookupError(
            "La consulta en vivo de existencias de Point no está habilitada."
        )
    stock = Decimal(str(live_result.stock_qty))
    if not stock.is_finite():
        raise PointLiveInventoryLookupError(
            "Point devolvió una existencia inválida."
        )
    return InsumoElegiblePoint(
        codigo_point=item.codigo_point,
        nombre_point=item.nombre_point,
        unidad_point=item.unidad_point,
        existencia=stock,
        snapshot_capturado_en=live_result.captured_at,
        ultimo_movimiento_en=None,
    )


@transaction.atomic
def enviar_merma_insumo(*, merma_id, usuario):
    """Congela la cadena literal de responsabilidad RRHH al enviar una merma."""
    merma = MermaInsumo.objects.select_for_update().get(pk=merma_id)
    if merma.reportado_por_id != getattr(usuario, "id", None):
        raise ValidationError("Solo el usuario reportante puede enviar esta merma.")
    if merma.estatus != MermaInsumo.ESTATUS_BORRADOR:
        raise ValidationError("Solo se puede enviar una merma en estado borrador.")

    reportante = empleado_vinculado_usuario(usuario)
    jefe = reportante.jefe_directo if reportante else None
    usuario_jefe = jefe.usuario_erp if jefe else None
    identidad_valida = bool(
        reportante
        and reportante.activo
        and reportante.sucursal_ref_id == merma.sucursal_id
        and jefe
        and jefe.activo
        and usuario_jefe
        and usuario_jefe.is_active
    )

    estado_anterior = merma.estatus
    if not merma.insumo_id:
        merma.insumo = (
            Insumo.objects.filter(codigo_point=merma.codigo_point, activo=True)
            .order_by("id")
            .first()
        )
    fecha_merma = timezone.localtime(merma.creado_en).date()
    costo = None
    if merma.insumo_id:
        costo = (
            CostoInsumo.objects.filter(insumo_id=merma.insumo_id, fecha__lte=fecha_merma)
            .order_by("-fecha", "-id")
            .first()
        )
    if costo:
        merma.costo_unitario_historico = costo.costo_unitario
        merma.costo_moneda = costo.moneda
        merma.costo_fecha = costo.fecha
        merma.costo_fuente_id = str(costo.id)
        merma.estado_valorizacion = MermaInsumo.VALORIZACION_CON_COSTO
    else:
        merma.costo_unitario_historico = None
        merma.costo_moneda = ""
        merma.costo_fecha = None
        merma.costo_fuente_id = ""
        merma.estado_valorizacion = MermaInsumo.VALORIZACION_SIN_COSTO
    merma.costo_resuelto_en = timezone.now()
    if identidad_valida:
        merma.reportante_empleado = reportante
        merma.jefe_empleado = jefe
        merma.jefe_inmediato = usuario_jefe
        merma.estatus = MermaInsumo.ESTATUS_ENVIADA
        motivo_evento = ""
    else:
        merma.reportante_empleado = None
        merma.jefe_empleado = None
        merma.jefe_inmediato = None
        merma.estatus = MermaInsumo.ESTATUS_SIN_RESPONSABLE
        motivo_evento = "No existe una cadena de responsabilidad RRHH válida para la sucursal."

    merma.save(
        update_fields=[
            "reportante_empleado",
            "jefe_empleado",
            "jefe_inmediato",
            "insumo",
            "estatus",
            "costo_unitario_historico",
            "costo_moneda",
            "costo_fecha",
            "costo_fuente_id",
            "estado_valorizacion",
            "costo_resuelto_en",
            "actualizado_en",
        ]
    )
    MermaInsumoEvento.objects.create(
        merma=merma,
        estado_anterior=estado_anterior,
        estado_nuevo=merma.estatus,
        actor=usuario,
        motivo=motivo_evento,
    )
    return merma


@transaction.atomic
def decidir_merma_insumo(*, merma_id, jefe, accion, motivo):
    merma = MermaInsumo.objects.select_for_update().filter(pk=merma_id, jefe_inmediato=jefe).first()
    if not merma:
        raise ValidationError("La merma no está asignada a este jefe inmediato.")
    if merma.estatus != MermaInsumo.ESTATUS_ENVIADA:
        raise ValidationError("La merma ya no está disponible para decisión.")
    accion = (accion or "").strip().upper()
    motivo = (motivo or "").strip()
    estados = {
        "ACLARAR": MermaInsumo.ESTATUS_EN_ACLARACION,
        "RECHAZAR": MermaInsumo.ESTATUS_RECHAZADA,
    }
    if accion not in estados:
        raise ValidationError("La decisión solicitada no es válida.")
    if not motivo:
        raise ValidationError("Es obligatorio indicar el motivo de la decisión.")
    anterior = merma.estatus
    merma.estatus = estados[accion]
    merma.save(update_fields=["estatus", "actualizado_en"])
    MermaInsumoEvento.objects.create(
        merma=merma, estado_anterior=anterior, estado_nuevo=merma.estatus, actor=jefe, motivo=motivo
    )
    return merma


@transaction.atomic
def reasignar_merma_sin_responsable(*, merma_id, actor, jefe_empleado, motivo):
    if not is_admin_or_dg(actor):
        raise ValidationError("Solo Administración o Dirección puede reasignar esta solicitud.")
    motivo = (motivo or "").strip()
    if not motivo:
        raise ValidationError("Es obligatorio indicar el motivo de la reasignación.")
    merma = MermaInsumo.objects.select_for_update().get(pk=merma_id)
    if merma.estatus != MermaInsumo.ESTATUS_SIN_RESPONSABLE:
        raise ValidationError("Solo puede reasignarse una merma sin responsable.")
    usuario_jefe = getattr(jefe_empleado, "usuario_erp", None)
    if not jefe_empleado.activo or not usuario_jefe or not usuario_jefe.is_active:
        raise ValidationError("El responsable debe ser un empleado activo con usuario ERP activo.")
    if usuario_jefe.id == merma.reportado_por_id or jefe_empleado.id == merma.reportante_empleado_id:
        raise ValidationError("El responsable no puede ser la misma persona que reportó la merma.")
    anterior = merma.estatus
    merma.jefe_empleado = jefe_empleado
    merma.jefe_inmediato = usuario_jefe
    merma.estatus = MermaInsumo.ESTATUS_ENVIADA
    merma.save(update_fields=["jefe_empleado", "jefe_inmediato", "estatus", "actualizado_en"])
    MermaInsumoEvento.objects.create(
        merma=merma, estado_anterior=anterior, estado_nuevo=merma.estatus,
        actor=actor, motivo=motivo,
        metadata={"jefe_empleado_id": jefe_empleado.id, "jefe_usuario_id": usuario_jefe.id},
    )
    return merma


@transaction.atomic
def reenviar_merma_aclarada(*, merma_id, usuario, cantidad, comentario, motivo):
    merma = MermaInsumo.objects.select_for_update().get(pk=merma_id)
    if merma.reportado_por_id != getattr(usuario, "id", None):
        raise ValidationError("Solo la persona reportante puede corregir esta merma.")
    if merma.estatus != MermaInsumo.ESTATUS_EN_ACLARACION:
        raise ValidationError("La merma no está pendiente de aclaración.")
    cantidad = Decimal(str(cantidad))
    if not cantidad.is_finite() or cantidad <= 0:
        raise ValidationError("Captura una cantidad positiva válida.")
    comentario = (comentario or "").strip()
    motivo = (motivo or "").strip()
    if not comentario or not motivo:
        raise ValidationError("El comentario corregido y el motivo de reenvío son obligatorios.")
    jefe = merma.jefe_empleado
    usuario_jefe = getattr(jefe, "usuario_erp", None) if jefe else None
    if not jefe or not jefe.activo or not usuario_jefe or not usuario_jefe.is_active:
        cantidad_anterior = merma.cantidad_reportada
        anterior = merma.estatus
        merma.cantidad_reportada = cantidad
        merma.cantidad_aprobada = None
        merma.comentario = comentario
        merma.jefe_empleado = None
        merma.jefe_inmediato = None
        merma.estatus = MermaInsumo.ESTATUS_SIN_RESPONSABLE
        merma.full_clean()
        merma.save(update_fields=[
            "cantidad_reportada", "cantidad_aprobada", "comentario", "jefe_empleado",
            "jefe_inmediato", "estatus", "actualizado_en",
        ])
        MermaInsumoEvento.objects.create(
            merma=merma, estado_anterior=anterior, estado_nuevo=merma.estatus, actor=usuario,
            motivo="El responsable anterior dejó de estar activo; requiere reasignación.",
            metadata={
                "cantidad_anterior": f"{cantidad_anterior:.3f}", "cantidad_nueva": f"{cantidad:.3f}",
                "motivo_aclaracion": motivo,
            },
        )
        return merma
    cantidad_anterior = merma.cantidad_reportada
    anterior = merma.estatus
    merma.cantidad_reportada = cantidad
    merma.cantidad_aprobada = None
    merma.comentario = comentario
    merma.estatus = MermaInsumo.ESTATUS_ENVIADA
    merma.full_clean()
    merma.save(update_fields=["cantidad_reportada", "cantidad_aprobada", "comentario", "estatus", "actualizado_en"])
    MermaInsumoEvento.objects.create(
        merma=merma, estado_anterior=anterior, estado_nuevo=merma.estatus,
        actor=usuario, motivo=motivo,
        metadata={
            "cantidad_anterior": f"{cantidad_anterior:.3f}",
            "cantidad_nueva": f"{cantidad:.3f}",
            "requiere_nueva_aprobacion": True,
        },
    )
    return merma


def simular_orden_ajuste_point(orden_id):
    """Valida el ajuste completo contra Point en vivo; nunca escribe en Point."""
    orden_previa = OrdenAjustePoint.objects.select_related("sucursal").get(pk=orden_id)
    if orden_previa.estatus != OrdenAjustePoint.ESTATUS_PENDIENTE:
        return orden_previa

    current_item = None
    lookup_error = ""
    try:
        current_item = consultar_existencia_insumo_point(
            orden_previa.sucursal,
            orden_previa.codigo_point,
        )
    except (ValidationError, PointLiveInventoryLookupError) as exc:
        lookup_error = f"No fue posible validar la existencia vigente en Point: {exc}"

    with transaction.atomic():
        orden = (
            OrdenAjustePoint.objects.select_for_update()
            .select_related("merma")
            .get(pk=orden_id)
        )
        if orden.estatus != OrdenAjustePoint.ESTATUS_PENDIENTE:
            return orden
        orden.intentos += 1
        aprobada = abs(orden.cantidad).quantize(Decimal("0.001"))
        raw_key = f"merma-insumo:{orden.merma_id}:{orden.sucursal_id}:{orden.codigo_point}:{orden.unidad_point}:{aprobada}"
        expected_hash = sha256(f"{raw_key}:-{aprobada}".encode("utf-8")).hexdigest()
        review_reason = lookup_error
        if not review_reason and orden.payload_hash != expected_hash:
            review_reason = "El hash del payload no coincide con la orden aprobada."
        elif not review_reason and (
            not current_item or current_item.unidad_point != orden.unidad_point
        ):
            review_reason = "La unidad o elegibilidad actual de Point ya no coincide con la orden aprobada."
        if review_reason:
            orden.estatus = OrdenAjustePoint.ESTATUS_REQUIERE_REVISION
            orden.ultimo_error = review_reason
            orden.merma.estatus = MermaInsumo.ESTATUS_REQUIERE_REVISION
            orden.merma.save(update_fields=["estatus", "actualizado_en"])
        else:
            requerida = aprobada
            orden.existencia_antes = current_item.existencia
            if current_item.existencia < requerida:
                orden.estatus = OrdenAjustePoint.ESTATUS_REQUIERE_REVISION
                orden.ultimo_error = "Existencia insuficiente; no se permite ajuste parcial."
                orden.merma.estatus = MermaInsumo.ESTATUS_REQUIERE_REVISION
                orden.merma.save(update_fields=["estatus", "actualizado_en"])
            else:
                orden.existencia_despues = current_item.existencia - requerida
                orden.estatus = OrdenAjustePoint.ESTATUS_SIMULADA
                orden.ultimo_error = ""
                orden.evidencia_tecnica = {
                    "modo": "SIMULACION",
                    "fuente_existencia": "POINT_EN_VIVO",
                    "capturado_en": current_item.snapshot_capturado_en.isoformat(),
                    "sin_escritura_point": True,
                }
        if orden.merma.estatus == MermaInsumo.ESTATUS_REQUIERE_REVISION:
            MermaInsumoEvento.objects.create(
                merma=orden.merma,
                estado_anterior=MermaInsumo.ESTATUS_APROBADA,
                estado_nuevo=MermaInsumo.ESTATUS_REQUIERE_REVISION,
                motivo=orden.ultimo_error,
                metadata={"orden_id": orden.id, "modo": "SIMULACION"},
            )
        orden.save(update_fields=[
            "estatus", "intentos", "ultimo_error", "existencia_antes", "existencia_despues",
            "evidencia_tecnica", "actualizado_en",
        ])
        return orden
