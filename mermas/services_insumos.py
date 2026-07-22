from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from core.access import is_admin_or_dg
from mermas.models import MermaInsumo, MermaInsumoEvento, OrdenAjustePoint
from pos_bridge.models import PointInventorySnapshot, PointTransferLine
from rrhh.services_identidad import empleado_vinculado_usuario


@dataclass(frozen=True)
class InsumoElegiblePoint:
    codigo_point: str
    nombre_point: str
    unidad_point: str
    existencia: Decimal
    snapshot_capturado_en: object
    ultimo_movimiento_en: object


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
            "estatus",
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


def insumos_elegibles_para_sucursal(sucursal):
    """Proyecta el selector desde recepciones y stock Point, sin usar inventario ERP/CEDIS."""
    transfers = PointTransferLine.objects.filter(
        erp_destination_branch=sucursal,
        is_current_snapshot=True,
        is_received=True,
        is_cancelled=False,
        is_insumo=True,
        received_quantity__gt=0,
    ).exclude(item_code="")

    by_code = {}
    for line in transfers.order_by("item_code", "-received_at", "-registered_at", "-id"):
        code = (line.item_code or "").strip()
        unit = (line.unit or "").strip().upper()
        if not code or not unit:
            continue
        received_at = line.received_at or line.registered_at
        row = by_code.setdefault(
            code,
            {"name": line.item_name, "units": set(), "branches": set(), "last_movement": received_at},
        )
        row["units"].add(unit)
        row["branches"].add(line.destination_branch_id)
        if received_at and (not row["last_movement"] or received_at > row["last_movement"]):
            row["last_movement"] = received_at

    if not by_code:
        return []

    tolerance_date = timezone.localdate() - timedelta(days=7)
    result = []
    for code, data in by_code.items():
        if len(data["units"]) != 1 or len(data["branches"]) != 1:
            continue
        snapshot = (
            PointInventorySnapshot.objects.filter(
                branch_id=next(iter(data["branches"])), product__sku=code
            )
            .select_related("product")
            .order_by("-captured_at", "-id")
            .first()
        )
        if snapshot is None:
            continue
        last_movement = data["last_movement"]
        visible_at_zero = bool(last_movement and timezone.localtime(last_movement).date() >= tolerance_date)
        if snapshot.stock <= 0 and not visible_at_zero:
            continue
        result.append(
            InsumoElegiblePoint(
                codigo_point=code,
                nombre_point=data["name"],
                unidad_point=next(iter(data["units"])),
                existencia=snapshot.stock,
                snapshot_capturado_en=snapshot.captured_at,
                ultimo_movimiento_en=last_movement,
            )
        )
    return sorted(result, key=lambda item: item.nombre_point.casefold())


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


@transaction.atomic
def simular_orden_ajuste_point(orden_id):
    """Valida el ajuste completo contra el snapshot Point; nunca escribe en Point."""
    orden = OrdenAjustePoint.objects.select_for_update().select_related("merma").get(pk=orden_id)
    if orden.estatus in {OrdenAjustePoint.ESTATUS_SIMULADA, OrdenAjustePoint.ESTATUS_APLICADA}:
        return orden
    if orden.estatus != OrdenAjustePoint.ESTATUS_PENDIENTE:
        return orden
    orden.intentos += 1
    aprobada = abs(orden.cantidad).quantize(Decimal("0.001"))
    raw_key = f"merma-insumo:{orden.merma_id}:{orden.sucursal_id}:{orden.codigo_point}:{orden.unidad_point}:{aprobada}"
    expected_hash = sha256(f"{raw_key}:-{aprobada}".encode("utf-8")).hexdigest()
    eligible = {row.codigo_point: row for row in insumos_elegibles_para_sucursal(orden.sucursal)}
    current_item = eligible.get(orden.codigo_point)
    review_reason = ""
    if orden.payload_hash != expected_hash:
        review_reason = "El hash del payload no coincide con la orden aprobada."
    elif not current_item or current_item.unidad_point != orden.unidad_point:
        review_reason = "La unidad o elegibilidad actual de Point ya no coincide con la orden aprobada."
    candidates = list(
        PointInventorySnapshot.objects.filter(
            branch__erp_branch=orden.sucursal, product__sku=orden.codigo_point
        ).select_related("branch", "product").order_by("branch_id", "-captured_at", "-id")
    )
    latest_by_branch = {}
    for snapshot in candidates:
        latest_by_branch.setdefault(snapshot.branch_id, snapshot)
    if review_reason or len(latest_by_branch) != 1:
        orden.estatus = OrdenAjustePoint.ESTATUS_REQUIERE_REVISION
        orden.ultimo_error = review_reason or "No existe un mapeo Point único y verificable para la sucursal."
        orden.merma.estatus = MermaInsumo.ESTATUS_REQUIERE_REVISION
        orden.merma.save(update_fields=["estatus", "actualizado_en"])
    else:
        snapshot = next(iter(latest_by_branch.values()))
        requerida = aprobada
        orden.existencia_antes = snapshot.stock
        if snapshot.stock < requerida:
            orden.estatus = OrdenAjustePoint.ESTATUS_REQUIERE_REVISION
            orden.ultimo_error = "Existencia insuficiente; no se permite ajuste parcial."
            orden.merma.estatus = MermaInsumo.ESTATUS_REQUIERE_REVISION
            orden.merma.save(update_fields=["estatus", "actualizado_en"])
        else:
            orden.existencia_despues = snapshot.stock - requerida
            orden.estatus = OrdenAjustePoint.ESTATUS_SIMULADA
            orden.ultimo_error = ""
            orden.evidencia_tecnica = {
                "modo": "SIMULACION",
                "snapshot_id": snapshot.id,
                "capturado_en": snapshot.captured_at.isoformat(),
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
