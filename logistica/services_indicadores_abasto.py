from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

from django.db.models import Prefetch, Q
from django.utils import timezone

from logistica.domain_ruta import point_transfer_enviada
from logistica.models import ParadaEntregaEvidencia, RutaCargaChecklistLinea
from pos_bridge.models import PointTransferLine


ZERO = Decimal("0")
ONE_DECIMAL = Decimal("0.1")
PIECE_UNITS = {"PZ", "PZA", "PZAS", "PIEZA", "PIEZAS"}


def normalize_unit(value: str) -> str:
    unit = (value or "SIN UNIDAD").strip().upper()
    return "PZA" if unit in PIECE_UNITS else unit


def _decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def _percentage(numerator: Decimal, denominator: Decimal):
    if denominator <= ZERO:
        return None
    return ((numerator / denominator) * Decimal("100")).quantize(ONE_DECIMAL, rounding=ROUND_HALF_UP)


def _point_evidence(linea):
    evidences = getattr(linea, "indicadores_evidencias", []) if linea else []
    for evidence in evidences:
        metadata = evidence.metadata if isinstance(evidence.metadata, dict) else {}
        if metadata.get("origen") == "point_transfer":
            return evidence
    return None


def _row_from_transfer(transfer: PointTransferLine) -> dict:
    active_lines = getattr(transfer, "indicadores_lineas", [])
    linea = active_lines[0] if active_lines else None
    evidence = _point_evidence(linea)
    requested = _decimal(transfer.requested_quantity)
    sent = _decimal(transfer.sent_quantity)
    loaded = _decimal(linea.cantidad_cargada) if linea and linea.cantidad_cargada is not None else None
    sent_transition = point_transfer_enviada(transfer)

    evaluated = False
    received = None
    if transfer.is_received:
        received = _decimal(evidence.cantidad_entregada if evidence else transfer.received_quantity)
        evaluated = True
    elif sent_transition and sent == ZERO:
        received = ZERO
        evaluated = True
    elif not transfer.is_open and not sent_transition:
        received = ZERO
        evaluated = True

    usable_sent = min(sent, requested) if evaluated else ZERO
    usable_received = min(received or ZERO, usable_sent) if evaluated else ZERO
    supply_gap = max(requested - usable_sent, ZERO) if evaluated else ZERO
    downstream_gap = max(usable_sent - usable_received, ZERO) if evaluated else ZERO
    load_gap = max(sent - loaded, ZERO) if loaded is not None else ZERO
    delivery_reference = loaded if loaded is not None else sent
    route_gap = max(delivery_reference - received, ZERO) if evaluated and received is not None else ZERO

    if not evaluated and sent > ZERO:
        state = "PENDIENTE_RECEPCION"
        next_owner = "Sucursal"
    elif not evaluated:
        state = "PENDIENTE_ENVIO"
        next_owner = "CEDIS"
    elif supply_gap > ZERO and downstream_gap > ZERO:
        state = "BRECHA_MIXTA"
        next_owner = "CEDIS y Logística"
    elif sent == ZERO and requested > ZERO:
        state = "NO_SURTIDO"
        next_owner = "CEDIS"
    elif load_gap > ZERO:
        state = "DIFERENCIA_CARGA"
        next_owner = "Logística"
    elif route_gap > ZERO:
        state = "DIFERENCIA_ENTREGA"
        next_owner = "Logística / sucursal"
    elif supply_gap > ZERO:
        state = "BRECHA_ABASTO"
        next_owner = "CEDIS"
    elif received is not None and (sent > requested or received > sent):
        state = "SOBRANTE"
        next_owner = "Revisar captura"
    else:
        state = "COMPLETO"
        next_owner = ""

    route = linea.checklist.ruta if linea else None
    branch = transfer.erp_destination_branch or transfer.destination_branch.erp_branch
    return {
        "point_line_id": transfer.id,
        "transfer_external_id": transfer.transfer_external_id,
        "detail_external_id": transfer.detail_external_id,
        "fecha": timezone.localtime(transfer.registered_at).date(),
        "sucursal_id": branch.id if branch else None,
        "sucursal": branch.nombre if branch else transfer.destination_branch.name,
        "item_code": transfer.item_code,
        "item_name": transfer.item_name,
        "unidad": normalize_unit(transfer.unit),
        "es_insumo": transfer.is_insumo,
        "solicitado": requested,
        "enviado": sent,
        "cargado": loaded,
        "recibido": received,
        "evaluado": evaluated,
        "pendiente": not evaluated,
        "enviado_evaluado": usable_sent,
        "recibido_evaluado": usable_received,
        "brecha_abasto": supply_gap,
        "brecha_entrega": downstream_gap,
        "sobrante_envio": max(sent - requested, ZERO) if evaluated else ZERO,
        "sobrante_recepcion": max((received or ZERO) - sent, ZERO) if evaluated else ZERO,
        "brecha_carga": load_gap,
        "brecha_ruta": route_gap,
        "estado": state,
        "responsable_siguiente_paso": next_owner,
        "ruta_id": route.id if route else None,
        "ruta_folio": route.folio if route else "",
        "ruta_nombre": route.nombre if route else "",
        "repartidor": str(route.repartidor) if route and route.repartidor_id else (route.chofer if route else ""),
        "sent_by": transfer.sent_by,
        "received_by": transfer.received_by,
        "received_at": transfer.received_at,
    }


def _totals(rows: list[dict]) -> dict:
    requested = sum((row["solicitado"] for row in rows), ZERO)
    sent = sum((row["enviado"] for row in rows), ZERO)
    received = sum((row["recibido"] for row in rows if row["recibido"] is not None), ZERO)
    loaded_rows = [row for row in rows if row["cargado"] is not None]
    loaded = sum((row["cargado"] for row in loaded_rows), ZERO)
    evaluated_loaded_rows = [row for row in loaded_rows if row["evaluado"]]
    load_denominator = sum((row["enviado"] for row in loaded_rows), ZERO)
    load_numerator = sum((min(row["cargado"], row["enviado"]) for row in loaded_rows), ZERO)
    route_denominator = sum((row["cargado"] for row in evaluated_loaded_rows), ZERO)
    route_numerator = sum(
        (min(row["recibido"] or ZERO, row["cargado"]) for row in evaluated_loaded_rows),
        ZERO,
    )

    evaluated_rows = [row for row in rows if row["evaluado"]]
    pending_shipping_rows = [row for row in rows if not row["evaluado"] and row["enviado"] <= ZERO]
    pending_receipt_rows = [row for row in rows if not row["evaluado"] and row["enviado"] > ZERO]
    requested_evaluated = sum((row["solicitado"] for row in evaluated_rows), ZERO)
    sent_evaluated = sum((row["enviado_evaluado"] for row in evaluated_rows), ZERO)
    received_evaluated = sum((row["recibido_evaluado"] for row in evaluated_rows), ZERO)
    return {
        "solicitado": requested,
        "enviado": sent,
        "recibido": received,
        "cargado": loaded,
        "lineas_con_carga": len(loaded_rows),
        "pendientes": sum(1 for row in rows if row["pendiente"]),
        "lineas": len(rows),
        "lineas_evaluadas": len(evaluated_rows),
        "solicitado_evaluado": requested_evaluated,
        "enviado_evaluado": sent_evaluated,
        "recibido_evaluado": received_evaluated,
        "brecha_abasto": sum((row["brecha_abasto"] for row in evaluated_rows), ZERO),
        "brecha_entrega": sum((row["brecha_entrega"] for row in evaluated_rows), ZERO),
        "brecha_carga": sum((row["brecha_carga"] for row in loaded_rows), ZERO),
        "brecha_ruta": sum((row["brecha_ruta"] for row in evaluated_loaded_rows), ZERO),
        "sobrante_envio": sum((row["sobrante_envio"] for row in evaluated_rows), ZERO),
        "sobrante_recepcion": sum((row["sobrante_recepcion"] for row in evaluated_rows), ZERO),
        "pendientes_envio_lineas": len(pending_shipping_rows),
        "pendientes_envio_solicitado": sum((row["solicitado"] for row in pending_shipping_rows), ZERO),
        "pendientes_recepcion_lineas": len(pending_receipt_rows),
        "pendientes_recepcion_enviado": sum((row["enviado"] for row in pending_receipt_rows), ZERO),
        "porcentaje_abasto": _percentage(sent_evaluated, requested_evaluated),
        "porcentaje_entrega": _percentage(received_evaluated, sent_evaluated),
        "porcentaje_total_evaluado": _percentage(received_evaluated, requested_evaluated),
        "porcentaje_carga": _percentage(load_numerator, load_denominator),
        "porcentaje_ruta": _percentage(route_numerator, route_denominator),
    }


def _group_rows(rows: list[dict], key_builder, label_builder) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[key_builder(row)].append(row)
    result = []
    for key, group_rows in groups.items():
        result.append(
            {
                "clave": key,
                "etiqueta": label_builder(group_rows[0]),
                "unidad": group_rows[0]["unidad"] if len({r["unidad"] for r in group_rows}) == 1 else "",
                "responsable": group_rows[0]["repartidor"],
                **_totals(group_rows),
            }
        )
    return sorted(result, key=lambda group: (str(group["etiqueta"]).casefold(), str(group["clave"])))


def build_indicadores_abasto(*, fecha_desde, fecha_hasta, tipo="productos", unidad="", sucursal_id=None, estado=""):
    active_lines = RutaCargaChecklistLinea.objects.exclude(
        estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA
    ).select_related("checklist__ruta__repartidor__user").order_by("-id")
    evidences = ParadaEntregaEvidencia.objects.filter(
        tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION
    ).order_by("-capturado_en", "-id")
    active_lines = active_lines.prefetch_related(
        Prefetch("evidencias_entrega", queryset=evidences, to_attr="indicadores_evidencias")
    )
    queryset = (
        PointTransferLine.objects.select_related(
            "erp_destination_branch", "destination_branch__erp_branch"
        )
        .filter(
            registered_at__date__gte=fecha_desde,
            registered_at__date__lte=fecha_hasta,
            is_current_snapshot=True,
            is_cancelled=False,
        )
        .filter(Q(erp_destination_branch__isnull=False) | Q(destination_branch__erp_branch__isnull=False))
        .prefetch_related(
            Prefetch("lineas_checklist_carga", queryset=active_lines, to_attr="indicadores_lineas")
        )
        .order_by("registered_at", "transfer_external_id", "detail_external_id", "id")
    )
    if tipo == "insumos":
        queryset = queryset.filter(is_insumo=True)
    elif tipo == "productos":
        queryset = queryset.filter(is_insumo=False)
    if sucursal_id:
        queryset = queryset.filter(
            Q(erp_destination_branch_id=sucursal_id) | Q(destination_branch__erp_branch_id=sucursal_id)
        )

    normalized_unit = normalize_unit(unidad) if unidad else ""
    rows = [_row_from_transfer(transfer) for transfer in queryset]
    if normalized_unit:
        rows = [row for row in rows if row["unidad"] == normalized_unit]
    if estado == "PENDIENTE":
        rows = [row for row in rows if row["pendiente"]]
    elif estado:
        rows = [row for row in rows if row["estado"] == estado]
    units = sorted({row["unidad"] for row in rows})
    route_rows = [row for row in rows if row["ruta_id"]]
    return {
        "rows": rows,
        "totals": _totals(rows),
        "unidades": units,
        "mezcla_unidades": len(units) > 1,
        "por_unidad": _group_rows(rows, lambda row: row["unidad"], lambda row: row["unidad"]),
        "por_sucursal": _group_rows(
            rows,
            lambda row: (row["sucursal_id"] or row["sucursal"], row["unidad"]),
            lambda row: row["sucursal"],
        ),
        "por_dia": _group_rows(rows, lambda row: (row["fecha"], row["unidad"]), lambda row: row["fecha"]),
        "por_producto": _group_rows(
            rows,
            lambda row: (row["item_code"], row["item_name"], row["unidad"]),
            lambda row: row["item_name"],
        ),
        "por_ruta": _group_rows(
            route_rows,
            lambda row: (row["ruta_id"], row["unidad"]),
            lambda row: " · ".join(filter(None, [row["ruta_folio"], row["ruta_nombre"]])),
        ),
    }
