from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from math import sqrt

from django.conf import settings
from django.db import transaction
from django.db.models import Sum

from compras.models import SolicitudCompra
from core.audit import log_event
from core.models import Sucursal
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo
from recetas.models import LineaReceta
from reportes.models import (
    AutoControlSettings,
    AutoPurchaseRequestSnapshot,
    FactProduccionDiaria,
    ProductionOrder,
    SupplierLeadTime,
)


ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
AUTO_PURCHASE_SCOPE_PREFIX = "AUTO_PRODUCCION"
AUTO_PURCHASE_SOLICITANTE = "AUTO_PRODUCTION_SERVICE"
LOOKBACK_DAYS = 28
MUTABLE_REQUEST_STATUSES = {
    SolicitudCompra.STATUS_BORRADOR,
    SolicitudCompra.STATUS_EN_REVISION,
}
LOCKED_REQUEST_STATUSES = {
    SolicitudCompra.STATUS_APROBADA,
    SolicitudCompra.STATUS_RECHAZADA,
}


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _mean(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    return sum(values, ZERO) / Decimal(len(values))


def _stddev(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    mean_value = _mean(values)
    variance = sum((value - mean_value) ** 2 for value in values) / Decimal(len(values))
    return Decimal(str(sqrt(float(variance)))) if variance > ZERO else ZERO


def _auto_purchase_enabled() -> bool:
    return bool(getattr(settings, "ERP_AUTO_PURCHASE_ENABLED", True))


def _minimum_shortage() -> Decimal:
    return _to_decimal(getattr(settings, "ERP_AUTO_PURCHASE_MIN_SHORTAGE", "0.001"), "0.001")


def _scope_area(target_date: date, branch_code: str) -> str:
    return f"{AUTO_PURCHASE_SCOPE_PREFIX}:{target_date.isoformat()}:{branch_code}"


def _control_settings() -> AutoControlSettings:
    return AutoControlSettings.get_solo()


def _effective_auto_purchase_enabled() -> bool:
    return _auto_purchase_enabled() and _control_settings().enable_auto_purchase


def _purchase_reason(
    target_date: date,
    branch_code: str,
    order_id: int,
    *,
    lead_time_dias: int,
    fecha_sugerida_compra: date | None,
    missing_provider: bool = False,
) -> str:
    base = (
        f"Derivado de producción aprobada {target_date.isoformat()} · {branch_code} · orden {order_id} · "
        f"lead time {lead_time_dias} día(s)"
    )
    if fecha_sugerida_compra:
        base = f"{base} · compra sugerida {fecha_sugerida_compra.isoformat()}"
    if missing_provider:
        base = f"{base} · sin proveedor principal"
    return base[:255]


def _safe_div(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= ZERO:
        return ZERO
    return numerator / denominator


def list_auto_purchase_requests(*, target_date: date, sucursal_id: int | None = None):
    queryset = SolicitudCompra.objects.select_related(
        "insumo",
        "insumo__unidad_base",
        "insumo__proveedor_principal",
        "proveedor_sugerido",
    ).filter(area__startswith=f"{AUTO_PURCHASE_SCOPE_PREFIX}:{target_date.isoformat()}:")
    if sucursal_id:
        branch_code = (
            Sucursal.objects.filter(pk=sucursal_id).values_list("codigo", flat=True).first() or ""
        )
        queryset = queryset.filter(area__endswith=f":{branch_code}")
    return queryset.order_by("area", "proveedor_sugerido__nombre", "insumo__nombre", "id")


def list_auto_purchase_snapshots(*, target_date: date, sucursal_id: int | None = None):
    queryset = AutoPurchaseRequestSnapshot.objects.select_related(
        "solicitud",
        "sucursal",
        "insumo",
        "insumo__unidad_base",
        "proveedor",
        "production_order",
    ).filter(fecha=target_date)
    if sucursal_id:
        queryset = queryset.filter(sucursal_id=sucursal_id)
    return queryset.order_by("-purchase_priority_score", "sucursal__codigo", "proveedor__nombre", "insumo__nombre")


def _load_supplier_policies(insumo_ids: list[int]) -> tuple[dict[tuple[int, int], SupplierLeadTime], dict[int, SupplierLeadTime]]:
    policies = list(
        SupplierLeadTime.objects.filter(insumo_id__in=insumo_ids, activo=True)
        .select_related("proveedor", "insumo")
        .order_by("insumo_id", "-activo", "lead_time_dias", "proveedor__nombre", "id")
    )
    by_pair: dict[tuple[int, int], SupplierLeadTime] = {}
    by_insumo: dict[int, SupplierLeadTime] = {}
    for policy in policies:
        by_pair[(int(policy.insumo_id), int(policy.proveedor_id))] = policy
        by_insumo.setdefault(int(policy.insumo_id), policy)
    return by_pair, by_insumo


def _resolve_supplier_policy(
    insumo: Insumo,
    by_pair: dict[tuple[int, int], SupplierLeadTime],
    by_insumo: dict[int, SupplierLeadTime],
) -> tuple[object | None, SupplierLeadTime | None]:
    provider = insumo.proveedor_principal
    policy = None
    if provider is not None:
        policy = by_pair.get((int(insumo.id), int(provider.id)))
    if policy is None:
        policy = by_insumo.get(int(insumo.id))
        if policy is not None:
            provider = policy.proveedor
    return provider, policy


def _latest_cost_map(insumo_ids: set[int]) -> dict[int, Decimal]:
    if not insumo_ids:
        return {}
    return {
        int(row["insumo_id"]): _to_decimal(row.get("costo_unitario"))
        for row in (
            CostoInsumo.objects.filter(insumo_id__in=sorted(insumo_ids))
            .order_by("insumo_id", "-fecha", "-id")
            .distinct("insumo_id")
            .values("insumo_id", "costo_unitario")
        )
    }


def _build_insumo_demand_context(
    *,
    target_date: date,
    branch_ids: set[int],
    bom_by_recipe: dict[int, list[LineaReceta]],
) -> dict[tuple[int, int], dict[str, Decimal]]:
    if not branch_ids or not bom_by_recipe:
        return {}
    start_date = target_date - timedelta(days=LOOKBACK_DAYS)
    production_rows = (
        FactProduccionDiaria.objects.filter(
            fecha__gte=start_date,
            fecha__lt=target_date,
            sucursal_id__in=sorted(branch_ids),
            receta_id__in=sorted(bom_by_recipe.keys()),
        )
        .values("fecha", "sucursal_id", "receta_id")
        .annotate(total=Sum("producido"))
    )
    demand_by_day: dict[tuple[int, int], dict[date, Decimal]] = defaultdict(lambda: defaultdict(lambda: ZERO))
    all_pairs: set[tuple[int, int]] = set()
    for branch_id in branch_ids:
        for recipe_id, bom_lines in bom_by_recipe.items():
            for bom_line in bom_lines:
                all_pairs.add((int(branch_id), int(bom_line.insumo_id)))
    for row in production_rows:
        recipe_id = int(row["receta_id"])
        produced = _to_decimal(row.get("total"))
        if produced <= ZERO:
            continue
        branch_id = int(row["sucursal_id"])
        day = row["fecha"]
        for bom_line in bom_by_recipe.get(recipe_id, []):
            key = (branch_id, int(bom_line.insumo_id))
            demand_by_day[key][day] += produced * _to_decimal(bom_line.cantidad)
    demand_context: dict[tuple[int, int], dict[str, Decimal]] = {}
    days = [start_date + timedelta(days=index) for index in range(LOOKBACK_DAYS)]
    for key in all_pairs:
        series = [_quantize_units(demand_by_day[key].get(day, ZERO)) for day in days]
        avg_daily = _mean(series)
        stddev = _stddev(series)
        volatility = _safe_div(stddev, avg_daily) if avg_daily > ZERO else ZERO
        if volatility >= Decimal("1.00"):
            buffer_days = Decimal("3")
        elif volatility >= Decimal("0.45"):
            buffer_days = Decimal("2")
        else:
            buffer_days = Decimal("1")
        demand_context[key] = {
            "avg_daily": _quantize_units(avg_daily),
            "stddev": _quantize_units(stddev),
            "volatility": volatility.quantize(Decimal("0.0001")) if volatility > ZERO else ZERO,
            "buffer_days": buffer_days,
        }
    return demand_context


def calculate_target_stock(
    *,
    branch_id: int,
    insumo_id: int,
    lead_time_dias: int,
    demand_context: dict[tuple[int, int], dict[str, Decimal]],
    safety_floor: Decimal = ZERO,
) -> dict[str, Decimal]:
    demand_metrics = demand_context.get((int(branch_id), int(insumo_id)), {})
    avg_daily = _to_decimal(demand_metrics.get("avg_daily"))
    buffer_days = _to_decimal(demand_metrics.get("buffer_days"), "1")
    volatility = _to_decimal(demand_metrics.get("volatility"))
    target_stock = max(avg_daily * (Decimal(max(lead_time_dias, 0)) + buffer_days), safety_floor)
    return {
        "avg_daily": _quantize_units(avg_daily),
        "buffer_days": buffer_days.quantize(Decimal("0.001")),
        "volatility": volatility.quantize(Decimal("0.0001")) if volatility > ZERO else ZERO,
        "target_stock": _quantize_units(target_stock),
    }


def _priority_score(
    *,
    immediate_shortage: Decimal,
    target_shortage: Decimal,
    required_qty: Decimal,
    target_stock: Decimal,
    decision_score_avg: Decimal,
    lead_time_dias: int,
    line_cost_total: Decimal,
    max_line_cost_total: Decimal,
) -> Decimal:
    urgency_ratio = max(
        _safe_div(immediate_shortage, max(required_qty, ONE)),
        _safe_div(target_shortage, max(target_stock, ONE)),
    )
    urgency_score = min(urgency_ratio * HUNDRED, HUNDRED)
    impact_score = min(max(decision_score_avg, ZERO), HUNDRED)
    lead_time_score = min((Decimal(max(lead_time_dias, 0)) / Decimal("14")) * HUNDRED, HUNDRED)
    cost_score = min(_safe_div(line_cost_total, max(max_line_cost_total, ONE)) * HUNDRED, HUNDRED)
    score = (
        (urgency_score * Decimal("0.40"))
        + (impact_score * Decimal("0.30"))
        + (lead_time_score * Decimal("0.20"))
        + (cost_score * Decimal("0.10"))
    )
    return score.quantize(Decimal("0.01"))


def generate_purchase_requests_from_production(
    target_date: date,
    *,
    sucursal_id: int | None = None,
    actor=None,
) -> dict[str, object]:
    if not _effective_auto_purchase_enabled():
        return {
            "target_date": target_date.isoformat(),
            "enabled": False,
            "generated": 0,
            "updated": 0,
            "deleted": 0,
            "skipped_locked": 0,
            "lines": 0,
            "note": "La compra automática está desactivada por control global.",
        }

    orders = list(
        ProductionOrder.objects.filter(fecha=target_date, status=ProductionOrder.STATUS_APPROVED)
        .select_related("sucursal", "approved_by", "created_by")
        .prefetch_related("lines__receta")
        .order_by("sucursal__codigo", "id")
    )
    if sucursal_id:
        orders = [order for order in orders if order.sucursal_id == sucursal_id]
    if not orders:
        return {
            "target_date": target_date.isoformat(),
            "enabled": True,
            "orders": 0,
            "generated": 0,
            "updated": 0,
            "deleted": 0,
            "skipped_locked": 0,
            "lines": 0,
            "note": "No hay órdenes APPROVED para generar compras.",
        }

    control = _control_settings()
    minimum_shortage = _minimum_shortage()
    safety_floor = _to_decimal(control.min_stock_seguridad)
    max_daily_purchase = _to_decimal(control.max_compra_diaria)
    recipe_ids = sorted(
        {
            int(line.receta_id)
            for order in orders
            for line in order.lines.all()
            if line.receta_id
            and (_to_decimal(line.cantidad_aprobada) if _to_decimal(line.cantidad_aprobada) > ZERO else _to_decimal(line.cantidad_recomendada)) > ZERO
        }
    )
    bom_lines = list(
        LineaReceta.objects.filter(receta_id__in=recipe_ids, insumo_id__isnull=False)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .exclude(insumo__tipo_item=Insumo.TIPO_INTERNO)
        .select_related("insumo", "insumo__proveedor_principal", "receta")
        .order_by("receta_id", "posicion", "id")
    )
    bom_by_recipe: dict[int, list[LineaReceta]] = defaultdict(list)
    insumo_ids: set[int] = set()
    for line in bom_lines:
        if line.insumo_id and _to_decimal(line.cantidad) > ZERO:
            bom_by_recipe[int(line.receta_id)].append(line)
            insumo_ids.add(int(line.insumo_id))

    supplier_policies_by_pair, supplier_policies_by_insumo = _load_supplier_policies(sorted(insumo_ids))
    inventory_pool = {
        int(existencia.insumo_id): _to_decimal(existencia.stock_actual)
        for existencia in ExistenciaInsumo.objects.filter(insumo_id__in=sorted(insumo_ids))
    }
    latest_cost_map = _latest_cost_map(insumo_ids)
    demand_context = _build_insumo_demand_context(
        target_date=target_date,
        branch_ids={int(order.sucursal_id) for order in orders},
        bom_by_recipe=bom_by_recipe,
    )

    created = 0
    updated = 0
    deleted = 0
    skipped_locked = 0
    total_lines = 0
    missing_provider = 0
    branch_payload: list[dict[str, object]] = []

    for order in orders:
        area = _scope_area(target_date, order.sucursal.codigo)
        requested_by = (
            getattr(actor, "username", "")
            or getattr(order.approved_by, "username", "")
            or getattr(order.created_by, "username", "")
            or AUTO_PURCHASE_SOLICITANTE
        )
        branch_requirements: dict[int, dict[str, object]] = {}
        for line in order.lines.all():
            approved_units = _to_decimal(line.cantidad_aprobada)
            units = approved_units if approved_units > ZERO else _to_decimal(line.cantidad_recomendada)
            if units <= ZERO:
                continue
            for bom_line in bom_by_recipe.get(int(line.receta_id), []):
                required = _quantize_units(units * _to_decimal(bom_line.cantidad))
                if required <= ZERO:
                    continue
                bucket = branch_requirements.setdefault(
                    int(bom_line.insumo_id),
                    {
                        "insumo": bom_line.insumo,
                        "required_qty": ZERO,
                        "recipes": set(),
                        "decision_scores": [],
                    },
                )
                bucket["required_qty"] += required
                bucket["recipes"].add(line.receta.nombre)
                bucket["decision_scores"].append(_to_decimal(line.decision_score))

        existing_requests = {
            int(request.insumo_id): request
            for request in SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido").filter(
                area=area,
                fecha_requerida=target_date,
            )
        }
        keep_insumo_ids: set[int] = set()
        branch_lines: list[dict[str, object]] = []

        candidate_rows: list[dict[str, object]] = []
        for insumo_id, requirement in branch_requirements.items():
            insumo = requirement["insumo"]
            provider, supplier_policy = _resolve_supplier_policy(
                insumo,
                supplier_policies_by_pair,
                supplier_policies_by_insumo,
            )
            if provider is None:
                missing_provider += 1
            lead_time_dias = int(
                supplier_policy.lead_time_dias
                if supplier_policy is not None
                else getattr(provider, "lead_time_dias", 0) or 0
            )
            frecuencia_pedido_dias = int(
                supplier_policy.frecuencia_pedido_dias if supplier_policy is not None else max(lead_time_dias, 7)
            )
            lote_minimo = _to_decimal(supplier_policy.lote_minimo if supplier_policy is not None else ZERO)
            required_qty = _quantize_units(_to_decimal(requirement["required_qty"]))
            available_qty = inventory_pool.get(insumo_id, ZERO)
            reserved_qty = min(required_qty, available_qty)
            immediate_shortage = _quantize_units(max(required_qty - available_qty, ZERO))
            post_reserve_available = _quantize_units(max(available_qty - reserved_qty, ZERO))
            stock_profile = calculate_target_stock(
                branch_id=int(order.sucursal_id),
                insumo_id=insumo_id,
                lead_time_dias=lead_time_dias,
                demand_context=demand_context,
                safety_floor=safety_floor,
            )
            target_stock = _to_decimal(stock_profile["target_stock"])
            target_shortage = _quantize_units(max(target_stock - post_reserve_available, ZERO))
            suggested_qty = _quantize_units(max(immediate_shortage, target_shortage))
            if max_daily_purchase > ZERO:
                suggested_qty = _quantize_units(min(suggested_qty, max_daily_purchase))
            if suggested_qty > ZERO and lote_minimo > ZERO:
                suggested_qty = _quantize_units(max(suggested_qty, lote_minimo))
            if suggested_qty < minimum_shortage:
                inventory_pool[insumo_id] = post_reserve_available
                continue
            inventory_pool[insumo_id] = post_reserve_available
            unit_cost = latest_cost_map.get(insumo_id, ZERO)
            decision_score_avg = _mean(list(requirement["decision_scores"]))
            fecha_sugerida_compra = target_date - timedelta(days=max(lead_time_dias, 0))
            candidate_rows.append(
                {
                    "insumo_id": insumo_id,
                    "insumo": insumo,
                    "provider": provider,
                    "required_qty": required_qty,
                    "reserved_qty": reserved_qty,
                    "immediate_shortage": immediate_shortage,
                    "target_shortage": target_shortage,
                    "suggested_qty": suggested_qty,
                    "target_stock": target_stock,
                    "avg_daily": stock_profile["avg_daily"],
                    "buffer_days": stock_profile["buffer_days"],
                    "volatility": stock_profile["volatility"],
                    "lead_time_dias": lead_time_dias,
                    "frecuencia_pedido_dias": frecuencia_pedido_dias,
                    "lote_minimo": lote_minimo,
                    "decision_score_avg": decision_score_avg,
                    "unit_cost": unit_cost,
                    "line_cost_total": _quantize_units(suggested_qty * unit_cost),
                    "recipes": sorted(requirement["recipes"]),
                    "fecha_sugerida_compra": fecha_sugerida_compra,
                }
            )

        max_line_cost_total = max((_to_decimal(row["line_cost_total"]) for row in candidate_rows), default=ONE)
        for row in candidate_rows:
            row["purchase_priority_score"] = _priority_score(
                immediate_shortage=_to_decimal(row["immediate_shortage"]),
                target_shortage=_to_decimal(row["target_shortage"]),
                required_qty=_to_decimal(row["required_qty"]),
                target_stock=_to_decimal(row["target_stock"]),
                decision_score_avg=_to_decimal(row["decision_score_avg"]),
                lead_time_dias=int(row["lead_time_dias"]),
                line_cost_total=_to_decimal(row["line_cost_total"]),
                max_line_cost_total=_to_decimal(max_line_cost_total),
            )

        candidate_rows.sort(
            key=lambda item: (
                -_to_decimal(item["purchase_priority_score"]),
                item["fecha_sugerida_compra"],
                (item["provider"].nombre if item["provider"] else "ZZZ"),
                item["insumo"].nombre,
            )
        )

        with transaction.atomic():
            for row in candidate_rows:
                insumo_id = int(row["insumo_id"])
                keep_insumo_ids.add(insumo_id)
                insumo = row["insumo"]
                provider = row["provider"]
                existing = existing_requests.get(insumo_id)
                explanation = _purchase_reason(
                    target_date,
                    order.sucursal.codigo,
                    order.id,
                    lead_time_dias=int(row["lead_time_dias"]),
                    fecha_sugerida_compra=row["fecha_sugerida_compra"],
                    missing_provider=provider is None,
                )
                if existing and existing.estatus in LOCKED_REQUEST_STATUSES:
                    skipped_locked += 1
                    branch_lines.append(
                        {
                            "insumo_id": insumo_id,
                            "insumo_nombre": insumo.nombre,
                            "status": f"LOCKED_{existing.estatus}",
                            "shortage_qty": str(row["suggested_qty"]),
                        }
                    )
                    continue

                defaults = {
                    "solicitante": requested_by,
                    "proveedor_sugerido": provider,
                    "cantidad": row["suggested_qty"],
                    "estatus": SolicitudCompra.STATUS_BORRADOR,
                    "fuera_de_catalogo": False,
                    "cotizaciones_requeridas": 0,
                    "cotizaciones_recibidas": 0,
                    "justificacion_excepcion": explanation,
                }
                if existing:
                    existing.solicitante = requested_by
                    existing.proveedor_sugerido = provider
                    existing.cantidad = row["suggested_qty"]
                    existing.estatus = SolicitudCompra.STATUS_BORRADOR
                    existing.fuera_de_catalogo = False
                    existing.cotizaciones_requeridas = 0
                    existing.cotizaciones_recibidas = 0
                    existing.justificacion_excepcion = explanation
                    existing.save(
                        update_fields=[
                            "solicitante",
                            "proveedor_sugerido",
                            "cantidad",
                            "estatus",
                            "fuera_de_catalogo",
                            "cotizaciones_requeridas",
                            "cotizaciones_recibidas",
                            "justificacion_excepcion",
                        ]
                    )
                    updated += 1
                    solicitud = existing
                    log_event(
                        actor,
                        "UPDATE",
                        "compras.SolicitudCompra",
                        solicitud.id,
                        {
                            "source": "auto_purchase_service",
                            "production_order_id": order.id,
                            "required_qty": str(row["required_qty"]),
                            "reserved_inventory_qty": str(row["reserved_qty"]),
                            "shortage_immediate": str(row["immediate_shortage"]),
                            "shortage_target": str(row["target_shortage"]),
                            "branch_code": order.sucursal.codigo,
                            "priority_score": str(row["purchase_priority_score"]),
                        },
                    )
                else:
                    solicitud = SolicitudCompra.objects.create(
                        area=area,
                        insumo=insumo,
                        fecha_requerida=target_date,
                        **defaults,
                    )
                    created += 1
                    log_event(
                        actor,
                        "CREATE",
                        "compras.SolicitudCompra",
                        solicitud.id,
                        {
                            "source": "auto_purchase_service",
                            "production_order_id": order.id,
                            "required_qty": str(row["required_qty"]),
                            "reserved_inventory_qty": str(row["reserved_qty"]),
                            "shortage_immediate": str(row["immediate_shortage"]),
                            "shortage_target": str(row["target_shortage"]),
                            "branch_code": order.sucursal.codigo,
                            "priority_score": str(row["purchase_priority_score"]),
                        },
                    )

                AutoPurchaseRequestSnapshot.objects.update_or_create(
                    solicitud=solicitud,
                    defaults={
                        "production_order": order,
                        "fecha": target_date,
                        "sucursal": order.sucursal,
                        "insumo": insumo,
                        "proveedor": provider,
                        "fecha_sugerida_compra": row["fecha_sugerida_compra"],
                        "stock_actual": inventory_pool.get(insumo_id, ZERO) + _to_decimal(row["reserved_qty"]),
                        "stock_objetivo": row["target_stock"],
                        "faltante_inmediato": row["immediate_shortage"],
                        "faltante_objetivo": row["target_shortage"],
                        "cantidad_sugerida": row["suggested_qty"],
                        "purchase_priority_score": row["purchase_priority_score"],
                        "lead_time_dias": int(row["lead_time_dias"]),
                        "frecuencia_pedido_dias": int(row["frecuencia_pedido_dias"]),
                        "lote_minimo": row["lote_minimo"],
                        "metadata": {
                            "area": area,
                            "required_qty": str(row["required_qty"]),
                            "reserved_inventory_qty": str(row["reserved_qty"]),
                            "avg_daily_demand": str(row["avg_daily"]),
                            "buffer_days": str(row["buffer_days"]),
                            "volatility": str(row["volatility"]),
                            "unit_cost": str(row["unit_cost"]),
                            "decision_score_avg": str(row["decision_score_avg"]),
                            "recipes": row["recipes"],
                            "reason": explanation,
                        },
                    },
                )

                total_lines += 1
                branch_lines.append(
                    {
                        "folio": solicitud.folio,
                        "insumo_id": insumo_id,
                        "insumo_nombre": insumo.nombre,
                        "provider": provider.nombre if provider else "",
                        "required_qty": str(row["required_qty"]),
                        "reserved_inventory_qty": str(row["reserved_qty"]),
                        "shortage_qty": str(row["suggested_qty"]),
                        "shortage_immediate": str(row["immediate_shortage"]),
                        "shortage_target": str(row["target_shortage"]),
                        "stock_actual": str(inventory_pool.get(insumo_id, ZERO) + _to_decimal(row["reserved_qty"])),
                        "stock_target": str(row["target_stock"]),
                        "lead_time_dias": int(row["lead_time_dias"]),
                        "fecha_sugerida_compra": row["fecha_sugerida_compra"].isoformat(),
                        "priority_score": str(row["purchase_priority_score"]),
                        "recipes": row["recipes"],
                        "status": solicitud.estatus,
                    }
                )

            stale_requests = [
                request
                for insumo_id, request in existing_requests.items()
                if insumo_id not in keep_insumo_ids and request.estatus in MUTABLE_REQUEST_STATUSES
            ]
            for request in stale_requests:
                request_id = request.id
                AutoPurchaseRequestSnapshot.objects.filter(solicitud=request).delete()
                request.delete()
                deleted += 1
                log_event(
                    actor,
                    "DELETE",
                    "compras.SolicitudCompra",
                    request_id,
                    {
                        "source": "auto_purchase_service",
                        "reason": "stale_auto_purchase_request",
                        "branch_code": order.sucursal.codigo,
                    },
                )

        branch_payload.append(
            {
                "order_id": order.id,
                "branch_id": order.sucursal_id,
                "branch_code": order.sucursal.codigo,
                "branch_name": order.sucursal.nombre,
                "area": area,
                "line_count": len(branch_lines),
                "lines": branch_lines,
            }
        )

    return {
        "target_date": target_date.isoformat(),
        "enabled": True,
        "orders": len(orders),
        "generated": created,
        "updated": updated,
        "deleted": deleted,
        "skipped_locked": skipped_locked,
        "lines": total_lines,
        "missing_provider": missing_provider,
        "branches": branch_payload,
        "inventory_scope": "GLOBAL_INSUMO",
        "control": {
            "min_stock_seguridad": str(safety_floor),
            "max_compra_diaria": str(max_daily_purchase),
        },
    }
