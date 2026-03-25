from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from django.db.models import Count, Sum

from pos_bridge.models import PointDailySale, PointProduct
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import Receta, RecetaAgrupacionAddon
from recetas.utils.costeo_versionado import calcular_costeo_receta


ZERO = Decimal("0")


@dataclass(slots=True)
class GroupedAddonCostBreakdown:
    base_receta: Receta
    addon_receta: Receta
    base_cost: Decimal
    addon_cost: Decimal
    grouped_cost: Decimal
    rule: RecetaAgrupacionAddon


def resolve_receta_from_term(term: str) -> Receta | None:
    cleaned = (term or "").strip()
    if not cleaned:
        return None
    matcher = PointSalesMatchingService()
    receta = matcher.resolve_receta(codigo_point=cleaned, point_name=cleaned)
    if receta is not None:
        return receta
    product = (
        PointProduct.objects.filter(name__icontains=cleaned).order_by("id").first()
        or PointProduct.objects.filter(sku__iexact=cleaned).order_by("id").first()
        or PointProduct.objects.filter(external_id__iexact=cleaned).order_by("id").first()
    )
    if product is not None:
        return matcher.resolve_receta(codigo_point=product.sku, point_name=product.name)
    return None


def build_addon_rule_evidence(*, base_receta: Receta, addon_codigo_point: str) -> dict[str, Decimal | int]:
    addon_code = (addon_codigo_point or "").strip()
    addon_sales = PointDailySale.objects.filter(product__sku__iexact=addon_code)
    if not addon_sales.exists():
        return {
            "days": 0,
            "branches": 0,
            "qty": ZERO,
            "confidence": ZERO,
        }

    paired_sales = PointDailySale.objects.filter(
        sale_date__in=addon_sales.values("sale_date"),
        branch_id__in=addon_sales.values("branch_id"),
        receta=base_receta,
        total_amount__gt=0,
    )

    days = addon_sales.values("sale_date").distinct().count()
    branches = addon_sales.values("branch_id").distinct().count()
    qty = paired_sales.aggregate(total=Sum("quantity")).get("total") or ZERO
    addon_pairs = set(addon_sales.values_list("sale_date", "branch_id").distinct())
    paired_pairs = set(paired_sales.values_list("sale_date", "branch_id").distinct())
    matched_rows = len(addon_pairs & paired_pairs)
    addon_rows = len(addon_pairs)
    confidence = ZERO
    if addon_rows:
        confidence = (Decimal(matched_rows) / Decimal(addon_rows)) * Decimal("100")

    return {
        "days": days,
        "branches": branches,
        "qty": Decimal(str(qty)),
        "confidence": confidence.quantize(Decimal("0.0001")),
    }


def upsert_addon_rule(
    *,
    base_receta: Receta,
    addon_receta: Receta,
    addon_codigo_point: str,
    addon_nombre_point: str,
    addon_familia: str = "",
    addon_categoria: str = "",
    status: str = RecetaAgrupacionAddon.STATUS_APPROVED,
    notas: str = "",
) -> RecetaAgrupacionAddon:
    evidence = build_addon_rule_evidence(base_receta=base_receta, addon_codigo_point=addon_codigo_point)
    rule, _ = RecetaAgrupacionAddon.objects.update_or_create(
        base_receta=base_receta,
        addon_codigo_point=(addon_codigo_point or "").strip().upper(),
        defaults={
            "addon_receta": addon_receta,
            "addon_nombre_point": (addon_nombre_point or addon_receta.nombre)[:250],
            "addon_familia": (addon_familia or "")[:120],
            "addon_categoria": (addon_categoria or "")[:120],
            "source": RecetaAgrupacionAddon.SOURCE_POINT_ZERO_REVENUE,
            "status": status,
            "cooccurrence_days": int(evidence["days"]),
            "cooccurrence_branches": int(evidence["branches"]),
            "cooccurrence_qty": evidence["qty"],
            "confidence_score": evidence["confidence"],
            "notas": notas,
            "activo": True,
        },
    )
    return rule


def approved_addons_for_recipe(receta: Receta):
    return (
        RecetaAgrupacionAddon.objects.filter(
            base_receta=receta,
            activo=True,
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )
        .select_related("addon_receta")
        .order_by("addon_nombre_point", "id")
    )


def resolve_grouped_rule(*, receta_a: Receta, receta_b: Receta) -> RecetaAgrupacionAddon | None:
    rule = (
        RecetaAgrupacionAddon.objects.filter(
            base_receta=receta_a,
            addon_receta=receta_b,
            activo=True,
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )
        .select_related("base_receta", "addon_receta")
        .first()
    )
    if rule is not None:
        return rule
    return (
        RecetaAgrupacionAddon.objects.filter(
            base_receta=receta_b,
            addon_receta=receta_a,
            activo=True,
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )
        .select_related("base_receta", "addon_receta")
        .first()
    )


def calculate_grouped_addon_cost(*, rule: RecetaAgrupacionAddon) -> GroupedAddonCostBreakdown:
    if not rule.addon_receta_id:
        raise ValueError("La regla no tiene receta add-on ligada.")
    base_cost = calcular_costeo_receta(rule.base_receta).costo_total
    addon_cost = calcular_costeo_receta(rule.addon_receta).costo_total
    grouped = (base_cost + addon_cost).quantize(Decimal("0.000001"))
    return GroupedAddonCostBreakdown(
        base_receta=rule.base_receta,
        addon_receta=rule.addon_receta,
        base_cost=base_cost,
        addon_cost=addon_cost,
        grouped_cost=grouped,
        rule=rule,
    )
