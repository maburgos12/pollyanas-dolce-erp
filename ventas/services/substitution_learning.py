from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from math import sqrt

from django.db import transaction
from django.db.models import Q
from django.db.models import Sum
from django.utils import timezone

from core.models import Sucursal
from recetas.models import Receta, VentaHistorica
from ventas.models import EventoVentaSubstitutionWeight


ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class LearnedWeightResolution:
    weight: Decimal
    source_level: str
    confidence: str
    sample_size: int
    version: str
    branch_weight: Decimal | None = None
    global_weight: Decimal | None = None
    lambda_branch: Decimal = ZERO


def _clamp(value: Decimal, *, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(high, value))


def _family_category_key(recipe: Receta) -> tuple[str, str]:
    return (
        (recipe.familia or "").strip() or "SIN_FAMILIA",
        (recipe.categoria or "").strip() or "SIN_CATEGORIA",
    )


def _family_key(recipe: Receta) -> str:
    return (recipe.familia or "").strip() or "SIN_FAMILIA"


def _attribute_similarity(recipe_a: Receta, recipe_b: Receta) -> Decimal:
    same_family = _family_key(recipe_a) == _family_key(recipe_b)
    same_family_category = _family_category_key(recipe_a) == _family_category_key(recipe_b)
    score = Decimal("0.40")
    if same_family:
        score = Decimal("0.75")
    if same_family_category:
        score = ONE
    if (recipe_a.temporalidad or "") == (recipe_b.temporalidad or ""):
        score = min(ONE, score + Decimal("0.05"))
    return score


def _pearson(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 2 or len(xs) != len(ys):
        return 0.0
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return 0.0
    return cov / sqrt(var_x * var_y)


def _bucket_start(window_start: date, target_day: date, window_days: int) -> date:
    offset = (target_day - window_start).days
    bucket_offset = (offset // window_days) * window_days
    return window_start + timedelta(days=bucket_offset)


def _build_group_definitions(
    *,
    recipes: dict[int, Receta],
    family_filter: str | None = None,
    category_filter: str | None = None,
) -> tuple[dict[str, tuple[int, ...]], dict[int, list[str]]]:
    family_category_groups: defaultdict[tuple[str, str], list[int]] = defaultdict(list)
    family_groups: defaultdict[str, list[int]] = defaultdict(list)
    for recipe in recipes.values():
        family = _family_key(recipe)
        category = _family_category_key(recipe)[1]
        if family_filter and family != family_filter:
            continue
        if category_filter and category != category_filter:
            continue
        family_category_groups[(family, category)].append(int(recipe.id))
        family_groups[family].append(int(recipe.id))

    group_products: dict[str, tuple[int, ...]] = {}
    recipe_to_groups: defaultdict[int, list[str]] = defaultdict(list)

    for (family, category), product_ids in family_category_groups.items():
        if len(product_ids) < 2:
            continue
        key = f"familia_categoria::{family}::{category}"
        sorted_ids = tuple(sorted(set(product_ids)))
        group_products[key] = sorted_ids
        for recipe_id in sorted_ids:
            recipe_to_groups[recipe_id].append(key)

    for family, product_ids in family_groups.items():
        if len(product_ids) < 2:
            continue
        key = f"familia::{family}"
        sorted_ids = tuple(sorted(set(product_ids)))
        group_products[key] = sorted_ids
        for recipe_id in sorted_ids:
            recipe_to_groups[recipe_id].append(key)

    return group_products, dict(recipe_to_groups)


def _confidence_label(sample_size: int, inverse_corr: Decimal, stability_score: Decimal) -> str:
    if sample_size >= 12 and inverse_corr >= Decimal("0.35") and stability_score >= Decimal("0.35"):
        return EventoVentaSubstitutionWeight.CONFIDENCE_HIGH
    if sample_size >= 6 and inverse_corr >= Decimal("0.15"):
        return EventoVentaSubstitutionWeight.CONFIDENCE_MEDIUM
    return EventoVentaSubstitutionWeight.CONFIDENCE_LOW


def rebuild_substitution_weights(
    *,
    lookback_days: int = 180,
    window_days: int = 7,
    branch_ids: list[int] | None = None,
    family: str | None = None,
    category: str | None = None,
    clear_existing: bool = True,
    version: str = "v7.2-learned",
) -> dict[str, int]:
    window_end = timezone.localdate() - timedelta(days=1)
    window_start = window_end - timedelta(days=lookback_days - 1)

    sales_qs = VentaHistorica.objects.filter(fecha__range=(window_start, window_end))
    if branch_ids:
        sales_qs = sales_qs.filter(sucursal_id__in=branch_ids)

    recipe_rows = (
        sales_qs.values("receta_id")
        .annotate(total=Sum("cantidad"))
        .filter(total__gt=0)
    )
    recipe_ids = [int(row["receta_id"]) for row in recipe_rows]
    recipes = {
        int(recipe.id): recipe
        for recipe in Receta.objects.filter(id__in=recipe_ids)
    }
    group_products, recipe_to_groups = _build_group_definitions(
        recipes=recipes,
        family_filter=family,
        category_filter=category,
    )
    if not group_products:
        return {"created": 0, "groups": 0, "rows_seen": 0}

    sales_rows = list(
        sales_qs.filter(receta_id__in=list(recipe_to_groups.keys()))
        .values("fecha", "sucursal_id", "receta_id")
        .annotate(qty=Sum("cantidad"))
    )

    bucket_totals: defaultdict[tuple[str, int | None, date, int], Decimal] = defaultdict(lambda: ZERO)
    for row in sales_rows:
        sale_date = row["fecha"]
        scope_branch_id = int(row["sucursal_id"]) if row["sucursal_id"] else None
        recipe_id = int(row["receta_id"])
        bucket = _bucket_start(window_start, sale_date, window_days)
        qty = Decimal(str(row["qty"] or 0))
        for group_key in recipe_to_groups.get(recipe_id, []):
            bucket_totals[(group_key, None, bucket, recipe_id)] += qty
            bucket_totals[(group_key, scope_branch_id, bucket, recipe_id)] += qty

    weight_rows: list[EventoVentaSubstitutionWeight] = []
    scopes_seen = 0
    for group_key, product_ids in group_products.items():
        relevant_branch_ids = sorted({
            scope_branch_id
            for scope_group_key, scope_branch_id, _bucket, _recipe_id in bucket_totals.keys()
            if scope_group_key == group_key and scope_branch_id is not None
        })
        for scope_branch_id in [None, *relevant_branch_ids]:
            bucket_dates = sorted({
                bucket
                for scope_group_key, branch_id, bucket, _recipe_id in bucket_totals.keys()
                if scope_group_key == group_key and branch_id == scope_branch_id
            })
            if len(bucket_dates) < 3:
                continue
            scopes_seen += 1

            shares_by_product: dict[int, list[Decimal]] = {product_id: [] for product_id in product_ids}
            for bucket in bucket_dates:
                group_total = sum(
                    bucket_totals[(group_key, scope_branch_id, bucket, product_id)]
                    for product_id in product_ids
                )
                if group_total <= ZERO:
                    continue
                for product_id in product_ids:
                    shares_by_product[product_id].append(
                        bucket_totals[(group_key, scope_branch_id, bucket, product_id)] / group_total
                    )

            delta_by_product = {
                product_id: [
                    shares[idx] - shares[idx - 1]
                    for idx in range(1, len(shares))
                ]
                for product_id, shares in shares_by_product.items()
            }

            for winner_product_id in product_ids:
                for loser_product_id in product_ids:
                    if winner_product_id == loser_product_id:
                        continue
                    winner_deltas = delta_by_product.get(winner_product_id, [])
                    loser_deltas = delta_by_product.get(loser_product_id, [])
                    sample_size = min(len(winner_deltas), len(loser_deltas))
                    if sample_size < 4:
                        continue

                    winner_series = [float(value) for value in winner_deltas[:sample_size]]
                    loser_series = [float(value) for value in loser_deltas[:sample_size]]
                    inverse_corr = Decimal(str(max(0.0, -_pearson(winner_series, loser_series))))

                    overlap_samples = []
                    stability_hits = 0
                    active_periods = 0
                    for idx in range(1, min(len(shares_by_product[winner_product_id]), len(shares_by_product[loser_product_id]))):
                        winner_share = shares_by_product[winner_product_id][idx]
                        loser_share = shares_by_product[loser_product_id][idx]
                        if winner_share > ZERO and loser_share > ZERO:
                            overlap_samples.append(min(winner_share, loser_share) / max(winner_share, loser_share))
                        winner_delta = winner_deltas[idx - 1]
                        loser_delta = loser_deltas[idx - 1]
                        if abs(winner_delta) > ZERO or abs(loser_delta) > ZERO:
                            active_periods += 1
                        if winner_delta > ZERO and loser_delta < ZERO:
                            stability_hits += 1

                    overlap_score = (
                        sum(overlap_samples, ZERO) / Decimal(len(overlap_samples))
                        if overlap_samples
                        else ZERO
                    )
                    stability_score = (
                        Decimal(stability_hits) / Decimal(active_periods)
                        if active_periods
                        else ZERO
                    )
                    attribute_similarity = _attribute_similarity(
                        recipes[winner_product_id],
                        recipes[loser_product_id],
                    )
                    sample_factor = min(ONE, Decimal(sample_size) / Decimal("12"))
                    learned_weight = _clamp(
                        (
                            inverse_corr * Decimal("0.45")
                            + attribute_similarity * Decimal("0.25")
                            + overlap_score * Decimal("0.15")
                            + stability_score * Decimal("0.15")
                        ) * sample_factor,
                        low=ZERO,
                        high=ONE,
                    )
                    if learned_weight < Decimal("0.05"):
                        continue

                    confidence = _confidence_label(sample_size, inverse_corr, stability_score)
                    weight_rows.append(
                        EventoVentaSubstitutionWeight(
                            group_key=group_key,
                            winner_product_id=winner_product_id,
                            loser_product_id=loser_product_id,
                            branch_id=scope_branch_id,
                            source_level=(
                                EventoVentaSubstitutionWeight.SOURCE_GLOBAL
                                if scope_branch_id is None
                                else EventoVentaSubstitutionWeight.SOURCE_BRANCH
                            ),
                            weight=learned_weight,
                            sample_size=sample_size,
                            confidence=confidence,
                            window_start=window_start,
                            window_end=window_end,
                            version=version,
                            metadata_json={
                                "inverse_corr": float(inverse_corr),
                                "attribute_similarity": float(attribute_similarity),
                                "overlap_score": float(overlap_score),
                                "stability_score": float(stability_score),
                                "window_days": window_days,
                                "lookback_days": lookback_days,
                            },
                        )
                    )

    with transaction.atomic():
        if clear_existing:
            delete_qs = EventoVentaSubstitutionWeight.objects.filter(version=version)
            if branch_ids:
                delete_qs = delete_qs.filter(branch_id__in=branch_ids)
            if family:
                delete_qs = delete_qs.filter(group_key__contains=f"::{family}")
            if category:
                delete_qs = delete_qs.filter(group_key__endswith=f"::{category}")
            delete_qs.delete()
        if weight_rows:
            EventoVentaSubstitutionWeight.objects.bulk_create(weight_rows, batch_size=500)

    return {
        "created": len(weight_rows),
        "groups": len(group_products),
        "rows_seen": len(sales_rows),
        "scopes": scopes_seen,
    }


def preload_learned_substitution_weights(
    *,
    group_keys: set[str],
    branch_ids: set[int],
    version: str = "v7.2-learned",
) -> dict[tuple[str, int, int, int | None], EventoVentaSubstitutionWeight]:
    if not group_keys:
        return {}
    rows = EventoVentaSubstitutionWeight.objects.filter(
        group_key__in=group_keys,
        version=version,
    ).select_related("branch")
    if branch_ids:
        rows = rows.filter(Q(branch_id__in=list(branch_ids)) | Q(branch__isnull=True))
    return {
        (row.group_key, int(row.winner_product_id), int(row.loser_product_id), int(row.branch_id) if row.branch_id else None): row
        for row in rows
    }


def resolve_learned_substitution_weight(
    *,
    learned_weights: dict[tuple[str, int, int, int | None], EventoVentaSubstitutionWeight],
    group_key: str,
    winner_product_id: int,
    loser_product_id: int,
    branch_id: int,
    version: str = "v7.2-learned",
) -> LearnedWeightResolution | None:
    branch_row = learned_weights.get((group_key, winner_product_id, loser_product_id, branch_id))
    global_row = learned_weights.get((group_key, winner_product_id, loser_product_id, None))

    def usable(row: EventoVentaSubstitutionWeight | None) -> bool:
        return bool(
            row
            and row.weight > ZERO
            and row.sample_size >= 4
            and row.confidence != EventoVentaSubstitutionWeight.CONFIDENCE_LOW
        )

    branch_usable = usable(branch_row)
    global_usable = usable(global_row)
    global_available = bool(global_row and global_row.weight > ZERO and global_row.sample_size >= 4)
    if not branch_usable and not global_usable:
        return None

    if branch_usable and global_available:
        lambda_branch = _clamp(
            (Decimal(branch_row.sample_size) - Decimal("4")) / Decimal("12"),
            low=ZERO,
            high=Decimal("0.75"),
        )
        weight = (Decimal(str(branch_row.weight)) * lambda_branch) + (
            Decimal(str(global_row.weight)) * (ONE - lambda_branch)
        )
        confidence = (
            EventoVentaSubstitutionWeight.CONFIDENCE_HIGH
            if (
                lambda_branch >= Decimal("0.50")
                and branch_row.confidence == EventoVentaSubstitutionWeight.CONFIDENCE_HIGH
                and global_row.confidence != EventoVentaSubstitutionWeight.CONFIDENCE_LOW
            )
            else EventoVentaSubstitutionWeight.CONFIDENCE_MEDIUM
        )
        return LearnedWeightResolution(
            weight=weight,
            source_level=EventoVentaSubstitutionWeight.SOURCE_BLENDED,
            confidence=confidence,
            sample_size=max(branch_row.sample_size, global_row.sample_size),
            version=version,
            branch_weight=Decimal(str(branch_row.weight)),
            global_weight=Decimal(str(global_row.weight)),
            lambda_branch=lambda_branch,
        )

    if branch_usable:
        return LearnedWeightResolution(
            weight=Decimal(str(branch_row.weight)),
            source_level=EventoVentaSubstitutionWeight.SOURCE_BRANCH,
            confidence=branch_row.confidence,
            sample_size=branch_row.sample_size,
            version=version,
            branch_weight=Decimal(str(branch_row.weight)),
        )

    return LearnedWeightResolution(
        weight=Decimal(str(global_row.weight)),
        source_level=EventoVentaSubstitutionWeight.SOURCE_GLOBAL,
        confidence=global_row.confidence,
        sample_size=global_row.sample_size,
        version=version,
        global_weight=Decimal(str(global_row.weight)),
    )
