from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from math import sqrt

from django.db.models import Max, Sum
from django.utils import timezone

from recetas.models import Receta
from reportes.forecast_calibration_service import (
    DEFAULT_BIAS_ADJUSTMENT,
    DEFAULT_BUFFER_MULTIPLIER,
    DEFAULT_MID_WEIGHT,
    DEFAULT_OLDER_WEIGHT,
    DEFAULT_RECENT_WEIGHT,
    load_latest_calibration_profiles,
    rotation_band_for_avg,
    weekly_pattern_for_day,
)
from reportes.models import FactVentaDiaria, ProductoSucursalContribucionMensual


ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
DEFAULT_LOOKBACK_WEEKS = 8
DEFAULT_TOP_ROWS = 40
DEFAULT_VALIDATION_DAYS = 14


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _safe_div(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= ZERO:
        return ZERO
    return numerator / denominator


def _quantize_units(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.001"))


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _clamp(value: Decimal, floor: Decimal, ceiling: Decimal) -> Decimal:
    return max(floor, min(value, ceiling))


def _limit_rows(rows: list[dict[str, object]], top_n: int | None) -> list[dict[str, object]]:
    if top_n is None or top_n <= 0:
        return rows
    return rows[:top_n]


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


def _history_window(series: dict[date, dict[str, Decimal]], target_day: date, start_offset: int, end_offset: int) -> list[Decimal]:
    return [
        _to_decimal((series.get(target_day - timedelta(days=offset)) or {}).get("qty"))
        for offset in range(start_offset, end_offset + 1)
    ]


def _same_weekday_history(
    series: dict[date, dict[str, Decimal]],
    target_day: date,
    lookback_weeks: int,
    history_anchor: date | None = None,
) -> list[Decimal]:
    if history_anchor is None:
        return [
            _to_decimal((series.get(target_day - timedelta(days=7 * week)) or {}).get("qty"))
            for week in range(1, lookback_weeks + 1)
        ]
    values = []
    cursor = history_anchor - timedelta(days=1)
    while len(values) < lookback_weeks and cursor >= history_anchor - timedelta(days=lookback_weeks * 14):
        if cursor.weekday() == target_day.weekday():
            values.append(_to_decimal((series.get(cursor) or {}).get("qty")))
        cursor -= timedelta(days=1)
    return values


def _weighted_moving_average(
    series: dict[date, dict[str, Decimal]],
    target_day: date,
    recent_weight: Decimal = DEFAULT_RECENT_WEIGHT,
    mid_weight: Decimal = DEFAULT_MID_WEIGHT,
    older_weight: Decimal = DEFAULT_OLDER_WEIGHT,
) -> Decimal:
    recent_14 = _mean(_history_window(series, target_day, 1, 14))
    previous_14 = _mean(_history_window(series, target_day, 15, 28))
    older_28 = _mean(_history_window(series, target_day, 29, 56))
    return (recent_14 * recent_weight) + (previous_14 * mid_weight) + (older_28 * older_weight)


def _weekday_factor(
    series: dict[date, dict[str, Decimal]],
    target_day: date,
    lookback_weeks: int,
    history_anchor: date | None = None,
) -> Decimal:
    history_anchor = history_anchor or target_day
    weekday_values = _same_weekday_history(series, target_day, lookback_weeks, history_anchor)
    trailing_values = _history_window(series, history_anchor, 1, max(lookback_weeks * 7, 7))
    weekday_avg = _mean(weekday_values)
    trailing_avg = _mean(trailing_values)
    if weekday_avg <= ZERO or trailing_avg <= ZERO:
        return ONE
    return _clamp(weekday_avg / trailing_avg, Decimal("0.70"), Decimal("1.35"))


def _trend_factor(series: dict[date, dict[str, Decimal]], target_day: date) -> tuple[Decimal, Decimal]:
    recent_14 = _mean(_history_window(series, target_day, 1, 14))
    previous_14 = _mean(_history_window(series, target_day, 15, 28))
    if recent_14 <= ZERO or previous_14 <= ZERO:
        return ONE, ZERO
    ratio = _clamp(recent_14 / previous_14, Decimal("0.75"), Decimal("1.30"))
    return ratio, ((ratio - ONE) * HUNDRED).quantize(Decimal("0.01"))


def _recent_avg_price(series: dict[date, dict[str, Decimal]], target_day: date) -> Decimal:
    trailing_rows = [
        series.get(target_day - timedelta(days=offset), {})
        for offset in range(1, 29)
    ]
    total_qty = sum(_to_decimal(row.get("qty")) for row in trailing_rows)
    total_revenue = sum(_to_decimal(row.get("revenue")) for row in trailing_rows)
    if total_qty <= ZERO:
        return ZERO
    return total_revenue / total_qty


def _latest_contribution_map(keys: set[tuple[int, int]]) -> dict[tuple[int, int], dict[str, Decimal]]:
    if not keys:
        return {}
    latest_period = ProductoSucursalContribucionMensual.objects.aggregate(v=Max("periodo")).get("v")
    if not latest_period:
        return {}
    recipe_ids = sorted({recipe_id for _, recipe_id in keys})
    branch_ids = sorted({branch_id for branch_id, _ in keys})
    rows = (
        ProductoSucursalContribucionMensual.objects.filter(
            periodo=latest_period,
            receta_id__in=recipe_ids,
            sucursal_id__in=branch_ids,
        )
        .values(
            "sucursal_id",
            "receta_id",
            "contribucion_unit",
            "margen_contribucion_pct",
            "contribucion_total",
        )
    )
    return {
        (int(row["sucursal_id"]), int(row["receta_id"])): {
            "contribution_unit": _to_decimal(row.get("contribucion_unit")),
            "margin_pct": _to_decimal(row.get("margen_contribucion_pct")) * HUNDRED,
            "contribution_total": _to_decimal(row.get("contribucion_total")),
        }
        for row in rows
    }


def _build_forecast_row(
    *,
    key: tuple[int, int],
    labels: dict[str, object],
    series: dict[date, dict[str, Decimal]],
    target_day: date,
    history_anchor: date | None = None,
    lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
    contribution: dict[str, Decimal] | None,
    calibration_profile: dict[str, Decimal] | None,
) -> dict[str, object]:
    history_anchor = history_anchor or target_day
    recent_avg_28 = _mean(_history_window(series, history_anchor, 1, 28))
    weekly_pattern = weekly_pattern_for_day(target_day)
    rotation_band = rotation_band_for_avg(recent_avg_28)
    calibration_profile = calibration_profile or {}
    recent_weight = _to_decimal(calibration_profile.get("recent_weight"), str(DEFAULT_RECENT_WEIGHT))
    mid_weight = _to_decimal(calibration_profile.get("mid_weight"), str(DEFAULT_MID_WEIGHT))
    older_weight = _to_decimal(calibration_profile.get("older_weight"), str(DEFAULT_OLDER_WEIGHT))
    bias_adjustment = _to_decimal(calibration_profile.get("bias_adjustment"), str(DEFAULT_BIAS_ADJUSTMENT))
    buffer_multiplier = _to_decimal(calibration_profile.get("buffer_multiplier"), str(DEFAULT_BUFFER_MULTIPLIER))
    segment_wape_pct = _to_decimal(calibration_profile.get("wape_after_pct") or calibration_profile.get("wape_before_pct"))
    execution_gap_pct = _to_decimal(calibration_profile.get("execution_gap_pct"))

    weighted_avg = _weighted_moving_average(
        series,
        history_anchor,
        recent_weight=recent_weight,
        mid_weight=mid_weight,
        older_weight=older_weight,
    )
    weekday_factor = _weekday_factor(series, target_day, lookback_weeks, history_anchor)
    trend_factor, trend_pct = _trend_factor(series, history_anchor)
    stddev_28 = _stddev(_history_window(series, history_anchor, 1, 28))
    base_forecast = weighted_avg * weekday_factor * trend_factor * bias_adjustment
    forecast_qty = _quantize_units(max(base_forecast, ZERO))
    dynamic_error_factor = min((segment_wape_pct / HUNDRED) * Decimal("0.35"), Decimal("0.25"))
    execution_guard = min((execution_gap_pct / HUNDRED) * Decimal("0.20"), Decimal("0.10"))
    raw_buffer = max(
        stddev_28 * buffer_multiplier,
        forecast_qty * (Decimal("0.12") + dynamic_error_factor + execution_guard),
    )
    buffer_units = _quantize_units(min(raw_buffer, forecast_qty * Decimal("0.65")))
    forecast_min = _quantize_units(max(forecast_qty - buffer_units, ZERO))
    forecast_max = _quantize_units(forecast_qty + buffer_units)
    avg_price = _recent_avg_price(series, history_anchor)
    forecast_amount = _quantize_money(forecast_qty * avg_price)
    recent_avg_7 = _mean(_history_window(series, history_anchor, 1, 7))
    same_weekday_avg = _mean(_same_weekday_history(series, target_day, lookback_weeks, history_anchor))
    contribution = contribution or {}
    why = (
        f"Promedio ponderado {weighted_avg.quantize(Decimal('0.01'))} pzs con pesos "
        f"{recent_weight.quantize(Decimal('0.01'))}/{mid_weight.quantize(Decimal('0.01'))}/{older_weight.quantize(Decimal('0.01'))}, "
        f"ajuste día {weekday_factor.quantize(Decimal('0.01'))}x, "
        f"sesgo {((bias_adjustment - ONE) * HUNDRED).quantize(Decimal('0.01'))}% y buffer {buffer_multiplier.quantize(Decimal('0.01'))}x."
    )
    return {
        "branch_id": key[0],
        "recipe_id": key[1],
        "branch_code": labels["branch_code"],
        "branch_name": labels["branch_name"],
        "recipe_name": labels["recipe_name"],
        "family": labels["family"],
        "category": labels["category"],
        "forecast_qty": forecast_qty,
        "forecast_min_qty": forecast_min,
        "forecast_max_qty": forecast_max,
        "forecast_amount": forecast_amount,
        "buffer_units": buffer_units,
        "trend_pct": trend_pct,
        "trend_factor": trend_factor.quantize(Decimal("0.0001")),
        "weekday_factor": weekday_factor.quantize(Decimal("0.0001")),
        "recent_avg_7": _quantize_units(recent_avg_7),
        "recent_avg_28": _quantize_units(recent_avg_28),
        "same_weekday_avg": _quantize_units(same_weekday_avg),
        "stddev_28": _quantize_units(stddev_28),
        "weekly_pattern": weekly_pattern,
        "rotation_band": rotation_band,
        "bias_adjustment": bias_adjustment.quantize(Decimal("0.0001")),
        "buffer_multiplier": buffer_multiplier.quantize(Decimal("0.0001")),
        "segment_wape_pct": segment_wape_pct.quantize(Decimal("0.01")),
        "execution_gap_pct": execution_gap_pct.quantize(Decimal("0.01")),
        "avg_price": _quantize_money(avg_price),
        "margin_pct": _to_decimal(contribution.get("margin_pct")),
        "contribution_unit": _to_decimal(contribution.get("contribution_unit")),
        "contribution_total": _to_decimal(contribution.get("contribution_total")),
        "history_days": sum(1 for day in series if day < history_anchor and _to_decimal((series.get(day) or {}).get("qty")) > ZERO),
        "why": why,
    }


def _build_backtest_summary(
    *,
    candidates: list[tuple[tuple[int, int], dict[str, object], dict[date, dict[str, Decimal]]]],
    reference_date: date,
    lookback_weeks: int,
    validation_days: int,
    calibration_profiles: dict[tuple[int, str, str, str], dict[str, Decimal]] | None = None,
) -> dict[str, object]:
    calibration_profiles = calibration_profiles or {}
    total_actual = ZERO
    total_abs_error = ZERO
    hits = 0
    observations = 0
    for key, labels, series in candidates:
        for offset in range(validation_days, 0, -1):
            target_day = reference_date - timedelta(days=offset)
            if sum(1 for day in series if day < target_day and _to_decimal((series.get(day) or {}).get("qty")) > ZERO) < 14:
                continue
            contribution = {
                "margin_pct": ZERO,
                "contribution_unit": ZERO,
                "contribution_total": ZERO,
            }
            avg_28 = _mean(_history_window(series, target_day, 1, 28))
            calibration_key = (
                int(key[0]),
                (labels.get("family") or "SIN_FAMILIA").strip()[:120],
                weekly_pattern_for_day(target_day),
                rotation_band_for_avg(avg_28),
            )
            row = _build_forecast_row(
                key=key,
                labels=labels,
                series=series,
                target_day=target_day,
                history_anchor=target_day,
                lookback_weeks=lookback_weeks,
                contribution=contribution,
                calibration_profile=calibration_profiles.get(calibration_key),
            )
            actual = _to_decimal((series.get(target_day) or {}).get("qty"))
            forecast_qty = _to_decimal(row.get("forecast_qty"))
            total_actual += actual
            total_abs_error += abs(forecast_qty - actual)
            if _to_decimal(row.get("forecast_min_qty")) <= actual <= _to_decimal(row.get("forecast_max_qty")):
                hits += 1
            observations += 1
    wape = (_safe_div(total_abs_error, total_actual) * HUNDRED).quantize(Decimal("0.01")) if total_actual > ZERO else ZERO
    hit_rate = (_safe_div(Decimal(hits), Decimal(observations)) * HUNDRED).quantize(Decimal("0.01")) if observations else ZERO
    return {
        "observations": observations,
        "wape_pct": wape,
        "interval_hit_rate_pct": hit_rate,
        "total_actual_units": _quantize_units(total_actual),
        "absolute_error_units": _quantize_units(total_abs_error),
    }


def build_daily_forecast_context(
    *,
    target_date: date | None = None,
    reference_date: date | None = None,
    lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
    top_n: int | None = DEFAULT_TOP_ROWS,
    validation_days: int = DEFAULT_VALIDATION_DAYS,
) -> dict[str, object]:
    reference_date = reference_date or timezone.localdate()
    target_date = target_date or reference_date
    history_anchor = min(target_date, reference_date)
    history_start = history_anchor - timedelta(days=(lookback_weeks * 7) + validation_days + 28)
    history_end = max(reference_date, target_date)
    sales_rows = list(
        FactVentaDiaria.objects.filter(
            fecha__range=(history_start, history_end),
            receta_id__isnull=False,
            sucursal_id__isnull=False,
            receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        .values(
            "fecha",
            "sucursal_id",
            "sucursal__codigo",
            "sucursal__nombre",
            "receta_id",
            "receta__nombre",
            "receta__familia",
            "receta__categoria",
        )
        .annotate(
            qty=Sum("cantidad"),
            revenue=Sum("venta_neta"),
            margin=Sum("margen"),
        )
        .order_by("sucursal__codigo", "receta__nombre", "fecha")
    )

    series_by_key: dict[tuple[int, int], dict[date, dict[str, Decimal]]] = defaultdict(dict)
    labels_by_key: dict[tuple[int, int], dict[str, object]] = {}
    recent_revenue_by_key: dict[tuple[int, int], Decimal] = defaultdict(lambda: ZERO)
    for row in sales_rows:
        key = (int(row["sucursal_id"]), int(row["receta_id"]))
        labels_by_key[key] = {
            "branch_code": row.get("sucursal__codigo") or "",
            "branch_name": row.get("sucursal__nombre") or "",
            "recipe_name": row.get("receta__nombre") or "",
            "family": row.get("receta__familia") or "",
            "category": row.get("receta__categoria") or "",
        }
        day = row["fecha"]
        qty = _to_decimal(row.get("qty"))
        revenue = _to_decimal(row.get("revenue"))
        margin = _to_decimal(row.get("margin"))
        series_by_key[key][day] = {"qty": qty, "revenue": revenue, "margin": margin}
        if day >= history_anchor - timedelta(days=28):
            recent_revenue_by_key[key] += revenue

    ranked_keys = sorted(
        recent_revenue_by_key,
        key=lambda key: (recent_revenue_by_key[key], key[0], key[1]),
        reverse=True,
    )
    selected_keys = ranked_keys if top_n is None or top_n <= 0 else ranked_keys[:top_n]
    contribution_map = _latest_contribution_map(set(selected_keys))
    calibration_profiles, calibration_summary = load_latest_calibration_profiles(reference_date=reference_date)
    rows: list[dict[str, object]] = []
    backtest_candidates: list[tuple[tuple[int, int], dict[str, object], dict[date, dict[str, Decimal]]]] = []
    for key in selected_keys:
        series = series_by_key[key]
        if sum(1 for day in series if day < history_anchor and _to_decimal((series.get(day) or {}).get("qty")) > ZERO) < 14:
            continue
        avg_28 = _mean(_history_window(series, history_anchor, 1, 28))
        calibration_key = (
            int(key[0]),
            (labels_by_key[key].get("family") or "SIN_FAMILIA").strip()[:120],
            weekly_pattern_for_day(target_date),
            rotation_band_for_avg(avg_28),
        )
        row = _build_forecast_row(
            key=key,
            labels=labels_by_key[key],
            series=series,
            target_day=target_date,
            history_anchor=history_anchor,
            lookback_weeks=lookback_weeks,
            contribution=contribution_map.get(key),
            calibration_profile=calibration_profiles.get(calibration_key),
        )
        rows.append(row)
        backtest_candidates.append((key, labels_by_key[key], series))
    rows.sort(key=lambda row: (_to_decimal(row.get("forecast_amount")), _to_decimal(row.get("forecast_qty"))), reverse=True)
    total_units = sum((_to_decimal(row.get("forecast_qty")) for row in rows), ZERO)
    total_amount = sum((_to_decimal(row.get("forecast_amount")) for row in rows), ZERO)
    validation = _build_backtest_summary(
        candidates=backtest_candidates[: min(len(backtest_candidates), 25)],
        reference_date=reference_date,
        lookback_weeks=lookback_weeks,
        validation_days=validation_days,
        calibration_profiles=calibration_profiles,
    )
    return {
        "target_date": target_date.isoformat(),
        "target_label": target_date.strftime("%d %b %Y"),
        "lookback_weeks": lookback_weeks,
        "rows": _limit_rows(rows, top_n),
        "summary": {
            "products": len(rows),
            "forecast_units": _quantize_units(total_units),
            "forecast_amount": _quantize_money(total_amount),
            "max_forecast_units": max((_to_decimal(row.get("forecast_qty")) for row in rows), default=ZERO),
        },
        "validation": validation,
        "calibration": calibration_summary,
        "formula": {
            "weighted_average": "pesos dinámicos por sucursal/familia/patrón semanal/rotación sobre ventanas 14/14/28 días",
            "weekday_factor": "promedio del mismo dia de la semana / promedio movil de 8 semanas",
            "trend_factor": "promedio ultimos 14 dias / promedio dias 15-28 con limite explicable",
            "interval": "forecast +/- buffer dinámico según volatilidad, error reciente y brecha de ejecución",
        },
    }
