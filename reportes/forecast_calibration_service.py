from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from math import sqrt

from django.db import transaction
from django.db.models import Max, Sum
from django.utils import timezone

from reportes.models import AnalyticAuditLog, FactVentaDiaria, ForecastCalibrationProfile, ProductionExecutionLog


ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
DEFAULT_RECENT_WEIGHT = Decimal("0.55")
DEFAULT_MID_WEIGHT = Decimal("0.30")
DEFAULT_OLDER_WEIGHT = Decimal("0.15")
DEFAULT_BIAS_ADJUSTMENT = ONE
DEFAULT_BUFFER_MULTIPLIER = ONE
DEFAULT_LOOKBACK_DAYS = 56
DEFAULT_VALIDATION_DAYS = 14
ROTATION_THRESHOLD = Decimal("5")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _safe_div(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= ZERO:
        return ZERO
    return numerator / denominator


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


def _clamp(value: Decimal, floor: Decimal, ceiling: Decimal) -> Decimal:
    return max(floor, min(value, ceiling))


def weekly_pattern_for_day(target_day: date) -> str:
    return (
        ForecastCalibrationProfile.PATTERN_MON_WED
        if target_day.weekday() <= 2
        else ForecastCalibrationProfile.PATTERN_THU_SUN
    )


def rotation_band_for_avg(avg_28: Decimal) -> str:
    return (
        ForecastCalibrationProfile.ROTATION_HIGH
        if avg_28 >= ROTATION_THRESHOLD
        else ForecastCalibrationProfile.ROTATION_LOW
    )


def _history_window(series: dict[date, Decimal], target_day: date, start_offset: int, end_offset: int) -> list[Decimal]:
    return [series.get(target_day - timedelta(days=offset), ZERO) for offset in range(start_offset, end_offset + 1)]


def _same_weekday_history(series: dict[date, Decimal], target_day: date, lookback_weeks: int) -> list[Decimal]:
    return [series.get(target_day - timedelta(days=7 * week), ZERO) for week in range(1, lookback_weeks + 1)]


def _weekday_factor(series: dict[date, Decimal], target_day: date, lookback_weeks: int) -> Decimal:
    weekday_values = _same_weekday_history(series, target_day, lookback_weeks)
    trailing_values = _history_window(series, target_day, 1, max(lookback_weeks * 7, 7))
    weekday_avg = _mean(weekday_values)
    trailing_avg = _mean(trailing_values)
    if weekday_avg <= ZERO or trailing_avg <= ZERO:
        return ONE
    return _clamp(weekday_avg / trailing_avg, Decimal("0.70"), Decimal("1.35"))


def _trend_factor(series: dict[date, Decimal], target_day: date) -> Decimal:
    recent_14 = _mean(_history_window(series, target_day, 1, 14))
    previous_14 = _mean(_history_window(series, target_day, 15, 28))
    if recent_14 <= ZERO or previous_14 <= ZERO:
        return ONE
    return _clamp(recent_14 / previous_14, Decimal("0.75"), Decimal("1.30"))


def _weighted_average(
    series: dict[date, Decimal],
    target_day: date,
    recent_weight: Decimal,
    mid_weight: Decimal,
    older_weight: Decimal,
) -> Decimal:
    recent_14 = _mean(_history_window(series, target_day, 1, 14))
    previous_14 = _mean(_history_window(series, target_day, 15, 28))
    older_28 = _mean(_history_window(series, target_day, 29, 56))
    return (recent_14 * recent_weight) + (previous_14 * mid_weight) + (older_28 * older_weight)


def _segment_key(
    *,
    branch_id: int,
    family: str,
    target_day: date,
    avg_28: Decimal,
) -> tuple[int, str, str, str]:
    return (
        int(branch_id),
        (family or "SIN_FAMILIA").strip()[:120],
        weekly_pattern_for_day(target_day),
        rotation_band_for_avg(avg_28),
    )


@dataclass(slots=True)
class _Observation:
    segment_key: tuple[int, str, str, str]
    actual: Decimal
    recent_14: Decimal
    previous_14: Decimal
    older_28: Decimal
    avg_28: Decimal
    stddev_28: Decimal
    weekday_factor: Decimal
    trend_factor: Decimal


def _segment_params_from_metrics(
    metrics: dict[str, Decimal],
    execution_gap_pct: Decimal,
    adoption_pct: Decimal,
    waste_rate_pct: Decimal,
) -> dict[str, Decimal]:
    volatility = metrics["volatility_ratio"]
    bias_pct = metrics["bias_pct"]
    hit_rate = metrics["hit_rate"]
    if volatility <= Decimal("0.35") and abs(bias_pct) <= Decimal("8"):
        recent_weight = Decimal("0.45")
        mid_weight = Decimal("0.35")
        older_weight = Decimal("0.20")
    elif volatility >= Decimal("0.75"):
        recent_weight = Decimal("0.70")
        mid_weight = Decimal("0.20")
        older_weight = Decimal("0.10")
    else:
        recent_weight = DEFAULT_RECENT_WEIGHT
        mid_weight = DEFAULT_MID_WEIGHT
        older_weight = DEFAULT_OLDER_WEIGHT
    if adoption_pct < Decimal("55"):
        recent_weight = min(recent_weight + Decimal("0.05"), Decimal("0.75"))
        older_weight = max(older_weight - Decimal("0.05"), Decimal("0.05"))
    elif adoption_pct >= Decimal("85") and hit_rate >= Decimal("60"):
        recent_weight = max(recent_weight - Decimal("0.05"), Decimal("0.35"))
        older_weight = min(older_weight + Decimal("0.05"), Decimal("0.25"))
    bias_adjustment = _clamp(ONE - (bias_pct / HUNDRED), Decimal("0.85"), Decimal("1.15"))
    buffer_multiplier = ONE
    if hit_rate < Decimal("50"):
        buffer_multiplier += Decimal("0.20")
    if hit_rate < Decimal("35"):
        buffer_multiplier += Decimal("0.20")
    buffer_multiplier += min(volatility, Decimal("1.20")) * Decimal("0.25")
    buffer_multiplier += min(execution_gap_pct / HUNDRED, Decimal("0.50")) * Decimal("0.25")
    if adoption_pct < Decimal("55"):
        buffer_multiplier += Decimal("0.15")
    elif adoption_pct >= Decimal("85") and hit_rate >= Decimal("60"):
        buffer_multiplier -= Decimal("0.10")
    if waste_rate_pct >= Decimal("12"):
        buffer_multiplier += Decimal("0.10")
    buffer_multiplier = _clamp(buffer_multiplier, Decimal("0.90"), Decimal("1.90"))
    return {
        "recent_weight": recent_weight,
        "mid_weight": mid_weight,
        "older_weight": older_weight,
        "bias_adjustment": bias_adjustment,
        "buffer_multiplier": buffer_multiplier,
    }


def _apply_calibrated_forecast(observation: _Observation, params: dict[str, Decimal]) -> tuple[Decimal, Decimal, Decimal]:
    base_weighted = (
        (observation.recent_14 * params["recent_weight"])
        + (observation.previous_14 * params["mid_weight"])
        + (observation.older_28 * params["older_weight"])
    )
    forecast_qty = max(base_weighted * observation.weekday_factor * observation.trend_factor * params["bias_adjustment"], ZERO)
    segment_error_pct = min(abs(params["bias_adjustment"] - ONE), Decimal("0.15"))
    dynamic_buffer = max(
        observation.stddev_28 * params["buffer_multiplier"],
        forecast_qty * (segment_error_pct + Decimal("0.12")),
    )
    buffer_units = min(dynamic_buffer, forecast_qty * Decimal("0.65"))
    return forecast_qty, max(forecast_qty - buffer_units, ZERO), forecast_qty + buffer_units


def _load_execution_feedback_map(reference_date: date) -> dict[tuple[int, str, str], dict[str, Decimal]]:
    start_date = reference_date - timedelta(days=28)
    rows = (
        ProductionExecutionLog.objects.filter(fecha__range=(start_date, reference_date))
        .exclude(recomendado=ZERO, producido_real=ZERO)
        .values("sucursal_id", "receta__familia", "fecha")
        .annotate(
            recommended=Sum("recomendado"),
            approved=Sum("aprobado"),
            produced=Sum("producido_real"),
            sold=Sum("vendido_real"),
            waste=Sum("merma"),
        )
    )
    gap_map: dict[tuple[int, str, str], dict[str, Decimal]] = defaultdict(
        lambda: {
            "gap": ZERO,
            "approval_gap": ZERO,
            "recommended": ZERO,
            "approved": ZERO,
            "produced": ZERO,
            "sold": ZERO,
            "waste": ZERO,
        }
    )
    for row in rows:
        branch_id = row.get("sucursal_id")
        if not branch_id:
            continue
        family = (row.get("receta__familia") or "SIN_FAMILIA").strip()[:120]
        weekly_pattern = weekly_pattern_for_day(row["fecha"])
        recommended = _to_decimal(row.get("recommended"))
        approved = _to_decimal(row.get("approved"))
        if approved <= ZERO:
            approved = recommended
        produced = _to_decimal(row.get("produced"))
        sold = _to_decimal(row.get("sold"))
        waste = _to_decimal(row.get("waste"))
        key = (int(branch_id), family, weekly_pattern)
        gap_map[key]["gap"] += abs(produced - recommended)
        gap_map[key]["approval_gap"] += abs(approved - recommended)
        gap_map[key]["recommended"] += recommended
        gap_map[key]["approved"] += approved
        gap_map[key]["produced"] += produced
        gap_map[key]["sold"] += sold
        gap_map[key]["waste"] += waste
    return {
        key: {
            "execution_gap_pct": (_safe_div(payload["gap"], max(payload["recommended"], ONE)) * HUNDRED).quantize(Decimal("0.01")),
            "adoption_pct": _clamp(
                HUNDRED - (_safe_div(payload["approval_gap"], max(payload["recommended"], ONE)) * HUNDRED),
                ZERO,
                HUNDRED,
            ).quantize(Decimal("0.01")),
            "sellthrough_pct": (_safe_div(payload["sold"], max(payload["produced"], ONE)) * HUNDRED).quantize(Decimal("0.01")),
            "waste_rate_pct": (_safe_div(payload["waste"], max(payload["produced"], ONE)) * HUNDRED).quantize(Decimal("0.01")),
        }
        for key, payload in gap_map.items()
        if payload["recommended"] > ZERO or payload["produced"] > ZERO
    }


def load_latest_calibration_profiles(reference_date: date | None = None) -> tuple[dict[tuple[int, str, str, str], dict[str, Decimal]], dict[str, object]]:
    latest_reference = reference_date or ForecastCalibrationProfile.objects.aggregate(v=Max("reference_date")).get("v")
    if not latest_reference:
        return {}, {}
    rows = list(
        ForecastCalibrationProfile.objects.filter(reference_date=latest_reference).values(
            "sucursal_id",
            "familia",
            "weekly_pattern",
            "rotation_band",
            "sample_size",
            "wape_before_pct",
            "wape_after_pct",
            "bias_pct",
            "hit_rate_before_pct",
            "hit_rate_after_pct",
            "volatility_pct",
            "recent_weight",
            "mid_weight",
            "older_weight",
            "bias_adjustment",
            "buffer_multiplier",
            "execution_gap_pct",
        )
    )
    profile_map = {
        (
            int(row["sucursal_id"]),
            (row.get("familia") or "SIN_FAMILIA").strip()[:120],
            row["weekly_pattern"],
            row["rotation_band"],
        ): {
            "sample_size": int(row.get("sample_size") or 0),
            "wape_before_pct": _to_decimal(row.get("wape_before_pct")),
            "wape_after_pct": _to_decimal(row.get("wape_after_pct")),
            "bias_pct": _to_decimal(row.get("bias_pct")),
            "hit_rate_before_pct": _to_decimal(row.get("hit_rate_before_pct")),
            "hit_rate_after_pct": _to_decimal(row.get("hit_rate_after_pct")),
            "volatility_pct": _to_decimal(row.get("volatility_pct")),
            "recent_weight": _to_decimal(row.get("recent_weight"), "0.55"),
            "mid_weight": _to_decimal(row.get("mid_weight"), "0.30"),
            "older_weight": _to_decimal(row.get("older_weight"), "0.15"),
            "bias_adjustment": _to_decimal(row.get("bias_adjustment"), "1"),
            "buffer_multiplier": _to_decimal(row.get("buffer_multiplier"), "1"),
            "execution_gap_pct": _to_decimal(row.get("execution_gap_pct")),
        }
        for row in rows
        if row.get("sucursal_id")
    }
    summary = summarize_latest_forecast_calibration(reference_date=latest_reference)
    return profile_map, summary


def rebuild_forecast_calibration_profiles(
    *,
    reference_date: date | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    validation_days: int = DEFAULT_VALIDATION_DAYS,
) -> dict[str, object]:
    reference_date = reference_date or timezone.localdate()
    history_start = reference_date - timedelta(days=lookback_days + validation_days + 56)
    sales_rows = list(
        FactVentaDiaria.objects.filter(
            fecha__range=(history_start, reference_date),
            receta_id__isnull=False,
            sucursal_id__isnull=False,
        )
        .values("fecha", "sucursal_id", "receta_id", "receta__familia")
        .annotate(total=Sum("cantidad"))
        .order_by("sucursal_id", "receta_id", "fecha")
    )
    series_by_key: dict[tuple[int, int], dict[date, Decimal]] = defaultdict(dict)
    family_by_key: dict[tuple[int, int], str] = {}
    for row in sales_rows:
        key = (int(row["sucursal_id"]), int(row["receta_id"]))
        family_by_key[key] = (row.get("receta__familia") or "SIN_FAMILIA").strip()[:120]
        series_by_key[key][row["fecha"]] = _to_decimal(row.get("total"))

    segment_aggregates: dict[tuple[int, str, str, str], dict[str, Decimal]] = defaultdict(
        lambda: {
            "actual": ZERO,
            "abs_error_before": ZERO,
            "signed_error_before": ZERO,
            "hits_before": ZERO,
            "observations": ZERO,
            "volatility_total": ZERO,
        }
    )
    observations: list[_Observation] = []
    validation_start = reference_date - timedelta(days=max(validation_days - 1, 0))
    for key, series in series_by_key.items():
        branch_id = key[0]
        family = family_by_key.get(key, "SIN_FAMILIA")
        for target_day in (validation_start + timedelta(days=index) for index in range(validation_days)):
            if target_day > reference_date:
                continue
            prior_positive_days = sum(1 for day, qty in series.items() if day < target_day and qty > ZERO)
            if prior_positive_days < 14:
                continue
            recent_14 = _mean(_history_window(series, target_day, 1, 14))
            previous_14 = _mean(_history_window(series, target_day, 15, 28))
            older_28 = _mean(_history_window(series, target_day, 29, 56))
            avg_28 = _mean(_history_window(series, target_day, 1, 28))
            stddev_28 = _stddev(_history_window(series, target_day, 1, 28))
            weekday_factor = _weekday_factor(series, target_day, 8)
            trend_factor = _trend_factor(series, target_day)
            base_forecast = max(
                _weighted_average(series, target_day, DEFAULT_RECENT_WEIGHT, DEFAULT_MID_WEIGHT, DEFAULT_OLDER_WEIGHT)
                * weekday_factor
                * trend_factor,
                ZERO,
            )
            base_buffer = min(stddev_28, base_forecast * Decimal("0.35"))
            actual = series.get(target_day, ZERO)
            segment_key = _segment_key(branch_id=branch_id, family=family, target_day=target_day, avg_28=avg_28)
            aggregate = segment_aggregates[segment_key]
            aggregate["actual"] += actual
            aggregate["abs_error_before"] += abs(base_forecast - actual)
            aggregate["signed_error_before"] += (base_forecast - actual)
            if max(base_forecast - base_buffer, ZERO) <= actual <= base_forecast + base_buffer:
                aggregate["hits_before"] += ONE
            aggregate["observations"] += ONE
            if avg_28 > ZERO:
                aggregate["volatility_total"] += _safe_div(stddev_28, avg_28)
            observations.append(
                _Observation(
                    segment_key=segment_key,
                    actual=actual,
                    recent_14=recent_14,
                    previous_14=previous_14,
                    older_28=older_28,
                    avg_28=avg_28,
                    stddev_28=stddev_28,
                    weekday_factor=weekday_factor,
                    trend_factor=trend_factor,
                )
            )

    execution_feedback_map = _load_execution_feedback_map(reference_date)
    profile_params: dict[tuple[int, str, str, str], dict[str, Decimal]] = {}
    profile_rows: list[ForecastCalibrationProfile] = []
    for segment_key, aggregate in segment_aggregates.items():
        actual = aggregate["actual"]
        observations_count = int(aggregate["observations"])
        if observations_count <= 0 or actual <= ZERO:
            continue
        wape_before = (_safe_div(aggregate["abs_error_before"], actual) * HUNDRED).quantize(Decimal("0.01"))
        bias_pct = (_safe_div(aggregate["signed_error_before"], actual) * HUNDRED).quantize(Decimal("0.01"))
        hit_before = (_safe_div(aggregate["hits_before"], aggregate["observations"]) * HUNDRED).quantize(Decimal("0.01"))
        volatility_pct = (_safe_div(aggregate["volatility_total"], aggregate["observations"]) * HUNDRED).quantize(Decimal("0.01"))
        execution_feedback = execution_feedback_map.get(segment_key[:3], {})
        execution_gap_pct = _to_decimal(execution_feedback.get("execution_gap_pct"))
        adoption_pct = _to_decimal(execution_feedback.get("adoption_pct"), "100")
        waste_rate_pct = _to_decimal(execution_feedback.get("waste_rate_pct"))
        params = _segment_params_from_metrics(
            {
                "volatility_ratio": _safe_div(aggregate["volatility_total"], aggregate["observations"]),
                "bias_pct": bias_pct,
                "hit_rate": hit_before,
            },
            execution_gap_pct,
            adoption_pct,
            waste_rate_pct,
        )
        profile_params[segment_key] = params
        profile_rows.append(
            ForecastCalibrationProfile(
                reference_date=reference_date,
                sucursal_id=segment_key[0],
                familia=segment_key[1],
                weekly_pattern=segment_key[2],
                rotation_band=segment_key[3],
                sample_size=observations_count,
                wape_before_pct=wape_before,
                wape_after_pct=ZERO,
                bias_pct=bias_pct,
                hit_rate_before_pct=hit_before,
                hit_rate_after_pct=ZERO,
                volatility_pct=volatility_pct,
                recent_weight=params["recent_weight"],
                mid_weight=params["mid_weight"],
                older_weight=params["older_weight"],
                bias_adjustment=params["bias_adjustment"],
                buffer_multiplier=params["buffer_multiplier"],
                execution_gap_pct=execution_gap_pct,
                metadata={
                    "lookback_days": lookback_days,
                    "validation_days": validation_days,
                    "adoption_pct": str(adoption_pct),
                    "sellthrough_pct": str(_to_decimal(execution_feedback.get("sellthrough_pct"))),
                    "waste_rate_pct": str(_to_decimal(execution_feedback.get("waste_rate_pct"))),
                },
            )
        )

    after_metrics: dict[tuple[int, str, str, str], dict[str, Decimal]] = defaultdict(
        lambda: {"actual": ZERO, "abs_error_after": ZERO, "hits_after": ZERO, "observations": ZERO}
    )
    for observation in observations:
        params = profile_params.get(observation.segment_key)
        if params is None:
            continue
        forecast_after, min_after, max_after = _apply_calibrated_forecast(observation, params)
        metrics = after_metrics[observation.segment_key]
        metrics["actual"] += observation.actual
        metrics["abs_error_after"] += abs(forecast_after - observation.actual)
        if min_after <= observation.actual <= max_after:
            metrics["hits_after"] += ONE
        metrics["observations"] += ONE

    global_before_actual = ZERO
    global_before_error = ZERO
    global_after_error = ZERO
    global_before_hits = ZERO
    global_after_hits = ZERO
    global_observations = ZERO
    for profile in profile_rows:
        segment_key = (
            int(profile.sucursal_id),
            profile.familia,
            profile.weekly_pattern,
            profile.rotation_band,
        )
        segment_before = segment_aggregates[segment_key]
        segment_after = after_metrics.get(segment_key, {})
        actual = segment_before["actual"]
        profile.wape_after_pct = (
            _safe_div(segment_after.get("abs_error_after", ZERO), actual) * HUNDRED
        ).quantize(Decimal("0.01")) if actual > ZERO else ZERO
        profile.hit_rate_after_pct = (
            _safe_div(segment_after.get("hits_after", ZERO), segment_after.get("observations", ZERO)) * HUNDRED
        ).quantize(Decimal("0.01")) if segment_after.get("observations", ZERO) > ZERO else ZERO
        global_before_actual += actual
        global_before_error += segment_before["abs_error_before"]
        global_after_error += segment_after.get("abs_error_after", ZERO)
        global_before_hits += segment_before["hits_before"]
        global_after_hits += segment_after.get("hits_after", ZERO)
        global_observations += segment_before["observations"]

    with transaction.atomic():
        ForecastCalibrationProfile.objects.filter(reference_date=reference_date).delete()
        if profile_rows:
            ForecastCalibrationProfile.objects.bulk_create(profile_rows, batch_size=500)

    summary = {
        "reference_date": reference_date.isoformat(),
        "segments": len(profile_rows),
        "wape_before_pct": (_safe_div(global_before_error, global_before_actual) * HUNDRED).quantize(Decimal("0.01"))
        if global_before_actual > ZERO
        else ZERO,
        "wape_after_pct": (_safe_div(global_after_error, global_before_actual) * HUNDRED).quantize(Decimal("0.01"))
        if global_before_actual > ZERO
        else ZERO,
        "hit_rate_before_pct": (_safe_div(global_before_hits, global_observations) * HUNDRED).quantize(Decimal("0.01"))
        if global_observations > ZERO
        else ZERO,
        "hit_rate_after_pct": (_safe_div(global_after_hits, global_observations) * HUNDRED).quantize(Decimal("0.01"))
        if global_observations > ZERO
        else ZERO,
        "observations": int(global_observations),
    }
    top_improved = sorted(
        profile_rows,
        key=lambda row: (_to_decimal(row.wape_before_pct) - _to_decimal(row.wape_after_pct), row.sample_size),
        reverse=True,
    )[:10]
    worst_segments = sorted(
        profile_rows,
        key=lambda row: (_to_decimal(row.wape_after_pct), row.sample_size),
        reverse=True,
    )[:10]
    AnalyticAuditLog.objects.create(
        audit_type="FORECAST_CALIBRATION",
        status=AnalyticAuditLog.STATUS_OK,
        date_from=validation_start,
        date_to=reference_date,
        discrepancy_count=0,
        message="Calibración segmentada de forecast actualizada",
        payload={
            "summary": {key: str(value) for key, value in summary.items()},
            "top_improved": [
                {
                    "branch_id": row.sucursal_id,
                    "family": row.familia,
                    "pattern": row.weekly_pattern,
                    "rotation": row.rotation_band,
                    "wape_before_pct": str(row.wape_before_pct),
                    "wape_after_pct": str(row.wape_after_pct),
                    "hit_rate_before_pct": str(row.hit_rate_before_pct),
                    "hit_rate_after_pct": str(row.hit_rate_after_pct),
                }
                for row in top_improved
            ],
            "worst_segments": [
                {
                    "branch_id": row.sucursal_id,
                    "family": row.familia,
                    "pattern": row.weekly_pattern,
                    "rotation": row.rotation_band,
                    "wape_after_pct": str(row.wape_after_pct),
                    "hit_rate_after_pct": str(row.hit_rate_after_pct),
                }
                for row in worst_segments
            ],
        },
    )
    return summary


def summarize_latest_forecast_calibration(reference_date: date | None = None) -> dict[str, object]:
    latest_reference = reference_date or ForecastCalibrationProfile.objects.aggregate(v=Max("reference_date")).get("v")
    if not latest_reference:
        return {}
    rows = list(
        ForecastCalibrationProfile.objects.filter(reference_date=latest_reference).values(
            "sucursal__codigo",
            "familia",
            "sample_size",
            "wape_before_pct",
            "wape_after_pct",
            "hit_rate_before_pct",
            "hit_rate_after_pct",
        )
    )
    if not rows:
        return {}
    global_before_num = ZERO
    global_after_num = ZERO
    global_samples = ZERO
    global_before_hit = ZERO
    global_after_hit = ZERO
    branch_metrics: dict[str, dict[str, Decimal]] = defaultdict(lambda: {"samples": ZERO, "before": ZERO, "after": ZERO})
    family_metrics: dict[str, dict[str, Decimal]] = defaultdict(lambda: {"samples": ZERO, "before": ZERO, "after": ZERO})
    for row in rows:
        samples = Decimal(int(row.get("sample_size") or 0))
        if samples <= ZERO:
            continue
        before = _to_decimal(row.get("wape_before_pct"))
        after = _to_decimal(row.get("wape_after_pct"))
        hit_before = _to_decimal(row.get("hit_rate_before_pct"))
        hit_after = _to_decimal(row.get("hit_rate_after_pct"))
        global_samples += samples
        global_before_num += before * samples
        global_after_num += after * samples
        global_before_hit += hit_before * samples
        global_after_hit += hit_after * samples
        branch_key = row.get("sucursal__codigo") or "SIN_SUCURSAL"
        family_key = row.get("familia") or "SIN_FAMILIA"
        branch_metrics[branch_key]["samples"] += samples
        branch_metrics[branch_key]["before"] += before * samples
        branch_metrics[branch_key]["after"] += after * samples
        family_metrics[family_key]["samples"] += samples
        family_metrics[family_key]["before"] += before * samples
        family_metrics[family_key]["after"] += after * samples
    as_rows = lambda source: [
        {
            "segment": key,
            "samples": int(values["samples"]),
            "wape_before_pct": (_safe_div(values["before"], values["samples"])).quantize(Decimal("0.01")),
            "wape_after_pct": (_safe_div(values["after"], values["samples"])).quantize(Decimal("0.01")),
        }
        for key, values in source.items()
        if values["samples"] > ZERO
    ]
    return {
        "reference_date": latest_reference.isoformat(),
        "global": {
            "wape_before_pct": (_safe_div(global_before_num, global_samples)).quantize(Decimal("0.01")),
            "wape_after_pct": (_safe_div(global_after_num, global_samples)).quantize(Decimal("0.01")),
            "hit_rate_before_pct": (_safe_div(global_before_hit, global_samples)).quantize(Decimal("0.01")),
            "hit_rate_after_pct": (_safe_div(global_after_hit, global_samples)).quantize(Decimal("0.01")),
            "samples": int(global_samples),
        },
        "by_branch": sorted(as_rows(branch_metrics), key=lambda item: item["wape_after_pct"], reverse=True),
        "by_family": sorted(as_rows(family_metrics), key=lambda item: item["wape_after_pct"], reverse=True),
    }
