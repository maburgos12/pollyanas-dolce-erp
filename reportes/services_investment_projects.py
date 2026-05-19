from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import logging

from django.db import transaction
from django.db.models import Prefetch, Sum
from django.utils import timezone

from core.audit import log_event
from pos_bridge.models import PointDailySale
from reportes.models import (
    CentroCosto,
    ExpansionPolicyConfig,
    FactVentaDiaria,
    GastoOperativoMensual,
    ProyectoInversion,
    ProyectoInversionAlerta,
    ProyectoInversionEscenario,
    ProyectoInversionPagoDeuda,
    ProyectoInversionSnapshotMensual,
)
from ventas.models import VentaAutoritativaPoint

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
TWO_PLACES = Decimal("0.01")
FOUR_PLACES = Decimal("0.0001")

# Guardrail: si COGS supera este múltiplo de ventas, el costo histórico es inválido.
# Un ratio de 2.0 significa COGS > 200% de ventas, físicamente imposible en operación normal.
_COGS_RATIO_THRESHOLD = Decimal("2")

# Límites del campo DecimalField(max_digits=8, decimal_places=4) usado en porcentajes de snapshot.
_PCT_FIELD_MAX = Decimal("9999.9999")
_PCT_FIELD_MIN = Decimal("-9999.9999")

SERVICE_CATEGORY_CODES = {"SERVICIOS_SUC", "LUZ_SUC", "AGUA_SUC", "GAS_SUC"}
MARKETING_CATEGORY_CODES = {"MARKETING_SUC", "MARKETING_APERTURA", "PUBLICIDAD_SUC"}
TOTAL_BRANCH_EXPENSE_CATEGORY_CODES = {"OPEX_TOTAL_SUC"}
ROI_ALERT_MIN_MONTHS = 3
FORECAST_NOISE_RATIO = Decimal("1.00")

EXPENSE_SOURCE_COMPLETE = "gasto_operativo_mensual_complete"
EXPENSE_SOURCE_PARTIAL = "gasto_operativo_mensual_partial"
EXPENSE_SOURCE_TOTAL_ONLY = "gasto_operativo_mensual_total_only"
EXPENSE_SOURCE_MISSING = "gasto_operativo_mensual_missing"

EXPENSE_COVERAGE_COMPLETE = "COMPLETE"
EXPENSE_COVERAGE_PARTIAL = "PARTIAL"
EXPENSE_COVERAGE_MISSING = "MISSING"


def _deep_merge_dict(base: dict, override: dict) -> dict:
    result = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def _default_calibration_settings() -> dict[str, object]:
    return {
        "mode_enabled": False,
        "minimum_sample_projects": 3,
        "minimum_months": 6,
        "health_weights": {
            "roi": Decimal("25"),
            "free_cashflow": Decimal("20"),
            "sales_growth": Decimal("15"),
            "capex": Decimal("20"),
            "recovery": Decimal("20"),
        },
        "classification_thresholds": {
            "expand_min_health_score": 80,
            "monitor_min_health_score": 50,
            "payback_tolerance_ratio": Decimal("1.00"),
            "roi_target_factor": Decimal("1.00"),
            "positive_free_cashflow_months": 3,
            "recurrent_negative_months": 2,
        },
        "forecast": {
            "preferred_window_months": 3,
            "fallback_window_months": 6,
            "noise_ratio": FORECAST_NOISE_RATIO,
            "negative_months_mode": "fallback_to_fallback_window",
            "moving_average_months": 3,
        },
    }


def _active_expansion_policy() -> ExpansionPolicyConfig | None:
    return ExpansionPolicyConfig.objects.filter(activa=True).order_by("-actualizado_en", "-id").first()


def _get_calibration_settings() -> dict[str, object]:
    defaults = _default_calibration_settings()
    policy = _active_expansion_policy()
    if policy is None:
        return defaults
    return _deep_merge_dict(defaults, (policy.metadata or {}).get("calibration", {}))


def _as_decimal(value) -> Decimal:
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _month_start(target: date) -> date:
    return target.replace(day=1)


def _month_end(target: date) -> date:
    return target.replace(day=monthrange(target.year, target.month)[1])


def _add_months(target: date, months: int) -> date:
    year = target.year + ((target.month - 1 + months) // 12)
    month = ((target.month - 1 + months) % 12) + 1
    day = min(target.day, monthrange(year, month)[1])
    return date(year, month, day)


def _iter_months(start: date, end: date) -> list[date]:
    cursor = _month_start(start)
    stop = _month_start(end)
    values: list[date] = []
    while cursor <= stop:
        values.append(cursor)
        cursor = _add_months(cursor, 1)
    return values


def _elapsed_months(start: date | None, end: date | None) -> int:
    if start is None or end is None:
        return 0
    return max(((end.year - start.year) * 12) + (end.month - start.month) + 1, 0)


def _safe_div(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator in {None, ZERO}:
        return None
    return numerator / denominator


def _quantize(value: Decimal | None, pattern: Decimal = TWO_PLACES) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(pattern, rounding=ROUND_HALF_UP)


def _ratio_from_percent_like(value: Decimal | None) -> Decimal:
    raw = _as_decimal(value)
    if raw > Decimal("1"):
        return raw / Decimal("100")
    return raw


def _percent_value(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    ratio = _safe_div(numerator, denominator)
    if ratio is None:
        return None
    return _quantize(ratio * Decimal("100"), FOUR_PLACES)


def _clamp_pct(value: Decimal | None) -> Decimal | None:
    """Evita overflow en DecimalField(max_digits=8, decimal_places=4) para porcentajes."""
    if value is None:
        return None
    if value > _PCT_FIELD_MAX:
        return _PCT_FIELD_MAX
    if value < _PCT_FIELD_MIN:
        return _PCT_FIELD_MIN
    return value


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _average_decimals(values: list[Decimal]) -> Decimal | None:
    clean = [item for item in values if item is not None]
    if not clean:
        return None
    return sum(clean, ZERO) / Decimal(len(clean))


def _derive_data_quality(
    *,
    sales_source: str,
    expense_source: str,
    debt_source: str,
    is_pre_opening: bool,
) -> tuple[str, int]:
    if is_pre_opening:
        return ProyectoInversionSnapshotMensual.DATA_SOURCE_ESTIMATED, 40
    debt_reliable = debt_source in {"actual", "no_debt", "estimated_outside_term"}
    debt_estimated = debt_source == "estimated"
    if sales_source == "fact_venta_diaria" and expense_source == EXPENSE_SOURCE_COMPLETE and debt_reliable:
        return ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT, 100
    if sales_source == "fact_venta_diaria" and expense_source in {
        EXPENSE_SOURCE_PARTIAL,
        EXPENSE_SOURCE_TOTAL_ONLY,
    }:
        return ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT, 75 if debt_reliable else 65
    if sales_source == "fact_venta_diaria" and expense_source == EXPENSE_SOURCE_COMPLETE and debt_estimated:
        return ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT, 85
    if sales_source == "fact_venta_diaria" and expense_source == EXPENSE_SOURCE_MISSING:
        return ProyectoInversionSnapshotMensual.DATA_SOURCE_ESTIMATED, 45 if debt_reliable else 35
    if sales_source in {"venta_autoritativa_point", "point_daily_sale"}:
        if expense_source in {EXPENSE_SOURCE_COMPLETE, EXPENSE_SOURCE_PARTIAL, EXPENSE_SOURCE_TOTAL_ONLY}:
            return ProyectoInversionSnapshotMensual.DATA_SOURCE_FALLBACK, 70 if debt_reliable else 60
        return ProyectoInversionSnapshotMensual.DATA_SOURCE_ESTIMATED, 45
    return ProyectoInversionSnapshotMensual.DATA_SOURCE_ESTIMATED, 40


def _health_status_from_score(score: int) -> str:
    if score > 80:
        return ProyectoInversionSnapshotMensual.HEALTH_GREEN
    if score >= 50:
        return ProyectoInversionSnapshotMensual.HEALTH_YELLOW
    return ProyectoInversionSnapshotMensual.HEALTH_RED


def _compute_health_score(
    project: ProyectoInversion,
    *,
    roi_cumulative: Decimal | None,
    free_cashflows: list[Decimal],
    sales_values: list[Decimal],
    actual_investment: Decimal,
    recovery_pct: Decimal | None,
    months_elapsed: int,
    calibration_settings: dict[str, object] | None = None,
) -> tuple[int, str, list[str]]:
    issues: list[str] = []
    settings = calibration_settings or _get_calibration_settings()
    weights_cfg = settings.get("health_weights", {})
    roi_weight = _as_decimal(weights_cfg.get("roi"))
    free_weight = _as_decimal(weights_cfg.get("free_cashflow"))
    growth_weight = _as_decimal(weights_cfg.get("sales_growth"))
    capex_weight = _as_decimal(weights_cfg.get("capex"))
    recovery_weight = _as_decimal(weights_cfg.get("recovery"))

    roi_target = _as_decimal(project.roi_objetivo)
    if roi_target > ZERO and roi_cumulative is not None:
        roi_component = min(max(roi_cumulative / roi_target, ZERO), Decimal("1")) * roi_weight
    else:
        roi_component = roi_weight / Decimal("2")
        issues.append("ROI objetivo no configurado; score usa ponderación neutral en rentabilidad.")

    recent_free = free_cashflows[-3:]
    if recent_free:
        positive_ratio = Decimal(sum(1 for value in recent_free if value > ZERO)) / Decimal(len(recent_free))
        free_component = positive_ratio * free_weight
    else:
        free_component = free_weight / Decimal("2")
        issues.append("Aún no hay suficientes meses con flujo libre para evaluar estabilidad.")

    recent_sales = sales_values[-3:]
    prior_sales = sales_values[-6:-3]
    if recent_sales and prior_sales and sum(prior_sales, ZERO) > ZERO:
        growth_ratio = max(
            (sum(recent_sales, ZERO) / Decimal(len(recent_sales)))
            / (sum(prior_sales, ZERO) / Decimal(len(prior_sales)))
            - Decimal("1"),
            Decimal("-1"),
        )
        growth_component = min(max((growth_ratio + Decimal("0.10")) / Decimal("0.20"), ZERO), Decimal("1")) * growth_weight
    else:
        growth_component = growth_weight / Decimal("2")
        issues.append("No hay histórico suficiente para medir crecimiento comparativo de ventas.")

    planned_investment = _as_decimal(project.monto_inversion_planeado)
    if planned_investment > ZERO:
        capex_ratio = actual_investment / planned_investment
        if capex_ratio <= Decimal("1"):
            capex_component = capex_weight
        elif capex_ratio <= Decimal("1.05"):
            capex_component = capex_weight * Decimal("0.80")
        elif capex_ratio <= Decimal("1.15"):
            capex_component = capex_weight * Decimal("0.50")
        elif capex_ratio <= Decimal("1.25"):
            capex_component = capex_weight * Decimal("0.20")
        else:
            capex_component = ZERO
    else:
        capex_component = capex_weight / Decimal("2")
        issues.append("Inversión planeada no configurada; score usa ponderación neutral en CAPEX.")

    payback_target = Decimal(project.payback_objetivo_meses or 0)
    if payback_target > ZERO and recovery_pct is not None:
        expected_progress = min((Decimal(months_elapsed) / payback_target) * Decimal("100"), Decimal("100"))
        if expected_progress <= ZERO:
            recovery_component = recovery_weight / Decimal("2")
        else:
            recovery_component = min(max(recovery_pct / expected_progress, ZERO), Decimal("1")) * recovery_weight
    elif recovery_pct is not None:
        recovery_component = min(max(recovery_pct / Decimal("100"), ZERO), Decimal("1")) * recovery_weight
        issues.append("Payback objetivo no configurado; score usa avance real sin benchmark objetivo.")
    else:
        recovery_component = recovery_weight / Decimal("2")

    score = int(
        _quantize(
            roi_component + free_component + growth_component + capex_component + recovery_component,
            pattern=Decimal("1"),
        )
        or 0
    )
    return score, _health_status_from_score(score), issues


def _forecast_payback_metrics(
    project: ProyectoInversion,
    snapshots_payload: list[dict[str, Decimal | None]],
    pending_balance: Decimal,
    *,
    calibration_settings: dict[str, object] | None = None,
) -> dict[str, object]:
    settings = calibration_settings or _get_calibration_settings()
    forecast_cfg = settings.get("forecast", {})
    preferred_window = int(forecast_cfg.get("preferred_window_months", 3) or 3)
    fallback_window = int(forecast_cfg.get("fallback_window_months", 6) or 6)
    negative_mode = str(forecast_cfg.get("negative_months_mode") or "fallback_to_fallback_window")
    noise_ratio_limit = _as_decimal(forecast_cfg.get("noise_ratio", FORECAST_NOISE_RATIO))

    if pending_balance <= ZERO:
        base_date = project.fecha_apertura or project.fecha_inicio
        return {
            "payback_months": ZERO,
            "estimated_date": base_date,
            "window_months": preferred_window,
            "average_cashflow": ZERO,
        }

    preferred_values = [
        _as_decimal(row["free_cashflow"])
        for row in snapshots_payload[-preferred_window:]
        if row["free_cashflow"] is not None
    ]
    fallback_values = [
        _as_decimal(row["free_cashflow"])
        for row in snapshots_payload[-fallback_window:]
        if row["free_cashflow"] is not None
    ]

    selected = preferred_values
    window_months = preferred_window
    if len(preferred_values) < preferred_window:
        selected = fallback_values
        window_months = fallback_window
    elif preferred_values:
        avg_preferred = _average_decimals(preferred_values)
        spread_ratio = ZERO
        if avg_preferred not in {None, ZERO}:
            spread_ratio = abs(max(preferred_values) - min(preferred_values)) / abs(avg_preferred)

        if negative_mode == "exclude":
            selected = [value for value in preferred_values if value > ZERO]
        elif negative_mode == "include":
            selected = preferred_values
        elif any(value <= ZERO for value in preferred_values) or spread_ratio > noise_ratio_limit:
            selected = fallback_values
            window_months = fallback_window

    average_cashflow = _average_decimals(selected)
    if average_cashflow is None or average_cashflow <= ZERO:
        return {
            "payback_months": None,
            "estimated_date": None,
            "window_months": window_months,
            "average_cashflow": average_cashflow,
        }

    payback_months = _quantize(pending_balance / average_cashflow)
    if payback_months is None:
        return {
            "payback_months": None,
            "estimated_date": None,
            "window_months": window_months,
            "average_cashflow": average_cashflow,
        }

    latest_period = snapshots_payload[-1]["period"] if snapshots_payload else _month_start(project.fecha_apertura or project.fecha_inicio)
    months_to_close = int(payback_months.to_integral_value(rounding=ROUND_HALF_UP))
    estimated_date = _month_start(_add_months(latest_period, max(months_to_close, 0)))
    return {
        "payback_months": payback_months,
        "estimated_date": estimated_date,
        "window_months": window_months,
        "average_cashflow": _quantize(average_cashflow),
    }


def _npv_from_cashflows(cashflows: list[Decimal], annual_discount_rate: Decimal) -> Decimal | None:
    if not cashflows:
        return None
    rate_ratio = _ratio_from_percent_like(annual_discount_rate)
    if rate_ratio <= ZERO:
        return None
    monthly_rate = rate_ratio / Decimal("12")
    total = ZERO
    for index, cashflow in enumerate(cashflows):
        total += cashflow / ((Decimal("1") + monthly_rate) ** Decimal(index))
    return _quantize(total)


def _irr_from_cashflows(cashflows: list[Decimal]) -> Decimal | None:
    if len(cashflows) < 2:
        return None
    if not any(value < ZERO for value in cashflows) or not any(value > ZERO for value in cashflows):
        return None

    low = Decimal("-0.99")
    high = Decimal("10")
    for _ in range(80):
        mid = (low + high) / Decimal("2")
        npv = ZERO
        for index, cashflow in enumerate(cashflows):
            try:
                npv += cashflow / ((Decimal("1") + mid) ** Decimal(index))
            except Exception:
                return None
        if abs(npv) <= Decimal("0.0001"):
            return _quantize(mid * Decimal("1200"), FOUR_PLACES)
        if npv > ZERO:
            low = mid
        else:
            high = mid
    return _quantize(((low + high) / Decimal("2")) * Decimal("1200"), FOUR_PLACES)


def _annuity_payment(principal: Decimal, annual_rate: Decimal, months: int) -> Decimal:
    if principal <= ZERO or months <= 0:
        return ZERO
    monthly_rate = _ratio_from_percent_like(annual_rate) / Decimal("12")
    if monthly_rate == ZERO:
        return _quantize(principal / Decimal(months))
    numerator = principal * monthly_rate
    denominator = Decimal("1") - (Decimal("1") + monthly_rate) ** Decimal(-months)
    if denominator == ZERO:
        return ZERO
    return _quantize(numerator / denominator)


@dataclass
class ProjectRefreshResult:
    project_id: int
    snapshots_updated: int
    latest_period: date | None
    project_status: str
    data_gaps: list[str]


class ProyectoInversionRefreshService:
    def _branch_center(self, project: ProyectoInversion) -> CentroCosto | None:
        if not project.sucursal_relacionada_id:
            return None
        return (
            CentroCosto.objects.filter(
                sucursal_id=project.sucursal_relacionada_id,
                tipo=CentroCosto.TIPO_SUCURSAL,
            )
            .order_by("id")
            .first()
        )

    def _monthly_sales(self, project: ProyectoInversion, month_start: date, month_finish: date) -> dict[str, object]:
        issues: list[str] = []
        if not project.sucursal_relacionada_id:
            issues.append("Proyecto sin sucursal relacionada para ventas.")
            return {
                "sales_total": ZERO,
                "cogs_total": None,
                "source": "missing_branch",
                "issues": issues,
            }

        facts_qs = FactVentaDiaria.objects.filter(
            sucursal_id=project.sucursal_relacionada_id,
            fecha__gte=month_start,
            fecha__lte=month_finish,
        )
        if facts_qs.exists():
            agg = facts_qs.aggregate(
                sales_total=Sum("venta_total"),
                cogs_total=Sum("costo_estimado"),
            )
            sales_agg = _as_decimal(agg.get("sales_total"))
            cogs_agg = _as_decimal(agg.get("cogs_total"))
            # Guardrail: costo histórico contaminado cuando COGS > 200% de ventas.
            # Esto ocurre cuando ProductoCostoOperativoMensual tiene costos de un mes
            # diferente al período analizado (p.ej. apertura con muy pocas ventas
            # pero costos completos aplicados). Se descarta el costo y se agrega gap.
            if sales_agg > ZERO and cogs_agg > ZERO and (cogs_agg / sales_agg) > _COGS_RATIO_THRESHOLD:
                issues.append(
                    f"Costo de venta ({cogs_agg:.2f}) excede {_COGS_RATIO_THRESHOLD * 100:.0f}% "
                    f"de ventas ({sales_agg:.2f}); costo histórico no confiable para este periodo — "
                    "se omite para evitar distorsión en flujo operativo."
                )
                cogs_agg = None
            return {
                "sales_total": sales_agg,
                "cogs_total": cogs_agg,
                "source": "fact_venta_diaria",
                "issues": issues,
            }

        fallback_qs = VentaAutoritativaPoint.objects.filter(
            branch_id=project.sucursal_relacionada_id,
            sale_date__gte=month_start,
            sale_date__lte=month_finish,
        )
        if fallback_qs.exists():
            agg = fallback_qs.aggregate(sales_total=Sum("total_amount"))
            issues.append("Costo de venta mensual pendiente: sólo existe venta autoritativa Point.")
            return {
                "sales_total": _as_decimal(agg.get("sales_total")),
                "cogs_total": None,
                "source": "venta_autoritativa_point",
                "issues": issues,
            }

        point_qs = PointDailySale.objects.filter(
            branch__erp_branch_id=project.sucursal_relacionada_id,
            sale_date__gte=month_start,
            sale_date__lte=month_finish,
        )
        if point_qs.exists():
            agg = point_qs.aggregate(sales_total=Sum("total_amount"))
            issues.append("Costo de venta mensual pendiente: usando Point diario como respaldo.")
            return {
                "sales_total": _as_decimal(agg.get("sales_total")),
                "cogs_total": None,
                "source": "point_daily_sale",
                "issues": issues,
            }

        return {
            "sales_total": ZERO,
            "cogs_total": None,
            "source": "no_sales",
            "issues": issues,
        }

    def _monthly_expenses(self, project: ProyectoInversion, month_start: date) -> dict[str, Decimal | list[str] | str]:
        issues: list[str] = []
        center = self._branch_center(project)
        if center is None:
            issues.append("Sucursal sin centro de costo sucursal en reportes.")
            return {
                "total": ZERO,
                "payroll": ZERO,
                "rent": ZERO,
                "services": ZERO,
                "marketing": ZERO,
                "other": ZERO,
                "coverage_status": EXPENSE_COVERAGE_MISSING,
                "coverage_label": "Faltante",
                "row_count": 0,
                "category_codes": [],
                "source": "missing_cost_center",
                "issues": issues,
            }

        expense_qs = GastoOperativoMensual.objects.filter(
            periodo=month_start,
            centro_costo=center,
            tipo_dato=GastoOperativoMensual.TIPO_DATO_REAL,
            es_estimado=False,
        ).select_related("categoria_gasto")
        if not expense_qs.exists():
            issues.append("Falta gasto operativo real sucursal para este periodo.")
            return {
                "total": ZERO,
                "payroll": ZERO,
                "rent": ZERO,
                "services": ZERO,
                "marketing": ZERO,
                "other": ZERO,
                "coverage_status": EXPENSE_COVERAGE_MISSING,
                "coverage_label": "Faltante",
                "row_count": 0,
                "category_codes": [],
                "source": EXPENSE_SOURCE_MISSING,
                "issues": issues,
            }

        payroll = ZERO
        rent = ZERO
        services = ZERO
        marketing = ZERO
        other = ZERO
        detail_total = ZERO
        total_only_amount = ZERO
        total_only_count = 0
        seen_codes: set[str] = set()
        for row in expense_qs:
            amount = _as_decimal(row.monto)
            code = (row.categoria_gasto.codigo or "").strip().upper() if row.categoria_gasto_id else ""
            seen_codes.add(code)
            if code in TOTAL_BRANCH_EXPENSE_CATEGORY_CODES:
                total_only_amount += amount
                total_only_count += 1
                continue
            detail_total += amount
            if code == "NOMINA_SUC":
                payroll += amount
            elif code == "RENTA_SUC":
                rent += amount
            elif code in SERVICE_CATEGORY_CODES:
                services += amount
            elif code in MARKETING_CATEGORY_CODES:
                marketing += amount
            else:
                other += amount

        total = detail_total
        coverage_status = EXPENSE_COVERAGE_COMPLETE
        coverage_label = "Completo"
        source = EXPENSE_SOURCE_COMPLETE
        if detail_total > ZERO and total_only_amount > ZERO:
            issues.append("Se detectó gasto total agregado y detalle real en el mismo mes; se usa sólo el detalle.")
            coverage_status = EXPENSE_COVERAGE_PARTIAL
            coverage_label = "Parcial"
            source = EXPENSE_SOURCE_PARTIAL
        elif detail_total <= ZERO and total_only_amount > ZERO:
            total = total_only_amount
            other = total_only_amount
            issues.append("Gasto real sucursal cargado sólo como total agregado; falta desglose por categoría.")
            coverage_status = EXPENSE_COVERAGE_PARTIAL
            coverage_label = "Parcial"
            source = EXPENSE_SOURCE_TOTAL_ONLY
        if total > ZERO and "NOMINA_SUC" not in seen_codes and source != EXPENSE_SOURCE_TOTAL_ONLY:
            issues.append("Nómina sucursal no clasificada en gasto mensual para este periodo.")
            coverage_status = EXPENSE_COVERAGE_PARTIAL
            coverage_label = "Parcial"
            source = EXPENSE_SOURCE_PARTIAL
        return {
            "total": _quantize(total) or ZERO,
            "payroll": _quantize(payroll) or ZERO,
            "rent": _quantize(rent) or ZERO,
            "services": _quantize(services) or ZERO,
            "marketing": _quantize(marketing) or ZERO,
            "other": _quantize(other) or ZERO,
            "coverage_status": coverage_status,
            "coverage_label": coverage_label,
            "row_count": expense_qs.count() - total_only_count if detail_total > ZERO else expense_qs.count(),
            "category_codes": sorted(seen_codes),
            "source": source,
            "issues": issues,
        }

    def _group_debt_payments(self, project: ProyectoInversion) -> dict[date, dict[str, Decimal]]:
        grouped: dict[date, dict[str, Decimal]] = {}
        for row in project.pagos_deuda.all().order_by("fecha_pago", "id"):
            key = _month_start(row.fecha_pago)
            bucket = grouped.setdefault(
                key,
                {
                    "payment": ZERO,
                    "interest": ZERO,
                    "principal": ZERO,
                    "balance": ZERO,
                },
            )
            bucket["payment"] += _as_decimal(row.monto_pago)
            bucket["interest"] += _as_decimal(row.interes_pagado)
            bucket["principal"] += _as_decimal(row.capital_amortizado)
            bucket["balance"] = _as_decimal(row.saldo_insoluto)
        return grouped

    def _monthly_debt(
        self,
        project: ProyectoInversion,
        month_start: date,
        *,
        month_index: int,
        previous_balance: Decimal,
        actual_payments: dict[date, dict[str, Decimal]],
    ) -> dict[str, Decimal | str]:
        if project.deuda_asociada <= ZERO and previous_balance <= ZERO:
            return {
                "service": ZERO,
                "interest": ZERO,
                "principal": ZERO,
                "balance": ZERO,
                "source": "no_debt",
            }

        actual = actual_payments.get(month_start)
        if actual is not None:
            balance = _as_decimal(actual.get("balance"))
            if balance <= ZERO and previous_balance > ZERO and _as_decimal(actual.get("principal")) <= previous_balance:
                balance = max(previous_balance - _as_decimal(actual.get("principal")), ZERO)
            return {
                "service": _quantize(_as_decimal(actual.get("payment"))) or ZERO,
                "interest": _quantize(_as_decimal(actual.get("interest"))) or ZERO,
                "principal": _quantize(_as_decimal(actual.get("principal"))) or ZERO,
                "balance": _quantize(balance) or ZERO,
                "source": "actual",
            }

        if project.plazo_deuda_meses <= 0 or month_index >= project.plazo_deuda_meses or previous_balance <= ZERO:
            return {
                "service": ZERO,
                "interest": ZERO,
                "principal": ZERO,
                "balance": _quantize(previous_balance) or ZERO,
                "source": "estimated_outside_term",
            }

        monthly_rate = _ratio_from_percent_like(project.tasa_interes_anual) / Decimal("12")
        payment = _as_decimal(project.pago_mensual_deuda_estimado) or _annuity_payment(
            _as_decimal(project.deuda_asociada),
            _as_decimal(project.tasa_interes_anual),
            project.plazo_deuda_meses,
        )
        interest = _quantize(previous_balance * monthly_rate) or ZERO
        principal = max(payment - interest, ZERO)
        balance = max(previous_balance - principal, ZERO)
        if balance < TWO_PLACES:
            principal = previous_balance
            balance = ZERO
            payment = principal + interest
        return {
            "service": _quantize(payment) or ZERO,
            "interest": interest,
            "principal": _quantize(principal) or ZERO,
            "balance": _quantize(balance) or ZERO,
            "source": "estimated",
        }

    def _recovery_amount(
        self,
        project: ProyectoInversion,
        *,
        free_cashflow: Decimal | None,
    ) -> Decimal | None:
        if free_cashflow is None:
            return None
        strategy = project.recovery_strategy
        percentage = _ratio_from_percent_like(
            project.recovery_percentage or project.porcentaje_utilidad_destinado_a_recuperacion or Decimal("1")
        )
        effective_percentage = Decimal("1")
        if strategy == ProyectoInversion.RECOVERY_PERCENTAGE_OF_PROFIT:
            effective_percentage = percentage
        return _quantize(max(free_cashflow, ZERO) * effective_percentage)

    def _projected_payback(
        self,
        investment_real: Decimal,
        recovery_total: Decimal,
        positive_recovery_months: int,
    ) -> Decimal | None:
        if investment_real <= ZERO or recovery_total <= ZERO or positive_recovery_months <= 0:
            return None
        avg_recovery = recovery_total / Decimal(positive_recovery_months)
        if avg_recovery <= ZERO:
            return None
        return _quantize(investment_real / avg_recovery)

    def _cash_on_cash(self, project: ProyectoInversion, free_cashflows: list[Decimal]) -> Decimal | None:
        capital = _as_decimal(project.capital_inicial_aportado)
        if capital <= ZERO or not free_cashflows:
            return None
        trailing = free_cashflows[-12:]
        annual_cashflow = sum(trailing, ZERO)
        if len(trailing) < 12:
            average = _average_decimals(trailing)
            if average is None:
                return None
            annual_cashflow = average * Decimal("12")
        return _percent_value(annual_cashflow, capital)

    def _forecast_payback(
        self,
        project: ProyectoInversion,
        snapshots_payload: list[dict[str, Decimal | None]],
        pending_balance: Decimal,
    ) -> dict[str, object]:
        return _forecast_payback_metrics(project, snapshots_payload, pending_balance)

    def _health_score(
        self,
        project: ProyectoInversion,
        *,
        roi_cumulative: Decimal | None,
        free_cashflows: list[Decimal],
        sales_values: list[Decimal],
        actual_investment: Decimal,
        recovery_pct: Decimal | None,
        months_elapsed: int,
    ) -> tuple[int, str, list[str]]:
        return _compute_health_score(
            project,
            roi_cumulative=roi_cumulative,
            free_cashflows=free_cashflows,
            sales_values=sales_values,
            actual_investment=actual_investment,
            recovery_pct=recovery_pct,
            months_elapsed=months_elapsed,
        )

    def _scenario_rows(self, project: ProyectoInversion) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for scenario in project.escenarios.all().order_by("tipo_escenario", "nombre"):
            rows.append(ProyectoInversionScenarioService().compute(project, scenario))
        return rows

    def _close_project_if_needed(
        self,
        project: ProyectoInversion,
        latest_snapshot: ProyectoInversionSnapshotMensual | None,
        user=None,
    ) -> None:
        if latest_snapshot is None or not project.auto_cierre_habilitado:
            return
        if project.estatus in {ProyectoInversion.ESTATUS_CERRADO, ProyectoInversion.ESTATUS_CANCELADO}:
            return
        conditions: list[bool] = []
        if project.cierre_por_recuperacion_total:
            conditions.append((_as_decimal(latest_snapshot.porcentaje_recuperado) >= Decimal("100")))
        if project.cierre_por_liquidacion_deuda:
            conditions.append((_as_decimal(latest_snapshot.saldo_insoluto) <= ZERO))
        if project.cierre_por_roi_minimo:
            conditions.append((_as_decimal(latest_snapshot.roi_acumulado) >= _as_decimal(project.roi_minimo_cierre)))
        if not conditions or not all(conditions):
            return
        project.estatus = ProyectoInversion.ESTATUS_CERRADO
        project.fecha_cierre = latest_snapshot.periodo_fin
        project.kpis_cierre = {
            "periodo": latest_snapshot.periodo.isoformat(),
            "ventas_mensuales": str(latest_snapshot.ventas_mensuales),
            "recuperacion_acumulada": str(latest_snapshot.recuperacion_acumulada or ZERO),
            "porcentaje_recuperado": str(latest_snapshot.porcentaje_recuperado or ZERO),
            "roi_acumulado": str(latest_snapshot.roi_acumulado or ZERO),
            "saldo_insoluto": str(latest_snapshot.saldo_insoluto or ZERO),
        }
        project.save(update_fields=["estatus", "fecha_cierre", "kpis_cierre", "actualizado_en"])
        log_event(
            user,
            "AUTO_CLOSE",
            "reportes.ProyectoInversion",
            project.pk,
            payload=project.kpis_cierre,
        )

    @transaction.atomic
    def refresh_project(
        self,
        project: ProyectoInversion,
        *,
        until: date | None = None,
        user=None,
    ) -> ProjectRefreshResult:
        reference_day = until or timezone.localdate()
        start_day = project.fecha_inicio
        month_starts = _iter_months(start_day, reference_day)
        actual_debt = self._group_debt_payments(project)
        investment_real = _as_decimal(
            project.gastos_inversion.aggregate(total=Sum("monto_total")).get("total")
        )
        previous_status = project.estatus
        if project.monto_inversion_real != investment_real:
            project.monto_inversion_real = investment_real

        cumulative_recovery = ZERO
        benefit_cumulative = ZERO
        positive_recovery_months = 0
        payback_real: Decimal | None = None
        debt_balance = _as_decimal(project.deuda_asociada)
        snapshots_updated = 0
        all_data_gaps: list[str] = []
        latest_snapshot: ProyectoInversionSnapshotMensual | None = None
        cashflow_history: list[Decimal] = []
        sales_history: list[Decimal] = []
        snapshot_series: list[dict[str, Decimal | None | date]] = []

        for month_index, period in enumerate(month_starts):
            period_end = min(_month_end(period), reference_day)
            sales_payload = self._monthly_sales(project, period, period_end)
            expense_payload = self._monthly_expenses(project, period)
            debt_payload = self._monthly_debt(
                project,
                period,
                month_index=month_index,
                previous_balance=debt_balance,
                actual_payments=actual_debt,
            )
            debt_balance = _as_decimal(debt_payload["balance"])

            sales_total = _as_decimal(sales_payload["sales_total"])
            cogs_total = sales_payload["cogs_total"]
            gross_profit = None if cogs_total is None else sales_total - _as_decimal(cogs_total)
            total_expenses = _as_decimal(expense_payload["total"])
            operating_profit = None if gross_profit is None else gross_profit - total_expenses
            debt_service = _quantize(_as_decimal(debt_payload["principal"]) + _as_decimal(debt_payload["interest"])) or ZERO
            operating_cashflow = _quantize(operating_profit)
            free_cashflow = None if operating_profit is None else operating_profit - debt_service
            recovery_amount = self._recovery_amount(
                project,
                free_cashflow=free_cashflow,
            )
            if recovery_amount is not None:
                cumulative_recovery += recovery_amount
                if recovery_amount > ZERO:
                    positive_recovery_months += 1
            if free_cashflow is not None:
                benefit_cumulative += free_cashflow
                cashflow_history.append(_quantize(free_cashflow) or ZERO)
            if sales_total > ZERO:
                sales_history.append(_quantize(sales_total) or ZERO)
            pending_balance = max(investment_real - cumulative_recovery, ZERO)
            recovery_pct = _percent_value(cumulative_recovery, investment_real)
            roi_monthly = _percent_value(free_cashflow, investment_real)
            roi_cumulative = _percent_value(benefit_cumulative, investment_real)
            months_elapsed = Decimal(month_index + 1)
            roi_annualized = None
            if roi_cumulative is not None and months_elapsed > ZERO:
                roi_annualized = _quantize((roi_cumulative * Decimal("12")) / months_elapsed, FOUR_PLACES)
            projected_payback = self._projected_payback(investment_real, cumulative_recovery, positive_recovery_months)
            estimated_month = None
            if projected_payback is not None:
                projected_whole = int(projected_payback.to_integral_value(rounding=ROUND_HALF_UP))
                estimated_month = _month_start(_add_months(project.fecha_apertura or project.fecha_inicio, max(projected_whole - 1, 0)))
            if payback_real is None and recovery_pct is not None and recovery_pct >= Decimal("100"):
                payback_real = _quantize(months_elapsed)

            data_gaps = list(sales_payload["issues"]) + list(expense_payload["issues"])
            if cogs_total is None and sales_total > ZERO:
                data_gaps.append("Costo de venta mensual pendiente para cálculo operativo completo.")
            if _as_decimal(project.deuda_asociada) > ZERO and debt_payload["source"] == "estimated":
                data_gaps.append("Servicio de deuda estimado; falta captura de pagos reales.")
            is_pre_opening_period = bool(project.fecha_apertura and period < _month_start(project.fecha_apertura))
            if is_pre_opening_period:
                data_gaps.append("Periodo previo a apertura; operación aún no aplica.")
            data_source, confidence_score = _derive_data_quality(
                sales_source=str(sales_payload["source"]),
                expense_source=str(expense_payload["source"]),
                debt_source=str(debt_payload["source"]),
                is_pre_opening=is_pre_opening_period,
            )

            snapshot_series.append(
                {
                    "period": period,
                    "free_cashflow": _quantize(free_cashflow),
                    "sales": _quantize(sales_total),
                }
            )
            forecast = self._forecast_payback(project, snapshot_series, pending_balance)
            cash_on_cash = self._cash_on_cash(project, cashflow_history)
            health_score, health_status, health_issues = self._health_score(
                project,
                roi_cumulative=roi_cumulative,
                free_cashflows=cashflow_history,
                sales_values=sales_history,
                actual_investment=investment_real,
                recovery_pct=recovery_pct,
                months_elapsed=month_index + 1,
            )

            valuation_cashflows = [investment_real * Decimal("-1")] + cashflow_history
            van = None
            tir = None
            if _as_decimal(project.discount_rate) > ZERO:
                van = _npv_from_cashflows(valuation_cashflows, _as_decimal(project.discount_rate))
                tir = _irr_from_cashflows(valuation_cashflows)
            else:
                data_gaps.append("Discount rate no configurada; VAN/TIR no disponibles.")

            data_gaps.extend(health_issues)
            all_data_gaps.extend(data_gaps)
            logger.info(
                "Snapshot proyecto=%s periodo=%s ventas=%s flujo_operativo=%s servicio_deuda=%s flujo_libre=%s recovery=%s fuente=%s confianza=%s",
                project.pk,
                period.isoformat(),
                sales_total,
                operating_cashflow,
                debt_service,
                _quantize(free_cashflow),
                _quantize(recovery_amount),
                data_source,
                confidence_score,
            )

            snapshot_defaults = {
                "periodo_fin": period_end,
                "es_parcial": period_end < _month_end(period),
                "ventas_mensuales": _quantize(sales_total) or ZERO,
                "costo_venta_mensual": _quantize(_as_decimal(cogs_total)) if cogs_total is not None else None,
                "utilidad_bruta": _quantize(gross_profit),
                "gastos_operativos": _quantize(total_expenses),
                "nomina": _quantize(_as_decimal(expense_payload["payroll"])),
                "renta": _quantize(_as_decimal(expense_payload["rent"])),
                "servicios": _quantize(_as_decimal(expense_payload["services"])),
                "marketing": _quantize(_as_decimal(expense_payload["marketing"])),
                "otros_gastos": _quantize(_as_decimal(expense_payload["other"])),
                "utilidad_operativa": _quantize(operating_profit),
                "flujo_operativo": operating_cashflow,
                "servicio_deuda": debt_service,
                "interes_pagado": _quantize(_as_decimal(debt_payload["interest"])),
                "capital_amortizado": _quantize(_as_decimal(debt_payload["principal"])),
                "saldo_insoluto": _quantize(_as_decimal(debt_payload["balance"])),
                "flujo_libre": _quantize(free_cashflow),
                "flujo_para_recuperacion": _quantize(recovery_amount),
                "flujo_neto": _quantize(free_cashflow),
                "monto_recuperacion_mes": _quantize(recovery_amount),
                "recuperacion_acumulada": _quantize(cumulative_recovery),
                "saldo_pendiente": _quantize(pending_balance),
                "porcentaje_recuperado": _clamp_pct(recovery_pct),
                "cash_on_cash": _clamp_pct(cash_on_cash),
                "roi_mensual": _clamp_pct(roi_monthly),
                "roi_acumulado": _clamp_pct(roi_cumulative),
                "roi_anualizado": _clamp_pct(roi_annualized),
                "payback_real_meses": payback_real,
                "payback_proyectado_meses": projected_payback,
                "payback_forecast_meses": forecast["payback_months"],
                "mes_estimado_recuperacion": estimated_month,
                "fecha_estimada_recuperacion_forecast": forecast["estimated_date"],
                "confidence_score": confidence_score,
                "data_source": data_source,
                "health_score": health_score,
                "health_status": health_status,
                "van": van,
                "tir": _clamp_pct(tir),
                "fuentes": {
                    "ventas_source": sales_payload["source"],
                    "gastos_source": expense_payload["source"],
                    "expense_coverage_status": expense_payload["coverage_status"],
                    "expense_coverage_label": expense_payload["coverage_label"],
                    "expense_row_count": int(expense_payload["row_count"] or 0),
                    "expense_category_codes": list(expense_payload["category_codes"] or []),
                    "deuda_source": debt_payload["source"],
                    "forecast_window_months": forecast["window_months"],
                    "forecast_average_free_cashflow": str(forecast["average_cashflow"] or ZERO),
                    "data_gaps": data_gaps,
                },
                "calculado_en": timezone.now(),
            }
            snapshot, created = ProyectoInversionSnapshotMensual.objects.update_or_create(
                proyecto=project,
                periodo=period,
                defaults=snapshot_defaults,
            )
            latest_snapshot = snapshot
            snapshots_updated += 1 if created else 0

        if project.estatus not in {ProyectoInversion.ESTATUS_CERRADO, ProyectoInversion.ESTATUS_CANCELADO}:
            if project.fecha_apertura and project.fecha_apertura <= reference_day:
                project.estatus = (
                    ProyectoInversion.ESTATUS_EN_RECUPERACION
                    if cumulative_recovery < investment_real
                    else ProyectoInversion.ESTATUS_ACTIVO
                )
            elif investment_real > ZERO:
                project.estatus = ProyectoInversion.ESTATUS_EJECUCION
            else:
                project.estatus = ProyectoInversion.ESTATUS_PLANEACION
        project.save(update_fields=["monto_inversion_real", "estatus", "actualizado_en"])
        if previous_status != project.estatus:
            logger.info("Proyecto %s cambió de estatus %s -> %s", project.pk, previous_status, project.estatus)
        self._close_project_if_needed(project, latest_snapshot, user=user)
        ProyectoInversionAlertService().sync_alerts(project, latest_snapshot, user=user)

        if latest_snapshot is not None:
            log_event(
                user,
                "REFRESH",
                "reportes.ProyectoInversion",
                project.pk,
                payload={
                    "latest_period": latest_snapshot.periodo.isoformat(),
                    "estatus": project.estatus,
                    "data_gaps": list(dict.fromkeys(all_data_gaps)),
                },
            )
        logger.info(
            "Refresh proyecto=%s latest_period=%s estatus=%s snapshots=%s",
            project.pk,
            latest_snapshot.periodo.isoformat() if latest_snapshot else None,
            project.estatus,
            len(month_starts),
        )
        return ProjectRefreshResult(
            project_id=project.pk,
            snapshots_updated=snapshots_updated,
            latest_period=latest_snapshot.periodo if latest_snapshot else None,
            project_status=project.estatus,
            data_gaps=list(dict.fromkeys(all_data_gaps)),
        )


class ProyectoInversionAlertService:
    def sync_alerts(
        self,
        project: ProyectoInversion,
        latest_snapshot: ProyectoInversionSnapshotMensual | None,
        *,
        user=None,
    ) -> list[ProyectoInversionAlerta]:
        if latest_snapshot is None:
            return []

        snapshots = list(project.snapshots_mensuales.order_by("periodo"))
        active_codes: set[str] = set()
        generated: list[ProyectoInversionAlerta] = []
        months_elapsed = len(snapshots)

        payback_target = Decimal(project.payback_objetivo_meses or 0)
        forecast_payback = _as_decimal(latest_snapshot.payback_forecast_meses)
        if payback_target > ZERO and forecast_payback > ZERO and forecast_payback > (payback_target * Decimal("1.2")):
            generated.append(
                self._upsert_alert(
                    project,
                    latest_snapshot,
                    codigo=ProyectoInversionAlerta.CODIGO_PAYBACK_RISK,
                    severidad=ProyectoInversionAlerta.SEVERITY_WARNING,
                    titulo="Payback en riesgo",
                    mensaje=(
                        f"El payback forecast ({forecast_payback:.2f} meses) excede en más de 20% "
                        f"el objetivo del proyecto ({payback_target:.0f} meses)."
                    ),
                    payload={
                        "payback_forecast_meses": str(forecast_payback),
                        "payback_objetivo_meses": str(payback_target),
                    },
                )
            )
            active_codes.add(ProyectoInversionAlerta.CODIGO_PAYBACK_RISK)

        roi_target = _as_decimal(project.roi_objetivo)
        roi_value = _as_decimal(latest_snapshot.roi_acumulado)
        if months_elapsed >= ROI_ALERT_MIN_MONTHS and roi_target > ZERO and roi_value < roi_target:
            generated.append(
                self._upsert_alert(
                    project,
                    latest_snapshot,
                    codigo=ProyectoInversionAlerta.CODIGO_LOW_ROI,
                    severidad=ProyectoInversionAlerta.SEVERITY_WARNING,
                    titulo="ROI bajo versus objetivo",
                    mensaje=(
                        f"El ROI acumulado ({roi_value:.2f}%) está por debajo del objetivo "
                        f"({roi_target:.2f}%) después de {months_elapsed} meses."
                    ),
                    payload={
                        "roi_acumulado": str(roi_value),
                        "roi_objetivo": str(roi_target),
                        "months_elapsed": months_elapsed,
                    },
                )
            )
            active_codes.add(ProyectoInversionAlerta.CODIGO_LOW_ROI)

        recent_two = snapshots[-2:]
        if len(recent_two) == 2 and all(_as_decimal(item.flujo_libre) < ZERO for item in recent_two):
            generated.append(
                self._upsert_alert(
                    project,
                    latest_snapshot,
                    codigo=ProyectoInversionAlerta.CODIGO_NEGATIVE_FREE_CASHFLOW,
                    severidad=ProyectoInversionAlerta.SEVERITY_CRITICAL,
                    titulo="Flujo libre negativo consecutivo",
                    mensaje="El proyecto acumula dos meses consecutivos con flujo libre negativo.",
                    payload={
                        "periods": [item.periodo.isoformat() for item in recent_two],
                        "values": [str(_as_decimal(item.flujo_libre)) for item in recent_two],
                    },
                )
            )
            active_codes.add(ProyectoInversionAlerta.CODIGO_NEGATIVE_FREE_CASHFLOW)

        planned_investment = _as_decimal(project.monto_inversion_planeado)
        actual_investment = _as_decimal(project.monto_inversion_real)
        if planned_investment > ZERO and actual_investment > (planned_investment * Decimal("1.15")):
            generated.append(
                self._upsert_alert(
                    project,
                    latest_snapshot,
                    codigo=ProyectoInversionAlerta.CODIGO_CAPEX_OVERRUN,
                    severidad=ProyectoInversionAlerta.SEVERITY_CRITICAL,
                    titulo="Sobreinversión CAPEX",
                    mensaje=(
                        f"La inversión real ({actual_investment:.2f}) supera en más de 15% "
                        f"la inversión planeada ({planned_investment:.2f})."
                    ),
                    payload={
                        "actual_investment": str(actual_investment),
                        "planned_investment": str(planned_investment),
                    },
                )
            )
            active_codes.add(ProyectoInversionAlerta.CODIGO_CAPEX_OVERRUN)

        stale_alerts = project.alertas.filter(activa=True).exclude(codigo__in=active_codes)
        if stale_alerts.exists():
            now = timezone.now()
            stale_alerts.update(activa=False, resolved_at=now, last_detected_at=now)

        if generated:
            logger.warning(
                "Proyecto %s generó %s alertas activas: %s",
                project.pk,
                len(generated),
                ", ".join(alert.codigo for alert in generated),
            )
            if user is not None:
                log_event(
                    user,
                    "ALERT_SYNC",
                    "reportes.ProyectoInversion",
                    project.pk,
                    payload={"active_alert_codes": sorted(active_codes)},
                )
        return generated

    def _upsert_alert(
        self,
        project: ProyectoInversion,
        snapshot: ProyectoInversionSnapshotMensual,
        *,
        codigo: str,
        severidad: str,
        titulo: str,
        mensaje: str,
        payload: dict[str, object],
    ) -> ProyectoInversionAlerta:
        alert, created = ProyectoInversionAlerta.objects.get_or_create(
            proyecto=project,
            codigo=codigo,
            activa=True,
            defaults={
                "snapshot": snapshot,
                "severidad": severidad,
                "titulo": titulo,
                "mensaje": mensaje,
                "payload": _json_safe(payload),
            },
        )
        if not created:
            alert.snapshot = snapshot
            alert.severidad = severidad
            alert.titulo = titulo
            alert.mensaje = mensaje
            alert.payload = _json_safe(payload)
            alert.last_detected_at = timezone.now()
            alert.resolved_at = None
            alert.save(
                update_fields=[
                    "snapshot",
                    "severidad",
                    "titulo",
                    "mensaje",
                    "payload",
                    "last_detected_at",
                    "resolved_at",
                ]
            )
        return alert


class ProyectoInversionScenarioService:
    @staticmethod
    def _scenario_recovery_amount(
        *,
        strategy: str,
        percentage: Decimal,
        free_cashflow: Decimal,
    ) -> Decimal:
        effective_percentage = Decimal("1")
        if strategy == ProyectoInversion.RECOVERY_PERCENTAGE_OF_PROFIT:
            effective_percentage = percentage
        return _quantize(max(free_cashflow, ZERO) * effective_percentage) or ZERO

    def compute(self, project: ProyectoInversion, scenario: ProyectoInversionEscenario) -> dict[str, object]:
        avg_sales = _as_decimal(scenario.ventas_promedio_mensuales)
        gross_margin_ratio = _ratio_from_percent_like(scenario.margen_bruto_pct)
        monthly_growth = _ratio_from_percent_like(scenario.crecimiento_mensual_pct)
        monthly_gross_profit = avg_sales * gross_margin_ratio
        monthly_operating_profit = monthly_gross_profit - _as_decimal(scenario.gastos_operativos_mensuales)
        monthly_debt_service = _as_decimal(project.pago_mensual_deuda_estimado) or _annuity_payment(
            _as_decimal(project.deuda_asociada),
            _as_decimal(project.tasa_interes_anual),
            int(project.plazo_deuda_meses or 0),
        )
        strategy = scenario.recovery_strategy_override or project.recovery_strategy
        percentage = scenario.recovery_percentage_override
        if percentage is None:
            percentage = project.recovery_percentage or project.porcentaje_utilidad_destinado_a_recuperacion
        percentage_ratio = _ratio_from_percent_like(percentage)
        monthly_net_cashflow = monthly_operating_profit - monthly_debt_service
        recovery_amount = self._scenario_recovery_amount(
            strategy=strategy,
            percentage=percentage_ratio,
            free_cashflow=monthly_net_cashflow,
        )
        investment_real = _as_decimal(project.monto_inversion_real or project.monto_inversion_planeado)
        payback_months = None
        if recovery_amount and recovery_amount > ZERO and investment_real > ZERO:
            payback_months = _quantize(investment_real / recovery_amount)
        annual_roi = _percent_value(monthly_net_cashflow * Decimal("12"), investment_real)
        annual_cash_on_cash = None
        if _as_decimal(project.capital_inicial_aportado) > ZERO:
            annual_cash_on_cash = _percent_value(monthly_net_cashflow * Decimal("12"), _as_decimal(project.capital_inicial_aportado))
        projected_close_date = None
        if payback_months is not None:
            months_to_close = int(payback_months.to_integral_value(rounding=ROUND_HALF_UP))
            projected_close_date = _add_months(project.fecha_apertura or timezone.localdate(), max(months_to_close - 1, 0))

        projection_rows: list[dict[str, object]] = []
        projected_sales = avg_sales
        projected_recovery = ZERO
        for index in range(max(int(scenario.horizonte_meses or 0), 1)):
            projected_sales = projected_sales if index == 0 else projected_sales * (Decimal("1") + monthly_growth)
            projected_gross = projected_sales * gross_margin_ratio
            projected_operating = projected_gross - _as_decimal(scenario.gastos_operativos_mensuales)
            projected_net = projected_operating - monthly_debt_service
            projected_month_recovery = self._scenario_recovery_amount(
                strategy=strategy,
                percentage=percentage_ratio,
                free_cashflow=projected_net,
            )
            projected_recovery += projected_month_recovery
            projection_rows.append(
                {
                    "month_index": index + 1,
                    "sales": _quantize(projected_sales) or ZERO,
                    "net_cashflow": _quantize(projected_net) or ZERO,
                    "recovery": _quantize(projected_month_recovery) or ZERO,
                    "recovery_cumulative": _quantize(projected_recovery) or ZERO,
                }
            )

        results = {
            "scenario_id": scenario.pk,
            "scenario_name": scenario.nombre,
            "scenario_type": scenario.tipo_escenario,
            "monthly_sales": _quantize(avg_sales) or ZERO,
            "monthly_growth_pct": _quantize(_ratio_from_percent_like(scenario.crecimiento_mensual_pct) * Decimal("100"), FOUR_PLACES) or ZERO,
            "gross_margin_pct": _quantize(gross_margin_ratio * Decimal("100"), FOUR_PLACES) or ZERO,
            "monthly_operating_expenses": _quantize(_as_decimal(scenario.gastos_operativos_mensuales)) or ZERO,
            "monthly_operating_cashflow": _quantize(monthly_operating_profit),
            "monthly_debt_service": _quantize(monthly_debt_service),
            "monthly_free_cashflow": _quantize(monthly_net_cashflow),
            "monthly_net_cashflow": _quantize(monthly_net_cashflow),
            "monthly_recovery_amount": _quantize(recovery_amount),
            "payback_months": payback_months,
            "annual_roi_pct": annual_roi,
            "annual_cash_on_cash_pct": annual_cash_on_cash,
            "projected_close_date": projected_close_date.isoformat() if projected_close_date else None,
            "projection_rows": projection_rows,
        }
        scenario.resultados = _json_safe(results)
        scenario.save(update_fields=["resultados", "actualizado_en"])
        return scenario.resultados


class ProyectoInversionDashboardService:
    def build_detail_context(self, project: ProyectoInversion) -> dict[str, object]:
        latest_snapshot = project.snapshots_mensuales.order_by("-periodo").first()
        snapshots = list(project.snapshots_mensuales.order_by("periodo"))
        expenses = list(project.gastos_inversion.select_related("proveedor").order_by("-fecha", "-id"))
        debt_payments = list(project.pagos_deuda.order_by("-fecha_pago", "-id"))
        scenarios = list(project.escenarios.order_by("tipo_escenario", "nombre"))
        active_alert_models = list(project.alertas.filter(activa=True).select_related("snapshot").order_by("-severidad", "-last_detected_at"))

        planned_investment = _as_decimal(project.monto_inversion_planeado)
        actual_investment = _as_decimal(project.monto_inversion_real)
        cumulative_sales = sum((_as_decimal(row.ventas_mensuales) for row in snapshots), ZERO)
        cumulative_operating_profit = sum((_as_decimal(row.utilidad_operativa) for row in snapshots if row.utilidad_operativa is not None), ZERO)
        cumulative_recovery = _as_decimal(latest_snapshot.recuperacion_acumulada if latest_snapshot else ZERO)
        pending_balance = _as_decimal(latest_snapshot.saldo_pendiente if latest_snapshot else actual_investment)
        debt_balance = _as_decimal(latest_snapshot.saldo_insoluto if latest_snapshot else project.deuda_asociada)
        recovery_pct = _as_decimal(latest_snapshot.porcentaje_recuperado if latest_snapshot else ZERO)
        roi_cumulative = _as_decimal(latest_snapshot.roi_acumulado if latest_snapshot else ZERO)
        cash_on_cash = _as_decimal(latest_snapshot.cash_on_cash if latest_snapshot else ZERO)
        current_forecast = _forecast_payback_metrics(
            project,
            [
                {"period": row.periodo, "free_cashflow": _as_decimal(row.flujo_libre)}
                for row in snapshots
            ],
            pending_balance,
        ) if latest_snapshot else {"payback_months": None, "estimated_date": None}
        project_payback = (
            latest_snapshot.payback_real_meses
            if latest_snapshot and latest_snapshot.payback_real_meses is not None
            else current_forecast["payback_months"]
            if latest_snapshot
            else None
        )
        health_score = 0
        if latest_snapshot:
            health_score, _, _ = _compute_health_score(
                project,
                roi_cumulative=roi_cumulative,
                free_cashflows=[_as_decimal(row.flujo_libre) for row in snapshots if row.flujo_libre is not None],
                sales_values=[_as_decimal(row.ventas_mensuales) for row in snapshots if row.ventas_mensuales is not None],
                actual_investment=actual_investment,
                recovery_pct=recovery_pct,
                months_elapsed=max(_elapsed_months(project.fecha_apertura or project.fecha_inicio, latest_snapshot.periodo), 1),
            )
        health_status = _health_status_from_score(health_score) if latest_snapshot else ProyectoInversionSnapshotMensual.HEALTH_RED
        recovery_progress_pct = max(min(float(recovery_pct or 0), 100.0), 0.0)

        expense_rows_by_category: dict[str, Decimal] = {}
        for row in expenses:
            expense_rows_by_category.setdefault(row.categoria, ZERO)
            expense_rows_by_category[row.categoria] += _as_decimal(row.monto_total)

        chart_rows = [
            {
                "period": row.periodo.strftime("%Y-%m"),
                "sales": float(row.ventas_mensuales or 0),
                "gross_profit": float(row.utilidad_bruta or 0),
                "operating_profit": float(row.utilidad_operativa or 0),
                "operating_cashflow": float(row.flujo_operativo or 0),
                "debt_service": float(row.servicio_deuda or 0),
                "free_cashflow": float(row.flujo_libre or 0),
                "net_cashflow": float(row.flujo_neto or 0),
                "recovery_cumulative": float(row.recuperacion_acumulada or 0),
                "recovery_pct": float(row.porcentaje_recuperado or 0),
                "debt_balance": float(row.saldo_insoluto or 0),
                "confidence_score": int(row.confidence_score or 0),
            }
            for row in snapshots
        ]
        investment_curve = []
        cumulative_investment = ZERO
        for row in sorted(expenses, key=lambda item: (item.fecha, item.id)):
            cumulative_investment += _as_decimal(row.monto_total)
            investment_curve.append(
                {
                    "date": row.fecha.isoformat(),
                    "investment_cumulative": float(_quantize(cumulative_investment) or ZERO),
                }
            )
        timeline_rows = [
            {"label": "Planeación", "date": project.fecha_inicio, "status": "done"},
            {
                "label": "Ejecución",
                "date": expenses[-1].fecha if expenses else project.fecha_inicio,
                "status": "done" if expenses else "pending",
            },
            {
                "label": "Apertura",
                "date": project.fecha_apertura,
                "status": "done" if project.fecha_apertura else "pending",
            },
            {
                "label": "Recuperación",
                "date": next((row.periodo_fin for row in snapshots if _as_decimal(row.monto_recuperacion_mes) > ZERO), None),
                "status": "done" if cumulative_recovery > ZERO else "pending",
            },
            {
                "label": "Cierre",
                "date": project.fecha_cierre,
                "status": "done" if project.fecha_cierre else "pending",
            },
        ]

        return {
            "project": project,
            "latest_snapshot": latest_snapshot,
            "snapshots": snapshots,
            "expenses": expenses,
            "debt_payments": debt_payments,
            "active_alerts": [
                {
                    "codigo": alert.codigo,
                    "severidad": alert.severidad,
                    "titulo": alert.titulo,
                    "mensaje": alert.mensaje,
                    "snapshot_period": alert.snapshot.periodo.isoformat() if alert.snapshot_id else None,
                    "payload": alert.payload,
                }
                for alert in active_alert_models
            ],
            "scenario_rows": [ProyectoInversionScenarioService().compute(project, scenario) for scenario in scenarios],
            "kpis": {
                "planned_investment": planned_investment,
                "actual_investment": actual_investment,
                "investment_deviation": actual_investment - planned_investment,
                "spent_pct": _percent_value(actual_investment, planned_investment),
                "debt_balance": debt_balance,
                "cumulative_sales": cumulative_sales,
                "cumulative_operating_profit": cumulative_operating_profit,
                "cumulative_recovery": cumulative_recovery,
                "pending_balance": pending_balance,
                "recovery_pct": recovery_pct,
                "recovery_progress_pct": recovery_progress_pct,
                "roi_cumulative": roi_cumulative,
                "cash_on_cash": cash_on_cash,
                "payback": project_payback,
                "forecast_payback": current_forecast["payback_months"] if latest_snapshot else None,
                "forecast_close_date": current_forecast["estimated_date"] if latest_snapshot else None,
                "forecast_vs_target": (_as_decimal(current_forecast["payback_months"]) - Decimal(project.payback_objetivo_meses or 0)) if latest_snapshot and current_forecast["payback_months"] is not None and project.payback_objetivo_meses else None,
                "health_score": health_score,
                "health_status": health_status,
                "health_label": dict(ProyectoInversionSnapshotMensual.HEALTH_STATUS_CHOICES).get(health_status, "Rojo"),
                "data_source": latest_snapshot.data_source if latest_snapshot else ProyectoInversionSnapshotMensual.DATA_SOURCE_ESTIMATED,
                "confidence_score": int(latest_snapshot.confidence_score if latest_snapshot else 40),
                "expense_coverage_status": (
                    (latest_snapshot.fuentes or {}).get("expense_coverage_status")
                    if latest_snapshot
                    else EXPENSE_COVERAGE_MISSING
                ),
                "expense_coverage_label": (
                    (latest_snapshot.fuentes or {}).get("expense_coverage_label")
                    if latest_snapshot
                    else "Faltante"
                ),
                "data_quality_badge": "Sin datos"
                if latest_snapshot is None
                else "Datos reales parciales"
                if (latest_snapshot.fuentes or {}).get("expense_coverage_status") == EXPENSE_COVERAGE_PARTIAL
                else "Datos faltantes"
                if (latest_snapshot.fuentes or {}).get("expense_coverage_status") == EXPENSE_COVERAGE_MISSING
                else "Datos estimados"
                if latest_snapshot.data_source != ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT
                else "Datos oficiales",
            },
            "expense_summary": [
                {"category": key, "amount": _quantize(value) or ZERO}
                for key, value in sorted(expense_rows_by_category.items(), key=lambda item: item[0])
            ],
            "timeline_rows": timeline_rows,
            "chart_rows": chart_rows,
            "investment_curve": investment_curve,
            "data_gaps": list(dict.fromkeys((latest_snapshot.fuentes or {}).get("data_gaps", []))) if latest_snapshot else [],
        }

    def build_portfolio_context(self) -> dict[str, object]:
        projects = list(
            ProyectoInversion.objects.select_related("sucursal_relacionada", "responsable").prefetch_related(
                Prefetch(
                    "snapshots_mensuales",
                    queryset=ProyectoInversionSnapshotMensual.objects.order_by("-periodo"),
                    to_attr="prefetched_snapshots_mensuales",
                ),
                Prefetch(
                    "alertas",
                    queryset=ProyectoInversionAlerta.objects.filter(activa=True).order_by("-last_detected_at"),
                    to_attr="prefetched_alertas_activas",
                ),
            ).order_by("-fecha_inicio", "-id")
        )
        project_rows = []
        total_planned = ZERO
        total_actual = ZERO
        total_recovery = ZERO
        total_pending = ZERO
        active_projects = 0
        for project in projects:
            latest_snapshot = project.prefetched_snapshots_mensuales[0] if getattr(project, "prefetched_snapshots_mensuales", None) else None
            prefetched_snapshots = list(reversed(getattr(project, "prefetched_snapshots_mensuales", [])))
            total_planned += _as_decimal(project.monto_inversion_planeado)
            total_actual += _as_decimal(project.monto_inversion_real)
            total_recovery += _as_decimal(latest_snapshot.recuperacion_acumulada if latest_snapshot else ZERO)
            total_pending += _as_decimal(latest_snapshot.saldo_pendiente if latest_snapshot else project.monto_inversion_real)
            if project.estatus not in {ProyectoInversion.ESTATUS_CERRADO, ProyectoInversion.ESTATUS_CANCELADO}:
                active_projects += 1
            dynamic_health_score = int(latest_snapshot.health_score if latest_snapshot else 0)
            if latest_snapshot:
                dynamic_health_score, _, _ = _compute_health_score(
                    project,
                    roi_cumulative=_as_decimal(latest_snapshot.roi_acumulado),
                    free_cashflows=[_as_decimal(row.flujo_libre) for row in prefetched_snapshots if row.flujo_libre is not None],
                    sales_values=[_as_decimal(row.ventas_mensuales) for row in prefetched_snapshots if row.ventas_mensuales is not None],
                    actual_investment=_as_decimal(project.monto_inversion_real or project.monto_inversion_planeado),
                    recovery_pct=_as_decimal(latest_snapshot.porcentaje_recuperado),
                    months_elapsed=max(_elapsed_months(project.fecha_apertura or project.fecha_inicio, latest_snapshot.periodo), 1),
                )
            project_rows.append(
                {
                    "project": project,
                    "latest_snapshot": latest_snapshot,
                    "recovery_pct": _as_decimal(latest_snapshot.porcentaje_recuperado if latest_snapshot else ZERO),
                    "roi_cumulative": _as_decimal(latest_snapshot.roi_acumulado if latest_snapshot else ZERO),
                    "payback": latest_snapshot.payback_real_meses if latest_snapshot and latest_snapshot.payback_real_meses is not None else latest_snapshot.payback_forecast_meses if latest_snapshot else None,
                    "cash_on_cash": _as_decimal(latest_snapshot.cash_on_cash if latest_snapshot else ZERO),
                    "health_score": dynamic_health_score,
                    "health_status": latest_snapshot.health_status if latest_snapshot else ProyectoInversionSnapshotMensual.HEALTH_RED,
                    "active_alert_count": len(getattr(project, "prefetched_alertas_activas", [])),
                }
            )
        return {
            "projects": project_rows,
            "portfolio_kpis": {
                "project_count": len(projects),
                "active_projects": active_projects,
                "planned_investment": _quantize(total_planned) or ZERO,
                "actual_investment": _quantize(total_actual) or ZERO,
                "recovered_amount": _quantize(total_recovery) or ZERO,
                "pending_amount": _quantize(total_pending) or ZERO,
                "recovery_pct": _percent_value(total_recovery, total_actual),
            },
        }

    def build_comparison_context(
        self,
        *,
        tipo_proyecto: str = "",
        estatus: str = "",
        fecha_inicio_desde: date | None = None,
        fecha_inicio_hasta: date | None = None,
        sort_by: str = "roi",
    ) -> dict[str, object]:
        queryset = ProyectoInversion.objects.select_related("sucursal_relacionada", "responsable").prefetch_related(
            Prefetch(
                "snapshots_mensuales",
                queryset=ProyectoInversionSnapshotMensual.objects.order_by("-periodo"),
                to_attr="prefetched_snapshots_mensuales",
            )
        )
        if tipo_proyecto:
            queryset = queryset.filter(tipo_proyecto=tipo_proyecto)
        if estatus:
            queryset = queryset.filter(estatus=estatus)
        if fecha_inicio_desde:
            queryset = queryset.filter(fecha_inicio__gte=fecha_inicio_desde)
        if fecha_inicio_hasta:
            queryset = queryset.filter(fecha_inicio__lte=fecha_inicio_hasta)

        rows = []
        for project in queryset.order_by("-fecha_inicio", "-id"):
            latest_snapshot = project.prefetched_snapshots_mensuales[0] if getattr(project, "prefetched_snapshots_mensuales", None) else None
            prefetched_snapshots = list(reversed(getattr(project, "prefetched_snapshots_mensuales", [])))
            recovered = _as_decimal(latest_snapshot.recuperacion_acumulada if latest_snapshot else ZERO)
            actual_investment = _as_decimal(project.monto_inversion_real)
            speed = _safe_div(recovered, actual_investment)
            dynamic_health_score = int(latest_snapshot.health_score if latest_snapshot else 0)
            if latest_snapshot:
                dynamic_health_score, _, _ = _compute_health_score(
                    project,
                    roi_cumulative=_as_decimal(latest_snapshot.roi_acumulado),
                    free_cashflows=[_as_decimal(row.flujo_libre) for row in prefetched_snapshots if row.flujo_libre is not None],
                    sales_values=[_as_decimal(row.ventas_mensuales) for row in prefetched_snapshots if row.ventas_mensuales is not None],
                    actual_investment=_as_decimal(project.monto_inversion_real or project.monto_inversion_planeado),
                    recovery_pct=_as_decimal(latest_snapshot.porcentaje_recuperado),
                    months_elapsed=max(_elapsed_months(project.fecha_apertura or project.fecha_inicio, latest_snapshot.periodo), 1),
                )
            rows.append(
                {
                    "project": project,
                    "latest_snapshot": latest_snapshot,
                    "roi": _as_decimal(latest_snapshot.roi_acumulado if latest_snapshot else ZERO),
                    "payback": _as_decimal(
                        latest_snapshot.payback_real_meses
                        if latest_snapshot and latest_snapshot.payback_real_meses is not None
                        else latest_snapshot.payback_forecast_meses if latest_snapshot else ZERO
                    ),
                    "cash_on_cash": _as_decimal(latest_snapshot.cash_on_cash if latest_snapshot else ZERO),
                    "recovery_speed": _quantize((speed or ZERO) * Decimal("100"), FOUR_PLACES) or ZERO,
                    "health_score": dynamic_health_score,
                }
            )

        sort_map = {
            "roi": lambda item: item["roi"],
            "payback": lambda item: item["payback"] * Decimal("-1"),
            "cash_on_cash": lambda item: item["cash_on_cash"],
            "recovery_speed": lambda item: item["recovery_speed"],
            "health_score": lambda item: Decimal(item["health_score"]),
        }
        rows.sort(key=sort_map.get(sort_by, sort_map["roi"]), reverse=True)
        return {
            "comparison_rows": rows,
            "comparison_filters": {
                "tipo_proyecto": tipo_proyecto,
                "estatus": estatus,
                "fecha_inicio_desde": fecha_inicio_desde,
                "fecha_inicio_hasta": fecha_inicio_hasta,
                "sort_by": sort_by,
            },
        }


def _benchmark_sucursales_activas(
    sucursal_ids: list[int] | None = None,
    meses: int = 12,
) -> dict[str, object]:
    """
    Calcula KPIs reales de sucursales activas para planeación de proyectos.

    Fuente primaria de ventas: VentaAutoritativaPoint.
    Fallback: FactVentaDiaria.
    Ticket promedio: PointDailyBranchIndicator ponderado por tickets.
    Costos/retorno: snapshots mensuales de proyectos activos o en recuperación.
    """
    from django.db.models import Avg, Count
    from django.db.models.functions import TruncMonth

    from core.models import Sucursal

    meses = max(int(meses or 12), 1)
    periodo_fin = timezone.localdate().replace(day=1) - timedelta(days=1)
    periodo_inicio = periodo_fin.replace(day=1)
    for _ in range(meses - 1):
        periodo_inicio = (periodo_inicio - timedelta(days=1)).replace(day=1)

    if sucursal_ids is None:
        sucursal_ids = list(Sucursal.objects.filter(activa=True).order_by("codigo").values_list("id", flat=True))
    else:
        sucursal_ids = [int(item) for item in sucursal_ids]

    sucursal_rows = {
        row.id: row
        for row in Sucursal.objects.filter(id__in=sucursal_ids).order_by("codigo", "nombre")
    }

    ventas_qs = VentaAutoritativaPoint.objects.filter(
        branch_id__in=sucursal_ids,
        sale_date__gte=periodo_inicio,
        sale_date__lte=periodo_fin,
        total_amount__gt=0,
    )
    ventas_por_sucursal_mes = list(
        ventas_qs.annotate(mes=TruncMonth("sale_date"))
        .values("branch_id", "mes")
        .annotate(total_mes=Sum("total_amount"))
        .order_by("branch_id", "mes")
    )
    data_source = "VentaAutoritativaPoint"

    if not ventas_por_sucursal_mes:
        facts_qs = FactVentaDiaria.objects.filter(
            sucursal_id__in=sucursal_ids,
            fecha__gte=periodo_inicio,
            fecha__lte=periodo_fin,
            venta_total__gt=0,
        )
        ventas_por_sucursal_mes = list(
            facts_qs.annotate(mes=TruncMonth("fecha"))
            .values("sucursal_id", "mes")
            .annotate(total_mes=Sum("venta_total"))
            .order_by("sucursal_id", "mes")
        )
        data_source = "FactVentaDiaria"

    total_mensual_rows = [_as_decimal(row.get("total_mes")) for row in ventas_por_sucursal_mes]
    ventas_mensuales_avg = (
        _quantize(sum(total_mensual_rows, ZERO) / Decimal(len(total_mensual_rows)))
        if total_mensual_rows
        else ZERO
    )

    ticket_promedio = ZERO
    try:
        from pos_bridge.models import PointDailyBranchIndicator

        indicator_qs = PointDailyBranchIndicator.objects.filter(
            branch__erp_branch_id__in=sucursal_ids,
            indicator_date__gte=periodo_inicio,
            indicator_date__lte=periodo_fin,
            total_amount__gt=0,
            total_tickets__gt=0,
        )
        indicator_totals = indicator_qs.aggregate(total_amount=Sum("total_amount"), total_tickets=Sum("total_tickets"))
        total_tickets = _as_decimal(indicator_totals.get("total_tickets"))
        if total_tickets > ZERO:
            ticket_promedio = _quantize(_as_decimal(indicator_totals.get("total_amount")) / total_tickets) or ZERO
    except Exception as exc:
        logger.warning("No se pudo calcular ticket promedio de PointDailyBranchIndicator: %s", exc)

    snapshots_qs = ProyectoInversionSnapshotMensual.objects.filter(
        proyecto__sucursal_relacionada_id__in=sucursal_ids,
        proyecto__estatus__in=[
            ProyectoInversion.ESTATUS_ACTIVO,
            ProyectoInversion.ESTATUS_EN_RECUPERACION,
        ],
        periodo__gte=periodo_inicio,
        periodo__lte=periodo_fin,
        data_source=ProyectoInversionSnapshotMensual.DATA_SOURCE_FACT,
    )
    snapshot_totals = snapshots_qs.aggregate(
        ventas=Sum("ventas_mensuales"),
        utilidad_bruta_total=Sum("utilidad_bruta"),
        utilidad_bruta_avg=Avg("utilidad_bruta"),
        gastos_operativos_avg=Avg("gastos_operativos"),
        utilidad_operativa_avg=Avg("utilidad_operativa"),
        payback_avg=Avg("payback_real_meses"),
        roi_avg=Avg("roi_acumulado"),
        snapshots=Count("id"),
    )
    ventas_snapshot = _as_decimal(snapshot_totals.get("ventas"))
    utilidad_bruta_snapshot = _as_decimal(snapshot_totals.get("utilidad_bruta_total"))
    margen_bruto_pct_avg = _percent_value(utilidad_bruta_snapshot, ventas_snapshot) or ZERO

    ventas_por_suc: dict[int, list[Decimal]] = {}
    key_name = "branch_id" if data_source == "VentaAutoritativaPoint" else "sucursal_id"
    for row in ventas_por_sucursal_mes:
        suc_id = int(row[key_name])
        ventas_por_suc.setdefault(suc_id, []).append(_as_decimal(row.get("total_mes")))

    detalle = []
    for suc_id in sucursal_ids:
        meses_data = ventas_por_suc.get(suc_id, [])
        avg_mes = _quantize(sum(meses_data, ZERO) / Decimal(len(meses_data))) if meses_data else ZERO
        sucursal = sucursal_rows.get(suc_id)
        detalle.append(
            {
                "sucursal_id": suc_id,
                "codigo": sucursal.codigo if sucursal else "",
                "nombre": sucursal.nombre if sucursal else "",
                "ventas_mensuales_avg": float(avg_mes or ZERO),
                "meses_con_datos": len(meses_data),
            }
        )

    return {
        "ticket_promedio": float(ticket_promedio),
        "ventas_mensuales_avg": float(ventas_mensuales_avg or ZERO),
        "ventas_diarias_avg": float(_quantize((ventas_mensuales_avg or ZERO) / Decimal("26")) or ZERO),
        "margen_bruto_pct_avg": float(_quantize(margen_bruto_pct_avg, FOUR_PLACES) or ZERO),
        "margen_bruto_abs_avg": float(_as_decimal(snapshot_totals.get("utilidad_bruta_avg"))),
        "gastos_operativos_avg": float(_as_decimal(snapshot_totals.get("gastos_operativos_avg"))),
        "utilidad_operativa_avg": float(_as_decimal(snapshot_totals.get("utilidad_operativa_avg"))),
        "payback_real_avg_meses": float(_as_decimal(snapshot_totals.get("payback_avg"))),
        "roi_acumulado_avg": float(_as_decimal(snapshot_totals.get("roi_avg"))),
        "snapshot_count": int(snapshot_totals.get("snapshots") or 0),
        "data_source": data_source,
        "periodo_inicio": periodo_inicio.isoformat(),
        "periodo_fin": periodo_fin.isoformat(),
        "sucursales_incluidas": sucursal_ids,
        "detalle_por_sucursal": detalle,
    }
