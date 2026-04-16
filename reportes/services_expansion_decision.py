from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import logging

from django.core.cache import cache
from django.db.models import Prefetch
from django.utils import timezone

from core.audit import log_event
from reportes.models import (
    EmpresaResultadoMensual,
    ExpansionPolicyConfig,
    ProyectoInversion,
    ProyectoInversionSnapshotMensual,
)
from reportes.services_investment_projects import (
    _as_decimal,
    _average_decimals,
    _compute_health_score,
    _elapsed_months,
    _get_calibration_settings,
    _json_safe,
    _quantize,
    _safe_div,
)

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
GLOBAL_SUMMARY_CACHE_KEY = "reportes:expansion:global-summary:v1"
GLOBAL_SUMMARY_TTL = 300


@dataclass(frozen=True)
class ExpansionProjectDecision:
    classification: str
    recommendation: str
    health_score: int
    roi: Decimal
    payback_real: Decimal | None
    average_free_cashflow_3m: Decimal | None
    sales_trend_pct: Decimal | None
    recurrent_negative_free_cashflow: bool


class ExpansionDecisionService:
    CLASSIFICATION_EXPAND = "EXPANDIR"
    CLASSIFICATION_MONITOR = "VIGILAR"
    CLASSIFICATION_RISK = "RIESGO"

    GLOBAL_RECOMMEND_EXPAND = "EXPANDIR"
    GLOBAL_RECOMMEND_WAIT = "ESPERAR"
    GLOBAL_RECOMMEND_STOP = "DETENER"

    def get_policy(self) -> ExpansionPolicyConfig:
        return (
            ExpansionPolicyConfig.objects.filter(activa=True).order_by("-actualizado_en", "-id").first()
            or ExpansionPolicyConfig(
                nombre="Política expansión por default",
                activa=True,
            )
        )

    def classify_project(
        self,
        project: ProyectoInversion,
        *,
        latest_snapshot: ProyectoInversionSnapshotMensual | None = None,
        recent_snapshots: list[ProyectoInversionSnapshotMensual] | None = None,
        calibration_settings: dict[str, object] | None = None,
        persist: bool = False,
        user=None,
    ) -> dict[str, object]:
        calibration_settings = calibration_settings or _get_calibration_settings()
        latest_snapshot = latest_snapshot or project.snapshots_mensuales.order_by("-periodo").first()
        if latest_snapshot is None:
            return {
                "project": project,
                "latest_snapshot": None,
                "classification": self.CLASSIFICATION_MONITOR,
                "recommendation": "Falta histórico operativo para evaluar expansión.",
                "health_score": 0,
                "roi": ZERO,
                "payback_real": None,
                "average_free_cashflow_3m": None,
                "sales_trend_pct": None,
                "recurrent_negative_free_cashflow": False,
                "available_months": 0,
                "real_classification": ((project.metadata or {}).get("calibration", {}) or {}).get("real_classification", ""),
                "matches_real_classification": None,
            }

        recent_snapshots = recent_snapshots or list(project.snapshots_mensuales.order_by("-periodo")[:6])
        recent_snapshots = sorted(recent_snapshots, key=lambda item: item.periodo)

        last_three = recent_snapshots[-3:]
        prev_three = recent_snapshots[-6:-3]
        free_cashflows = [_as_decimal(item.flujo_libre) for item in last_three if item.flujo_libre is not None]
        average_free_cashflow_3m = _quantize(_average_decimals(free_cashflows)) if free_cashflows else None
        recurrent_negative = len(last_three) >= 2 and all(_as_decimal(item.flujo_libre) < ZERO for item in last_three[-2:])

        recent_sales = [_as_decimal(item.ventas_mensuales) for item in last_three if _as_decimal(item.ventas_mensuales) > ZERO]
        previous_sales = [_as_decimal(item.ventas_mensuales) for item in prev_three if _as_decimal(item.ventas_mensuales) > ZERO]
        sales_trend_pct = None
        if recent_sales and previous_sales:
            sales_trend_pct = _quantize(
                ((sum(recent_sales, ZERO) / Decimal(len(recent_sales))) / (sum(previous_sales, ZERO) / Decimal(len(previous_sales))) - Decimal("1")) * Decimal("100"),
                pattern=Decimal("0.0001"),
            )

        months_elapsed = max(_elapsed_months(project.fecha_apertura or project.fecha_inicio, latest_snapshot.periodo), 1)
        health_score, _, _ = _compute_health_score(
            project,
            roi_cumulative=_as_decimal(latest_snapshot.roi_acumulado),
            free_cashflows=[_as_decimal(item.flujo_libre) for item in recent_snapshots if item.flujo_libre is not None],
            sales_values=[_as_decimal(item.ventas_mensuales) for item in recent_snapshots if item.ventas_mensuales is not None],
            actual_investment=_as_decimal(project.monto_inversion_real or project.monto_inversion_planeado),
            recovery_pct=_as_decimal(latest_snapshot.porcentaje_recuperado),
            months_elapsed=months_elapsed,
            calibration_settings=calibration_settings,
        )
        roi = _as_decimal(latest_snapshot.roi_acumulado)
        payback_real = latest_snapshot.payback_real_meses
        payback_target = Decimal(project.payback_objetivo_meses or 0)
        roi_target = _as_decimal(project.roi_objetivo)
        threshold_cfg = calibration_settings.get("classification_thresholds", {})
        expand_min = int(threshold_cfg.get("expand_min_health_score", 80) or 80)
        monitor_min = int(threshold_cfg.get("monitor_min_health_score", 50) or 50)
        payback_tolerance = _as_decimal(threshold_cfg.get("payback_tolerance_ratio", Decimal("1.00")))
        roi_target_factor = _as_decimal(threshold_cfg.get("roi_target_factor", Decimal("1.00")))
        positive_free_cashflow_months = int(threshold_cfg.get("positive_free_cashflow_months", 3) or 3)
        recurrent_negative_months = int(threshold_cfg.get("recurrent_negative_months", 2) or 2)

        recent_negative_window = recent_snapshots[-recurrent_negative_months:]
        recurrent_negative = len(recent_negative_window) >= recurrent_negative_months and all(
            _as_decimal(item.flujo_libre) < ZERO for item in recent_negative_window
        )

        recent_positive_window = recent_snapshots[-positive_free_cashflow_months:]
        free_positive_three = len(recent_positive_window) >= positive_free_cashflow_months and all(
            _as_decimal(item.flujo_libre) > ZERO for item in recent_positive_window
        )

        if (
            health_score >= expand_min
            and payback_real is not None
            and (payback_target <= ZERO or _as_decimal(payback_real) <= (payback_target * payback_tolerance))
            and free_positive_three
            and (roi_target <= ZERO or roi >= (roi_target * roi_target_factor))
        ):
            classification = self.CLASSIFICATION_EXPAND
            recommendation = "Sucursal candidata a expansión: desempeño sólido y repago alineado con objetivo."
        elif health_score < monitor_min or recurrent_negative:
            classification = self.CLASSIFICATION_RISK
            recommendation = "Sucursal en riesgo: revisar operación, deuda y disciplina CAPEX antes de expandir."
        else:
            classification = self.CLASSIFICATION_MONITOR
            recommendation = "Sucursal en observación: continuar seguimiento antes de autorizar expansión."

        real_classification = ((project.metadata or {}).get("calibration", {}) or {}).get("real_classification", "")
        matches_real = None
        if real_classification:
            matches_real = real_classification.upper() == classification

        result = {
            "project": project,
            "latest_snapshot": latest_snapshot,
            "classification": classification,
            "recommendation": recommendation,
            "health_score": health_score,
            "roi": roi,
            "payback_real": payback_real,
            "average_free_cashflow_3m": average_free_cashflow_3m,
            "sales_trend_pct": sales_trend_pct,
            "recurrent_negative_free_cashflow": recurrent_negative,
            "cash_on_cash": _as_decimal(latest_snapshot.cash_on_cash),
            "payback_target": payback_target,
            "available_months": len(recent_snapshots),
            "real_classification": real_classification,
            "matches_real_classification": matches_real,
        }
        if persist:
            self._persist_decision_marker(project, result, user=user)
        return result

    def build_expansion_context(
        self,
        *,
        tipo_proyecto: str = "",
        estatus: str = "",
        fecha_inicio_desde: date | None = None,
        fecha_inicio_hasta: date | None = None,
        calibration_settings: dict[str, object] | None = None,
        persist: bool = True,
        user=None,
    ) -> dict[str, object]:
        calibration_settings = calibration_settings or _get_calibration_settings()
        rows = self._project_rows(
            tipo_proyecto=tipo_proyecto,
            estatus=estatus,
            fecha_inicio_desde=fecha_inicio_desde,
            fecha_inicio_hasta=fecha_inicio_hasta,
        )
        decisions = [
            self.classify_project(
                row["project"],
                latest_snapshot=row["latest_snapshot"],
                recent_snapshots=row["recent_snapshots"],
                calibration_settings=calibration_settings,
                persist=persist,
                user=user,
            )
            for row in rows
        ]
        global_summary = self._global_summary(decisions)
        strategic_alerts = self._strategic_alerts(global_summary)

        logger.info(
            "Expansion map generado candidates=%s monitor=%s risk=%s global=%s",
            global_summary["counts"]["expand"],
            global_summary["counts"]["monitor"],
            global_summary["counts"]["risk"],
            global_summary["global_recommendation"]["decision"],
        )
        if user is not None:
            log_event(
                user,
                "EXPANSION_DECISION",
                "reportes.ExpansionPolicyConfig",
                0,
                payload=_json_safe(
                    {
                        "global_decision": global_summary["global_recommendation"]["decision"],
                        "blocked": global_summary["global_recommendation"]["blocked"],
                        "counts": global_summary["counts"],
                    }
                ),
            )

        return {
            "decision_rows": decisions,
            "candidate_rows": [row for row in decisions if row["classification"] == self.CLASSIFICATION_EXPAND],
            "risk_rows": [row for row in decisions if row["classification"] == self.CLASSIFICATION_RISK],
            "global_summary": global_summary,
            "strategic_alerts": strategic_alerts,
            "filters": {
                "tipo_proyecto": tipo_proyecto,
                "estatus": estatus,
                "fecha_inicio_desde": fecha_inicio_desde,
                "fecha_inicio_hasta": fecha_inicio_hasta,
            },
        }

    def _project_rows(
        self,
        *,
        tipo_proyecto: str,
        estatus: str,
        fecha_inicio_desde: date | None,
        fecha_inicio_hasta: date | None,
    ) -> list[dict[str, object]]:
        queryset = ProyectoInversion.objects.select_related("sucursal_relacionada", "responsable").prefetch_related(
            Prefetch(
                "snapshots_mensuales",
                queryset=ProyectoInversionSnapshotMensual.objects.order_by("-periodo"),
                to_attr="prefetched_snapshots_for_expansion",
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
            prefetched = getattr(project, "prefetched_snapshots_for_expansion", [])
            latest_snapshot = prefetched[0] if prefetched else None
            recent_snapshots = prefetched[:6]
            rows.append(
                {
                    "project": project,
                    "latest_snapshot": latest_snapshot,
                    "recent_snapshots": recent_snapshots,
                }
            )
        return rows

    def _global_summary(self, decisions: list[dict[str, object]]) -> dict[str, object]:
        cached = cache.get(GLOBAL_SUMMARY_CACHE_KEY)
        policy = self.get_policy()
        latest_company = EmpresaResultadoMensual.objects.order_by("-periodo").first()
        free_cashflow_total = sum((_as_decimal(row["latest_snapshot"].flujo_libre) for row in decisions if row["latest_snapshot"] is not None), ZERO)
        debt_total = sum((_as_decimal(row["latest_snapshot"].saldo_insoluto) for row in decisions if row["latest_snapshot"] is not None), ZERO)
        company_income = _as_decimal(latest_company.venta_total if latest_company else ZERO)
        debt_to_income_ratio = _safe_div(debt_total, company_income)
        average_payback = _average_decimals(
            [
                _as_decimal(row["latest_snapshot"].payback_real_meses or row["latest_snapshot"].payback_forecast_meses)
                for row in decisions
                if row["latest_snapshot"] is not None
                and (row["latest_snapshot"].payback_real_meses is not None or row["latest_snapshot"].payback_forecast_meses is not None)
            ]
        )
        if average_payback is not None:
            average_payback = _quantize(average_payback)

        candidate_count = sum(1 for row in decisions if row["classification"] == self.CLASSIFICATION_EXPAND)
        risk_count = sum(1 for row in decisions if row["classification"] == self.CLASSIFICATION_RISK)
        blocked_reasons: list[str] = []
        advisory_reasons: list[str] = []

        if free_cashflow_total < _as_decimal(policy.min_free_cashflow_total):
            blocked_reasons.append("El flujo libre total del portafolio de expansión está por debajo del umbral permitido.")
        if debt_to_income_ratio is not None and debt_to_income_ratio > _as_decimal(policy.max_debt_to_income_ratio):
            blocked_reasons.append("La deuda del portafolio de expansión excede la relación máxima deuda/ingreso permitida.")
        if average_payback is not None and average_payback > _as_decimal(policy.max_average_payback_months):
            advisory_reasons.append("El payback promedio del portafolio supera el límite definido por Dirección.")
        if risk_count > int(policy.max_projects_in_risk):
            advisory_reasons.append("El número de sucursales en riesgo excede el máximo tolerado.")

        if blocked_reasons:
            decision = self.GLOBAL_RECOMMEND_STOP
            explanation = "Expansión no recomendada hasta corregir condiciones financieras críticas."
        elif advisory_reasons:
            decision = self.GLOBAL_RECOMMEND_WAIT
            explanation = "Conviene esperar: el portafolio requiere estabilización antes de abrir nuevas sucursales."
        elif candidate_count > 0:
            decision = self.GLOBAL_RECOMMEND_EXPAND
            explanation = "Hay condiciones para evaluar nuevas aperturas con disciplina financiera."
        else:
            decision = self.GLOBAL_RECOMMEND_WAIT
            explanation = "No hay candidatas suficientemente maduras para expandir en este momento."

        capacity_investment = max(free_cashflow_total, ZERO) * _as_decimal(policy.max_average_payback_months)
        summary = {
            "policy": {
                "id": policy.pk,
                "name": policy.nombre,
                "min_free_cashflow_total": _quantize(_as_decimal(policy.min_free_cashflow_total)) or ZERO,
                "max_debt_to_income_ratio": _quantize(_as_decimal(policy.max_debt_to_income_ratio) * Decimal("100"), pattern=Decimal("0.0001")) or ZERO,
                "max_average_payback_months": _quantize(_as_decimal(policy.max_average_payback_months)) or ZERO,
                "max_projects_in_risk": int(policy.max_projects_in_risk),
            },
            "metrics": {
                "free_cashflow_total": _quantize(free_cashflow_total) or ZERO,
                "debt_total": _quantize(debt_total) or ZERO,
                "company_income": _quantize(company_income) or ZERO,
                "debt_to_income_ratio_pct": _quantize((debt_to_income_ratio or ZERO) * Decimal("100"), pattern=Decimal("0.0001")) or ZERO,
                "average_payback": average_payback,
                "capacity_investment": _quantize(capacity_investment) or ZERO,
                "company_scope_note": "La deuda total usa la cartera de expansión porque el ERP aún no expone una fuente corporativa canónica de pasivos.",
            },
            "counts": {
                "expand": candidate_count,
                "monitor": sum(1 for row in decisions if row["classification"] == self.CLASSIFICATION_MONITOR),
                "risk": risk_count,
                "total": len(decisions),
            },
            "global_recommendation": {
                "decision": decision,
                "explanation": explanation,
                "blocked": bool(blocked_reasons),
                "blocked_reasons": blocked_reasons,
                "advisory_reasons": advisory_reasons,
            },
            "source_period": latest_company.periodo.isoformat() if latest_company else None,
        }
        cache.set(GLOBAL_SUMMARY_CACHE_KEY, _json_safe(summary), GLOBAL_SUMMARY_TTL)
        return summary if cached is None else summary

    def _strategic_alerts(self, global_summary: dict[str, object]) -> list[dict[str, object]]:
        alerts: list[dict[str, object]] = []
        metrics = global_summary["metrics"]
        policy = global_summary["policy"]
        counts = global_summary["counts"]

        if _as_decimal(metrics["debt_to_income_ratio_pct"]) > _as_decimal(policy["max_debt_to_income_ratio"]):
            alerts.append(
                {
                    "code": "OVERLEVERAGED",
                    "severity": "CRITICAL",
                    "message": "Empresa sobreapalancada respecto al umbral de expansión.",
                }
            )
        if counts["risk"] > int(policy["max_projects_in_risk"]):
            alerts.append(
                {
                    "code": "TOO_MANY_RISK_PROJECTS",
                    "severity": "WARNING",
                    "message": "Hay demasiadas sucursales en riesgo para continuar un plan agresivo de expansión.",
                }
            )
        if counts["expand"] > 0 and _as_decimal(metrics["capacity_investment"]) <= ZERO:
            alerts.append(
                {
                    "code": "AGGRESSIVE_EXPANSION_VS_CASH",
                    "severity": "CRITICAL",
                    "message": "La expansión potencial excede la capacidad financiera actual del portafolio.",
                }
            )
        average_payback = metrics["average_payback"]
        if average_payback is not None and _as_decimal(average_payback) > _as_decimal(policy["max_average_payback_months"]):
            alerts.append(
                {
                    "code": "PAYBACK_DETERIORATING",
                    "severity": "WARNING",
                    "message": "El payback promedio del portafolio se está deteriorando frente al límite aceptado.",
                }
            )
        return alerts

    def _persist_decision_marker(self, project: ProyectoInversion, result: dict[str, object], *, user=None) -> None:
        marker = {
            "classification": result["classification"],
            "health_score": result["health_score"],
            "roi": str(result["roi"]),
            "evaluated_at": timezone.now().isoformat(),
        }
        metadata = dict(project.metadata or {})
        previous = metadata.get("expansion_decision", {})
        if previous.get("classification") != marker["classification"]:
            logger.info(
                "Proyecto %s cambió clasificación de expansión %s -> %s",
                project.pk,
                previous.get("classification"),
                marker["classification"],
            )
            if user is not None:
                log_event(
                    user,
                    "EXPANSION_CLASSIFICATION_CHANGE",
                    "reportes.ProyectoInversion",
                    project.pk,
                    payload=_json_safe({"from": previous, "to": marker}),
                )
        metadata["expansion_decision"] = marker
        if metadata != (project.metadata or {}):
            project.metadata = metadata
            project.save(update_fields=["metadata", "actualizado_en"])
