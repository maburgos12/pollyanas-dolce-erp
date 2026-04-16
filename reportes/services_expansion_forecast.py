from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import logging
from math import floor

from django.db.models import Prefetch
from django.utils import timezone

from core.audit import log_event
from reportes.models import ProyectoInversion, ProyectoInversionSnapshotMensual
from reportes.services_expansion_decision import ExpansionDecisionService
from reportes.services_investment_projects import (
    _as_decimal,
    _average_decimals,
    _compute_health_score,
    _json_safe,
    _quantize,
    _safe_div,
)

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
HUNDRED = Decimal("100")


class ExpansionForecastService:
    SCENARIO_CONSERVADOR = "CONSERVADOR"
    SCENARIO_BASE = "BASE"
    SCENARIO_OPTIMISTA = "OPTIMISTA"
    SCENARIO_CHOICES = (
        (SCENARIO_CONSERVADOR, "Conservador"),
        (SCENARIO_BASE, "Base"),
        (SCENARIO_OPTIMISTA, "Optimista"),
    )
    SCENARIO_FACTORS = {
        SCENARIO_CONSERVADOR: {
            "sales_factor": Decimal("0.88"),
            "gross_margin_factor": Decimal("0.96"),
            "operating_margin_factor": Decimal("0.94"),
            "operating_expense_factor": Decimal("1.06"),
            "debt_service_factor": Decimal("1.05"),
            "risk_buffer": 18,
        },
        SCENARIO_BASE: {
            "sales_factor": Decimal("1.00"),
            "gross_margin_factor": Decimal("1.00"),
            "operating_margin_factor": Decimal("1.00"),
            "operating_expense_factor": Decimal("1.00"),
            "debt_service_factor": Decimal("1.00"),
            "risk_buffer": 0,
        },
        SCENARIO_OPTIMISTA: {
            "sales_factor": Decimal("1.12"),
            "gross_margin_factor": Decimal("1.04"),
            "operating_margin_factor": Decimal("1.05"),
            "operating_expense_factor": Decimal("0.97"),
            "debt_service_factor": Decimal("0.96"),
            "risk_buffer": -10,
        },
    }

    def forecast(
        self,
        *,
        base_project: ProyectoInversion | None = None,
        tipo_proyecto: str = "",
        investment_estimate: Decimal | None = None,
        monthly_rent: Decimal | None = None,
        sales_adjustment_pct: Decimal | None = None,
        scenario: str = SCENARIO_BASE,
        location_reference: str = "",
        user=None,
    ) -> dict[str, object]:
        normalized_scenario = (scenario or self.SCENARIO_BASE).strip().upper()
        factors = self.SCENARIO_FACTORS.get(normalized_scenario, self.SCENARIO_FACTORS[self.SCENARIO_BASE])

        if base_project is not None and not tipo_proyecto:
            tipo_proyecto = base_project.tipo_proyecto
        tipo_proyecto = tipo_proyecto or ProyectoInversion.TIPO_APERTURA_SUCURSAL

        peer_rows = self._peer_rows(base_project=base_project, tipo_proyecto=tipo_proyecto)
        summary = self._peer_summary(peer_rows)
        data_gaps = list(summary["data_gaps"])
        base_row = self._base_reference_row(base_project=base_project, rows=peer_rows)

        effective_investment = investment_estimate if investment_estimate is not None else summary["average_investment"]
        if effective_investment is None or _as_decimal(effective_investment) <= ZERO:
            if base_project is not None:
                effective_investment = _as_decimal(base_project.monto_inversion_real or base_project.monto_inversion_planeado)
            if _as_decimal(effective_investment) <= ZERO:
                data_gaps.append("No hay suficiente histórico para inferir inversión objetivo de una nueva sucursal.")
                effective_investment = None

        reference_sales = self._metric_or_summary(base_row, "average_sales_3m", summary["average_sales_3m"])
        reference_gross_margin_pct = self._metric_or_summary(
            base_row,
            "average_gross_margin_pct",
            summary["average_gross_margin_pct"],
        )
        reference_operating_margin_pct = self._metric_or_summary(
            base_row,
            "average_operating_margin_pct",
            summary["average_operating_margin_pct"],
        )
        reference_operating_expenses = self._metric_or_summary(
            base_row,
            "average_operating_expenses",
            summary["average_operating_expenses"],
        )
        reference_operating_profit = self._metric_or_summary(
            base_row,
            "average_operating_profit_3m",
            summary["average_operating_profit_3m"],
        )
        reference_debt_service = self._metric_or_summary(base_row, "average_debt_service", summary["average_debt_service"])
        reference_payback = self._metric_or_summary(base_row, "payback_reference", summary["average_payback"])
        reference_health_score = int(base_row["health_score_reference"] if base_row is not None else summary["average_health_score"] or 0)
        reference_investment = self._metric_or_summary(base_row, "investment", summary["average_investment"])
        reference_recovery_rate = self._resolve_recovery_rate(base_project=base_project, base_row=base_row)
        reference_project = self._reference_project(
            base_project=base_project,
            summary=summary,
            effective_investment=effective_investment,
        )
        normalized_sales_adjustment_pct = _as_decimal(sales_adjustment_pct)
        user_sales_factor = Decimal("1") + (normalized_sales_adjustment_pct / HUNDRED)
        if user_sales_factor <= ZERO:
            data_gaps.append("El ajuste de ventas hace no positiva la base comercial; se usará el valor mínimo permitido.")
            user_sales_factor = Decimal("0.05")
        incremental_rent = _as_decimal(monthly_rent)

        projected_sales = None
        projected_gross_profit = None
        projected_operating_expenses = None
        projected_operating_profit = None
        projected_recovery_cashflow = None
        projected_free_cashflow = None
        projected_payback = None
        projected_roi = None
        projected_close_date = None
        projected_health_score = 0
        projected_recovery_pct_year_1 = None

        if reference_sales is not None:
            projected_sales = _quantize(_as_decimal(reference_sales) * factors["sales_factor"] * user_sales_factor)
        else:
            data_gaps.append("Las ventas históricas comparables no están completas para proyectar una nueva apertura.")

        if projected_sales is not None and reference_gross_margin_pct is not None:
            projected_gross_margin_pct = min(
                max(_as_decimal(reference_gross_margin_pct) * factors["gross_margin_factor"], ZERO),
                Decimal("1.5"),
            )
            projected_gross_profit = _quantize(projected_sales * projected_gross_margin_pct)
        else:
            projected_gross_margin_pct = None

        if reference_operating_expenses is not None:
            projected_operating_expenses = _quantize(
                (_as_decimal(reference_operating_expenses) * factors["operating_expense_factor"]) + incremental_rent
            )
        elif incremental_rent > ZERO:
            projected_operating_expenses = _quantize(incremental_rent)
            data_gaps.append(
                "El gasto operativo histórico no está completo; la renta capturada se aplicó como presión incremental aislada."
            )

        if projected_gross_profit is not None and projected_operating_expenses is not None:
            projected_operating_profit = _quantize(projected_gross_profit - projected_operating_expenses)
        elif projected_sales is not None and reference_operating_margin_pct is not None:
            projected_operating_margin_pct = min(
                max(_as_decimal(reference_operating_margin_pct) * factors["operating_margin_factor"], Decimal("-0.5")),
                Decimal("1"),
            )
            projected_operating_profit = _quantize((projected_sales * projected_operating_margin_pct) - incremental_rent)
            if projected_operating_expenses is None and projected_gross_profit is not None:
                projected_operating_expenses = _quantize(max(projected_gross_profit - projected_operating_profit, ZERO))
            data_gaps.append(
                "La utilidad operativa se estimó con margen operativo histórico porque no hay suficiente desglose de gasto operativo."
            )
        elif reference_operating_profit is not None:
            projected_operating_profit = _quantize(
                (_as_decimal(reference_operating_profit) * factors["sales_factor"] * user_sales_factor * factors["operating_margin_factor"])
                - incremental_rent
            )
            data_gaps.append(
                "La utilidad operativa se aproximó con utilidad histórica comparable por falta de márgenes detallados."
            )
        else:
            data_gaps.append("No se pudo calcular utilidad operativa proyectada por falta de margen histórico comparable.")

        if projected_operating_profit is not None:
            debt_scale = Decimal("1")
            if effective_investment is not None and reference_investment is not None and _as_decimal(reference_investment) > ZERO:
                debt_scale = _as_decimal(effective_investment) / _as_decimal(reference_investment)
            projected_debt_service = _quantize(
                (_as_decimal(reference_debt_service) if reference_debt_service is not None else ZERO)
                * factors["debt_service_factor"]
                * debt_scale
            ) or ZERO
            projected_free_cashflow = _quantize(projected_operating_profit - projected_debt_service)
            projected_recovery_cashflow = self._projected_recovery_cashflow(
                project=reference_project,
                projected_operating_profit=projected_operating_profit,
                projected_debt_service=projected_debt_service,
                projected_free_cashflow=projected_free_cashflow,
                recovery_rate=reference_recovery_rate,
            )
        else:
            projected_debt_service = None

        if (
            projected_recovery_cashflow is not None
            and projected_recovery_cashflow > ZERO
            and effective_investment is not None
        ):
            projected_payback = _quantize(_as_decimal(effective_investment) / projected_recovery_cashflow)
        if projected_free_cashflow is not None and projected_free_cashflow > ZERO and effective_investment is not None:
            projected_roi = _quantize(((projected_free_cashflow * Decimal("12")) / _as_decimal(effective_investment)) * HUNDRED)
        if (
            projected_recovery_cashflow is not None
            and projected_recovery_cashflow > ZERO
            and effective_investment is not None
        ):
            projected_close_date = self._projected_close_date(projected_payback)
            projected_recovery_pct_year_1 = _quantize(
                min(((projected_recovery_cashflow * Decimal("12")) / _as_decimal(effective_investment)) * HUNDRED, HUNDRED)
            )

        if projected_free_cashflow is not None and projected_sales is not None and effective_investment is not None:
            projected_health_score, _, _ = _compute_health_score(
                reference_project,
                roi_cumulative=_as_decimal(projected_roi),
                free_cashflows=[_as_decimal(projected_free_cashflow)] * 3,
                sales_values=[_as_decimal(projected_sales)] * 6,
                actual_investment=_as_decimal(effective_investment),
                recovery_pct=_as_decimal(projected_recovery_pct_year_1),
                months_elapsed=12,
            )

        risk_level = self._risk_level(
            projected_free_cashflow=projected_free_cashflow,
            projected_payback=projected_payback,
            average_payback=reference_payback,
            peer_health_score=projected_health_score or reference_health_score,
            data_gaps=data_gaps,
            scenario_buffer=factors["risk_buffer"],
        )
        assumptions = [
            f"Escenario aplicado: {dict(self.SCENARIO_CHOICES).get(normalized_scenario, normalized_scenario)}.",
            "La proyección usa históricos de proyectos comparables del portafolio de expansión.",
        ]
        if effective_investment is not None and investment_estimate is None:
            assumptions.append("La inversión estimada se infirió del CAPEX real promedio de proyectos comparables.")
        elif investment_estimate is not None:
            assumptions.append("La inversión estimada fue capturada manualmente por dirección y sustituye el promedio histórico.")
        if monthly_rent is not None and incremental_rent > ZERO:
            assumptions.append(
                "La renta capturada se aplica como presión mensual adicional sobre el escenario proyectado."
            )
        if sales_adjustment_pct is not None and normalized_sales_adjustment_pct != ZERO:
            assumptions.append(
                f"El ajuste de ventas del usuario ({normalized_sales_adjustment_pct:,.2f}%) se aplicó sobre la base histórica antes del escenario."
            )
        if location_reference:
            assumptions.append(f"Referencia operativa capturada para planeación: {location_reference}.")
        decision_rules = [
            {
                "label": "Flujo libre proyectado positivo",
                "passed": projected_free_cashflow is not None and projected_free_cashflow > ZERO,
                "detail": f"${float(projected_free_cashflow or ZERO):,.2f}",
            },
            {
                "label": "Payback dentro del objetivo",
                "passed": bool(
                    projected_payback is not None
                    and (
                        reference_project.payback_objetivo_meses <= 0
                        or _as_decimal(projected_payback) <= Decimal(reference_project.payback_objetivo_meses)
                    )
                ),
                "detail": (
                    f"{float(projected_payback):,.2f} meses"
                    if projected_payback is not None
                    else "No disponible"
                ),
            },
            {
                "label": "ROI proyectado suficiente",
                "passed": bool(
                    projected_roi is not None
                    and (
                        _as_decimal(reference_project.roi_objetivo) <= ZERO
                        or _as_decimal(projected_roi) >= _as_decimal(reference_project.roi_objetivo)
                    )
                ),
                "detail": (
                    f"{float(projected_roi):,.2f}%"
                    if projected_roi is not None
                    else "No disponible"
                ),
            },
            {
                "label": "Health score estimado aceptable",
                "passed": projected_health_score >= 65,
                "detail": f"{projected_health_score}/100",
            },
        ]

        payload = {
            "scenario": normalized_scenario,
            "scenario_label": dict(self.SCENARIO_CHOICES).get(normalized_scenario, normalized_scenario),
            "tipo_proyecto": tipo_proyecto,
            "base_project": {
                "id": base_project.pk,
                "name": base_project.nombre_proyecto,
            }
            if base_project is not None
            else None,
            "peer_count": len(peer_rows),
            "peer_summary": {
                "average_sales_3m": summary["average_sales_3m"],
                "average_operating_margin_pct": _quantize((summary["average_operating_margin_pct"] or ZERO) * HUNDRED, pattern=Decimal("0.0001")) if summary["average_operating_margin_pct"] is not None else None,
                "average_gross_margin_pct": _quantize((summary["average_gross_margin_pct"] or ZERO) * HUNDRED, pattern=Decimal("0.0001")) if summary["average_gross_margin_pct"] is not None else None,
                "average_operating_expenses": summary["average_operating_expenses"],
                "average_debt_service": summary["average_debt_service"],
                "average_payback": summary["average_payback"],
                "average_payback_target": summary["average_payback_target"],
                "average_health_score": summary["average_health_score"],
                "average_cash_on_cash": summary["average_cash_on_cash"],
                "average_roi_target": summary["average_roi_target"],
            },
            "historical_reference": {
                "source_project_id": base_project.pk if base_project is not None else None,
                "source_project_name": base_project.nombre_proyecto if base_project is not None else "Portafolio comparable",
                "months_used": int(base_row["snapshot_count"] if base_row is not None else summary["average_months_used"] or 0),
                "sales_base": _quantize(_as_decimal(reference_sales)) if reference_sales is not None else None,
                "gross_margin_pct": _quantize((_as_decimal(reference_gross_margin_pct) or ZERO) * HUNDRED, pattern=Decimal("0.0001")) if reference_gross_margin_pct is not None else None,
                "operating_margin_pct": _quantize((_as_decimal(reference_operating_margin_pct) or ZERO) * HUNDRED, pattern=Decimal("0.0001")) if reference_operating_margin_pct is not None else None,
                "operating_expenses_base": _quantize(_as_decimal(reference_operating_expenses)) if reference_operating_expenses is not None else None,
                "operating_profit_base": _quantize(_as_decimal(reference_operating_profit)) if reference_operating_profit is not None else None,
                "free_cashflow_base": self._historical_free_cashflow(
                    base_row=base_row,
                    reference_operating_profit=reference_operating_profit,
                    reference_debt_service=reference_debt_service,
                ),
                "rent_base": _quantize(_as_decimal(base_row["average_rent"]) if base_row is not None else ZERO) if base_row is not None and base_row["average_rent"] is not None else None,
                "debt_service_base": _quantize(_as_decimal(reference_debt_service)) if reference_debt_service is not None else None,
                "recovery_rate_pct": _quantize(_as_decimal(reference_recovery_rate) * HUNDRED, pattern=Decimal("0.0001")) if reference_recovery_rate is not None else None,
                "health_score_base": reference_health_score,
                "payback_base": self._historical_payback(base_row=base_row, summary=summary),
                "roi_base": self._historical_roi(base_row=base_row),
            },
            "inputs": {
                "investment_estimate": _quantize(_as_decimal(effective_investment)) if effective_investment is not None else None,
                "monthly_rent": _quantize(incremental_rent) if monthly_rent is not None else None,
                "sales_adjustment_pct": _quantize(normalized_sales_adjustment_pct, pattern=Decimal("0.0001")) if sales_adjustment_pct is not None else None,
                "location_reference": location_reference,
            },
            "outputs": {
                "projected_sales": projected_sales,
                "projected_gross_profit": projected_gross_profit,
                "projected_operating_expenses": projected_operating_expenses,
                "projected_operating_profit": projected_operating_profit,
                "projected_debt_service": projected_debt_service,
                "projected_free_cashflow": projected_free_cashflow,
                "projected_recovery_cashflow": projected_recovery_cashflow,
                "projected_payback_months": projected_payback,
                "projected_roi_pct": projected_roi,
                "projected_close_date": projected_close_date,
                "projected_health_score": projected_health_score,
                "projected_recovery_pct_year_1": projected_recovery_pct_year_1,
                "risk_level": risk_level,
            },
            "decision_rules": decision_rules,
            "assumptions": assumptions,
            "data_gaps": sorted(set(data_gaps)),
            "generated_at": timezone.now(),
        }
        logger.info(
            "Expansion forecast generado scenario=%s tipo=%s peers=%s risk=%s",
            normalized_scenario,
            tipo_proyecto,
            len(peer_rows),
            risk_level,
        )
        if user is not None:
            log_event(
                user,
                "EXPANSION_FORECAST",
                "reportes.ProyectoInversion",
                base_project.pk if base_project is not None else 0,
                payload=_json_safe(payload),
            )
        return payload

    def recommend_opening(
        self,
        *,
        base_project: ProyectoInversion | None = None,
        tipo_proyecto: str = "",
        investment_estimate: Decimal | None = None,
        monthly_rent: Decimal | None = None,
        sales_adjustment_pct: Decimal | None = None,
        scenario: str = SCENARIO_BASE,
        location_reference: str = "",
        forecast_payload: dict[str, object] | None = None,
        expansion_context: dict[str, object] | None = None,
        user=None,
    ) -> dict[str, object]:
        decision_service = ExpansionDecisionService()
        expansion_context = expansion_context or decision_service.build_expansion_context(persist=False)
        forecast = forecast_payload or self.forecast(
            base_project=base_project,
            tipo_proyecto=tipo_proyecto,
            investment_estimate=investment_estimate,
            monthly_rent=monthly_rent,
            sales_adjustment_pct=sales_adjustment_pct,
            scenario=scenario,
            location_reference=location_reference,
            user=user,
        )
        global_recommendation = expansion_context["global_summary"]["global_recommendation"]
        capacity_investment = _as_decimal(expansion_context["global_summary"]["metrics"]["capacity_investment"])
        effective_investment = _as_decimal(forecast["inputs"]["investment_estimate"])
        projected_payback = _as_decimal(forecast["outputs"]["projected_payback_months"])
        projected_roi = _as_decimal(forecast["outputs"]["projected_roi_pct"])
        projected_health_score = int(forecast["outputs"].get("projected_health_score") or 0)
        reference_project = self._reference_project(
            base_project=base_project,
            summary=forecast["peer_summary"],
            effective_investment=effective_investment,
        )
        can_open = (
            not global_recommendation["blocked"]
            and forecast["outputs"]["risk_level"] != "ALTO"
            and projected_health_score >= 65
            and effective_investment > ZERO
            and capacity_investment >= effective_investment
        )
        recommended_count = 0
        if can_open and effective_investment > ZERO:
            recommended_count = max(int(floor(capacity_investment / effective_investment)), 1)

        projected_free_cashflow_positive = _as_decimal(forecast["outputs"]["projected_free_cashflow"]) > ZERO
        payback_within_target = (
            projected_payback > ZERO
            and (
                reference_project.payback_objetivo_meses <= 0
                or projected_payback <= Decimal(reference_project.payback_objetivo_meses)
            )
        )
        roi_within_target = (
            projected_roi > ZERO
            and (
                _as_decimal(reference_project.roi_objetivo) <= ZERO
                or projected_roi >= _as_decimal(reference_project.roi_objetivo)
            )
        )
        decision_rules = [
            {
                "label": "Flujo proyectado",
                "passed": projected_free_cashflow_positive,
                "detail": f"${float(_as_decimal(forecast['outputs']['projected_free_cashflow'])):,.2f}",
            },
            {
                "label": "Payback vs objetivo",
                "passed": payback_within_target,
                "detail": f"{float(projected_payback or ZERO):,.2f} meses",
            },
            {
                "label": "ROI vs objetivo",
                "passed": roi_within_target,
                "detail": f"{float(projected_roi or ZERO):,.2f}%",
            },
            {
                "label": "Health score estimado",
                "passed": projected_health_score >= 65,
                "detail": f"{projected_health_score}/100",
            },
        ]

        if global_recommendation["blocked"]:
            decision = "NO_ABRIR"
            explanation = "La política financiera global bloquea nuevas aperturas hasta corregir condiciones críticas."
        elif not projected_free_cashflow_positive:
            decision = "NO_ABRIR"
            explanation = "La apertura no es viable con flujo libre proyectado negativo o nulo."
        elif forecast["outputs"]["risk_level"] == "ALTO":
            decision = "ESPERAR"
            explanation = "La nueva sucursal proyecta riesgo alto; conviene estabilizar el portafolio o ajustar inversión."
        elif not payback_within_target or not roi_within_target:
            decision = "ESPERAR"
            explanation = "La apertura aún no cumple los objetivos financieros proyectados de repago y retorno."
        elif recommended_count > 0:
            decision = "ABRIR"
            explanation = "Las condiciones actuales permiten abrir nuevas sucursales con disciplina financiera."
        else:
            decision = "ESPERAR"
            explanation = "La expansión no está bloqueada, pero la capacidad financiera actual no soporta una apertura cómoda."

        payload = {
            "decision": decision,
            "explanation": explanation,
            "recommended_branch_count": recommended_count,
            "suggested_investment": _quantize(min(capacity_investment, effective_investment if effective_investment > ZERO else capacity_investment)) if capacity_investment > ZERO else ZERO,
            "capacity_investment": _quantize(capacity_investment) or ZERO,
            "global_recommendation": global_recommendation,
            "decision_rules": decision_rules,
            "forecast": forecast,
        }
        logger.info(
            "Expansion recommendation decision=%s branches=%s capacity=%s",
            decision,
            recommended_count,
            payload["capacity_investment"],
        )
        if user is not None:
            log_event(
                user,
                "EXPANSION_OPENING_RECOMMENDATION",
                "reportes.ExpansionPolicyConfig",
                expansion_context["global_summary"]["policy"]["id"] or 0,
                payload=_json_safe(payload),
            )
        return payload

    def _peer_rows(
        self,
        *,
        base_project: ProyectoInversion | None,
        tipo_proyecto: str,
    ) -> list[dict[str, object]]:
        queryset = ProyectoInversion.objects.prefetch_related(
            Prefetch(
                "snapshots_mensuales",
                queryset=ProyectoInversionSnapshotMensual.objects.order_by("-periodo"),
                to_attr="prefetched_snapshots_for_forecast",
            )
        )
        if tipo_proyecto:
            queryset = queryset.filter(tipo_proyecto=tipo_proyecto)
        queryset = queryset.exclude(estatus=ProyectoInversion.ESTATUS_CANCELADO)
        rows: list[dict[str, object]] = []
        for project in queryset.order_by("-fecha_apertura", "-fecha_inicio", "-id"):
            snapshots = list(getattr(project, "prefetched_snapshots_for_forecast", [])[:6])
            if not snapshots:
                continue
            last_three = list(reversed(snapshots[:3]))
            sales_3m = [_as_decimal(row.ventas_mensuales) for row in last_three if _as_decimal(row.ventas_mensuales) > ZERO]
            gross_margin_pcts = [
                _safe_div(_as_decimal(row.utilidad_bruta), _as_decimal(row.ventas_mensuales))
                for row in last_three
                if _as_decimal(row.ventas_mensuales) > ZERO and row.utilidad_bruta is not None
            ]
            margin_pcts = [
                _safe_div(_as_decimal(row.utilidad_operativa), _as_decimal(row.ventas_mensuales))
                for row in last_three
                if _as_decimal(row.ventas_mensuales) > ZERO and row.utilidad_operativa is not None
            ]
            operating_profits = [_as_decimal(row.utilidad_operativa) for row in last_three if row.utilidad_operativa is not None]
            operating_expenses = [_as_decimal(row.gastos_operativos) for row in last_three if row.gastos_operativos is not None]
            rents = [_as_decimal(row.renta) for row in last_three if row.renta is not None]
            debt_services = [_as_decimal(row.servicio_deuda) for row in last_three if row.servicio_deuda is not None]
            recovery_rates = [
                _safe_div(_as_decimal(row.flujo_para_recuperacion), _as_decimal(row.flujo_libre))
                for row in last_three
                if _as_decimal(row.flujo_libre) > ZERO and row.flujo_para_recuperacion is not None
            ]
            latest = snapshots[0]
            rows.append(
                {
                    "project": project,
                    "latest_snapshot": latest,
                    "snapshot_count": len(last_three),
                    "average_sales_3m": _average_decimals(sales_3m),
                    "average_gross_margin_pct": _average_decimals([value for value in gross_margin_pcts if value is not None]),
                    "average_operating_margin_pct": _average_decimals([value for value in margin_pcts if value is not None]),
                    "average_operating_profit_3m": _average_decimals(operating_profits),
                    "average_operating_expenses": _average_decimals(operating_expenses),
                    "average_rent": _average_decimals(rents),
                    "average_debt_service": _average_decimals(debt_services),
                    "average_recovery_rate": _average_decimals([value for value in recovery_rates if value is not None]),
                    "investment": _as_decimal(project.monto_inversion_real or project.monto_inversion_planeado),
                    "health_score_reference": int(latest.health_score or 0),
                    "roi_target": _as_decimal(project.roi_objetivo),
                    "payback_target": Decimal(project.payback_objetivo_meses or 0),
                }
            )
        if base_project is not None:
            base_first = [row for row in rows if row["project"].pk == base_project.pk]
            peers = [row for row in rows if row["project"].pk != base_project.pk]
            return base_first + peers
        return rows

    def _peer_summary(self, rows: list[dict[str, object]]) -> dict[str, object]:
        data_gaps: list[str] = []
        if not rows:
            data_gaps.append("No existen proyectos comparables con snapshots suficientes.")
            return {
                "average_sales_3m": None,
                "average_gross_margin_pct": None,
                "average_operating_margin_pct": None,
                "average_operating_profit_3m": None,
                "average_operating_expenses": None,
                "average_debt_service": None,
                "average_payback": None,
                "average_health_score": 0,
                "average_cash_on_cash": None,
                "average_investment": None,
                "average_recovery_rate": None,
                "average_roi_target": None,
                "average_payback_target": None,
                "average_months_used": 0,
                "data_gaps": data_gaps,
            }

        average_sales = _average_decimals([_as_decimal(row["average_sales_3m"]) for row in rows if row["average_sales_3m"] is not None])
        average_gross_margin_pct = _average_decimals([_as_decimal(row["average_gross_margin_pct"]) for row in rows if row["average_gross_margin_pct"] is not None])
        average_operating_margin_pct = _average_decimals([_as_decimal(row["average_operating_margin_pct"]) for row in rows if row["average_operating_margin_pct"] is not None])
        average_operating_profit = _average_decimals([_as_decimal(row["average_operating_profit_3m"]) for row in rows if row["average_operating_profit_3m"] is not None])
        average_operating_expenses = _average_decimals([_as_decimal(row["average_operating_expenses"]) for row in rows if row["average_operating_expenses"] is not None])
        average_debt_service = _average_decimals([_as_decimal(row["average_debt_service"]) for row in rows if row["average_debt_service"] is not None])
        average_payback = _average_decimals(
            [
                _as_decimal(row["latest_snapshot"].payback_real_meses or row["latest_snapshot"].payback_forecast_meses)
                for row in rows
                if row["latest_snapshot"].payback_real_meses is not None or row["latest_snapshot"].payback_forecast_meses is not None
            ]
        )
        average_cash_on_cash = _average_decimals(
            [
                _as_decimal(row["latest_snapshot"].cash_on_cash)
                for row in rows
                if row["latest_snapshot"].cash_on_cash is not None
            ]
        )
        average_investment = _average_decimals([_as_decimal(row["investment"]) for row in rows if _as_decimal(row["investment"]) > ZERO])
        average_recovery_rate = _average_decimals([_as_decimal(row["average_recovery_rate"]) for row in rows if row["average_recovery_rate"] is not None])
        average_roi_target = _average_decimals([_as_decimal(row["roi_target"]) for row in rows if _as_decimal(row["roi_target"]) > ZERO])
        average_payback_target = _average_decimals([_as_decimal(row["payback_target"]) for row in rows if _as_decimal(row["payback_target"]) > ZERO])
        health_scores = [int(row["latest_snapshot"].health_score or 0) for row in rows]

        if average_sales is None:
            data_gaps.append("No hay suficientes ventas mensuales comparables en snapshots.")
        if average_gross_margin_pct is None:
            data_gaps.append("No hay suficiente margen bruto comparable en snapshots.")
        if average_operating_margin_pct is None:
            data_gaps.append("No hay suficiente margen operativo comparable en snapshots.")
        if average_operating_expenses is None:
            data_gaps.append("No hay suficiente gasto operativo comparable en snapshots.")
        if average_investment is None:
            data_gaps.append("No hay suficiente inversión histórica comparable.")

        return {
            "average_sales_3m": _quantize(average_sales) if average_sales is not None else None,
            "average_gross_margin_pct": average_gross_margin_pct,
            "average_operating_margin_pct": average_operating_margin_pct,
            "average_operating_profit_3m": _quantize(average_operating_profit) if average_operating_profit is not None else None,
            "average_operating_expenses": _quantize(average_operating_expenses) if average_operating_expenses is not None else None,
            "average_debt_service": _quantize(average_debt_service) if average_debt_service is not None else None,
            "average_payback": _quantize(average_payback) if average_payback is not None else None,
            "average_health_score": round(sum(health_scores) / len(health_scores)) if health_scores else 0,
            "average_cash_on_cash": _quantize(average_cash_on_cash) if average_cash_on_cash is not None else None,
            "average_investment": _quantize(average_investment) if average_investment is not None else None,
            "average_recovery_rate": average_recovery_rate,
            "average_roi_target": _quantize(average_roi_target, pattern=Decimal("0.0001")) if average_roi_target is not None else None,
            "average_payback_target": _quantize(average_payback_target) if average_payback_target is not None else None,
            "average_months_used": round(sum(int(row["snapshot_count"] or 0) for row in rows) / len(rows)) if rows else 0,
            "data_gaps": data_gaps,
        }

    def _base_reference_row(
        self,
        *,
        base_project: ProyectoInversion | None,
        rows: list[dict[str, object]],
    ) -> dict[str, object] | None:
        if base_project is None:
            return None
        for row in rows:
            if row["project"].pk == base_project.pk:
                return row
        return None

    def _metric_or_summary(
        self,
        base_row: dict[str, object] | None,
        key: str,
        summary_value,
    ):
        if base_row is not None and base_row.get(key) is not None:
            return base_row.get(key)
        return summary_value

    def _resolve_recovery_rate(
        self,
        *,
        base_project: ProyectoInversion | None,
        base_row: dict[str, object] | None,
    ) -> Decimal:
        if base_row is not None and base_row.get("average_recovery_rate") is not None:
            return _as_decimal(base_row["average_recovery_rate"])
        if base_project is not None:
            if _as_decimal(base_project.recovery_percentage) > ZERO:
                return _as_decimal(base_project.recovery_percentage)
            if _as_decimal(base_project.porcentaje_utilidad_destinado_a_recuperacion) > ZERO:
                return _as_decimal(base_project.porcentaje_utilidad_destinado_a_recuperacion)
        return Decimal("1")

    def _reference_project(
        self,
        *,
        base_project: ProyectoInversion | None,
        summary: dict[str, object],
        effective_investment: Decimal | None,
    ) -> ProyectoInversion:
        if base_project is not None:
            return base_project
        return ProyectoInversion(
            nombre_proyecto="Referencia expansión",
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            fecha_inicio=timezone.localdate(),
            monto_inversion_planeado=_as_decimal(effective_investment),
            monto_inversion_real=_as_decimal(effective_investment),
            roi_objetivo=_as_decimal(summary.get("average_roi_target")),
            payback_objetivo_meses=int(_as_decimal(summary.get("average_payback_target")) or 0),
            recovery_strategy=ProyectoInversion.RECOVERY_FULL_NET_CASHFLOW,
            recovery_percentage=Decimal("1"),
        )

    def _projected_recovery_cashflow(
        self,
        *,
        project: ProyectoInversion,
        projected_operating_profit: Decimal | None,
        projected_debt_service: Decimal | None,
        projected_free_cashflow: Decimal | None,
        recovery_rate: Decimal,
    ) -> Decimal | None:
        if projected_free_cashflow is None:
            return None
        normalized_rate = recovery_rate if recovery_rate > ZERO else Decimal("1")
        if project.recovery_strategy == ProyectoInversion.RECOVERY_PERCENTAGE_OF_PROFIT:
            base_amount = max(_as_decimal(projected_operating_profit), ZERO)
            return _quantize(base_amount * normalized_rate)
        if project.recovery_strategy == ProyectoInversion.RECOVERY_PROFIT_AFTER_DEBT_SERVICE:
            return _quantize(max(_as_decimal(projected_operating_profit) - _as_decimal(projected_debt_service), ZERO))
        return _quantize(max(_as_decimal(projected_free_cashflow), ZERO) * normalized_rate)

    def _historical_free_cashflow(
        self,
        *,
        base_row: dict[str, object] | None,
        reference_operating_profit: Decimal | None,
        reference_debt_service: Decimal | None,
    ) -> Decimal | None:
        latest_snapshot = base_row.get("latest_snapshot") if base_row is not None else None
        if latest_snapshot is not None and latest_snapshot.flujo_libre is not None:
            return _quantize(_as_decimal(latest_snapshot.flujo_libre))
        if reference_operating_profit is None and reference_debt_service is None:
            return None
        return _quantize(_as_decimal(reference_operating_profit) - _as_decimal(reference_debt_service))

    def _historical_payback(
        self,
        *,
        base_row: dict[str, object] | None,
        summary: dict[str, object],
    ) -> Decimal | None:
        latest_snapshot = base_row.get("latest_snapshot") if base_row is not None else None
        if latest_snapshot is not None:
            payback_value = latest_snapshot.payback_real_meses or latest_snapshot.payback_forecast_meses
            if payback_value is not None:
                return _quantize(_as_decimal(payback_value))
        average_payback = summary.get("average_payback")
        return _quantize(_as_decimal(average_payback)) if average_payback is not None else None

    def _historical_roi(self, *, base_row: dict[str, object] | None) -> Decimal | None:
        latest_snapshot = base_row.get("latest_snapshot") if base_row is not None else None
        if latest_snapshot is None or latest_snapshot.roi_acumulado is None:
            return None
        return _quantize(_as_decimal(latest_snapshot.roi_acumulado))

    def _risk_level(
        self,
        *,
        projected_free_cashflow: Decimal | None,
        projected_payback: Decimal | None,
        average_payback: Decimal | None,
        peer_health_score: int,
        data_gaps: list[str],
        scenario_buffer: int,
    ) -> str:
        score = 100
        if projected_free_cashflow is None or projected_free_cashflow <= ZERO:
            score -= 45
        if projected_payback is not None and average_payback is not None and projected_payback > (average_payback * Decimal("1.20")):
            score -= 25
        if peer_health_score < 65:
            score -= 20
        score -= min(len(set(data_gaps)) * 5, 20)
        score += scenario_buffer

        if score >= 75:
            return "BAJO"
        if score >= 50:
            return "MEDIO"
        return "ALTO"

    def _projected_close_date(self, payback_months: Decimal | None) -> date | None:
        if payback_months is None:
            return None
        months = int(max(payback_months, ZERO).to_integral_value(rounding=ROUND_HALF_UP))
        if months <= 0:
            return timezone.localdate()
        return timezone.localdate() + timedelta(days=months * 30)


def recomendar_apertura(
    *,
    base_project: ProyectoInversion | None = None,
    tipo_proyecto: str = "",
    investment_estimate: Decimal | None = None,
    monthly_rent: Decimal | None = None,
    sales_adjustment_pct: Decimal | None = None,
    scenario: str = ExpansionForecastService.SCENARIO_BASE,
    location_reference: str = "",
    forecast_payload: dict[str, object] | None = None,
    expansion_context: dict[str, object] | None = None,
    user=None,
) -> dict[str, object]:
    return ExpansionForecastService().recommend_opening(
        base_project=base_project,
        tipo_proyecto=tipo_proyecto,
        investment_estimate=investment_estimate,
        monthly_rent=monthly_rent,
        sales_adjustment_pct=sales_adjustment_pct,
        scenario=scenario,
        location_reference=location_reference,
        forecast_payload=forecast_payload,
        expansion_context=expansion_context,
        user=user,
    )
