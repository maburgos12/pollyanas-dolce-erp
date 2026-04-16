from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db.models import QuerySet
from django.utils import timezone

from core.audit import log_event
from core.models import AuditLog
from reportes.models import CargaGastoOperativoArchivo, ProyectoInversion, ProyectoInversionEscenario
from reportes.services_investment_projects import _as_decimal, _json_safe, _quantize

User = get_user_model()


class ExpansionSimulationRegistryService:
    RECENT_ACTIVITY_MODELS = {
        "reportes.ProyectoInversionEscenario",
        "reportes.CargaGastoOperativoArchivo",
        "reportes.GastoOperativoMensual",
        "reportes.ProyectoInversion",
    }

    def build_simulation_hash(
        self,
        *,
        project: ProyectoInversion,
        forecast_payload: dict[str, object],
    ) -> str:
        raw = {
            "project_id": project.pk,
            "scenario": forecast_payload.get("scenario"),
            "inputs": forecast_payload.get("inputs", {}),
        }
        payload = json.dumps(_json_safe(raw), sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def save_simulation(
        self,
        *,
        project: ProyectoInversion,
        forecast_payload: dict[str, object],
        opening_recommendation: dict[str, object],
        comparison_rows: list[dict[str, object]],
        sensitivity_rows: list[dict[str, object]],
        executive_note: str = "",
        status: str = ProyectoInversionEscenario.ESTATUS_EN_REVISION,
        user=None,
    ) -> tuple[ProyectoInversionEscenario, bool]:
        simulation_hash = self.build_simulation_hash(project=project, forecast_payload=forecast_payload)
        created_at = timezone.localtime()
        scenario_name = self._scenario_name(
            project=project,
            scenario_label=str(forecast_payload.get("scenario_label") or "Base"),
            created_at=created_at,
        )
        outputs = forecast_payload.get("outputs", {}) or {}
        projected_sales = _as_decimal(outputs.get("projected_sales"))
        projected_gross_profit = _as_decimal(outputs.get("projected_gross_profit"))
        margin_pct = Decimal("0")
        if projected_sales > 0 and projected_gross_profit > 0:
            margin_pct = _quantize((projected_gross_profit / projected_sales) * Decimal("100")) or Decimal("0")

        scenario_defaults = {
            "nombre": scenario_name,
            "tipo_escenario": str(forecast_payload.get("scenario") or ProyectoInversionEscenario.TIPO_BASE),
            "ventas_promedio_mensuales": projected_sales,
            "crecimiento_mensual_pct": _as_decimal((forecast_payload.get("inputs", {}) or {}).get("sales_adjustment_pct")),
            "margen_bruto_pct": margin_pct,
            "gastos_operativos_mensuales": _as_decimal(outputs.get("projected_operating_expenses")),
            "horizonte_meses": 12,
            "estatus_simulacion": status or ProyectoInversionEscenario.ESTATUS_EN_REVISION,
            "capturado_por": user if getattr(user, "is_authenticated", False) else None,
            "notas": executive_note.strip(),
            "resultados": self._result_payload(
                project=project,
                forecast_payload=forecast_payload,
                opening_recommendation=opening_recommendation,
                comparison_rows=comparison_rows,
                sensitivity_rows=sensitivity_rows,
                simulation_hash=simulation_hash,
                executive_note=executive_note,
                status=status,
                created_at=created_at,
                user=user,
            ),
        }
        scenario, created = ProyectoInversionEscenario.objects.update_or_create(
            proyecto=project,
            simulacion_hash=simulation_hash,
            defaults=scenario_defaults,
        )
        action = "CREATE" if created else "UPDATE"
        log_event(
            user,
            action,
            "reportes.ProyectoInversionEscenario",
            scenario.pk,
            payload={
                "project_id": project.pk,
                "simulation_hash": simulation_hash,
                "decision": opening_recommendation.get("decision"),
                "status": scenario.estatus_simulacion,
            },
        )
        return scenario, created

    def update_status(
        self,
        *,
        scenario: ProyectoInversionEscenario,
        status: str,
        executive_note: str = "",
        user=None,
    ) -> ProyectoInversionEscenario:
        scenario.estatus_simulacion = status
        scenario.notas = executive_note.strip()
        payload = dict(scenario.resultados or {})
        payload["saved_status"] = status
        payload["executive_note"] = scenario.notas
        payload["updated_at"] = timezone.now().isoformat()
        scenario.resultados = _json_safe(payload)
        update_fields = ["estatus_simulacion", "notas", "resultados", "actualizado_en"]
        if not scenario.capturado_por_id and getattr(user, "is_authenticated", False):
            scenario.capturado_por = user
            update_fields.append("capturado_por")
        scenario.save(update_fields=update_fields)
        log_event(
            user,
            "UPDATE_STATUS",
            "reportes.ProyectoInversionEscenario",
            scenario.pk,
            payload={"status": status, "executive_note": scenario.notas},
        )
        return scenario

    def recent_simulations(self, *, limit: int = 8) -> list[ProyectoInversionEscenario]:
        queryset = (
            ProyectoInversionEscenario.objects.select_related(
                "proyecto",
                "proyecto__sucursal_relacionada",
                "capturado_por",
            )
            .exclude(simulacion_hash="")
            .order_by("-actualizado_en", "-id")
        )
        return list(queryset[:limit])

    def serialize_simulation(self, scenario: ProyectoInversionEscenario) -> dict[str, object]:
        payload = scenario.resultados or {}
        forecast_payload = payload.get("forecast_payload", {}) or {}
        recommendation = payload.get("opening_recommendation", {}) or {}
        outputs = forecast_payload.get("outputs", {}) or {}
        inputs = forecast_payload.get("inputs", {}) or {}
        return {
            "id": scenario.pk,
            "name": scenario.nombre,
            "project_id": scenario.proyecto_id,
            "project_name": scenario.proyecto.nombre_proyecto,
            "branch_label": scenario.proyecto.sucursal_relacionada.nombre if scenario.proyecto.sucursal_relacionada else scenario.proyecto.nombre_proyecto,
            "created_at": scenario.creado_en,
            "updated_at": scenario.actualizado_en,
            "captured_by": scenario.capturado_por,
            "status": scenario.estatus_simulacion,
            "executive_note": scenario.notas,
            "scenario_label": forecast_payload.get("scenario_label") or scenario.get_tipo_escenario_display(),
            "investment_estimate": inputs.get("investment_estimate"),
            "monthly_rent": inputs.get("monthly_rent"),
            "sales_adjustment_pct": inputs.get("sales_adjustment_pct"),
            "recommendation": recommendation.get("decision"),
            "recommendation_label": self._decision_display_label(recommendation.get("decision")),
            "projected_sales": outputs.get("projected_sales"),
            "projected_operating_expenses": outputs.get("projected_operating_expenses"),
            "projected_operating_profit": outputs.get("projected_operating_profit"),
            "projected_free_cashflow": outputs.get("projected_free_cashflow"),
            "projected_recovery_cashflow": outputs.get("projected_recovery_cashflow"),
            "projected_payback_months": outputs.get("projected_payback_months"),
            "projected_roi_pct": outputs.get("projected_roi_pct"),
            "projected_health_score": outputs.get("projected_health_score"),
            "comparison_rows": payload.get("comparison_rows", []),
            "sensitivity_rows": payload.get("sensitivity_rows", []),
            "decision_rules": recommendation.get("decision_rules", []),
            "forecast_payload": forecast_payload,
            "opening_recommendation": recommendation,
        }

    def load_saved_simulation(self, scenario: ProyectoInversionEscenario) -> dict[str, object]:
        payload = scenario.resultados or {}
        forecast_payload = payload.get("forecast_payload", {}) or {}
        opening_recommendation = payload.get("opening_recommendation", {}) or {}
        inputs = forecast_payload.get("inputs", {}) or {}
        return {
            "forecast_payload": forecast_payload,
            "opening_recommendation": opening_recommendation,
            "comparison_rows": payload.get("comparison_rows", []),
            "sensitivity_rows": payload.get("sensitivity_rows", []),
            "forecast_form": {
                "base_project_id": str(scenario.proyecto_id),
                "scenario": str(forecast_payload.get("scenario") or scenario.tipo_escenario),
                "investment_estimate": str(inputs.get("investment_estimate") or ""),
                "monthly_rent_reference": str(inputs.get("monthly_rent") or ""),
                "sales_adjustment_reference": str(inputs.get("sales_adjustment_pct") or ""),
            },
        }

    def compare_simulations(self, scenario_ids: list[int]) -> dict[str, object]:
        simulations = list(
            ProyectoInversionEscenario.objects.select_related("proyecto", "proyecto__sucursal_relacionada")
            .filter(pk__in=scenario_ids)
            .exclude(simulacion_hash="")
            .order_by("-actualizado_en", "-id")
        )
        serialized = [self.serialize_simulation(item) for item in simulations]
        metric_specs = [
            ("Escenario", "scenario_label", "text"),
            ("Inversion", "investment_estimate", "currency"),
            ("Ventas", "projected_sales", "currency"),
            ("Flujo libre", "projected_free_cashflow", "currency"),
            ("Payback", "projected_payback_months", "months"),
            ("ROI", "projected_roi_pct", "percent"),
            ("Health score", "projected_health_score", "score"),
            ("Recomendacion", "recommendation_label", "text"),
            ("Estatus", "status", "status"),
        ]
        rows: list[dict[str, object]] = []
        for label, key, metric_type in metric_specs:
            values = []
            for item in serialized:
                values.append(
                    {
                        "scenario_id": item["id"],
                        "display": self._format_metric(item.get(key), metric_type),
                    }
                )
            rows.append({"label": label, "values": values})
        return {"scenarios": serialized, "rows": rows}

    def latest_import_runs(self, *, limit: int = 5) -> list[CargaGastoOperativoArchivo]:
        queryset = (
            CargaGastoOperativoArchivo.objects.select_related("uploaded_by")
            .order_by("-uploaded_at", "-id")
        )
        return list(queryset[:limit])

    def recent_activity(self, *, limit: int = 10) -> list[AuditLog]:
        queryset: QuerySet[AuditLog] = (
            AuditLog.objects.select_related("user")
            .filter(model__in=self.RECENT_ACTIVITY_MODELS)
            .order_by("-timestamp", "-id")
        )
        return list(queryset[:limit])

    def _result_payload(
        self,
        *,
        project: ProyectoInversion,
        forecast_payload: dict[str, object],
        opening_recommendation: dict[str, object],
        comparison_rows: list[dict[str, object]],
        sensitivity_rows: list[dict[str, object]],
        simulation_hash: str,
        executive_note: str,
        status: str,
        created_at,
        user=None,
    ) -> dict[str, object]:
        outputs = forecast_payload.get("outputs", {}) or {}
        return _json_safe(
            {
                "simulation_hash": simulation_hash,
                "saved_at": created_at.isoformat(),
                "saved_status": status,
                "executive_note": executive_note.strip(),
                "base_project_id": project.pk,
                "base_project_name": project.nombre_proyecto,
                "forecast_payload": forecast_payload,
                "opening_recommendation": opening_recommendation,
                "comparison_rows": comparison_rows,
                "sensitivity_rows": sensitivity_rows,
                "payback_months": outputs.get("projected_payback_months"),
                "annual_roi_pct": outputs.get("projected_roi_pct"),
                "annual_cash_on_cash_pct": None,
                "monthly_operating_expenses": outputs.get("projected_operating_expenses"),
                "monthly_operating_cashflow": outputs.get("projected_operating_profit"),
                "monthly_free_cashflow": outputs.get("projected_free_cashflow"),
                "monthly_net_cashflow": outputs.get("projected_free_cashflow"),
                "monthly_recovery_amount": outputs.get("projected_recovery_cashflow"),
                "projected_close_date": outputs.get("projected_close_date"),
                "projection_rows": [],
                "saved_by": getattr(user, "username", "") if getattr(user, "is_authenticated", False) else "",
            }
        )

    def _scenario_name(self, *, project: ProyectoInversion, scenario_label: str, created_at) -> str:
        base_label = project.sucursal_relacionada.codigo if project.sucursal_relacionada_id else project.nombre_proyecto
        return f"Sim {base_label} {scenario_label} {created_at:%Y%m%d-%H%M}"

    def _decision_display_label(self, decision: str | None) -> str:
        return {
            "ABRIR": "RECOMENDADO",
            "ESPERAR": "RIESGOSO",
            "NO_ABRIR": "NO ABRIR",
        }.get((decision or "").strip().upper(), decision or "N/D")

    def _format_metric(self, value, metric_type: str) -> str:
        if value in (None, ""):
            return "N/D"
        if metric_type == "text":
            return str(value)
        if metric_type == "status":
            return str(value).replace("_", " ").title()
        if metric_type == "score":
            return f"{int(value)}/100"
        decimal_value = _as_decimal(value)
        if metric_type == "currency":
            return f"${decimal_value:,.2f}"
        if metric_type == "percent":
            return f"{decimal_value:,.2f}%"
        if metric_type == "months":
            return f"{decimal_value:,.2f} meses"
        return str(value)
