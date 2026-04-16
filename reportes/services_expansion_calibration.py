from __future__ import annotations

from decimal import Decimal
import logging

from django.core.cache import cache
from django.utils import timezone

from core.audit import log_event
from reportes.models import ExpansionPolicyConfig, ProyectoInversion
from reportes.services_expansion_decision import ExpansionDecisionService
from reportes.services_investment_projects import (
    _as_decimal,
    _average_decimals,
    _deep_merge_dict,
    _default_calibration_settings,
    _forecast_payback_metrics,
    _get_calibration_settings,
    _json_safe,
    _percent_value,
    _quantize,
)

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
CALIBRATION_CACHE_TTL = 300


class ExpansionCalibrationService:
    REAL_CLASSIFICATION_CHOICES = (
        ("EXPANDIR", "Expandir"),
        ("VIGILAR", "Vigilar"),
        ("RIESGO", "Riesgo"),
    )

    def __init__(self) -> None:
        self.decision_service = ExpansionDecisionService()

    def get_policy(self) -> ExpansionPolicyConfig:
        return self.decision_service.get_policy()

    def get_settings(self) -> dict[str, object]:
        return _get_calibration_settings()

    def set_real_classification(self, project: ProyectoInversion, classification: str, *, user=None) -> None:
        normalized = (classification or "").strip().upper()
        metadata = dict(project.metadata or {})
        calibration_meta = dict(metadata.get("calibration") or {})
        previous = calibration_meta.get("real_classification", "")
        calibration_meta["real_classification"] = normalized
        calibration_meta["updated_at"] = timezone.now().isoformat()
        metadata["calibration"] = calibration_meta
        if metadata != (project.metadata or {}):
            project.metadata = metadata
            project.save(update_fields=["metadata", "actualizado_en"])
        cache.delete(self._cache_key("context"))
        if user is not None:
            log_event(
                user,
                "EXPANSION_REAL_CLASSIFICATION_SET",
                "reportes.ProyectoInversion",
                project.pk,
                payload=_json_safe({"from": previous, "to": normalized}),
            )

    def update_settings(self, *, payload: dict[str, object], user=None) -> dict[str, object]:
        policy = self.get_policy()
        metadata = dict(policy.metadata or {})
        current = metadata.get("calibration", {})
        metadata["calibration"] = _deep_merge_dict(current, payload)
        policy.metadata = metadata
        policy.save(update_fields=["metadata", "actualizado_en"])
        cache.delete(self._cache_key("context"))
        if user is not None:
            log_event(
                user,
                "EXPANSION_CALIBRATION_SETTINGS_UPDATE",
                "reportes.ExpansionPolicyConfig",
                policy.pk or 0,
                payload=_json_safe(payload),
            )
        return self.get_settings()

    def build_context(
        self,
        *,
        tipo_proyecto: str = "",
        estatus: str = "",
        fecha_inicio_desde=None,
        fecha_inicio_hasta=None,
        calibration_settings: dict[str, object] | None = None,
        use_cache: bool = True,
    ) -> dict[str, object]:
        settings = calibration_settings or self.get_settings()
        cache_key = self._cache_key(
            "context",
            tipo_proyecto=tipo_proyecto,
            estatus=estatus,
            fecha_inicio_desde=str(fecha_inicio_desde or ""),
            fecha_inicio_hasta=str(fecha_inicio_hasta or ""),
            signature=self._settings_signature(settings),
        )
        if use_cache:
            cached = cache.get(cache_key)
            if cached is not None:
                return cached

        expansion_context = self.decision_service.build_expansion_context(
            tipo_proyecto=tipo_proyecto,
            estatus=estatus,
            fecha_inicio_desde=fecha_inicio_desde,
            fecha_inicio_hasta=fecha_inicio_hasta,
            calibration_settings=settings,
            persist=False,
        )
        min_months = int(settings.get("minimum_months", 6) or 6)
        rows = []
        labeled_rows = []
        insufficient_history_count = 0
        for row in expansion_context["decision_rows"]:
            recent_count = int(row.get("available_months", 0) or 0)
            enriched = {
                **row,
                "recent_month_count": recent_count,
                "is_eligible": recent_count >= min_months,
                "difference": "OK"
                if row["matches_real_classification"] is True
                else "SIN REFERENCIA"
                if row["matches_real_classification"] is None
                else "DIFIERE",
            }
            rows.append(enriched)
            if not enriched["is_eligible"]:
                insufficient_history_count += 1
            if row["real_classification"]:
                labeled_rows.append(enriched)

        accurate = sum(1 for row in labeled_rows if row["matches_real_classification"] is True)
        total_labeled = len(labeled_rows)
        accuracy_pct = _percent_value(Decimal(accurate), Decimal(total_labeled)) if total_labeled else None
        incorrect_cases = [row for row in labeled_rows if row["matches_real_classification"] is False]
        forecast_metrics = self._forecast_accuracy(rows, settings=settings)

        context = {
            "rows": rows,
            "labeled_rows": labeled_rows,
            "incorrect_cases": incorrect_cases,
            "accuracy": {
                "accurate_cases": accurate,
                "labeled_cases": total_labeled,
                "accuracy_pct": accuracy_pct,
                "insufficient_history_count": insufficient_history_count,
                "minimum_sample_projects": int(settings.get("minimum_sample_projects", 3) or 3),
                "minimum_months": min_months,
            },
            "forecast_accuracy": forecast_metrics,
            "settings": settings,
            "data_gaps": self._data_gaps(total_labeled, rows, settings),
            "errors_common": self._common_errors(incorrect_cases),
            "generated_at": timezone.now(),
        }
        if use_cache:
            cache.set(cache_key, context, CALIBRATION_CACHE_TTL)
        return context

    def calibrate(self, *, user=None) -> dict[str, object]:
        base_settings = self.get_settings()
        baseline = self.build_context(calibration_settings=base_settings, use_cache=False)
        candidate_rows = [row for row in baseline["rows"] if row["real_classification"] and row["is_eligible"]]
        minimum_sample_projects = int(base_settings.get("minimum_sample_projects", 3) or 3)
        if len(candidate_rows) < minimum_sample_projects:
            return {
                "applied": False,
                "reason": "No hay suficientes sucursales etiquetadas con clasificación real y al menos 6 meses de histórico.",
                "baseline": baseline,
            }

        current_accuracy = _as_decimal(baseline["accuracy"]["accuracy_pct"])
        current_forecast_mae = _as_decimal(baseline["forecast_accuracy"]["payback_mae_months"])
        best = {
            "settings": base_settings,
            "accuracy_pct": current_accuracy,
            "forecast_mae": current_forecast_mae if current_forecast_mae > ZERO else Decimal("999999"),
            "context": baseline,
        }

        for candidate in self._candidate_settings(base_settings):
            context = self.build_context(calibration_settings=candidate, use_cache=False)
            accuracy_pct = _as_decimal(context["accuracy"]["accuracy_pct"])
            forecast_mae = _as_decimal(context["forecast_accuracy"]["payback_mae_months"])
            forecast_rank = forecast_mae if forecast_mae > ZERO else Decimal("999999")
            if (
                accuracy_pct > best["accuracy_pct"]
                or (accuracy_pct == best["accuracy_pct"] and forecast_rank < best["forecast_mae"])
            ):
                best = {
                    "settings": candidate,
                    "accuracy_pct": accuracy_pct,
                    "forecast_mae": forecast_rank,
                    "context": context,
                }

        improvement = _quantize(best["accuracy_pct"] - current_accuracy, pattern=Decimal("0.0001"))
        calibration_payload = {
            "mode_enabled": True,
            "health_weights": best["settings"]["health_weights"],
            "classification_thresholds": best["settings"]["classification_thresholds"],
            "forecast": best["settings"]["forecast"],
            "last_calibration_at": timezone.now().isoformat(),
            "last_accuracy_pct": str(best["accuracy_pct"]),
            "last_forecast_mae_months": str(best["context"]["forecast_accuracy"]["payback_mae_months"] or ""),
            "last_improvement_pct": str(improvement or ZERO),
        }
        self.update_settings(payload=calibration_payload, user=user)
        logger.info(
            "Expansion calibration aplicada accuracy_before=%s accuracy_after=%s",
            current_accuracy,
            best["accuracy_pct"],
        )
        if user is not None:
            log_event(
                user,
                "EXPANSION_CALIBRATION_RUN",
                "reportes.ExpansionPolicyConfig",
                self.get_policy().pk or 0,
                payload=_json_safe(
                    {
                        "accuracy_before_pct": current_accuracy,
                        "accuracy_after_pct": best["accuracy_pct"],
                        "forecast_mae_after": best["context"]["forecast_accuracy"]["payback_mae_months"],
                        "improvement_pct": improvement,
                    }
                ),
            )
        cache.delete(self._cache_key("context"))
        return {
            "applied": True,
            "baseline": baseline,
            "result": best["context"],
            "improvement_pct": improvement,
            "settings": best["settings"],
        }

    def _forecast_accuracy(self, rows: list[dict[str, object]], *, settings: dict[str, object]) -> dict[str, object]:
        errors: list[Decimal] = []
        cases = 0
        for row in rows:
            latest_snapshot = row.get("latest_snapshot")
            project = row["project"]
            if latest_snapshot is None or latest_snapshot.payback_real_meses is None:
                continue
            snapshots = list(project.snapshots_mensuales.order_by("periodo")[:12])
            payload = [{"period": snapshot.periodo, "free_cashflow": _as_decimal(snapshot.flujo_libre)} for snapshot in snapshots]
            forecast = _forecast_payback_metrics(
                project,
                payload,
                _as_decimal(latest_snapshot.saldo_pendiente),
                calibration_settings=settings,
            )
            estimated = forecast["payback_months"]
            if estimated is None:
                continue
            errors.append(abs(_as_decimal(estimated) - _as_decimal(latest_snapshot.payback_real_meses)))
            cases += 1
        mae = _average_decimals(errors) if errors else None
        return {
            "evaluated_cases": cases,
            "payback_mae_months": _quantize(mae) if mae is not None else None,
        }

    def _candidate_settings(self, base_settings: dict[str, object]) -> list[dict[str, object]]:
        base = _deep_merge_dict(_default_calibration_settings(), base_settings)
        weight_sets = [
            {"roi": Decimal("25"), "free_cashflow": Decimal("20"), "sales_growth": Decimal("15"), "capex": Decimal("20"), "recovery": Decimal("20")},
            {"roi": Decimal("30"), "free_cashflow": Decimal("20"), "sales_growth": Decimal("10"), "capex": Decimal("15"), "recovery": Decimal("25")},
            {"roi": Decimal("20"), "free_cashflow": Decimal("30"), "sales_growth": Decimal("10"), "capex": Decimal("15"), "recovery": Decimal("25")},
            {"roi": Decimal("20"), "free_cashflow": Decimal("20"), "sales_growth": Decimal("25"), "capex": Decimal("15"), "recovery": Decimal("20")},
            {"roi": Decimal("20"), "free_cashflow": Decimal("20"), "sales_growth": Decimal("10"), "capex": Decimal("30"), "recovery": Decimal("20")},
            {"roi": Decimal("20"), "free_cashflow": Decimal("20"), "sales_growth": Decimal("10"), "capex": Decimal("15"), "recovery": Decimal("35")},
        ]
        candidates: list[dict[str, object]] = []
        for weights in weight_sets:
            for expand_min in (75, 80, 85):
                for monitor_min in (45, 50, 55):
                    if monitor_min >= expand_min:
                        continue
                    for payback_tolerance in (Decimal("0.95"), Decimal("1.00"), Decimal("1.10"), Decimal("1.20")):
                        for roi_target_factor in (Decimal("0.90"), Decimal("1.00"), Decimal("1.10")):
                            for preferred_window in (3, 6):
                                candidate = _deep_merge_dict(
                                    base,
                                    {
                                        "health_weights": weights,
                                        "classification_thresholds": {
                                            "expand_min_health_score": expand_min,
                                            "monitor_min_health_score": monitor_min,
                                            "payback_tolerance_ratio": payback_tolerance,
                                            "roi_target_factor": roi_target_factor,
                                        },
                                        "forecast": {
                                            "preferred_window_months": preferred_window,
                                            "fallback_window_months": 6,
                                            "negative_months_mode": "fallback_to_fallback_window" if preferred_window == 3 else "include",
                                            "moving_average_months": preferred_window,
                                        },
                                    },
                                )
                                candidates.append(candidate)
        return candidates

    def _common_errors(self, incorrect_cases: list[dict[str, object]]) -> list[dict[str, object]]:
        summary: dict[str, int] = {}
        for row in incorrect_cases:
            key = f"{row['classification']} vs {row['real_classification']}"
            summary[key] = summary.get(key, 0) + 1
        return [{"pattern": key, "count": value} for key, value in sorted(summary.items(), key=lambda item: (-item[1], item[0]))[:5]]

    def _data_gaps(self, labeled_count: int, rows: list[dict[str, object]], settings: dict[str, object]) -> list[str]:
        gaps: list[str] = []
        if labeled_count < int(settings.get("minimum_sample_projects", 3) or 3):
            gaps.append("Se necesitan al menos 3 sucursales con clasificación real capturada para calibrar con utilidad estadística.")
        eligible_rows = [row for row in rows if row["is_eligible"]]
        if len(eligible_rows) < int(settings.get("minimum_sample_projects", 3) or 3):
            gaps.append("Se necesitan al menos 3 sucursales con 6 meses o más de snapshots para calibrar correctamente.")
        return gaps

    def _settings_signature(self, settings: dict[str, object]) -> str:
        weights = settings.get("health_weights", {})
        thresholds = settings.get("classification_thresholds", {})
        forecast = settings.get("forecast", {})
        return "|".join(
            [
                str(weights.get("roi")),
                str(weights.get("free_cashflow")),
                str(weights.get("sales_growth")),
                str(weights.get("capex")),
                str(weights.get("recovery")),
                str(thresholds.get("expand_min_health_score")),
                str(thresholds.get("monitor_min_health_score")),
                str(thresholds.get("payback_tolerance_ratio")),
                str(thresholds.get("roi_target_factor")),
                str(forecast.get("preferred_window_months")),
                str(forecast.get("negative_months_mode")),
            ]
        )

    def _cache_key(self, suffix: str, **kwargs) -> str:
        policy = self.get_policy()
        parts = [f"{key}={value}" for key, value in sorted(kwargs.items())]
        return f"reportes:expansion:calibration:{suffix}:{policy.pk or 0}:{policy.actualizado_en.isoformat() if policy.pk else 'na'}:{'|'.join(parts)}"
