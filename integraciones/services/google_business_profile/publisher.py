from __future__ import annotations

from datetime import timedelta

from django.core.exceptions import ImproperlyConfigured

from horarios_especiales.models import HorarioEspecialDetalle, SucursalPlataformaExterna
from integraciones.services.google_business_profile.client import GoogleBusinessProfileClient


class GoogleBusinessProfilePublisher:
    def __init__(self, client: GoogleBusinessProfileClient | None = None):
        self.client = client or GoogleBusinessProfileClient()

    @staticmethod
    def _build_special_hour_periods(*, detail: HorarioEspecialDetalle) -> list[dict]:
        target = detail.target_date
        if detail.closed_all_day:
            return [
                {
                    "startDate": {
                        "year": target.year,
                        "month": target.month,
                        "day": target.day,
                    },
                    "closed": True,
                }
            ]
        periods: list[dict] = []
        for window in detail.time_windows_json:
            periods.append(
                {
                    "startDate": {"year": target.year, "month": target.month, "day": target.day},
                    "endDate": {"year": target.year, "month": target.month, "day": target.day},
                    "openTime": window["open"],
                    "closeTime": window["close"],
                }
            )
        return periods

    @staticmethod
    def _filter_existing_periods(*, existing_periods: list[dict], detail: HorarioEspecialDetalle) -> list[dict]:
        target = detail.target_date
        next_day = target + timedelta(days=1)
        filtered: list[dict] = []
        for period in existing_periods:
            start = period.get("startDate") or {}
            end = period.get("endDate") or start
            start_tuple = (start.get("year"), start.get("month"), start.get("day"))
            end_tuple = (end.get("year"), end.get("month"), end.get("day"))
            target_tuple = (target.year, target.month, target.day)
            next_day_tuple = (next_day.year, next_day.month, next_day.day)
            if start_tuple == target_tuple or end_tuple in {target_tuple, next_day_tuple}:
                continue
            filtered.append(period)
        return filtered

    def publish_detail(self, *, detail: HorarioEspecialDetalle, config: SucursalPlataformaExterna) -> dict:
        location_name = str(config.external_location_name or "").strip()
        if not location_name:
            raise ImproperlyConfigured("La configuración externa no contiene external_location_name.")

        current = self.client.get_location(location_name=location_name)
        if not ((current.get("regularHours") or {}).get("periods") or []):
            raise ValueError("Google Business Profile requiere regularHours antes de publicar specialHours.")

        existing_periods = list((((current.get("specialHours") or {}).get("specialHourPeriods")) or []))
        replacement_periods = self._build_special_hour_periods(detail=detail)
        merged_periods = self._filter_existing_periods(existing_periods=existing_periods, detail=detail) + replacement_periods

        if existing_periods == merged_periods:
            return {
                "noop": True,
                "request_payload": {"name": location_name, "specialHours": {"specialHourPeriods": merged_periods}},
                "response_payload": current,
                "operation_id": "",
            }

        response = self.client.patch_special_hours(
            location_name=location_name,
            special_hour_periods=merged_periods,
            validate_only=False,
        )
        return {
            "noop": False,
            "request_payload": {"name": location_name, "specialHours": {"specialHourPeriods": merged_periods}},
            "response_payload": response,
            "operation_id": str(response.get("name") or ""),
        }

