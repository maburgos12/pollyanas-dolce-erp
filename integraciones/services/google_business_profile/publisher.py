from __future__ import annotations

import json

from django.core.exceptions import ImproperlyConfigured
from django.db import connection

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
                    "openTime": google_time(window["open"]),
                    "closeTime": google_time(window["close"]),
                }
            )
        return periods

    @staticmethod
    def _filter_existing_periods(*, existing_periods: list[dict], detail: HorarioEspecialDetalle) -> list[dict]:
        target = detail.target_date
        filtered: list[dict] = []
        for period in existing_periods:
            start = period.get("startDate") or {}
            if (start.get("year"), start.get("month"), start.get("day")) == (target.year, target.month, target.day):
                continue
            filtered.append(period)
        return filtered

    def publish_detail(self, *, detail: HorarioEspecialDetalle, config: SucursalPlataformaExterna, attempt=None) -> dict:
        location_name = str(config.external_location_name or "").strip()
        if not location_name:
            raise ImproperlyConfigured("La configuración externa no contiene external_location_name.")

        current = self.client.get_location(location_name=location_name)
        if not ((current.get("regularHours") or {}).get("periods") or []):
            raise ValueError("Google Business Profile requiere regularHours antes de publicar specialHours.")

        existing_periods = list((((current.get("specialHours") or {}).get("specialHourPeriods")) or []))
        replacement_periods = self._build_special_hour_periods(detail=detail)
        merged_periods = self._filter_existing_periods(existing_periods=existing_periods, detail=detail) + replacement_periods

        return self.publish_fields(location_name=location_name, current=current,
            fields={"specialHours": {"specialHourPeriods": merged_periods}}, attempt=attempt)

    def publish_fields(self, *, location_name, current, fields, attempt):
        if attempt is not None and connection.in_atomic_block:
            raise ValueError("La publicación debe ejecutarse fuera de transaction.atomic para guardar el respaldo antes de Google.")
        payload = {"name": location_name, **fields}
        noop = hours_equal(current, fields)
        if not noop and attempt is None:
            raise ValueError("Se requiere un intento persistido para respaldar la publicación.")
        if attempt is not None:
            attempt.request_payload_json = payload
            attempt.response_payload_json = {"before": current, "mutation_started": False}
            attempt.save(update_fields=["request_payload_json", "response_payload_json"])
        if not noop:
            self.client.patch_hours(location_name=location_name, fields=fields, validate_only=True)
            if attempt is not None:
                attempt.response_payload_json["mutation_started"] = True
                attempt.save(update_fields=["response_payload_json"])
            self.client.patch_hours(location_name=location_name, fields=fields, validate_only=False)
        readback = self.client.get_location(location_name=location_name)
        receipt = "READBACK_MISMATCH"
        public = {}
        if hours_equal(readback, fields):
            receipt = "API_ACCEPTED"
            try:
                public = self.client.get_google_updated(location_name=location_name)
                masks = str(public.get("pendingMask", "")) + "," + str(public.get("diffMask", ""))
                affected = any(part.strip().split(".")[0] in fields or part.strip() == "*" for part in masks.split(","))
                if affected:
                    receipt = "GOOGLE_PENDING"
                elif isinstance(public.get("location"), dict) and hours_equal(public["location"], fields):
                    receipt = "GOOGLE_CONFIRMED"
            except Exception as exc:
                public = {"verification_error": str(exc)}
        response = {"before": current, "readback": readback, "google_updated": public,
                    "mutation_started": not noop, "receipt": receipt}
        return {"noop": noop, "request_payload": payload, "response_payload": response,
                "operation_id": location_name, "receipt": receipt,
                "public_confirmed": receipt == "GOOGLE_CONFIRMED"}


def google_time(value):
    hours, minutes = map(int, value.split(":"))
    return {"hours": hours, "minutes": minutes}


def hours_equal(location, fields):
    # Google omits zero-valued TimeOfDay components and may reorder periods.
    def normalize(value):
        if isinstance(value, dict):
            if "startDate" in value:
                value = {"endDate": value["startDate"], **value}
            return {key: normalize(item) for key, item in value.items()
                    if not ((key in {"hours", "minutes", "seconds", "nanos"} and item == 0)
                            or (key == "closed" and item is False)
                            or (key in {"periods", "specialHourPeriods"} and item == []))}
        if isinstance(value, list):
            return sorted((normalize(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
        return value
    return all(normalize(location.get(key) or {}) == normalize(value) for key, value in fields.items())
