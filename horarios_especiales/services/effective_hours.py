"""Read approved dated hours and official regular hours without operational writes."""

import json
import re
from datetime import datetime

from django.conf import settings
from django.utils import timezone
import requests

from horarios_especiales.models import HorarioEspecialDetalle, normalize_text
from horarios_especiales.services.branch_resolution import resolve_branch_token
from horarios_especiales.services.validation import validate_canonical_payload


OFFICIAL_BRANCHES_URL = "https://www.pollyanasdolce.com/api/branches/"
_DAYS = {name: day for day, name in enumerate(("lun", "mar", "mie", "jue", "vie", "sab", "dom"))}


def _branch_name(value):
    return normalize_text(value).removeprefix("sucursal ") if isinstance(value, str) else ""


def _code(value):
    return normalize_text(value).replace("_", " ") if isinstance(value, str) else ""


def _windows(windows, closed):
    if type(closed) is not bool or not isinstance(windows, list) or len(windows) > 24:
        return None
    if any(not isinstance(window, dict) or set(window) != {"open", "close"}
           or any(not isinstance(value, str) or not re.fullmatch(r"[0-9]{2}:[0-9]{2}", value)
                  for value in window.values()) for window in windows):
        return None
    if validate_canonical_payload({"locations": [True], "effective_date": "valid",
                                   "closed_all_day": closed, "time_windows": windows}):
        return None
    ordered = sorted(windows, key=lambda window: window["open"])
    if any(left["close"] > right["open"] for left, right in zip(ordered, ordered[1:])):
        return None
    return ordered


def _clock(value):
    normalized = normalize_text(value).replace(".", "").replace(" ", "")
    match = re.fullmatch(r"([0-9]{1,2}):([0-9]{2})(am|pm)?", normalized)
    if not match:
        raise ValueError("Invalid clock")
    hour, minute, meridian = match.groups()
    hour, minute = int(hour), int(minute)
    if meridian:
        if not 1 <= hour <= 12:
            raise ValueError("Invalid 12 hour clock")
        hour = hour % 12 + (12 if meridian == "pm" else 0)
    return datetime.strptime(f"{hour:02}:{minute:02}", "%H:%M").strftime("%H:%M")


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate schedule key")
        result[key] = value
    return result


def _regular_windows(schedule, weekday):
    schedule = json.loads(schedule, object_pairs_hook=_unique_json_object) if isinstance(schedule, str) else schedule
    if not isinstance(schedule, dict) or not schedule or len(schedule) > 20:
        return None
    days = {}
    for key, value in schedule.items():
        if not isinstance(key, str) or not isinstance(value, str):
            return None
        pieces = re.split(r"\s+a\s+|-", value.strip())
        if len(pieces) != 2:
            return None
        window = {"open": _clock(pieces[0]), "close": _clock(pieces[1])}
        if _windows([window], False) is None:
            return None
        for segment in normalize_text(key).split(","):
            boundaries = segment.strip().split("-")
            if len(boundaries) not in (1, 2) or any(day not in _DAYS for day in boundaries):
                return None
            first, last = _DAYS[boundaries[0]], _DAYS[boundaries[-1]]
            if first > last:
                return None
            for day in range(first, last + 1):
                if day in days:
                    return None
                days[day] = window
    return [days[weekday]] if weekday in days else None


def _read_regular(branch, target_date):
    regular = {"status": "UNKNOWN", "source": "OFFICIAL_WEBSITE",
               "source_url": OFFICIAL_BRANCHES_URL, "windows": []}
    public_code = None
    try:
        response = requests.get(OFFICIAL_BRANCHES_URL, timeout=5, allow_redirects=False)
        if response.status_code != 200:
            return regular, public_code
        rows = response.json()
        if not isinstance(rows, list) or len(rows) > 100 or any(not isinstance(row, dict) for row in rows):
            return regular, public_code
        branch_name = _branch_name(branch.nombre)
        candidates = [row for row in rows if _branch_name(row.get("name")) == branch_name
                      or _code(row.get("erp_branch_code")) == _code(branch.codigo)]
        if len(candidates) != 1:
            return regular, public_code
        row = candidates[0]
        code = row.get("erp_branch_code")
        if not isinstance(code, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,39}", code):
            return regular, public_code
        legacy_bamoa = branch.codigo == "CRUCERO" and branch_name == "bamoa" and code == "BAMOA"
        if (_branch_name(row.get("name")) != branch_name
                or not (_code(code) == _code(branch.codigo) or legacy_bamoa)):
            return regular, public_code
        public_code = code
        windows = _regular_windows(row.get("schedule"), target_date.weekday())
        if windows is not None:
            regular.update(status="VERIFIED", windows=windows)
    except (requests.RequestException, ValueError, TypeError, OverflowError):
        pass
    return regular, public_code


def read_effective_hours(branch_name, target_date):
    matches, errors = resolve_branch_token(branch_name, reference_date=target_date)
    if errors or len(matches) != 1:
        raise ValueError("No se pudo identificar una sucursal operativa única.")
    branch = matches[0].branch
    regular, public_code = _read_regular(branch, target_date)
    effective = {"status": "REGULAR_ONLY" if regular["status"] == "VERIFIED" else "UNKNOWN",
                 "source": None, "closed_all_day": None, "windows": [],
                 "reason": "SPECIAL_HOURS_NOT_CONFIRMED"}
    # ponytail: Google stays unqueried until OAuth and the actual location mapping are verified.
    details = list(HorarioEspecialDetalle.objects.filter(
        sucursal=branch, target_date=target_date,
        request__status__in=["APROBADO", "EJECUTADO", "FALLIDO"],
    ).exclude(execution_status="CANCELADO").select_related("request")[:101])
    candidates = []
    invalid = len(details) > 100
    for detail in details:
        windows = _windows(detail.time_windows_json, detail.closed_all_day)
        if (detail.request.status == "FALLIDO" or detail.execution_status not in {"PENDIENTE", "EXITOSO"}
                or (detail.request.status == "EJECUTADO" and detail.execution_status != "EXITOSO")
                or detail.request.approved_at is None
                or detail.request.cancelled_at is not None or detail.validation_errors_json != [] or windows is None):
            invalid = True
        else:
            candidate = {"closed_all_day": detail.closed_all_day, "windows": windows}
            if candidate not in candidates:
                candidates.append(candidate)
    if invalid or len(candidates) > 1:
        effective.update(status="UNKNOWN", reason="SPECIAL_HOURS_CONFLICT_OR_INVALID")
    elif candidates:
        effective.update(status="VERIFIED", source="ERP_APPROVED_SPECIAL_HOURS", reason=None, **candidates[0])
    return {"branch_id": str(branch.id), "branch_code": branch.codigo, "branch_name": branch.nombre,
            "public_branch_code": public_code, "target_date": target_date.isoformat(), "timezone": settings.TIME_ZONE,
            "checked_at": timezone.now().isoformat(), "regular": regular, "effective": effective}
