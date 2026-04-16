from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from django.utils import timezone

from horarios_especiales.models import normalize_text
from horarios_especiales.services.branch_resolution import resolve_branch_token, split_branch_tokens


MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

DATE_RE = re.compile(
    r"(?:(?:el|para el|dia|del dia)\s+)?(?P<day>\d{1,2})\s+de\s+(?P<month>[a-z]+)(?:\s+de\s+(?P<year>\d{4}))?"
)
ISO_DATE_RE = re.compile(r"(?P<iso>\d{4}-\d{2}-\d{2})")
OPEN_RE = re.compile(r"(?:abriran|abrira|abre|abrir[a-z]*)\s+(?:a\s+las\s+|a\s+la\s+)?(?P<time>\d{1,2}(?::\d{2})?\s*(?:am|pm)?)")
CLOSE_RE = re.compile(r"(?:cerraran|cerrara|cierra|cerrar[a-z]*)\s+(?:a\s+las\s+|a\s+la\s+)?(?P<time>\d{1,2}(?::\d{2})?\s*(?:am|pm)?)")


@dataclass
class ParsePreview:
    canonical_payload: dict
    validation_errors: list[str]


def _parse_date(text_norm: str) -> tuple[date | None, list[str]]:
    iso_match = ISO_DATE_RE.search(text_norm)
    if iso_match:
        return date.fromisoformat(iso_match.group("iso")), []

    match = DATE_RE.search(text_norm)
    if not match:
        return None, ["No se detectó una fecha válida en la instrucción."]

    day = int(match.group("day"))
    month_label = str(match.group("month") or "").strip()
    month = MONTHS.get(month_label)
    if month is None:
        return None, [f"Mes no reconocido en la instrucción: '{month_label}'."]
    year = int(match.group("year") or timezone.localdate().year)
    try:
        return date(year, month, day), []
    except ValueError:
        return None, [f"Fecha inválida detectada: {day:02d}/{month:02d}/{year}."]


def _parse_time_value(raw_value: str) -> str:
    raw = normalize_text(raw_value).replace(".", "")
    match = re.fullmatch(r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>am|pm)?", raw)
    if not match:
        raise ValueError(f"Hora inválida: '{raw_value}'.")
    hour = int(match.group("hour"))
    minute = int(match.group("minute") or 0)
    meridiem = match.group("ampm")
    if meridiem:
        if hour < 1 or hour > 12:
            raise ValueError(f"Hora fuera de rango: '{raw_value}'.")
        if meridiem == "am":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
    else:
        if ":" not in raw_value and hour <= 12:
            raise ValueError(f"La hora '{raw_value}' es ambigua; usa am/pm o formato 24h.")
        if hour > 23:
            raise ValueError(f"Hora fuera de rango: '{raw_value}'.")
    if minute > 59:
        raise ValueError(f"Minutos fuera de rango: '{raw_value}'.")
    return f"{hour:02d}:{minute:02d}"


def _extract_branch_segment(text_norm: str) -> str:
    markers = []
    for regex in (DATE_RE, ISO_DATE_RE, OPEN_RE, CLOSE_RE):
        match = regex.search(text_norm)
        if match:
            markers.append(match.start())
    if not markers:
        segment = text_norm.strip()
    else:
        segment = text_norm[: min(markers)].strip(" ,")
    segment = re.sub(r"\b(?:para el dia|del dia|el dia|dia|el)\s*$", "", segment).strip(" ,")
    return segment


def build_preview_from_command(raw_text: str) -> ParsePreview:
    text_norm = normalize_text(raw_text)
    errors: list[str] = []
    target_date, date_errors = _parse_date(text_norm)
    errors.extend(date_errors)

    closed_all_day = any(
        phrase in text_norm
        for phrase in ("cerrado", "cerrada", "no abrira", "no abrira", "permanecera cerrado")
    )
    time_windows: list[dict[str, str]] = []
    if not closed_all_day:
        open_match = OPEN_RE.search(text_norm)
        close_match = CLOSE_RE.search(text_norm)
        if open_match and close_match:
            try:
                time_windows.append(
                    {
                        "open": _parse_time_value(open_match.group("time")),
                        "close": _parse_time_value(close_match.group("time")),
                    }
                )
            except ValueError as exc:
                errors.append(str(exc))
        else:
            errors.append("No se detectaron hora de apertura y cierre válidas.")

    branch_segment = _extract_branch_segment(text_norm)
    branch_tokens = split_branch_tokens(branch_segment)
    if not branch_tokens:
        errors.append("No se detectaron sucursales en la instrucción.")

    locations = []
    ambiguities: list[str] = []
    for token in branch_tokens:
        matches, token_errors = resolve_branch_token(token, reference_date=target_date)
        if token_errors:
            errors.extend(token_errors)
        if matches:
            chosen = matches[0]
            locations.append(
                {
                    "input": token,
                    "branch_id": chosen.branch.id,
                    "branch_code": chosen.branch.codigo,
                    "branch_name": chosen.branch.nombre,
                    "matched_by": chosen.matched_by,
                }
            )
            if len(matches) > 1:
                ambiguities.append(
                    {
                        "input": token,
                        "candidates": [row.branch.codigo for row in matches],
                    }
                )

    canonical_payload = {
        "source_text": raw_text.strip(),
        "source_channel": "UNSPECIFIED",
        "effective_date": target_date.isoformat() if target_date else "",
        "locations": locations,
        "closed_all_day": closed_all_day,
        "time_windows": time_windows,
        "reason": "",
        "ambiguities": ambiguities,
        "validation_errors": errors,
    }
    return ParsePreview(canonical_payload=canonical_payload, validation_errors=errors)
