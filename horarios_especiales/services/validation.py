from __future__ import annotations

from datetime import datetime


def validate_canonical_payload(payload: dict) -> list[str]:
    errors = list(payload.get("validation_errors") or [])
    locations = list(payload.get("locations") or [])
    if not locations:
        errors.append("La solicitud no contiene sucursales resolubles.")

    effective_date = str(payload.get("effective_date") or "").strip()
    if not effective_date:
        errors.append("La solicitud no contiene fecha efectiva.")

    closed_all_day = bool(payload.get("closed_all_day"))
    time_windows = list(payload.get("time_windows") or [])
    if closed_all_day and time_windows:
        errors.append("Una sucursal cerrada todo el día no debe incluir rangos horarios.")
    if not closed_all_day and not time_windows:
        errors.append("La solicitud debe incluir al menos un rango horario.")

    for idx, window in enumerate(time_windows, start=1):
        open_at = str(window.get("open") or "").strip()
        close_at = str(window.get("close") or "").strip()
        try:
            open_dt = datetime.strptime(open_at, "%H:%M")
            close_dt = datetime.strptime(close_at, "%H:%M")
        except ValueError:
            errors.append(f"Rango horario #{idx} inválido.")
            continue
        if open_dt >= close_dt:
            errors.append(f"Rango horario #{idx} inválido: apertura debe ser menor que cierre.")

    return list(dict.fromkeys(errors))

