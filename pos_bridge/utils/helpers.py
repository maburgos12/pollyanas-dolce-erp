from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from unidecode import unidecode

SENSITIVE_KEYS = {"password", "passwd", "token", "secret", "authorization", "cookie"}


def normalize_text(value: Any) -> str:
    return " ".join(unidecode(str(value or "")).strip().lower().split())


def decimal_from_value(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    text = str(value).strip().replace(",", "")
    if not text:
        return Decimal(default)
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal(default)


def safe_slug(value: Any) -> str:
    normalized = normalize_text(value).replace(" ", "_")
    return "".join(ch for ch in normalized if ch.isalnum() or ch in {"_", "-"}).strip("_") or "item"


def deterministic_id(*parts: Any) -> str:
    raw = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def sanitize_sensitive_data(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            if str(key).lower() in SENSITIVE_KEYS:
                sanitized[key] = "***"
            else:
                sanitized[key] = sanitize_sensitive_data(item)
        return sanitized
    if isinstance(value, list):
        return [sanitize_sensitive_data(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_sensitive_data(item) for item in value)
    return value


def select_candidates(overrides: dict[str, Any], key: str, defaults: Iterable[str]) -> list[str]:
    override = overrides.get(key)
    if isinstance(override, str) and override.strip():
        return [override.strip()]
    if isinstance(override, list):
        cleaned = [str(item).strip() for item in override if str(item).strip()]
        if cleaned:
            return cleaned
    return list(defaults)


def write_json_file(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path
