from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "America/Mazatlan"))

HIKCONNECT_EMAIL = os.getenv("HIKCONNECT_EMAIL", "").strip()
HIKCONNECT_PASSWORD = os.getenv("HIKCONNECT_PASSWORD", "").strip()
HIKCONNECT_LOGIN_URL = "https://www.hik-connect.com/views/login/index.html#/login"
HIKCONNECT_PORTAL_URL = "https://www.hik-connect.com/views/login/index.html#/portal"
HIKCONNECT_API_BASE = os.getenv("HIKCONNECT_API_BASE", "https://ius-team.hikcentralconnect.com/hcc")

ERP_BASE_URL = os.getenv("ERP_BASE_URL", "https://erp.pollyanasdolce.com").rstrip("/")
ERP_API_KEY = os.getenv("ERP_API_KEY", "").strip()
ERP_ENDPOINT = os.getenv("ERP_ENDPOINT", "/rrhh/api/asistencia-hik/")

SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "300"))
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "8"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
HEADLESS = os.getenv("HEADLESS", "true").lower() in {"1", "true", "yes", "on"}

DB_PATH = BASE_DIR / os.getenv("DB_PATH", "state.db")
LOG_FILE = BASE_DIR / os.getenv("LOG_FILE", "agente_hikconnect.log")
STORAGE_STATE_PATH = BASE_DIR / os.getenv("STORAGE_STATE_PATH", "storage_state.json")
DOWNLOAD_DIR = BASE_DIR / os.getenv("DOWNLOAD_DIR", "downloads")


def _parse_aliases(raw: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for item in raw.split(","):
        if not item.strip() or "=" not in item:
            continue
        source, target = item.split("=", 1)
        aliases[source.strip()] = target.strip()
    return aliases


EMPLOYEE_CODE_ALIASES = _parse_aliases(os.getenv("EMPLOYEE_CODE_ALIASES", ""))
