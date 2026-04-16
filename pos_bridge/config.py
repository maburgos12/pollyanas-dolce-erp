from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from django.conf import settings


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _env_json(name: str) -> dict[str, Any]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _setting_or_env_list(name: str, default: list[str] | None = None) -> list[str]:
    default = list(default or [])
    setting_value = getattr(settings, name, None)
    if isinstance(setting_value, (list, tuple)):
        return [str(item).strip() for item in setting_value if str(item).strip()]
    if isinstance(setting_value, str):
        parsed = [item.strip() for item in setting_value.split(",") if item.strip()]
        if parsed:
            return parsed

    env_value = _env_list(name)
    return env_value or default


@dataclass(slots=True)
class PointBridgeSettings:
    base_url: str
    username: str
    password: str
    headless: bool
    timeout_ms: int
    browser_slow_mo_ms: int
    retry_attempts: int
    recipe_discovery_fallback_days: int
    inventory_cost_capture_enabled: bool
    inventory_cost_capture_branch: str
    sync_interval_hours: int
    max_branches: int
    max_pages_per_branch: int
    sales_excluded_branches: list[str]
    production_storage_branches: list[str]
    transfer_storage_branches: list[str]
    sales_sync_source_mode: str
    sales_sync_credito_scopes: list[str]
    storage_root: Path
    logs_dir: Path
    screenshots_dir: Path
    raw_exports_dir: Path
    selector_overrides: dict[str, Any]

    def ensure_directories(self) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)
        self.raw_exports_dir.mkdir(parents=True, exist_ok=True)

    def safe_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "username": self.username,
            "password": "***",
            "headless": self.headless,
            "timeout_ms": self.timeout_ms,
            "browser_slow_mo_ms": self.browser_slow_mo_ms,
            "retry_attempts": self.retry_attempts,
            "recipe_discovery_fallback_days": self.recipe_discovery_fallback_days,
            "inventory_cost_capture_enabled": self.inventory_cost_capture_enabled,
            "inventory_cost_capture_branch": self.inventory_cost_capture_branch,
            "sync_interval_hours": self.sync_interval_hours,
            "max_branches": self.max_branches,
            "max_pages_per_branch": self.max_pages_per_branch,
            "sales_excluded_branches": self.sales_excluded_branches,
            "production_storage_branches": self.production_storage_branches,
            "transfer_storage_branches": self.transfer_storage_branches,
            "sales_sync_source_mode": self.sales_sync_source_mode,
            "sales_sync_credito_scopes": self.sales_sync_credito_scopes,
            "storage_root": str(self.storage_root),
        }


def load_point_bridge_settings() -> PointBridgeSettings:
    storage_root = Path(
        os.getenv("POINT_BRIDGE_STORAGE_ROOT", getattr(settings, "POINT_BRIDGE_STORAGE_ROOT", "storage/pos_bridge"))
    ).expanduser()

    if not storage_root.is_absolute():
        storage_root = Path(settings.BASE_DIR) / storage_root

    config = PointBridgeSettings(
        base_url=os.getenv("POINT_BASE_URL", "").strip(),
        username=os.getenv("POINT_USERNAME", "").strip(),
        password=os.getenv("POINT_PASSWORD", "").strip(),
        headless=_env_bool("POINT_HEADLESS", True),
        timeout_ms=_env_int("POINT_TIMEOUT", 30000, minimum=1000),
        browser_slow_mo_ms=_env_int("POINT_BROWSER_SLOW_MO", 0, minimum=0),
        retry_attempts=_env_int(
            "POINT_RETRY_ATTEMPTS",
            getattr(settings, "POINT_BRIDGE_RETRY_ATTEMPTS", 3),
            minimum=1,
        ),
        recipe_discovery_fallback_days=_env_int(
            "POINT_RECIPE_DISCOVERY_FALLBACK_DAYS",
            getattr(settings, "POINT_RECIPE_DISCOVERY_FALLBACK_DAYS", 7),
            minimum=1,
        ),
        inventory_cost_capture_enabled=_env_bool(
            "POINT_INVENTORY_COST_CAPTURE_ENABLED",
            getattr(settings, "POINT_INVENTORY_COST_CAPTURE_ENABLED", True),
        ),
        inventory_cost_capture_branch=str(
            os.getenv(
                "POINT_INVENTORY_COST_CAPTURE_BRANCH",
                getattr(settings, "POINT_INVENTORY_COST_CAPTURE_BRANCH", "ALMACEN"),
            )
            or "ALMACEN"
        ).strip(),
        sync_interval_hours=_env_int(
            "POINT_SYNC_INTERVAL_HOURS",
            getattr(settings, "POINT_BRIDGE_SYNC_INTERVAL_HOURS", 24),
            minimum=1,
        ),
        max_branches=_env_int("POINT_SYNC_MAX_BRANCHES", 8, minimum=1),
        max_pages_per_branch=_env_int("POINT_SYNC_MAX_PAGES_PER_BRANCH", 50, minimum=1),
        sales_excluded_branches=_env_list("POINT_SALES_EXCLUDED_BRANCHES"),
        production_storage_branches=_env_list("POINT_PRODUCTION_STORAGE_BRANCHES") or ["CEDIS"],
        transfer_storage_branches=_env_list("POINT_TRANSFER_STORAGE_BRANCHES") or ["CEDIS"],
        sales_sync_source_mode=str(
            os.getenv(
                "POINT_SALES_SYNC_SOURCE_MODE",
                getattr(settings, "POINT_SALES_SYNC_SOURCE_MODE", "OFFICIAL"),
            )
            or "OFFICIAL"
        ).strip().upper(),
        sales_sync_credito_scopes=_setting_or_env_list("POINT_SALES_SYNC_CREDITO_SCOPES", default=["null"]),
        storage_root=storage_root,
        logs_dir=storage_root / "logs",
        screenshots_dir=storage_root / "screenshots",
        raw_exports_dir=storage_root / "raw_exports",
        selector_overrides=_env_json("POINT_SELECTOR_OVERRIDES_JSON"),
    )
    if config.sales_sync_source_mode not in {"OFFICIAL", "LEGACY"}:
        config.sales_sync_source_mode = "OFFICIAL"
    config.ensure_directories()
    return config
