from __future__ import annotations

from pathlib import Path

from pos_bridge.config import PointBridgeSettings
from pos_bridge.utils.dates import timestamp_token
from pos_bridge.utils.helpers import safe_slug


def capture_screenshot(page, settings: PointBridgeSettings, label: str, job_id: int | None = None) -> Path:
    filename = f"{timestamp_token()}_{safe_slug(label)}"
    if job_id is not None:
        filename = f"job_{job_id}_{filename}"
    path = settings.screenshots_dir / f"{filename}.png"
    page.screenshot(path=str(path), full_page=True)
    return path
