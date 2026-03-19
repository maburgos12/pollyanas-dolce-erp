from __future__ import annotations

from pos_bridge.utils.logger import get_pos_bridge_logger


class PointAlertService:
    def __init__(self):
        self.logger = get_pos_bridge_logger("pos_bridge.alerts")

    def emit_failure(self, *, job_id: int, message: str, context: dict | None = None) -> None:
        self.logger.error("job=%s failure=%s context=%s", job_id, message, context or {})
