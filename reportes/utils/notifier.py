from __future__ import annotations

import json
import logging


logger = logging.getLogger(__name__)


def _payload(upload, **extra) -> dict[str, object]:
    data = {
        "upload_id": getattr(upload, "pk", None),
        "filename": getattr(upload, "original_filename", ""),
        "file_hash": getattr(upload, "file_hash", ""),
        "status": getattr(upload, "status", ""),
        "branches": list(getattr(upload, "affected_branches", []) or []),
        "periods": list(getattr(upload, "covered_periods", []) or []),
        "errors": list(getattr(upload, "error_log", []) or []),
    }
    data.update({key: value for key, value in extra.items() if value is not None})
    return data


def notify_error(upload, error) -> None:
    logger.error(
        "operating_expense_notifier %s",
        json.dumps(
            _payload(upload, event="notify_error", error_type=error.__class__.__name__, error_message=str(error)),
            ensure_ascii=False,
            default=str,
            sort_keys=True,
        ),
    )


def notify_duplicate(upload) -> None:
    logger.warning(
        "operating_expense_notifier %s",
        json.dumps(
            _payload(upload, event="notify_duplicate"),
            ensure_ascii=False,
            default=str,
            sort_keys=True,
        ),
    )


def notify_validation_issue(upload) -> None:
    logger.warning(
        "operating_expense_notifier %s",
        json.dumps(
            _payload(upload, event="notify_validation_issue"),
            ensure_ascii=False,
            default=str,
            sort_keys=True,
        ),
    )
