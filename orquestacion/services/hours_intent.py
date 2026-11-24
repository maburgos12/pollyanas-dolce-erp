"""Bounded contract for interpreting edits to an existing Telegram hours draft."""
from datetime import timedelta

AGENT = "telegram_hours"
KIND = "hours_draft_edit"
MAX_WAIT = timedelta(minutes=4)


def validate_input(data):
    if set(data) != {"text", "draft"}:
        raise ValueError("invalid_fields")
    for key, limit in (("text", 800), ("draft", 1600)):
        value = data[key]
        if (not isinstance(value, str) or not value.strip() or len(value.encode()) > limit
                or any(ord(char) < 32 and char not in "\n\t" for char in value)):
            raise ValueError("invalid_text")


def validate_output(data, result):
    if not isinstance(result, dict) or set(result) != {"action", "branch", "detail"}:
        raise ValueError("invalid_output")
    action, branch, detail = (result[key] for key in ("action", "branch", "detail"))
    if action == "clarify" and branch is None and detail is None:
        return
    if action not in ("keep_closed", "change_close", "change_open"):
        raise ValueError("invalid_output")
    if any(not isinstance(span, str) or not span.strip() or len(span) > 80 or span not in data["text"]
           for span in (branch, detail)):
        raise ValueError("invalid_output")
