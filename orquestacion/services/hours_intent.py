"""Bounded contract for interpreting edits to an existing Telegram hours draft."""
import re
from datetime import date, timedelta

from unidecode import unidecode

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


def replacement_dates(detail):
    if not isinstance(detail, str) or not detail or len(detail) > 340:
        raise ValueError("invalid_output")
    values = [value.strip() for value in detail.split(",")]
    if (not 1 <= len(values) <= 31 or len(set(values)) != len(values)
            or any(len(value) != 10 or date.fromisoformat(value).isoformat() != value for value in values)):
        raise ValueError("invalid_output")
    return values


def validate_output(data, result):
    if not isinstance(result, dict) or set(result) != {"action", "branch", "detail"}:
        raise ValueError("invalid_output")
    action, branch, detail = (result[key] for key in ("action", "branch", "detail"))
    if action == "clarify" and branch is None and detail is None:
        return
    if action == "replace_draft_dates":
        if not isinstance(branch, str) or not branch.strip() or len(branch) > 80:
            raise ValueError("invalid_output")
        name = " ".join(unidecode(branch).casefold().replace("_", " ").split())
        evidence = " ".join(unidecode(data["text"] + "\n" + data["draft"]).casefold().replace("_", " ").split())
        if not name or not re.search(rf"\b{re.escape(name)}\b", evidence):
            raise ValueError("invalid_output")
        replacement_dates(detail)
        return
    if action not in ("keep_closed", "replace_with_draft", "change_close", "change_open"):
        raise ValueError("invalid_output")
    if any(not isinstance(span, str) or not span.strip() or len(span) > 80 or span not in data["text"]
           for span in (branch, detail)):
        raise ValueError("invalid_output")
