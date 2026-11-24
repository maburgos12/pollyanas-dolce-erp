"""Bounded contract for interpreting edits to an existing Telegram hours draft."""
import re
from datetime import date, timedelta

from unidecode import unidecode

AGENT = "telegram_hours"
KIND = "hours_draft_edit"
MAX_WAIT = timedelta(minutes=4)
INITIAL_WAIT = timedelta(minutes=6)


def validate_input(data):
    if set(data) not in ({"text", "draft"}, {"text", "draft", "mode", "windows"}) or data.get("mode", "initial") != "initial":
        raise ValueError("invalid_fields")
    for key, limit in (("text", 800), ("draft", 1600)):
        value = data[key]
        if (not isinstance(value, str) or not value.strip() or len(value.encode()) > limit
                or any(ord(char) < 32 and char not in "\n\t" for char in value)):
            raise ValueError("invalid_text")
    windows = data.get("windows", [])
    if not isinstance(windows, list) or len(windows) > 10:
        raise ValueError("invalid_fields")
    for value in windows:
        if initial_window(value)['closed_all_day']:
            raise ValueError("invalid_fields")


def replacement_dates(detail):
    if not isinstance(detail, str) or not detail or len(detail) > 340:
        raise ValueError("invalid_output")
    values = [value.strip() for value in detail.split(",")]
    if (not 1 <= len(values) <= 31 or len(set(values)) != len(values)
            or any(len(value) != 10 or date.fromisoformat(value).isoformat() != value for value in values)):
        raise ValueError("invalid_output")
    return values


def validate_output(data, result):
    if data.get("mode") == "initial":
        validate_initial_output(result)
        allowed = set(data['windows']) | {'closed', None}
        if data['windows'] and result['start'] and result['end']:
            if any(window not in allowed for window in [result['general'], *(group['hours'] for group in result['groups'])]):
                raise ValueError("invalid_output")
        return
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


def initial_period(start, end):
    if any(not isinstance(value, str) or len(value) != 10 for value in (start, end)):
        raise ValueError("invalid_output")
    first, last = (date.fromisoformat(value) for value in (start, end))
    if first.isoformat() != start or last.isoformat() != end or not 0 <= (last - first).days < 31:
        raise ValueError("invalid_output")
    return first, last


def initial_window(value):
    if value == "closed":
        return {'closed_all_day': True, 'time_windows': []}
    match = re.fullmatch(r"((?:[01]\d|2[0-3]):[0-5]\d)-((?:[01]\d|2[0-3]):[0-5]\d)", value or "") if isinstance(value, str) else None
    if not match or match[1] >= match[2]:
        raise ValueError("invalid_output")
    return {'closed_all_day': False, 'time_windows': [{'open': match[1], 'close': match[2]}]}


def validate_initial_output(result):
    if (not isinstance(result, dict) or set(result) != {'start', 'end', 'general', 'groups'}
            or not isinstance(result['groups'], list) or len(result['groups']) > 10):
        raise ValueError("invalid_output")
    if result['start'] is None or result['end'] is None:
        return
    if (result['general'] is None and not result['groups']):
        raise ValueError("invalid_output")
    start, end = initial_period(result['start'], result['end'])
    if result['general'] is not None:
        initial_window(result['general'])
    for group in result['groups']:
        if (not isinstance(group, dict) or set(group) != {'branches', 'start', 'end', 'hours'}
                or not isinstance(group['branches'], list) or not 1 <= len(group['branches']) <= 10
                or any(not isinstance(branch, str) or not branch.strip() or len(branch) > 80 for branch in group['branches'])):
            raise ValueError("invalid_output")
        first, last = initial_period(
            result['start'] if group['start'] is None else group['start'],
            result['end'] if group['end'] is None else group['end'],
        )
        if not start <= first <= last <= end:
            raise ValueError("invalid_output")
        initial_window(group['hours'])
