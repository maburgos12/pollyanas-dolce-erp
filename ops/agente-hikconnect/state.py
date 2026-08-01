from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from config import DB_PATH, LOOKBACK_HOURS, TIMEZONE

OUTBOX_PENDING = "pending"
OUTBOX_ACKED = "acked"
OUTBOX_REVIEW = "review"


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS event_outbox (
                event_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                employee_no TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                acked_at TEXT,
                ack_payload TEXT NOT NULL DEFAULT ''
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS event_outbox_pending_idx
            ON event_outbox(status, created_at)
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cloud_records_sent (
                record_guid TEXT PRIMARY KEY,
                employee_no TEXT NOT NULL,
                device_time TEXT NOT NULL,
                sent_at TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS employee_day_state (
                employee_no TEXT NOT NULL,
                work_date TEXT NOT NULL,
                first_time TEXT,
                last_time TEXT,
                PRIMARY KEY (employee_no, work_date)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cloud_record_quarantine (
                record_guid TEXT PRIMARY KEY,
                page_index INTEGER NOT NULL,
                reason TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'review',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
            """
        )


def _now_iso() -> str:
    return datetime.now(TIMEZONE).isoformat()


def _payload_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def quarantine_cloud_record(record) -> None:
    """Conserva una fila cloud no proyectable antes de permitir avanzar página."""
    now = _now_iso()
    raw_payload = _payload_json(record.raw)
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            """
            INSERT INTO cloud_record_quarantine (
                record_guid, page_index, reason, raw_payload, status,
                first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, 'review', ?, ?)
            ON CONFLICT(record_guid) DO UPDATE SET
                page_index=excluded.page_index,
                reason=excluded.reason,
                raw_payload=excluded.raw_payload,
                last_seen_at=excluded.last_seen_at
            """,
            (
                record.record_guid,
                record.page_index,
                record.reason,
                raw_payload,
                now,
                now,
            ),
        )


def enqueue_event(event: dict[str, Any]) -> bool:
    """Persiste un evento inmutable; True solo cuando el GUID era nuevo."""
    event_id = str(event.get("event_id") or "").strip()
    if not event_id:
        raise ValueError("event_id es obligatorio")
    now = _now_iso()
    with sqlite3.connect(DB_PATH) as con:
        cursor = con.execute(
            """
            INSERT OR IGNORE INTO event_outbox (
                event_id, source, employee_no, occurred_at, kind, payload,
                status, attempts, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?)
            """,
            (
                event_id,
                str(event.get("source") or ""),
                str(event.get("employee_external_id") or ""),
                str(event.get("occurred_at") or ""),
                str(event.get("kind") or ""),
                _payload_json(event),
                now,
                now,
            ),
        )
    return cursor.rowcount == 1


def _outbox_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    result["payload"] = json.loads(result["payload"])
    result["ack_payload"] = (
        json.loads(result["ack_payload"]) if result["ack_payload"] else None
    )
    return result


def get_outbox_event(event_id: str) -> dict[str, Any] | None:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM event_outbox WHERE event_id=?",
            (event_id,),
        ).fetchone()
    return _outbox_row(row)


def list_pending_events(limit: int = 1000) -> list[dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT * FROM event_outbox
            WHERE status=?
            ORDER BY created_at, event_id
            LIMIT ?
            """,
            (OUTBOX_PENDING, limit),
        ).fetchall()
    return [_outbox_row(row) for row in rows]


def _where_event_ids(event_ids: list[str]) -> tuple[str, tuple[str, ...]]:
    if not event_ids:
        return "", ()
    return ",".join("?" for _ in event_ids), tuple(event_ids)


def mark_delivery_attempt(event_ids: list[str]) -> None:
    placeholders, params = _where_event_ids(event_ids)
    if not placeholders:
        return
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            f"""
            UPDATE event_outbox
            SET attempts=attempts+1, updated_at=?
            WHERE event_id IN ({placeholders}) AND status=?
            """,
            (_now_iso(), *params, OUTBOX_PENDING),
        )


def record_delivery_error(event_ids: list[str], error: str) -> None:
    placeholders, params = _where_event_ids(event_ids)
    if not placeholders:
        return
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            f"""
            UPDATE event_outbox
            SET last_error=?, updated_at=?
            WHERE event_id IN ({placeholders}) AND status=?
            """,
            (str(error)[:2000], _now_iso(), *params, OUTBOX_PENDING),
        )


def apply_delivery_results(event_ids: list[str], results: list[dict[str, Any]]) -> None:
    """Aplica un acuse completo; cualquier correlacion dudosa deja todo pending."""
    expected = list(dict.fromkeys(event_ids))
    received = [str(item.get("event_id") or "") for item in results if isinstance(item, dict)]
    if len(received) != len(results) or len(received) != len(set(received)) or set(received) != set(expected):
        raise ValueError("respuesta ERP incompleta, duplicada o con GUID desconocido")

    allowed = {"accepted", "duplicate", "deferred", "payload_conflict", "rejected"}
    if any(str(item.get("outcome") or "") not in allowed for item in results):
        raise ValueError("respuesta ERP con outcome desconocido")

    now = _now_iso()
    with sqlite3.connect(DB_PATH) as con:
        for item in results:
            event_id = str(item["event_id"])
            outcome = str(item["outcome"])
            reason = str(item.get("reason_code") or outcome)
            ack_payload = _payload_json(item)
            if outcome in {"accepted", "duplicate"}:
                con.execute(
                    """
                    UPDATE event_outbox
                    SET status=?, last_error='', acked_at=?, ack_payload=?, updated_at=?
                    WHERE event_id=? AND status=?
                    """,
                    (
                        OUTBOX_ACKED,
                        now,
                        ack_payload,
                        now,
                        event_id,
                        OUTBOX_PENDING,
                    ),
                )
            elif outcome == "deferred":
                con.execute(
                    """
                    UPDATE event_outbox
                    SET last_error=?, ack_payload=?, updated_at=?
                    WHERE event_id=? AND status=?
                    """,
                    (reason, ack_payload, now, event_id, OUTBOX_PENDING),
                )
            else:
                con.execute(
                    """
                    UPDATE event_outbox
                    SET status=?, last_error=?, ack_payload=?, updated_at=?
                    WHERE event_id=? AND status=?
                    """,
                    (
                        OUTBOX_REVIEW,
                        reason,
                        ack_payload,
                        now,
                        event_id,
                        OUTBOX_PENDING,
                    ),
                )


def stable_punch_kind(employee_no: str, dt: datetime) -> str:
    """Clasifica sin mutar estado; enqueue_event vuelve inmutable el resultado."""
    work_date = dt.date().isoformat()
    with sqlite3.connect(DB_PATH) as con:
        outbox_exists = con.execute(
            """
            SELECT 1 FROM event_outbox
            WHERE employee_no=? AND substr(occurred_at, 1, 10)=?
            LIMIT 1
            """,
            (employee_no, work_date),
        ).fetchone()
        legacy_exists = con.execute(
            """
            SELECT 1 FROM employee_day_state
            WHERE employee_no=? AND work_date=?
            LIMIT 1
            """,
            (employee_no, work_date),
        ).fetchone()
    return "check_out" if outbox_exists or legacy_exists else "check_in"


def get_discovery_page() -> int:
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT value FROM sync_state WHERE key='discovery_page'"
        ).fetchone()
    return max(1, int(row[0])) if row else 1


def set_discovery_page(page: int) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT OR REPLACE INTO sync_state VALUES ('discovery_page', ?)",
            (str(max(1, page)),),
        )


def get_last_sync_time() -> datetime:
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("SELECT value FROM sync_state WHERE key='last_sync'").fetchone()
    if row:
        return datetime.fromisoformat(row[0]) - timedelta(hours=LOOKBACK_HOURS)
    return datetime.now(TIMEZONE) - timedelta(hours=LOOKBACK_HOURS)


def set_last_sync_time(dt: datetime) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT OR REPLACE INTO sync_state VALUES ('last_sync', ?)", (dt.isoformat(),))


def record_cycle_success(*, completed_at: datetime, last_cloud_record_at: datetime | None) -> None:
    values = {
        "last_cycle_at": completed_at.isoformat(),
        "last_success_at": completed_at.isoformat(),
        "failure_count": "0",
        "failure_category": "",
        "last_error": "",
    }
    if last_cloud_record_at:
        values["last_cloud_record_at"] = last_cloud_record_at.isoformat()
    with sqlite3.connect(DB_PATH) as con:
        con.executemany(
            "INSERT OR REPLACE INTO sync_state(key, value) VALUES (?, ?)",
            values.items(),
        )


def record_cycle_failure(*, failed_at: datetime, category: str, error: str) -> None:
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT value FROM sync_state WHERE key='failure_count'"
        ).fetchone()
        failures = max(int(row[0]) if row else 0, 0) + 1
        con.executemany(
            "INSERT OR REPLACE INTO sync_state(key, value) VALUES (?, ?)",
            {
                "last_cycle_at": failed_at.isoformat(),
                "failure_count": str(failures),
                "failure_category": str(category)[:128],
                "last_error": str(error)[:2000],
            }.items(),
        )


def was_sent(record_guid: str) -> bool:
    with sqlite3.connect(DB_PATH) as con:
        legacy = con.execute(
            "SELECT 1 FROM cloud_records_sent WHERE record_guid=?",
            (record_guid,),
        ).fetchone()
        outbox = con.execute(
            "SELECT status FROM event_outbox WHERE event_id=?",
            (record_guid,),
        ).fetchone()
    return legacy is not None or (outbox is not None and outbox[0] in {OUTBOX_ACKED, OUTBOX_REVIEW})


def mark_sent(record_guid: str, employee_no: str, device_time: str) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT OR IGNORE INTO cloud_records_sent VALUES (?, ?, ?, ?)",
            (record_guid, employee_no, device_time, datetime.now(TIMEZONE).isoformat()),
        )


def day_state_exists(employee_no: str, dt: datetime) -> bool:
    work_date = dt.date().isoformat()
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT 1 FROM employee_day_state WHERE employee_no=? AND work_date=?",
            (employee_no, work_date),
        ).fetchone()
    return row is not None


def classify_punch(employee_no: str, dt: datetime) -> str:
    work_date = dt.date().isoformat()
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT first_time, last_time FROM employee_day_state WHERE employee_no=? AND work_date=?",
            (employee_no, work_date),
        ).fetchone()
        status = "checkIn" if row is None else "checkOut"
        if row is None:
            con.execute(
                "INSERT INTO employee_day_state VALUES (?, ?, ?, ?)",
                (employee_no, work_date, dt.isoformat(), dt.isoformat()),
            )
        else:
            first_time = min(row[0] or dt.isoformat(), dt.isoformat())
            last_time = max(row[1] or dt.isoformat(), dt.isoformat())
            con.execute(
                """
                UPDATE employee_day_state
                SET first_time=?, last_time=?
                WHERE employee_no=? AND work_date=?
                """,
                (first_time, last_time, employee_no, work_date),
            )
    return status
