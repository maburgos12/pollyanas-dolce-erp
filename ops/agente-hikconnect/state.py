from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from config import DB_PATH, LOOKBACK_HOURS, TIMEZONE


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


def get_last_sync_time() -> datetime:
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("SELECT value FROM sync_state WHERE key='last_sync'").fetchone()
    if row:
        return datetime.fromisoformat(row[0])
    return datetime.now(TIMEZONE) - timedelta(hours=LOOKBACK_HOURS)


def set_last_sync_time(dt: datetime) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT OR REPLACE INTO sync_state VALUES ('last_sync', ?)", (dt.isoformat(),))


def was_sent(record_guid: str) -> bool:
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("SELECT 1 FROM cloud_records_sent WHERE record_guid=?", (record_guid,)).fetchone()
    return row is not None


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
