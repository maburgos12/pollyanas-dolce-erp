from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
import sqlite3
import ssl
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Callable
from urllib.request import Request, urlopen


log = logging.getLogger("hik_health")
HEALTH_PATH = "/rrhh/api/asistencia-hik/v2/health/"
RECOVERY_ATTEMPTS = 5


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _sync_state(con: sqlite3.Connection) -> dict[str, str]:
    try:
        return dict(con.execute("SELECT key, value FROM sync_state"))
    except sqlite3.OperationalError:
        return {}


def inspect_health(
    db_path: str | Path,
    *,
    now: datetime | None = None,
    slo_minutes: int = 10,
) -> dict:
    """Deriva salud del journal local; no importa código del agente ni Django."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=slo_minutes)
    with sqlite3.connect(db_path, timeout=5) as con:
        state = _sync_state(con)
        try:
            rows = con.execute(
                """
                SELECT status, attempts, last_error, created_at, occurred_at
                FROM event_outbox
                WHERE status != 'acked'
                """
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []

    pending = [row for row in rows if row[0] == "pending"]
    review = [row for row in rows if row[0] == "review"]
    identity_deferred = sum(row[2] == "identity_unresolved" for row in pending)
    oldest_pending = min((_parse_dt(row[3]) for row in pending), default=None)
    max_attempts = max((int(row[1]) for row in pending), default=0)
    last_cycle = _parse_dt(state.get("last_cycle_at"))
    last_success = _parse_dt(state.get("last_success_at"))
    last_cloud = _parse_dt(state.get("last_cloud_record_at"))
    failure_count = max(int(state.get("failure_count", "0") or 0), 0)
    last_error = state.get("last_error", "")

    pending_exhausted = (
        oldest_pending is not None
        and oldest_pending < cutoff
        and max_attempts >= RECOVERY_ATTEMPTS
    )
    exhausted = bool(review) or (
        identity_deferred > 0 and pending_exhausted
    ) or (
        (last_success is None or last_success < cutoff)
        and (failure_count >= RECOVERY_ATTEMPTS or pending_exhausted)
    )
    if exhausted:
        status = "action_required"
        incident_key = state.get("failure_category", "").strip()
        if not incident_key:
            incident_key = "identity_unresolved" if identity_deferred else "outbox_review" if review else "sync_stale"
    elif pending or failure_count or last_success is None or last_success < cutoff:
        status = "recovering"
        incident_key = ""
    else:
        status = "healthy"
        incident_key = ""

    return {
        "status": status,
        "last_cycle_at": last_cycle.isoformat() if last_cycle else None,
        "last_success_at": last_success.isoformat() if last_success else None,
        "last_cloud_record_at": last_cloud.isoformat() if last_cloud else None,
        "outbox_pending": len(pending),
        "identity_deferred": identity_deferred,
        "failure_count": failure_count,
        "incident_key": incident_key[:128],
        "last_error": last_error[:4000],
    }


def _post_json(url: str, payload: dict, headers: dict[str, str], timeout: float) -> dict:
    request = Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"POST {url} respondio {response.status}")
        return json.loads(response.read() or b"{}")


def post_erp_health(
    report: dict,
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    timeout: float = 10,
) -> dict:
    base_url = (base_url or os.getenv("ERP_BASE_URL", "")).rstrip("/")
    api_key = api_key if api_key is not None else os.getenv("ERP_API_KEY", "")
    if not base_url or not api_key:
        raise RuntimeError("ERP_BASE_URL y ERP_API_KEY son obligatorios")
    return _post_json(
        f"{base_url}{HEALTH_PATH}",
        report,
        {"X-API-Key": api_key, "Accept": "application/json"},
        timeout,
    )


def _send_maya(message: str, incident_key: str) -> bool:
    url = os.getenv("HIK_MAYA_WEBHOOK_URL", "").strip()
    if not url:
        return False
    payload = {"message": message, "incident_key": incident_key}
    recipient = os.getenv("HIK_ALERT_WHATSAPP_TO", "").strip()
    if recipient:
        payload["to"] = recipient
    _post_json(url, payload, {}, 10)
    return True


def _send_email(message: str, incident_key: str) -> bool:
    host = os.getenv("SMTP_HOST", "").strip()
    recipient = os.getenv("HIK_ALERT_EMAIL_TO", "").strip()
    if not host or not recipient:
        return False
    port = int(os.getenv("SMTP_PORT", "587"))
    sender = os.getenv("HIK_ALERT_EMAIL_FROM", "").strip() or os.getenv("SMTP_USERNAME", "").strip()
    mail = EmailMessage()
    mail["Subject"] = f"[Hik-Connect] Acción requerida: {incident_key}"
    mail["From"] = sender
    mail["To"] = recipient
    mail.set_content(message)
    use_ssl = os.getenv("SMTP_USE_SSL", "").lower() in {"1", "true", "yes"}
    client_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with client_class(host, port, timeout=10) as client:
        if not use_ssl and os.getenv("SMTP_USE_TLS", "true").lower() in {"1", "true", "yes"}:
            client.starttls(context=ssl.create_default_context())
        username = os.getenv("SMTP_USERNAME", "").strip()
        if username:
            client.login(username, os.getenv("SMTP_PASSWORD", ""))
        client.send_message(mail)
    return True


def send_configured_alerts(message: str, incident_key: str) -> int:
    sent = 0
    errors = []
    for channel in (_send_maya, _send_email):
        try:
            sent += int(channel(message, incident_key))
        except Exception as exc:
            errors.append(f"{channel.__name__}: {exc}")
    if errors:
        raise RuntimeError("; ".join(errors))
    return sent


def _init_incidents(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS health_incidents (
            incident_key TEXT PRIMARY KEY,
            detected_at TEXT NOT NULL,
            notified_at TEXT,
            resolved_at TEXT
        )
        """
    )


def notify_if_required(
    db_path: str | Path,
    report: dict,
    *,
    sender: Callable[[str, str], object] = send_configured_alerts,
    now: datetime | None = None,
) -> bool:
    now = now or datetime.now(timezone.utc)
    now_iso = now.isoformat()
    key = str(report.get("incident_key") or "")
    with sqlite3.connect(db_path, timeout=5) as con:
        _init_incidents(con)
        if report.get("status") != "action_required" or not key:
            con.execute(
                "UPDATE health_incidents SET resolved_at=? WHERE resolved_at IS NULL",
                (now_iso,),
            )
            return False
        row = con.execute(
            "SELECT notified_at, resolved_at FROM health_incidents WHERE incident_key=?",
            (key,),
        ).fetchone()
        if row and row[0] and row[1] is None:
            return False
        con.execute(
            """
            INSERT INTO health_incidents(incident_key, detected_at, notified_at, resolved_at)
            VALUES (?, ?, NULL, NULL)
            ON CONFLICT(incident_key) DO UPDATE SET
                detected_at=excluded.detected_at, notified_at=NULL, resolved_at=NULL
            """,
            (key, now_iso),
        )

    message = (
        f"Hik-Connect requiere acción humana ({key}). "
        f"Pendientes: {report.get('outbox_pending', 0)}. "
        f"Error: {report.get('last_error') or 'sin detalle'}"
    )
    sender(message, key)
    with sqlite3.connect(db_path, timeout=5) as con:
        _init_incidents(con)
        con.execute(
            "UPDATE health_incidents SET notified_at=? WHERE incident_key=? AND resolved_at IS NULL",
            (now_iso, key),
        )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Salud operativa de la ingesta Hik-Connect")
    parser.add_argument("--db-path", default=os.getenv("DB_PATH", "state.db"))
    parser.add_argument("--slo-minutes", type=int, default=10)
    args = parser.parse_args()
    report = inspect_health(args.db_path, slo_minutes=args.slo_minutes)
    post_erp_health(report)
    notify_if_required(args.db_path, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
