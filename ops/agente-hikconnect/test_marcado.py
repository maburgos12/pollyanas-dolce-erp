"""Checks de entrega y clasificacion estable. Correr: python3 test_marcado.py."""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import TIMEZONE
from hikconnect_client import CloudRecord
import main
import state

AHORA = datetime(2026, 7, 28, 8, 0, tzinfo=TIMEZONE)


def registro(guid: str, employee_no: str = "328") -> CloudRecord:
    return CloudRecord(
        record_guid=guid,
        employee_no=employee_no,
        name="Prueba",
        department="",
        device_time=AHORA,
        device_name="Checador",
        device_serial_no="hik-01",
        raw={},
    )


def isolated_db():
    temp_dir = tempfile.TemporaryDirectory()
    state.DB_PATH = Path(temp_dir.name) / "state.db"
    main.DB_PATH = state.DB_PATH if hasattr(main, "DB_PATH") else state.DB_PATH
    state.init_db()
    return temp_dir


def test_outbox_existe_antes_del_post_y_ack_cierra():
    tmp = isolated_db()
    original_send = main.send_events
    try:
        records = [registro("guid-antes-post")]
        events, selected = main.build_events(records, dry_run=False)

        def fake_send(outgoing):
            getter = getattr(state, "get_outbox_event", None)
            assert getter is not None, "el evento no se persistio en un outbox antes del POST"
            queued = getter("guid-antes-post")
            assert queued["status"] == "pending"
            assert queued["payload"] == outgoing[0]
            return {
                "contract_version": 2,
                "batch_id": "fake",
                "results": [
                    {"event_id": "guid-antes-post", "outcome": "accepted"},
                ],
            }

        main.send_events = fake_send
        result = main.send_and_mark(events, selected, dry_run=False)
        assert result["acked"] == 1
        assert state.get_outbox_event("guid-antes-post")["status"] == "acked"
    finally:
        main.send_events = original_send
        tmp.cleanup()


def test_timeout_deja_pending_y_clasificacion_inmutable():
    tmp = isolated_db()
    original_send = main.send_events
    try:
        record = registro("guid-reintento")
        events, selected = main.build_events([record], dry_run=False)
        first_kind = events[0]["kind"]

        def timeout(_events):
            raise TimeoutError("sin conexion")

        main.send_events = timeout
        result = main.send_and_mark(events, selected, dry_run=False)
        assert result["errors"] == 1
        pending = state.get_outbox_event("guid-reintento")
        assert pending["status"] == "pending"
        assert pending["attempts"] == 1

        retry_events, _ = main.build_events([record], dry_run=False)
        assert retry_events[0]["kind"] == first_kind
        assert state.get_outbox_event("guid-reintento")["payload"]["kind"] == first_kind
    finally:
        main.send_events = original_send
        tmp.cleanup()


def test_lote_solo_duplicados_termina_correctamente():
    tmp = isolated_db()
    original_send = main.send_events
    try:
        records = [registro("a", "255"), registro("b", "298")]
        events, selected = main.build_events(records, dry_run=False)
        main.send_events = lambda outgoing: {
            "contract_version": 2,
            "batch_id": "fake",
            "results": [
                {"event_id": item["event_id"], "outcome": "duplicate"}
                for item in outgoing
            ],
        }
        result = main.send_and_mark(events, selected, dry_run=False)
        assert result == {"acked": 2, "pending": 0, "review": 0, "errors": 0}
        assert state.list_pending_events() == []
    finally:
        main.send_events = original_send
        tmp.cleanup()


if __name__ == "__main__":
    test_outbox_existe_antes_del_post_y_ack_cierra()
    test_timeout_deja_pending_y_clasificacion_inmutable()
    test_lote_solo_duplicados_termina_correctamente()
    print("OK: los 3 checks de entrega y marcado pasan")
