"""Checks de entrega y clasificacion estable. Correr: python3 test_marcado.py."""
from __future__ import annotations

import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import TIMEZONE
from hikconnect_client import CloudRecord, DiscoveryRecords
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


def test_sync_ignora_cursor_101_y_entrega_ventanas_por_fecha():
    tmp = isolated_db()
    original_client = main.HikConnectClient
    original_send = main.send_events

    class ClientePorVentanas:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def fetch_records_between(self, **kwargs):
            on_page_records = kwargs.get("on_page_records")
            assert callable(on_page_records), "sync_once debe procesar cada pagina al recibirla"
            on_page_records(AHORA.date(), 1, [registro("guid-reciente")])
            on_page_records(
                (AHORA - timedelta(days=1)).date(),
                1,
                [registro("guid-historico", employee_no="97")],
            )
            return DiscoveryRecords(
                [registro("guid-reciente"), registro("guid-historico", employee_no="97")],
                complete=True,
                next_page=1,
            )

    try:
        main.HikConnectClient = ClientePorVentanas
        main.send_events = lambda outgoing: {
            "contract_version": 2,
            "batch_id": "fake",
            "results": [
                {"event_id": item["event_id"], "outcome": "accepted"}
                for item in outgoing
            ],
        }
        state.set_discovery_page(101)
        result = main.sync_once(headless=True, dry_run=False)

        assert result["acked"] == 2
        assert state.get_outbox_event("guid-reciente")["status"] == "acked"
        assert state.get_outbox_event("guid-historico")["status"] == "acked"
        assert state.get_discovery_page() == 1
    finally:
        main.HikConnectClient = original_client
        main.send_events = original_send
        tmp.cleanup()


def test_sync_conserva_punch_neutro_aunque_las_paginas_lleguen_al_reves():
    tmp = isolated_db()
    original_client = main.HikConnectClient
    original_send = main.send_events
    salida = replace(registro("guid-salida"), device_time=AHORA + timedelta(hours=9))
    entrada = replace(registro("guid-entrada"), device_time=AHORA)

    class ClienteConPaginasInvertidas:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def fetch_records_between(self, **kwargs):
            on_page_records = kwargs["on_page_records"]
            on_page_records(AHORA.date(), 1, [salida])
            on_page_records(AHORA.date(), 2, [entrada])
            return DiscoveryRecords([salida, entrada], complete=True, next_page=1)

    try:
        main.HikConnectClient = ClienteConPaginasInvertidas
        main.send_events = lambda outgoing: {
            "contract_version": 2,
            "batch_id": "fake",
            "results": [
                {"event_id": item["event_id"], "outcome": "accepted"}
                for item in outgoing
            ],
        }

        main.sync_once(headless=True, dry_run=False, since=AHORA)

        assert state.get_outbox_event("guid-entrada")["kind"] == "punch"
        assert state.get_outbox_event("guid-salida")["kind"] == "punch"
    finally:
        main.HikConnectClient = original_client
        main.send_events = original_send
        tmp.cleanup()


def test_sync_no_reintenta_un_diferido_en_cada_pagina_del_mismo_ciclo():
    tmp = isolated_db()
    original_client = main.HikConnectClient
    original_send = main.send_events
    llamadas = []

    class ClienteDosPaginas:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def fetch_records_between(self, **kwargs):
            on_page_records = kwargs["on_page_records"]
            primero = registro("guid-diferido")
            segundo = registro("guid-aceptado", employee_no="329")
            on_page_records(AHORA.date(), 1, [primero])
            on_page_records(AHORA.date(), 2, [segundo])
            return DiscoveryRecords([primero, segundo], complete=True, next_page=1)

    def fake_send(outgoing):
        ids = [item["event_id"] for item in outgoing]
        llamadas.append(ids)
        return {
            "contract_version": 2,
            "batch_id": "fake",
            "results": [
                {
                    "event_id": event_id,
                    "outcome": "deferred" if event_id == "guid-diferido" else "accepted",
                    "reason_code": "identity_unresolved" if event_id == "guid-diferido" else "",
                }
                for event_id in ids
            ],
        }

    try:
        main.HikConnectClient = ClienteDosPaginas
        main.send_events = fake_send

        main.sync_once(headless=True, dry_run=False, since=AHORA)

        assert llamadas == [["guid-diferido"], ["guid-aceptado"]]
        assert state.get_outbox_event("guid-diferido")["attempts"] == 1
    finally:
        main.HikConnectClient = original_client
        main.send_events = original_send
        tmp.cleanup()


if __name__ == "__main__":
    test_outbox_existe_antes_del_post_y_ack_cierra()
    test_timeout_deja_pending_y_clasificacion_inmutable()
    test_lote_solo_duplicados_termina_correctamente()
    print("OK: los 3 checks de entrega y marcado pasan")
