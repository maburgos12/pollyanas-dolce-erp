"""Checks del outbox durable. Correr: python3 test_outbox.py."""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import state


def event(guid: str, kind: str = "check_in") -> dict:
    return {
        "event_id": guid,
        "source": "hikconnect_cloud",
        "employee_external_id": "328",
        "occurred_at": "2026-07-28T08:00:00-07:00",
        "kind": kind,
        "device_id": "hik-01",
    }


def isolated_db():
    temp_dir = tempfile.TemporaryDirectory()
    state.DB_PATH = Path(temp_dir.name) / "state.db"
    state.init_db()
    return temp_dir


def test_persiste_payload_antes_de_entregar_y_sobrevive_reinicio():
    tmp = isolated_db()
    try:
        assert hasattr(state, "enqueue_event"), "falta el outbox durable"
        assert state.enqueue_event(event("guid-persistente")) is True
        row = state.get_outbox_event("guid-persistente")
        assert row["payload"] == event("guid-persistente")
        assert row["kind"] == "check_in"
        assert row["status"] == "pending"
        assert row["attempts"] == 0

        state.init_db()
        restarted = state.get_outbox_event("guid-persistente")
        assert restarted["payload"] == event("guid-persistente")
        assert restarted["attempts"] == 0
    finally:
        tmp.cleanup()


def test_guid_existente_no_reclasifica_payload():
    tmp = isolated_db()
    try:
        state.enqueue_event(event("guid-inmutable", "check_in"))
        assert state.enqueue_event(event("guid-inmutable", "check_out")) is False
        row = state.get_outbox_event("guid-inmutable")
        assert row["kind"] == "check_in"
        assert row["payload"]["kind"] == "check_in"
    finally:
        tmp.cleanup()


def test_timeout_conserva_pending_intentos_y_error():
    tmp = isolated_db()
    try:
        state.enqueue_event(event("guid-timeout"))
        state.mark_delivery_attempt(["guid-timeout"])
        state.record_delivery_error(["guid-timeout"], "timeout")
        state.init_db()
        row = state.get_outbox_event("guid-timeout")
        assert row["status"] == "pending"
        assert row["attempts"] == 1
        assert row["last_error"] == "timeout"
    finally:
        tmp.cleanup()


def test_ack_individual_cierra_o_retiene_segun_outcome():
    tmp = isolated_db()
    try:
        guids = [
            "accepted",
            "duplicate",
            "deferred",
            "payload-conflict",
            "rejected",
        ]
        for guid in guids:
            state.enqueue_event(event(guid))

        state.apply_delivery_results(
            guids,
            [
                {"event_id": "accepted", "outcome": "accepted"},
                {"event_id": "duplicate", "outcome": "duplicate"},
                {
                    "event_id": "deferred",
                    "outcome": "deferred",
                    "reason_code": "identity_unresolved",
                },
                {
                    "event_id": "payload-conflict",
                    "outcome": "payload_conflict",
                    "reason_code": "event_id_payload_mismatch",
                },
                {
                    "event_id": "rejected",
                    "outcome": "rejected",
                    "reason_code": "invalid_event",
                },
            ],
        )

        accepted = state.get_outbox_event("accepted")
        assert accepted["status"] == "acked"
        assert accepted.get("ack_payload") == {
            "event_id": "accepted",
            "outcome": "accepted",
        }, "el acuse individual debe quedar durable"
        assert state.get_outbox_event("duplicate")["status"] == "acked"
        deferred = state.get_outbox_event("deferred")
        assert deferred["status"] == "pending"
        assert deferred["last_error"] == "identity_unresolved"
        assert deferred.get("ack_payload", {}).get("outcome") == "deferred"
        assert state.get_outbox_event("payload-conflict")["status"] == "review"
        assert state.get_outbox_event("rejected")["status"] == "review"
    finally:
        tmp.cleanup()


def test_respuesta_incompleta_o_guid_desconocido_falla_cerrada():
    for results in (
        [{"event_id": "a", "outcome": "accepted"}],
        [
            {"event_id": "a", "outcome": "accepted"},
            {"event_id": "intruso", "outcome": "duplicate"},
        ],
    ):
        tmp = isolated_db()
        try:
            state.enqueue_event(event("a"))
            state.enqueue_event(event("b"))
            try:
                state.apply_delivery_results(["a", "b"], results)
            except ValueError:
                pass
            else:
                raise AssertionError("la respuesta invalida debio rechazarse completa")
            assert state.get_outbox_event("a")["status"] == "pending"
            assert state.get_outbox_event("b")["status"] == "pending"
        finally:
            tmp.cleanup()


def test_continuacion_de_descubrimiento_sobrevive_reinicio():
    tmp = isolated_db()
    try:
        assert hasattr(state, "set_discovery_page"), "falta persistir la continuacion de paginas"
        state.set_discovery_page(9)
        state.init_db()
        assert state.get_discovery_page() == 9
        state.set_discovery_page(1)
        assert state.get_discovery_page() == 1
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    test_persiste_payload_antes_de_entregar_y_sobrevive_reinicio()
    test_guid_existente_no_reclasifica_payload()
    test_timeout_conserva_pending_intentos_y_error()
    test_ack_individual_cierra_o_retiene_segun_outcome()
    test_respuesta_incompleta_o_guid_desconocido_falla_cerrada()
    test_continuacion_de_descubrimiento_sobrevive_reinicio()
    print("OK: los 6 checks del outbox pasan")
