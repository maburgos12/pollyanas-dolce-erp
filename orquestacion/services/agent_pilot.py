"""Finite READ pilot: server admission and committed, cumulative reservations."""
from __future__ import annotations

import json
from decimal import Decimal, ROUND_CEILING
from uuid import UUID

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import connection, transaction

from core.models import AuditLog

PILOT_ID = "read-assets-20261008"
MODEL = "gpt-6.1-sol"
LEDGER_MODEL = "AI_AGENT_PILOT"
ACTION = "AI_AGENT_PILOT_RESERVE"
MAX_TURNS = 20
MAX_USD = Decimal("5.00")
MAX_BYTES = 60000
MAX_ROUNDS = 6
OUTPUT_TOKENS = 1400
# Standard input/cache-write ceiling and output, including 10% regional premium.
INPUT_USD = Decimal("2.75") / 1000000
OUTPUT_USD = Decimal("11") / 1000000
LOCK_ID = 731620261008


class PilotStopped(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def is_pilot_account(user) -> bool:
    participant = getattr(settings, "AI_AGENT_PILOT_USER_ID", 0)
    return (type(participant) is int and participant > 0
            and bool(user and user.is_authenticated and user.pk == participant))


def is_pilot_participant(user) -> bool:
    return (is_pilot_account(user)
            and get_user_model().objects.filter(pk=user.pk, is_active=True).exists())


def _ledger():
    rows = list(AuditLog.objects.filter(model=LEDGER_MODEL, object_id=PILOT_ID)
                .order_by("id").values("action", "payload")[:MAX_TURNS * MAX_ROUNDS + 1])
    if len(rows) > MAX_TURNS * MAX_ROUNDS:
        raise PilotStopped("pilot_ledger_invalid")
    seen, turns, total = set(), set(), Decimal("0")
    for row in rows:
        data = row["payload"]
        try:
            turn = str(UUID(data["turn_id"]))
            cycle = data["cycle"]
            amount = Decimal(data["reserved_usd"])
            valid = (row["action"] == ACTION and data["model"] == MODEL
                     and type(cycle) is int and 1 <= cycle <= MAX_ROUNDS
                     and type(data["reserved_usd"]) is str and amount.is_finite() and Decimal("0") < amount <= MAX_USD
                     and (turn, cycle) not in seen)
        except (TypeError, ValueError, KeyError, ArithmeticError):
            valid = False
        if not valid:
            raise PilotStopped("pilot_ledger_invalid")
        seen.add((turn, cycle)); turns.add(turn); total += amount
    if len(turns) > MAX_TURNS or total > MAX_USD:
        raise PilotStopped("pilot_ledger_invalid")
    return seen, turns, total


def pilot_status(user) -> dict:
    if not is_pilot_participant(user):
        raise PilotStopped("pilot_participant_denied")
    if getattr(settings, "AI_AGENT_READ_MODEL", "") != MODEL:
        raise PilotStopped("pilot_configuration_invalid")
    _, turns, total = _ledger()
    return {"turns": len(turns), "max_turns": MAX_TURNS,
            "reserved_usd": str(total), "max_usd": str(MAX_USD)}


def reserve_request(*, user, turn_id, cycle: int, payload: dict) -> dict:
    # Commit before network I/O; an outer transaction could erase a billed attempt.
    if connection.vendor != "postgresql" or connection.in_atomic_block:
        raise PilotStopped("pilot_transaction_boundary")
    if payload.get("model") != MODEL or payload.get("service_tier") != "default":
        raise PilotStopped("pilot_configuration_invalid")
    if payload.get("max_output_tokens") != OUTPUT_TOKENS or payload.get("store") is not False:
        raise PilotStopped("pilot_configuration_invalid")
    if type(cycle) is not int or not 1 <= cycle <= MAX_ROUNDS:
        raise PilotStopped("pilot_request_invalid")
    turn = str(UUID(str(turn_id)))
    size = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    if size > MAX_BYTES:
        raise PilotStopped("pilot_request_limit")
    amount = ((size + 4096) * INPUT_USD + OUTPUT_TOKENS * OUTPUT_USD).quantize(
        Decimal("0.00000001"), rounding=ROUND_CEILING)
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '1500ms'")
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", [LOCK_ID])
        pilot_status(user)
        seen, turns, total = _ledger()
        if (turn, cycle) in seen or (cycle > 1 and (turn, cycle - 1) not in seen):
            raise PilotStopped("pilot_duplicate_request")
        if turn not in turns and len(turns) >= MAX_TURNS:
            raise PilotStopped("pilot_turn_limit")
        if total + amount > MAX_USD:
            raise PilotStopped("pilot_spend_limit")
        AuditLog.objects.create(user=user, action=ACTION, model=LEDGER_MODEL, object_id=PILOT_ID,
            payload={"turn_id": turn, "cycle": cycle, "model": MODEL,
                     "reserved_usd": str(amount), "request_bytes": size})
    return {"reserved_usd": str(amount), "total_reserved_usd": str(total + amount),
            "turns": len(turns | {turn})}
