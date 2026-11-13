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
MAX_TURNS = 40
MAX_USD = Decimal("10.00")
MAX_BYTES = 60000
MAX_ROUNDS = 6
AUDIO_MODEL = "gpt-transcribe"
# Three minutes at $0.0045/min plus regional premium and conservative headroom.
AUDIO_RESERVE_USD = Decimal("0.03")
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


def can_use_read_agent(user) -> bool:
    return (getattr(settings, "AI_AGENT_READ_ENABLED", False) is True
            and is_pilot_participant(user))


def estimate_turn(message) -> dict | None:
    """Read-only standard-price reference; never releases a committed reservation."""
    from .agent_audio import MAX_SECONDS as max_audio_seconds
    rows = list(AuditLog.objects.filter(model=LEDGER_MODEL, object_id=PILOT_ID, action=ACTION,
        user_id=message.conversation.owner_id, payload__turn_id=str(message.public_id))
        .values_list('payload', flat=True)[:MAX_ROUNDS + 2])
    if not rows:
        return None
    try:
        amounts = [Decimal(row['reserved_usd']) for row in rows]
        if (len(rows) > MAX_ROUNDS + 1 or len({row['cycle'] for row in rows}) != len(rows)
                or any(type(row['cycle']) is not int or not 0 <= row['cycle'] <= MAX_ROUNDS
                       or row['model'] != (AUDIO_MODEL if row['cycle'] == 0 else MODEL)
                       or type(row['reserved_usd']) is not str for row in rows)
                or any(not amount.is_finite() or not 0 < amount <= MAX_USD for amount in amounts)):
            return None
        reserved = sum(amounts)
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None
    proof = message.metadata_json.get('agent_read', {}) if isinstance(message.metadata_json, dict) else {}
    proof = proof if isinstance(proof, dict) else {}
    usage = proof.get('usage', []) if isinstance(proof, dict) else []
    known, estimate = 0, Decimal('0')
    # Reference tariffs verified 2026-10-11; cache reads/writes and regional fees are not inferred.
    providers = proof.get('provider', []) if isinstance(proof, dict) else []
    standard = (proof.get('model_name') == MODEL and isinstance(providers, list)
        and all(isinstance(row, dict) and row.get('model', MODEL) == MODEL
                and row.get('service_tier', 'default') == 'default' for row in providers))
    model_calls = sum(row.get('cycle', 0) != 0 for row in rows)
    if standard and isinstance(usage, list) and len(usage) <= model_calls:
        for item in usage:
            if not isinstance(item, dict): continue
            incoming, outgoing = item.get('input_tokens'), item.get('output_tokens')
            if all(type(value) is int and 0 <= value <= 1000000 for value in (incoming, outgoing)):
                estimate += (Decimal(incoming) * 2 + Decimal(outgoing) * 10) / 1000000
                known += 1
    audio_calls = sum(row.get('cycle') == 0 for row in rows)
    if audio_calls == 1:
        request = message.conversation.messages.filter(role='user', sequence=message.sequence - 1).first()
        audio = request.metadata_json.get('audio', {}) if request and isinstance(request.metadata_json, dict) else {}
        seconds = audio.get('duration_seconds')
        if (audio.get('state') == 'complete' and audio.get('model') == AUDIO_MODEL
                and type(seconds) in (int, float) and 0.5 <= seconds <= max_audio_seconds):
            estimate += Decimal(str(seconds)) / 60 * Decimal('0.0045')
            known += 1
    return {'estimated_usd':str(estimate.quantize(Decimal('0.000001'), rounding=ROUND_CEILING)) if known else None,
            'reserved_usd':str(reserved), 'unmeasured_calls':len(rows) - known,
            'basis':'standard_reference', 'rates_verified_on':'2026-10-11'}


def _ledger():
    rows = list(AuditLog.objects.filter(model=LEDGER_MODEL, object_id=PILOT_ID)
                .order_by("id").values("action", "payload")[:MAX_TURNS * (MAX_ROUNDS + 1) + 1])
    if len(rows) > MAX_TURNS * (MAX_ROUNDS + 1):
        raise PilotStopped("pilot_ledger_invalid")
    seen, turns, total = set(), set(), Decimal("0")
    for row in rows:
        data = row["payload"]
        try:
            turn = str(UUID(data["turn_id"]))
            cycle = data["cycle"]
            amount = Decimal(data["reserved_usd"])
            valid = (row["action"] == ACTION and type(cycle) is int
                     and ((1 <= cycle <= MAX_ROUNDS and data["model"] == MODEL)
                          or (cycle == 0 and data["model"] == AUDIO_MODEL and amount == AUDIO_RESERVE_USD))
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


def reserve_request(*, user, turn_id, cycle: int, payload: dict, token_counter=None) -> dict:
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
    input_tokens = None
    if token_counter is not None:
        from .agent_images import text_size
        if size > 32 * 1024 * 1024 or text_size(payload) > MAX_BYTES:
            raise PilotStopped('pilot_request_limit')
        pilot_status(user)
        seen, turns, total = _ledger()
        if (turn, cycle) in seen or (cycle > 1 and (turn, cycle - 1) not in seen):
            raise PilotStopped('pilot_duplicate_request')
        if turn not in turns and len(turns) >= MAX_TURNS:
            raise PilotStopped('pilot_turn_limit')
        if total + OUTPUT_TOKENS * OUTPUT_USD > MAX_USD:
            raise PilotStopped('pilot_spend_limit')
        try:
            counted = token_counter(**{key:payload[key] for key in ('model', 'input', 'tools', 'text', 'tool_choice', 'parallel_tool_calls') if key in payload})
            input_tokens = counted.input_tokens
        except Exception:
            raise PilotStopped('pilot_image_count_failed') from None
        if type(input_tokens) is not int or not 1 <= input_tokens <= MAX_BYTES:
            raise PilotStopped('pilot_request_limit')
    elif size > MAX_BYTES:
        raise PilotStopped("pilot_request_limit")
    amount = (((input_tokens if input_tokens is not None else size) + 4096) * INPUT_USD + OUTPUT_TOKENS * OUTPUT_USD).quantize(
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
                     "reserved_usd": str(amount), "request_bytes": size,
                     **({'counted_input_tokens':input_tokens} if input_tokens is not None else {})})
    return {"reserved_usd": str(amount), "total_reserved_usd": str(total + amount),
            "turns": len(turns | {turn})}


def reserve_audio(*, user, turn_id):
    if connection.vendor != "postgresql" or connection.in_atomic_block:
        raise PilotStopped("pilot_transaction_boundary")
    turn = str(UUID(str(turn_id)))
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '1500ms'")
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", [LOCK_ID])
        pilot_status(user)
        seen, turns, total = _ledger()
        if any(key[0] == turn for key in seen):
            raise PilotStopped("pilot_duplicate_request")
        if len(turns) >= MAX_TURNS:
            raise PilotStopped("pilot_turn_limit")
        # Reserve enough room for the subsequent Agent Core request as well.
        if total + AUDIO_RESERVE_USD + ((MAX_BYTES + 4096) * INPUT_USD + OUTPUT_TOKENS * OUTPUT_USD) > MAX_USD:
            raise PilotStopped("pilot_spend_limit")
        AuditLog.objects.create(user=user, action=ACTION, model=LEDGER_MODEL, object_id=PILOT_ID,
            payload={"turn_id":turn, "cycle":0, "model":AUDIO_MODEL, "reserved_usd":str(AUDIO_RESERVE_USD)})
