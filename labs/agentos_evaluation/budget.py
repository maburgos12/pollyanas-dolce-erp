"""Local admission ledger. Reservations survive network errors and are never refunded."""
import fcntl
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

MODEL = "gpt-6.1-sol"
OUTPUT_TOKENS = 1800
CAP = Decimal("1.00")
# Includes cache writes and 10% regional premium; standard tier only.
INPUT_RATE = Decimal("2.75") / 1000000
OUTPUT_RATE = Decimal("11") / 1000000


class BudgetStopped(Exception):
    pass


class Budget:
    def __init__(self, path: Path):
        self.path = path

    def reserve(self, payload: bytes) -> str:
        data = json.loads(payload)
        if (len(payload) > 60000 or data.get("model") != MODEL
                or data.get("max_output_tokens") != OUTPUT_TOKENS
                or data.get("service_tier") != "default"
                or data.get("store") is not False or data.get("previous_response_id")):
            raise BudgetStopped("request_outside_evaluation_limits")
        if any(tool.get("type") != "function" for tool in data.get("tools", [])):
            raise BudgetStopped("paid_builtin_tools_not_budgeted")
        def unsupported(value):
            if isinstance(value, dict):
                return value.get("type") in {"input_image", "input_audio", "input_file", "item_reference"} or any(unsupported(v) for v in value.values())
            return isinstance(value, list) and any(unsupported(v) for v in value)
        if unsupported(data.get("input")):
            raise BudgetStopped("modality_not_budgeted")
        # UTF-8 bytes bound byte-token input; padding covers message framing.
        amount = (len(payload) + 4096) * INPUT_RATE + OUTPUT_TOKENS * OUTPUT_RATE
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+", encoding="utf-8") as file:
            fcntl.flock(file, fcntl.LOCK_EX)
            file.seek(0)
            raw = file.read()
            try:
                rows = json.loads(raw) if raw else []
            except ValueError as exc:
                raise BudgetStopped("invalid_ledger") from exc
            if not isinstance(rows, list):
                raise BudgetStopped("invalid_ledger")
            try:
                costs = [Decimal(row["reserved_usd"]) for row in rows]
                if any(not cost.is_finite() or cost <= 0 for cost in costs):
                    raise ValueError("invalid cost")
            except (KeyError, TypeError, ArithmeticError, ValueError) as exc:
                raise BudgetStopped("invalid_ledger") from exc
            if len(rows) >= 40 or sum(costs) + amount > CAP:
                raise BudgetStopped("evaluation_budget_exhausted")
            request_id = str(uuid4())
            rows.append({"request_id": request_id, "reserved_usd": str(amount),
                         "request_sha256": hashlib.sha256(payload).hexdigest(),
                         "request_bytes": len(payload)})
            file.seek(0)
            file.truncate()
            json.dump(rows, file, indent=2)
            file.flush()
            import os
            os.fsync(file.fileno())
            return request_id

    def before_request(self, request):
        if request.method != "POST" or request.url.host != "api.openai.com" or request.url.path != "/v1/responses":
            raise BudgetStopped("provider_endpoint_forbidden")
        request.extensions["evaluation_request_id"] = self.reserve(request.content)

    async def before_async_request(self, request):
        self.before_request(request)
