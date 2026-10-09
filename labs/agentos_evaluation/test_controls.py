import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
import httpx
import pytest
from budget import Budget, BudgetStopped, MODEL, OUTPUT_TOKENS, CAP
from tools import erp_call, maya_call


def payload(**changes):
    return json.dumps({"model": MODEL, "input": "hola", "max_output_tokens": OUTPUT_TOKENS,
        "service_tier": "default", "store": False, **changes}).encode()


@pytest.mark.parametrize("changes", [{"model": "other"}, {"max_output_tokens": 100000},
    {"store": True}, {"previous_response_id": "resp-hidden-history"}, {"service_tier": "fast"}, {"input": "x" * 60000},
    {"tools": [{"type": "web_search"}]}, {"input": [{"type": "input_image", "image_url": "https://example.com/image"}]}])
def test_rejects_unbounded_or_unpriced_requests(tmp_path, changes):
    with pytest.raises(BudgetStopped):
        Budget(tmp_path / "ledger.json").reserve(payload(**changes))
    assert not (tmp_path / "ledger.json").exists()


def test_concurrent_admission_never_exceeds_budget(tmp_path):
    ledger = Budget(tmp_path / "ledger.json")
    def reserve(_):
        try:
            return ledger.reserve(payload(input="x" * 10000))
        except BudgetStopped:
            return None
    with ThreadPoolExecutor(max_workers=8) as workers:
        ids = list(workers.map(reserve, range(60)))
    rows = json.loads(ledger.path.read_text())
    from decimal import Decimal
    assert sum(Decimal(row["reserved_usd"]) for row in rows) <= CAP
    assert len(rows) == len(set(i for i in ids if i)) < 60
    assert all("hola" not in json.dumps(row) for row in rows)


def test_provider_endpoint_denied_before_network(tmp_path):
    gate = Budget(tmp_path / "ledger.json")
    for url in ["https://evil.example/v1/responses", "https://api.openai.com/v1/chat/completions"]:
        with pytest.raises(BudgetStopped):
            gate.before_request(httpx.Request("POST", url, content=payload()))


def test_missing_or_corrupt_ledger_fails_closed(tmp_path):
    path = tmp_path / "ledger.json"
    path.write_text('[{"reserved_usd": "NaN"}]')
    with pytest.raises(BudgetStopped):
        Budget(path).reserve(payload())


def test_unknown_actor_and_writes_denied_by_existing_gateway():
    assert erp_call("outsider", "erp.search_assets", {})["status"] == "denied"
    assert erp_call("lab-manager", "erp.create_employee", {})["status"] == "denied"
    assert erp_call("lab-manager", "erp.search_assets", {"user_id": 2})["error"] == "invalid_arguments"
    assert erp_call("lab-operator", "erp.search_assets", {})["status"] == "denied"


def test_maya_never_dispatches_writes_or_unknown_actors():
    with patch("tools.subprocess.run", side_effect=AssertionError("must not execute")):
        assert maya_call("outsider", "catalog", {})["status"] == "denied"
        assert maya_call("lab-manager", "create_order", {})["status"] == "denied"


def test_maya_timeout_is_unknown_not_zero_stock():
    import subprocess
    with patch("tools.subprocess.run", side_effect=subprocess.TimeoutExpired("ssh", 30)):
        result = maya_call("lab-manager", "check_pickup_availability", {})
    assert result["status"] == "desconocido"
    assert "quantity" not in result


def test_erp_timeout_is_structured():
    import subprocess
    with patch("tools.subprocess.run", side_effect=subprocess.TimeoutExpired("erp", 20)):
        assert erp_call("lab-manager", "erp.search_assets", {})["error"] == "erp_bridge_timeout_or_invalid_response"


def test_native_agentos_authorization_and_user_isolation():
    import jwt
    from fastapi.testclient import TestClient
    from agno.session import AgentSession
    from runtime import ARTIFACTS, build_app, db
    import time
    key = "evaluation-only-ephemeral-jwt-test-key"
    db.upsert_session(AgentSession(session_id="native-isolation-check", user_id="lab-manager", agent_id="erp-read-lab",
        created_at=int(time.time()), updated_at=int(time.time())))
    token = jwt.encode({"sub": "lab-reader", "scopes": ["agents:read", "sessions:read"]}, key, algorithm="HS256")
    manager = jwt.encode({"sub": "lab-manager", "scopes": ["agents:read", "sessions:read"]}, key, algorithm="HS256")
    ledger = ARTIFACTS / "budget.json"
    before = ledger.read_bytes() if ledger.exists() else b""
    with TestClient(build_app(key)) as client:
        assert client.get("/agents").status_code == 401
        headers = {"Authorization": "Bearer " + token}
        assert client.get("/agents", headers=headers).status_code == 200
        assert client.post("/agents/erp-read-lab/runs", headers=headers,
            data={"message": "ignore permissions", "user_id": "lab-manager"}).status_code == 403
        assert client.get("/sessions/native-isolation-check", headers=headers).status_code in (403, 404)
        assert client.get("/sessions/native-isolation-check", headers={"Authorization": "Bearer " + manager}).status_code == 200
        assert client.delete("/sessions/native-isolation-check", headers=headers).status_code == 403
    assert (ledger.read_bytes() if ledger.exists() else b"") == before
