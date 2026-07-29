"""Check del contrato HTTP v2. Correr: python3 test_contract_v2.py."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import erp_client


class Response:
    status_code = 200
    text = ""

    def json(self):
        return {
            "contract_version": 2,
            "batch_id": "server-echo",
            "results": [{"event_id": "guid-v2", "outcome": "accepted"}],
        }


def test_envia_contrato_v2_al_endpoint_v2():
    captured = {}
    original_post = erp_client.requests.post
    try:
        def fake_post(url, json, headers, timeout):
            captured.update(url=url, json=json, timeout=timeout)
            return Response()

        erp_client.requests.post = fake_post
        result = erp_client.send_events(
            [
                {
                    "event_id": "guid-v2",
                    "source": "hikconnect_cloud",
                    "employee_external_id": "328",
                    "occurred_at": "2026-07-28T08:00:00-07:00",
                    "kind": "check_in",
                    "device_id": "hik-01",
                }
            ]
        )
        assert captured["url"].endswith("/rrhh/api/asistencia-hik/v2/")
        assert captured["json"]["contract_version"] == 2
        assert captured["json"]["events"][0]["event_id"] == "guid-v2"
        assert captured["json"]["batch_id"]
        assert result["results"][0]["event_id"] == "guid-v2"
    finally:
        erp_client.requests.post = original_post


if __name__ == "__main__":
    test_envia_contrato_v2_al_endpoint_v2()
    print("OK: contrato HTTP v2")
