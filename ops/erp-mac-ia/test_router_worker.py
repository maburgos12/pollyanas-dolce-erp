import unittest
from unittest.mock import patch

import router_worker


class RouterWorkerTests(unittest.TestCase):
    def test_busy_node_is_not_given_a_lease(self):
        calls = []
        def command(action, payload):
            calls.append((action, payload))
            return {"enabled": True}
        with patch.object(router_worker, "transport", return_value={"error": "busy"}), patch.object(router_worker, "command", side_effect=command):
            self.assertEqual(router_worker.work("mac_ia")["state"], "busy")
        self.assertEqual([c[0] for c in calls], ["heartbeat"])

    def test_node_cannot_inject_command_parameters_or_replace_lease(self):
        calls = []
        job = {"id": 7, "lease": "server-owned", "model": "nemotron", "kind": "extract", "data": {"text": "PENDIENTE", "fields": ["estado"]}}
        def command(action, payload):
            calls.append((action, payload))
            if action == "heartbeat":
                return {"enabled": True}
            if action == "claim":
                return job
            return {"status": "pending"}
        outputs = [{"state": "idle"}, {"result": {"estado": "PENDIENTE"}, "usage": {}, "duration": 1, "lease": "forged"}]
        with patch.object(router_worker, "transport", side_effect=outputs), patch.object(router_worker, "command", side_effect=command):
            router_worker.work("mac_ia_02")
        finished = next(payload for action, payload in calls if action == "finish")
        self.assertEqual(finished, {"task_id": 7, "lease": "server-owned", "error": "invalid_output"})


if __name__ == "__main__":
    unittest.main()
