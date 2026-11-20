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

    def test_finish_rejection_retains_safe_reason_without_exception_text(self):
        import subprocess
        job = {"id": 7, "lease": "private", "model": "nemotron", "kind": "extract", "data": {}}
        def command(action, payload):
            if action == "claim": return job
            if action == "finish" and "error" not in payload:
                raise subprocess.CalledProcessError(1, ["SECRET"], stderr='CommandError: {"router_error":"invalid_output"}\n')
            return {"enabled": True, "status": "pending"}
        with patch.object(router_worker, "transport", side_effect=[{"state": "idle"}, {"result": {}, "usage": {}, "duration": 1}]), patch.object(router_worker, "command", side_effect=command):
            result = router_worker.work("mac_ia_02")
        self.assertEqual(result["diagnostic"], {"phase": "finish", "category": "process_exit", "reason": "invalid_output"})
        self.assertEqual(result["id"], 7)
        self.assertNotIn("SECRET", str(result))
        self.assertNotIn("private", str(result))

    def test_busy_probe_with_diagnostic_still_yields_without_claim(self):
        output = {"error": "busy", "diagnostic": {"phase": "guard", "category": "runtime", "reason": "unknown"}}
        with patch.object(router_worker, "transport", return_value=output), patch.object(router_worker, "command", return_value={"enabled": True}) as command:
            result = router_worker.work("mac_ia_02")
        self.assertEqual(result["state"], "busy")
        self.assertEqual(command.call_count, 1)

    def test_untrusted_stderr_is_never_logged(self):
        import subprocess
        for raw in ['{"router_error":"SECRET"}', '{"router_error":"invalid_output","prompt":"SECRET"}', 'SECRET' * 300]:
            with self.subTest(raw_length=len(raw)):
                error = subprocess.CalledProcessError(255, ["SECRET"], stderr=raw)
                with patch.object(router_worker, "transport", side_effect=error), patch.object(router_worker, "command", return_value={"enabled": True}):
                    result = router_worker.work("mac_ia_02")
                self.assertEqual(result["diagnostic"], {"phase": "probe", "category": "process_exit", "reason": "unknown"})
                self.assertNotIn("SECRET", str(result))

    def test_node_diagnostics_are_allowlisted_and_not_forwarded_to_finish(self):
        job = {"id": 7, "lease": "private", "model": "nemotron", "kind": "extract", "data": {}}
        calls = []
        def command(action, payload):
            calls.append((action, payload))
            return job if action == "claim" else {"enabled": True, "status": "pending"}
        output = {"error": "invalid_output", "diagnostic": {"phase": "parse", "category": "json", "reason": "unknown"}}
        with patch.object(router_worker, "transport", side_effect=[{"state": "idle"}, output]), patch.object(router_worker, "command", side_effect=command):
            result = router_worker.work("mac_ia_02")
        self.assertEqual(result["diagnostic"], output["diagnostic"])
        self.assertEqual(next(p for a, p in calls if a == "finish"), {"task_id": 7, "lease": "private", "error": "invalid_output"})

    def test_malformed_node_diagnostics_and_timeout_details_are_not_logged(self):
        import subprocess
        for value in [None, {"phase": "PRIVATE", "category": "json", "reason": "unknown"}, {"phase": [], "category": "json", "reason": "unknown"}, {"phase": "parse", "category": "json", "reason": "unknown", "prompt": "PRIVATE"}]:
            self.assertIsNone(router_worker.node_diagnostic(value))
        result = router_worker.diagnostic(subprocess.TimeoutExpired(["PRIVATE"], 480, output="SECRET", stderr="SECRET"), "execute")
        self.assertEqual(result, {"phase": "execute", "category": "timeout", "reason": "unknown"})
        self.assertNotIn("PRIVATE", str(result))
        self.assertNotIn("SECRET", str(result))

    def test_recovery_failures_keep_the_original_diagnostic(self):
        import json
        import subprocess
        with patch.object(router_worker, "transport", return_value={"state": "idle"}), patch.object(router_worker, "command", side_effect=subprocess.TimeoutExpired(["PRIVATE"], 60)):
            result = router_worker.work("mac_ia_02")
        self.assertEqual(result["diagnostic"], {"phase": "heartbeat", "category": "timeout", "reason": "unknown"})
        job = {"id": 7, "lease": "PRIVATE", "model": "nemotron", "kind": "extract", "data": {}}
        def command(action, payload):
            if action == "claim": return job
            if action == "finish":
                if "error" in payload: raise json.JSONDecodeError("PRIVATE", "PRIVATE", 0)
                raise subprocess.CalledProcessError(1, ["PRIVATE"], stderr='CommandError: {"router_error":"invalid_output"}')
            return {"enabled": True}
        with patch.object(router_worker, "transport", side_effect=[{"state": "idle"}, {"result": {}, "usage": {}, "duration": 1}]), patch.object(router_worker, "command", side_effect=command):
            result = router_worker.work("mac_ia_02")
        self.assertEqual(result["diagnostic"], {"phase": "finish", "category": "process_exit", "reason": "invalid_output"})
        self.assertNotIn("PRIVATE", str(result))

    def test_backlog_drains_bounded_jobs_without_an_idle_minute_between(self):
        with patch.object(router_worker, "work", return_value={"id": 1, "status": "resolved"}) as work, patch.object(router_worker.time, "monotonic", return_value=0):
            self.assertEqual(len(router_worker.drain("mac_ia")), 4)
            self.assertEqual(work.call_count, 4)

    def test_drain_yields_when_node_busy_or_budget_expires(self):
        with patch.object(router_worker, "work", return_value={"state": "busy"}) as work:
            self.assertEqual(router_worker.drain("mac_ia"), [{"state": "busy"}])
            self.assertEqual(work.call_count, 1)
        with patch.object(router_worker, "work", return_value={"id": 1, "status": "resolved"}) as work, patch.object(router_worker.time, "monotonic", side_effect=[0, 0, 130]):
            self.assertEqual(len(router_worker.drain("mac_ia")), 1)
            self.assertEqual(work.call_count, 1)


if __name__ == "__main__":
    unittest.main()
