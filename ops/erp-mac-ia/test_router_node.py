import importlib.util
from pathlib import Path
import unittest
import tempfile
from threading import Event
from unittest.mock import MagicMock, patch


def module():
    spec = importlib.util.spec_from_file_location("router_node", Path(__file__).with_name("router_node.py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


class RouterNodeTests(unittest.TestCase):
    def test_contracts_are_data_only(self):
        node = module()
        with self.assertRaises(ValueError):
            node.prepare({"model": "shell", "kind": "verify", "data": {}})
        with self.assertRaises(ValueError):
            node.prepare({"model": "nemotron", "kind": "summary", "data": {"text": "hola"}})
        prompt, schema = node.prepare({"model": "nemotron", "kind": "extract", "data": {"text": "Estado PENDIENTE", "fields": ["estado"]}})
        self.assertIn("PENDIENTE", prompt)
        self.assertEqual(schema["required"], ["estado"])
        self.assertFalse(schema["additionalProperties"])

    def test_error_response_contains_only_fixed_diagnostic_enums(self):
        import json
        node = module()
        node.PHASE = "parse"
        result = node.error_response(json.JSONDecodeError("SECRET", "PRIVATE-PROMPT", 0))
        self.assertEqual(result, {"error": "worker_error", "diagnostic": {"phase": "parse", "category": "json", "reason": "unknown"}})
        self.assertNotIn("SECRET", str(result))
        self.assertNotIn("PRIVATE", str(result))
        self.assertEqual(node.error_response(RuntimeError("busy"))["error"], "busy")

    def test_mac_rejected_completion_explains_token_limit_without_inference(self):
        node = module()
        response = {"model": "pastelerias-qwen3-8b", "choices": [{"finish_reason": "length", "message": {"content": "SECRET"}}]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "api.key").write_text("PRIVATE")
            (root / "api.key").chmod(0o600)
            with patch.object(node, "OP", root), patch.object(node, "mac_busy", return_value=False), patch.object(node, "mac_memory", return_value=(50, 0)), patch.object(node, "request", return_value=response):
                with self.assertRaises(ValueError) as error:
                    node.mac_infer("qwen8b", "PRIVATE-PROMPT", {}, root)
            self.assertEqual(str(error.exception), "invalid_output")
            result = node.error_response(error.exception)
        self.assertEqual(result["diagnostic"], {"phase": "parse", "category": "contract", "reason": "token_limit"})
        self.assertNotIn("PRIVATE", str(result))
        self.assertNotIn("SECRET", str(result))
        self.assertEqual(node.error_response(node.InvalidOutput("PRIVATE"))["diagnostic"]["reason"], "unknown")

    def test_exact_arithmetic_and_cli_completion_marker(self):
        node = module()
        self.assertEqual(node.verify({"rows": [{"expected": "0.3", "actual": "0.30"}, {"expected": "2", "actual": "1"}]}),
                         {"checked": 2, "mismatch_indices": [1]})
        self.assertEqual(node.parse_cli('{"summary":"Bien"} [end of text]\n'), {"summary": "Bien"})
        with self.assertRaises(ValueError):
            node.parse_cli('{"summary":"Incompleto"}')
        with self.assertRaises(ValueError):
            node.parse_cli('{"summary":"Bien"} [end of text] basura')

    def test_debian_cron_window_preserves_existing_work(self):
        node = module()
        self.assertFalse(node.cron_window(14, 55))
        self.assertFalse(node.cron_window(0, 10))
        self.assertFalse(node.cron_window(12, 0))
        self.assertTrue(node.cron_window(1, 0))
        self.assertTrue(node.cron_window(10, 0))

    def test_interrupt_stops_own_nemotron_before_waiting_for_http(self):
        node = module()
        started, stopped = Event(), Event()
        observed = []
        def request(port, key, path, payload=None):
            if path == "/health":
                return {"status": "ok"}
            started.set()
            observed.append(stopped.wait(0.5))
            return {"model": "pastelerias-nemotron-bolt", "choices": [{"finish_reason": "stop", "message": {"content": "{}"}}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}}
        def interrupted(seconds):
            self.assertTrue(started.wait(1))
            raise KeyboardInterrupt
        fake_socket = MagicMock()
        fake_socket.__enter__.return_value.connect_ex.return_value = 1
        proc = MagicMock()
        proc.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "api.key").write_text("test-only")
            (root / "api.key").chmod(0o600)
            with patch.object(node, "OP", root), patch.object(node, "mac_busy", return_value=False), patch.object(node, "mac_memory", return_value=(50, 0)), patch.object(node, "request", side_effect=request), patch.object(node.subprocess, "Popen", return_value=proc), patch("socket.socket", return_value=fake_socket), patch.object(node.time, "sleep", side_effect=interrupted), patch.object(node, "terminate", side_effect=lambda child: stopped.set()):
                with self.assertRaises(KeyboardInterrupt):
                    node.mac_infer("nemotron", "Evidencia", {}, root)
        self.assertEqual(observed, [True])


if __name__ == "__main__":
    unittest.main()
