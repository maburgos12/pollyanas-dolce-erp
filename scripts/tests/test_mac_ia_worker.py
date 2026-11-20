import importlib.util
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("mac_worker", Path(__file__).resolve().parents[2] / "ops/erp-mac-ia/worker.py")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


class LocalWorkerTests(unittest.TestCase):
    def test_http_is_local_authenticated_and_bounded(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                self.server.received = (self.headers["Authorization"], json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'x' * 33000)
        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self.assertEqual(worker.request("/test", "test-only-key", {"fixed": True}, port=server.server_port), {"status": "ok"})
            self.assertEqual(server.received, ("Bearer test-only-key", {"fixed": True}))
            with self.assertRaisesRegex(ValueError, "http_error"):
                worker.request("/test", "test-only-key", port=server.server_port)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_schema_budget_and_reject_truncated_invented_or_wrong_model(self):
        job = {"cases": [{"key": "ajuste:1", "status": "pending", "priority": "high"}]}
        good = {"model": worker.MODEL, "choices": [{"finish_reason": "stop", "message": {"content": json.dumps({"summary": "Revisar evidencia", "case_keys": ["ajuste:1"]})}}], "usage": {"prompt_tokens": 80, "completion_tokens": 20}}
        with patch.object(worker, "request", return_value=good) as request:
            result = worker.infer(job, "test-key")
            self.assertEqual(result["usage"]["prompt_tokens"], 80)
            self.assertEqual(request.call_args.args[2]["max_tokens"], 192)
            self.assertEqual(request.call_args.args[2]["response_format"]["type"], "json_schema")
        for field, value in [("model", "paid-provider"), ("usage", {"prompt_tokens": -1})]:
            with patch.object(worker, "request", return_value={**good, field: value}):
                with self.assertRaises(ValueError):
                    worker.infer(job, "test-key")
        good["choices"][0]["finish_reason"] = "length"
        with patch.object(worker, "request", return_value=good):
            with self.assertRaises(ValueError):
                worker.infer(job, "test-key")

    def test_offline_does_not_claim_and_has_no_fallback(self):
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as directory:
            key = Path(directory) / "key"
            key.write_text("test-only")
            key.chmod(0o600)
            real_open = open
            def locked_file(*args, **kwargs):
                return real_open(Path(directory) / "lock", "w")
            with patch.object(worker, "KEY", key), patch("builtins.open", side_effect=locked_file), patch.object(worker, "request", side_effect=ConnectionRefusedError), patch.object(worker, "command", return_value={"enabled": True}) as command:
                worker.main()
            self.assertEqual(command.call_args_list[0].args, ("heartbeat", "offline"))
            self.assertEqual(command.call_count, 1)


if __name__ == "__main__":
    unittest.main()
