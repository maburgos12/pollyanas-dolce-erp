"""Protocol and information-preservation tests for the Codex graph adapter."""
import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

try:
    from scripts.codebase_memory_compact import GraphAdapter
except ImportError:
    GraphAdapter = None
try:
    from scripts.install_codebase_memory_compact import plan_install, apply_install
except ImportError:
    plan_install = apply_install = None


class CompactOutputTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(GraphAdapter, "The compact graph adapter is not implemented")
        self.adapter = GraphAdapter()

    def request(self, name="get_code_snippet", **arguments):
        return self.adapter.request({"jsonrpc": "2.0", "id": 7, "method": "tools/call",
                                     "params": {"name": name, "arguments": arguments}})

    def result(self, payload, **extra):
        return {"jsonrpc": "2.0", "id": 7, "result": {
            "content": [{"type": "text", "text": json.dumps(payload)}], **extra}}

    def test_compacts_only_fingerprints_and_preserves_all_other_node_fields(self):
        node = {"qualified_name": "erp.pkg.f", "file_path": "pkg.py", "label": "Function",
                "source": "def f():\n    return 'fp sp bt'\n", "fp": "fingerprint",
                "sp": "structure", "bt": "body tokens", "complexity": 18,
                "signature": "()", "docstring": "Useful documentation", "future_field": [1, 2]}
        before = copy.deepcopy(node)
        self.request()
        result = self.adapter.response(self.result(node))
        compact = json.loads(result["result"]["content"][0]["text"])
        for key, value in node.items():
            if key in {"fp", "sp", "bt"}:
                self.assertNotIn(key, compact)
            else:
                self.assertEqual(compact[key], value)
        self.assertEqual(compact["_response_detail"]["omitted_fields"], ["bt", "fp", "sp"])
        self.assertEqual(node, before)

    def test_pagination_warnings_rank_and_unknown_fields_survive(self):
        payload = {"results": [{"qualified_name": "p.f", "file_path": "p.py", "fp": "x", "rank": -3.5}],
                   "total": 55, "has_more": True, "offset": 5, "warnings": ["partial index"],
                   "future": {"fp": "application data, not a symbol"}}
        self.request("search_graph", limit=1)
        output = json.loads(self.adapter.response(self.result(payload))["result"]["content"][0]["text"])
        self.assertEqual(output["total"], 55)
        self.assertTrue(output["has_more"])
        self.assertEqual(output["warnings"], payload["warnings"])
        self.assertEqual(output["results"][0]["rank"], -3.5)
        self.assertEqual(output["future"], payload["future"])
        self.assertNotIn("fp", output["results"][0])

    def test_full_mode_strips_only_adapter_argument_and_preserves_response_exactly(self):
        forwarded = self.request(response_detail="full", project="erp")
        self.assertEqual(forwarded["params"]["arguments"], {"project": "erp"})
        result = self.result({"qualified_name": "p.f", "file_path": "p.py", "fp": "x"})
        self.assertEqual(self.adapter.response(result), result)

    def test_other_tools_including_explicit_cypher_are_unchanged(self):
        self.request("query_graph", query="RETURN f.fp")
        result = self.result({"qualified_name": "p.f", "file_path": "p.py", "fp": "x"})
        self.assertEqual(self.adapter.response(result), result)

    def test_errors_and_non_json_text_are_preserved(self):
        self.request()
        result = self.result({"qualified_name": "p.f", "file_path": "p.py", "fp": "x"}, isError=True)
        self.assertEqual(self.adapter.response(result), result)
        self.request()
        result = {"id": 7, "result": {"content": [{"type": "text", "text": "parse error: fp"}]}}
        self.assertEqual(self.adapter.response(result), result)

    def test_structured_content_is_consistent_and_non_text_blocks_survive(self):
        self.request()
        node = {"qualified_name": "p.f", "file_path": "p.py", "fp": "x"}
        result = self.result(node, structuredContent=node)
        result["result"]["content"].append({"type": "resource_link", "uri": "local://evidence", "name": "evidence"})
        output = self.adapter.response(result)
        self.assertEqual(json.loads(output["result"]["content"][0]["text"]), output["result"]["structuredContent"])
        self.assertEqual(output["result"]["content"][1], result["result"]["content"][1])

    def test_schema_adds_full_option_without_changing_existing_contracts(self):
        self.adapter.request({"id": "list", "method": "tools/list"})
        schema = {"type": "object", "properties": {"limit": {"type": "integer"}}, "required": ["project"], "additionalProperties": False}
        result = {"id": "list", "result": {"tools": [{"name": "search_graph", "inputSchema": schema},
                                                            {"name": "query_graph", "inputSchema": schema}]}}
        output = self.adapter.response(result)
        changed = output["result"]["tools"][0]["inputSchema"]
        self.assertEqual(changed["properties"]["response_detail"]["enum"], ["compact", "full"])
        self.assertEqual(changed["required"], ["project"])
        self.assertEqual(changed["properties"]["limit"], {"type": "integer"})
        self.assertEqual(output["result"]["tools"][1], result["result"]["tools"][1])

    def test_out_of_order_replies_and_server_requests_do_not_mix_modes(self):
        self.request(response_detail="full")
        self.adapter.request({"id": 8, "method": "tools/call", "params": {"name": "search_graph", "arguments": {}}})
        server_request = {"id": 7, "method": "sampling/createMessage", "params": {}}
        self.assertEqual(self.adapter.response(server_request), server_request)
        server_reply = {"id": 8, "result": {"content": "sampling reply"}}
        self.assertEqual(self.adapter.request(server_reply), server_reply)
        full = self.result({"qualified_name": "p.f", "file_path": "p.py", "fp": "x"})
        compact = copy.deepcopy(full); compact["id"] = 8
        self.assertNotIn("fp", json.loads(self.adapter.response(compact)["result"]["content"][0]["text"]))
        self.assertEqual(self.adapter.response(full), full)

    def test_metadata_collision_fails_open_instead_of_overwriting_information(self):
        self.request()
        result = self.result({"qualified_name": "p.f", "file_path": "p.py", "fp": "x", "_response_detail": "upstream data"})
        self.assertEqual(self.adapter.response(result), result)


class StdioIntegrationTests(unittest.TestCase):
    def test_proxy_forwards_notifications_stderr_and_child_exit(self):
        script = Path(__file__).resolve().parents[1] / "codebase_memory_compact.py"
        self.assertTrue(script.exists(), "The stdio adapter is not implemented")
        with tempfile.TemporaryDirectory() as directory:
            child = Path(directory) / "server.py"
            child.write_text('''import json,sys
print("upstream diagnostic",file=sys.stderr)
for line in sys.stdin:
    message=json.loads(line)
    if "id" not in message:
        print(json.dumps({"method":"notifications/progress","params":{"seen":message["method"]}}),flush=True)
    else:
        node={"qualified_name":"p.f","file_path":"p.py","source":"def f(): pass","fp":"x"}
        print(json.dumps({"id":message["id"],"result":{"content":[{"type":"text","text":json.dumps(node)}]}}),flush=True)
sys.exit(3)
''')
            messages = [{"method": "notifications/initialized"}, {"id": 1, "method": "tools/call", "params": {"name": "get_code_snippet", "arguments": {}}}]
            result = subprocess.run([sys.executable, str(script), "--upstream", sys.executable, "--", str(child)],
                                    input="".join(json.dumps(x)+"\n" for x in messages),text=True,capture_output=True,timeout=10)
            self.assertEqual(result.returncode, 3, result.stderr)
            replies = [json.loads(line) for line in result.stdout.splitlines()]
            self.assertEqual(replies[0]["params"]["seen"], "notifications/initialized")
            self.assertNotIn("fp", json.loads(replies[1]["result"]["content"][0]["text"]))
            self.assertIn("upstream diagnostic",result.stderr)


class InstallerTests(unittest.TestCase):
    CONFIG = '''model = "example"
[mcp_servers.codebase-memory-mcp]
command = "/original/graph"
args = ["--ui=false"]
startup_timeout_sec = 60
[mcp_servers.codebase-memory-mcp.tools.search_graph]
approval_mode = "approve"
[mcp_servers.other]
command = "/keep/this"
'''

    def setUp(self):
        self.assertIsNotNone(plan_install, "The reversible installer is not implemented")
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = self.root / "config.toml"
        self.config.write_text(self.CONFIG)
        self.source = self.root / "adapter.py"
        self.source.write_text("# adapter runtime\n")

    def test_plan_is_read_only_and_keeps_other_settings_and_upstream_arguments(self):
        plan = plan_install(self.source, self.config, self.root / "installed", sys.executable)
        self.assertEqual(self.config.read_text(), self.CONFIG)
        self.assertFalse((self.root / "installed").exists())
        self.assertIn('startup_timeout_sec = 60', plan["config_after"])
        suffix = self.CONFIG[self.CONFIG.index('[mcp_servers.codebase-memory-mcp.tools'):]
        self.assertTrue(plan["config_after"].endswith(suffix))
        self.assertIn('"--ui=false"', plan["config_after"])
        self.assertEqual(plan["upstream_command"], "/original/graph")

    def test_apply_preserves_backup_and_is_idempotent(self):
        plan = plan_install(self.source, self.config, self.root / "installed", sys.executable)
        receipt = apply_install(plan)
        self.assertEqual(Path(receipt["config_backup"]).read_text(), self.CONFIG)
        self.assertEqual(Path(receipt["runtime_path"]).read_bytes(), self.source.read_bytes())
        next_plan = plan_install(self.source, self.config, self.root / "installed", sys.executable)
        self.assertFalse(next_plan["changed"])
        self.assertEqual(next_plan["upstream_command"], "/original/graph")
        self.assertEqual(next_plan["upstream_args"], ["--ui=false"])

    def test_refuses_to_overwrite_a_concurrent_configuration_change(self):
        plan = plan_install(self.source, self.config, self.root / "installed", sys.executable)
        self.config.write_text(self.CONFIG + "# added by another task\n")
        with self.assertRaisesRegex(RuntimeError, "changed"):
            apply_install(plan)


if __name__ == "__main__":
    unittest.main()
