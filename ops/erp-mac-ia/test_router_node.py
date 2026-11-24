import importlib.util
import json
import re
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


def rendered_edges(value):
    edges={}
    for line in value.splitlines():
        match=re.fullmatch(r'span-(\d+) ::= (.+)',line)
        if not match:continue
        index=int(match[1]);body=match[2];at=0;edges[index]=[]
        while True:
            literal,end=json.JSONDecoder().raw_decode(body[at:]);at+=end
            target=re.match(r' span-(\d+)\?',body[at:])
            dest=int(target[1]) if target else None
            if target:at+=target.end()
            edges[index].append((literal,dest))
            if at==len(body):break
            assert body[at:at+3]==' | ';at+=3
    return edges

def accepts(edges,value):
    if not value:return False
    encoded=json.dumps(value,ensure_ascii=False)[1:-1];pending=[(0,0)]
    while pending:
        state,at=pending.pop()
        for token,target in edges[state]:
            if encoded.startswith(token,at):
                end=at+len(token)
                if end==len(encoded):return True
                if target is not None:pending.append((target,end))
    return False


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

    def test_hours_contract_limits_model_input_and_output_shape(self):
        node = module()
        data = {"text": "Túnel sigue cerrado los miércoles", "draft": "Túnel: 09:00–19:00"}
        job = {"kind": "hours_draft_edit", "model": "qwen8b", "data": data}
        prompt, schema = node.prepare(job)
        self.assertIn(data["text"], prompt)
        self.assertEqual(set(schema["required"]), {"action", "branch", "detail"})
        self.assertFalse(schema["additionalProperties"])
        with self.assertRaises(ValueError):
            node.prepare({**job, "model": "qwen4b"})
        with self.assertRaises(ValueError):
            node.prepare({**job, "data": {**data, "text": "á" * 401}})

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

    def test_extract_grammar_preserves_all_spans_escapes_and_null(self):
        import itertools
        import json
        node = module()
        for text in ('a', 'ababa', 'No está demostrada la causa.', 'café cafe\u0301 "A\\B"\n\t\0 😀'):
            grammar = node._source_grammar(text, ['estado', 'causa'])
            edges = rendered_edges(grammar)
            self.assertIn('"null"', grammar)
            self.assertIn(json.dumps(json.dumps('causa')), grammar)
            for start in range(len(text)):
                for end in range(start + 1, len(text) + 1):
                    self.assertTrue(accepts(edges, text[start:end]))
            self.assertFalse(accepts(edges, ''))
            self.assertFalse(accepts(edges, 'NONLITERAL_NOT_IN_SOURCE'))
            for candidate in map(''.join, itertools.product('ab"', repeat=3)):
                self.assertEqual(accepts(edges, candidate), candidate in text)

    def test_extract_grammar_is_bounded_at_full_input_and_six_fields(self):
        import random
        node = module()
        rng = random.Random(42)
        sources = ['a' * 2500, ''.join(rng.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(2500)),
                   ''.join(chr(rng.randrange(0x400, 0x7e8)) for _ in range(1250)), ('\\"\n\t\0' * 500)]
        for text in sources:
            self.assertEqual(len(text.encode()), 2500)
            fields = ['f' + str(i) for i in range(6)]
            node.prepare({'model': 'nemotron', 'kind': 'extract', 'data': {'text': text, 'fields': fields}})
            grammar = node._source_grammar(text, fields)
            edges = rendered_edges(grammar)
            self.assertLessEqual(len(edges), 2 * len(text) + 1)
            self.assertLessEqual(sum(map(len, edges.values())), 3 * len(text) + 1)
            self.assertLessEqual(len(grammar.encode()), 512 * 1024)
            self.assertTrue(accepts(edges, text))

    def test_extract_dispatch_passes_grammar_and_keeps_prompt_limits(self):
        import io
        node = module()
        job = {'model': 'nemotron', 'kind': 'extract', 'data': {'text': 'No está demostrada.', 'fields': ['causa']}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'api.key').write_text('private')
            with patch.object(node, 'ROOT', root), patch.object(node, 'OP', root), patch.object(node, 'MAC', True), patch.object(node, 'mac_busy', return_value=False), patch.object(node.signal, 'signal'), patch.object(node.sys, 'stdin', io.StringIO(json.dumps(job))), patch.object(node, 'mac_infer', return_value={'result': {'causa': 'No está demostrada.'}, 'usage': {}}) as infer:
                result = node.main()
            self.assertEqual(result['result']['causa'], 'No está demostrada.')
            args = infer.call_args.args
            self.assertEqual(args[4], node._source_grammar(job['data']['text'], ['causa']))
            self.assertIn('contexto y negación', args[1])

    def test_http_grammar_replaces_schema_without_changing_generation_limits(self):
        node = module()
        response = {'model': 'pastelerias-qwen3-8b', 'choices': [{'finish_reason': 'stop', 'message': {'content': '{"causa":null}'}}], 'usage': {'prompt_tokens': 10, 'completion_tokens': 4}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'api.key').write_text('private')
            (root / 'api.key').chmod(0o600)
            with patch.object(node, 'OP', root), patch.object(node, 'mac_busy', return_value=False), patch.object(node, 'mac_memory', return_value=(50, 0)), patch.object(node, 'request', return_value=response) as request:
                node.mac_infer('qwen8b', 'source', {}, root, 'test-grammar')
            payload = request.call_args.args[3]
        self.assertEqual(payload['grammar'], 'test-grammar')
        self.assertNotIn('response_format', payload)
        self.assertEqual(payload['max_tokens'], 192)
        self.assertEqual(payload['temperature'], 0)

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
