from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4

from django.db import close_old_connections
from django.test import TransactionTestCase
from django.utils import timezone

from orquestacion.models import AgentTask, AgentDefinition
from orquestacion.services import local_router


class LocalRouterTests(TransactionTestCase):
    def setUp(self):
        self.agent = AgentDefinition.objects.create(code="test_router", name="Prueba", domain="technical", status="active")
        local_router.configure(enabled=True)

    def submit(self, kind="summary", data=None, request_id=None):
        return local_router.submit(agent=self.agent.code, kind=kind,
            data=data or {"text": "Evidencia registrada: dos tareas pendientes. No hay autorización de cambios."},
            request_id=str(request_id or uuid4()))

    def test_routes_by_contract_and_size(self):
        self.assertEqual(self.submit()["model"], "qwen4b")
        self.assertEqual(self.submit(data={"text": "Evidencia " * 150})["model"], "qwen8b")
        self.assertEqual(self.submit("extract", {"text": "Estado: PENDIENTE.", "fields": ["estado"]})["model"], "nemotron")
        self.assertEqual(self.submit("verify", {"rows": [{"expected": "0.30", "actual": "0.3"}]})["model"], "python")
        result = self.submit("deploy", {"text": "Publicar"})
        self.assertEqual(result["status"], "blocked")
        self.assertIsNone(result["node"])

    def test_idempotency_and_payload_conflict(self):
        key = uuid4()
        first = self.submit(request_id=key)
        self.assertEqual(first["id"], self.submit(request_id=key)["id"])
        with self.assertRaises(ValueError):
            self.submit(data={"text": "Otra evidencia"}, request_id=key)

    def test_invalid_boundaries_and_inactive_agent(self):
        for kind, data in [("extract", {"text": "hola", "fields": []}),
                           ("verify", {"rows": [{"expected": "NaN", "actual": "0"}]}),
                           ("summary", {"text": "a" * 2501}),
                           ("summary", {"text": "hola", "command": "pwd"})]:
            with self.subTest(kind=kind, data=data):
                with self.assertRaises(ValueError):
                    self.submit(kind, data)
        self.agent.status = "paused"
        self.agent.save()
        with self.assertRaises(ValueError):
            self.submit()

    def test_claim_is_per_node_and_exclusive(self):
        self.submit()
        self.submit()
        self.submit("extract", {"text": "PENDIENTE", "fields": ["estado"]})
        def claim(_):
            close_old_connections()
            try:
                return local_router.claim("mac_ia")
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = list(pool.map(claim, range(2)))
        self.assertEqual(sum(j is not None for j in jobs), 1)
        self.assertIsNotNone(local_router.claim("mac_ia_02"))

    def test_extract_rejects_invention_and_preserves_missing(self):
        task = self.submit("extract", {"text": "Estado PENDIENTE; importe no aportado", "fields": ["estado", "importe"]})
        job = local_router.claim("mac_ia_02")
        with self.assertRaises(ValueError):
            local_router.finish(job["id"], job["lease"], result={"estado": "APROBADO", "importe": "0"})
        result = local_router.finish(job["id"], job["lease"], result={"estado": "PENDIENTE", "importe": None}, duration=2)
        self.assertEqual(result["status"], "resolved")
        with self.assertRaises(ValueError):
            local_router.finish(job["id"], job["lease"], result={"estado": "PENDIENTE", "importe": None})
        self.assertNotIn(job["lease"], str(local_router.panel()))
        self.assertEqual(AgentTask.objects.get(pk=task["id"]).output_payload["result"]["importe"], None)

    def test_verify_recomputes_exact_numbers(self):
        self.submit("verify", {"rows": [{"expected": "0.3", "actual": "0.30"}, {"expected": "2", "actual": "1"}]})
        job = local_router.claim("mac_ia")
        with self.assertRaises(ValueError):
            local_router.finish(job["id"], job["lease"], result={"checked": 2, "mismatch_indices": []})
        self.assertEqual(local_router.finish(job["id"], job["lease"], result={"checked": 2, "mismatch_indices": [1]})["status"], "resolved")

    def test_expiry_pause_retry_and_agent_revocation(self):
        first = self.submit()
        job = local_router.claim("mac_ia")
        task = AgentTask.objects.get(pk=first["id"])
        task.due_at = timezone.now() - timedelta(seconds=1)
        task.save()
        with self.assertRaises(ValueError):
            local_router.finish(job["id"], job["lease"], result={"summary": "Viejo"})
        replacement = local_router.claim("mac_ia")
        self.assertNotEqual(job["lease"], replacement["lease"])
        local_router.finish(replacement["id"], replacement["lease"], error="busy")
        self.assertIsNone(local_router.claim("mac_ia"))
        local_router.configure(enabled=False)
        self.assertIsNone(local_router.claim("mac_ia"))
        with self.assertRaises(ValueError):
            self.submit()

    def test_capacity_is_bounded(self):
        for _ in range(16):
            self.submit()
        with self.assertRaises(ValueError):
            self.submit()

    def test_numeric_result_types_are_exact(self):
        self.submit("verify", {"rows": [{"expected": "2", "actual": "1"}]})
        job = local_router.claim("mac_ia")
        for result in ({"checked": True, "mismatch_indices": [0]}, {"checked": 1, "mismatch_indices": [0.0]}):
            with self.assertRaises(ValueError):
                local_router.finish(job["id"], job["lease"], result=result)

    def test_source_references_refresh_and_idempotency_survives_change(self):
        from orquestacion.models import OrchestrationRun
        from orquestacion.services import local_ai
        run = OrchestrationRun.objects.create(run_key="router-source")
        source = AgentTask.objects.create(run=run, agent=self.agent, title="Source", task_type="plan_purchase_chain_review", input_payload={"plan_id": 1})
        cases, refs, _ = local_ai._sources()
        data = {"cases": list(cases.values()), "references": list(refs.values())}
        key = uuid4()
        first = self.submit("summary_cases", data, key)
        self.assertEqual(first["references"], data["references"])
        job = local_router.claim("mac_ia")
        AgentTask.objects.create(run=run, agent=self.agent, title="Replacement", task_type=source.task_type, input_payload=source.input_payload)
        self.assertEqual(self.submit("summary_cases", data, key)["id"], first["id"])
        result = local_router.finish(job["id"], job["lease"], result={"summary": "Revisión", "case_keys": ["purchase:1"]})
        self.assertEqual(result["status"], "cancelled")

    def test_existing_enqueuer_uses_router_without_duplicate_sources(self):
        from orquestacion.models import OrchestrationRun
        from orquestacion.services import local_ai
        local_ai.configure(enabled=True)
        run = OrchestrationRun.objects.create(run_key="automatic-source")
        source = AgentTask.objects.create(run=run, agent=self.agent, title="Source", task_type="plan_purchase_chain_review", input_payload={"plan_id": 1})
        self.assertEqual(local_ai.enqueue()["created"], 1)
        self.assertTrue(AgentTask.objects.filter(task_type=local_router.TASK).exists())
        self.assertFalse(AgentTask.objects.filter(task_type=local_ai.TASK).exists())
        self.assertEqual(local_ai.enqueue()["created"], 0)
        source.refresh_from_db()
        self.assertEqual(source.status, "pending")

    def test_periodic_cancelled_work_can_resume_after_router_pause(self):
        from orquestacion.models import OrchestrationRun
        from orquestacion.services import local_ai
        local_ai.configure(enabled=True)
        run = OrchestrationRun.objects.create(run_key="resume-source")
        AgentTask.objects.create(run=run, agent=self.agent, title="Source", task_type="plan_purchase_chain_review", input_payload={"plan_id": 1})
        local_ai.enqueue()
        job = local_router.claim("mac_ia")
        local_router.configure(enabled=False)
        result = local_router.finish(job["id"], job["lease"], result={"summary": "Revisión", "case_keys": ["purchase:1"]})
        self.assertEqual(result["status"], "cancelled")
        local_router.configure(enabled=True)
        self.assertEqual(local_ai.enqueue()["created"], 1)
        replacement = local_router.claim("mac_ia")
        self.assertIsNotNone(replacement)
        self.assertEqual(local_ai.enqueue()["created"], 0)

    def test_utf8_budget_protects_context_limits(self):
        self.assertEqual(self.submit(data={"text": "á" * 600})["model"], "qwen8b")
        with self.assertRaises(ValueError):
            self.submit(data={"text": "á" * 1300})

    def test_short_work_uses_spare_mac_when_debian_already_has_work(self):
        self.assertEqual(self.submit()["node"], "mac_ia")
        spare = self.submit()
        self.assertEqual((spare["node"], spare["model"]), ("mac_ia_02", "qwen8b"))
        self.assertEqual(self.submit()["node"], "mac_ia")

    def test_periodic_batches_can_work_on_both_nodes(self):
        from orquestacion.models import OrchestrationRun
        from orquestacion.services import local_ai
        local_ai.configure(enabled=True)
        run = OrchestrationRun.objects.create(run_key="two-node-source")
        for entity in range(1, 5):
            AgentTask.objects.create(run=run, agent=self.agent, title="Source", task_type="plan_purchase_chain_review", input_payload={"plan_id": entity})
        self.assertEqual(local_ai.enqueue()["created"], 2)
        self.assertEqual(set(AgentTask.objects.filter(task_type=local_router.TASK).values_list("input_payload__node", flat=True)), {"mac_ia", "mac_ia_02"})
