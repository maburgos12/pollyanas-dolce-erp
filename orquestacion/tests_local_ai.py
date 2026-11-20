from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db import close_old_connections
from django.test import TransactionTestCase
from django.urls import reverse
from django.utils import timezone

from orquestacion.models import AgentDefinition, AgentTask, OrchestrationRun
from orquestacion.services import local_ai


class LocalAISummaryTests(TransactionTestCase):
    def setUp(self):
        local_ai.configure(enabled=True)
        self.agent = AgentDefinition.objects.get(code=local_ai.NODE)
        self.run = OrchestrationRun.objects.create(run_key="source", trigger_source="test")

    def source(self, entity=1, task_type="inventory_adjustment_authorization_guard", **kwargs):
        field = "ajuste_id" if task_type.startswith("inventory") else "plan_id"
        return AgentTask.objects.create(
            run=self.run, agent=self.agent, title="No enviar nombre ni notas privadas",
            task_type=task_type, input_payload={field: entity, "reasons": ["dato privado"]},
            **kwargs,
        )

    def test_group_duplicates_keep_roles_and_exclude_private_data(self):
        for _ in range(6):
            self.source()
        self.source(task_type="plan_demand_chain_review")
        self.source(task_type="plan_purchase_chain_review")
        stats = local_ai.enqueue()
        self.assertEqual(stats["source_count"], 8)
        self.assertEqual(stats["case_count"], 3)
        self.assertEqual(stats["created"], 1)
        job = local_ai.claim()
        self.assertEqual(len(job["cases"]), 3)
        self.assertNotIn("privado", str(job))
        self.assertEqual(local_ai.enqueue()["created"], 0)
        self.source()  # Another routine run with identical facts is not new work.
        self.assertEqual(local_ai.enqueue()["created"], 0)

    def test_changed_evidence_enqueues_once(self):
        task = self.source()
        local_ai.enqueue()
        task.priority = AgentTask.PRIORITY_CRITICAL
        task.save()
        self.assertEqual(local_ai.enqueue()["created"], 1)
        self.assertEqual(local_ai.enqueue()["created"], 0)

    def test_claim_is_exclusive_across_connections(self):
        for entity in range(6):
            self.source(entity + 1)
        local_ai.enqueue()
        def claim():
            close_old_connections()
            try:
                return local_ai.claim()
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = list(pool.map(lambda _: claim(), range(2)))
        self.assertEqual(sum(job is not None for job in jobs), 1)

    def finish(self, job, result=None):
        return local_ai.finish(
            job["id"], job["lease"], result=result or {
                "summary": "Casos agrupados para revisión humana.",
                "case_keys": [case["key"] for case in job["cases"]],
            }, usage={"prompt_tokens": 200, "completion_tokens": 25}, duration=4.5,
        )

    def test_success_preserves_sources_and_redacts_lease(self):
        source = self.source()
        local_ai.enqueue()
        job = local_ai.claim()
        self.finish(job)
        source.refresh_from_db()
        self.assertEqual(source.status, "pending")
        self.assertEqual(source.output_payload, {})
        panel = local_ai.panel()
        self.assertEqual(panel["jobs"][0]["status"], "resolved")
        self.assertEqual(panel["jobs"][0]["tokens"], 225)
        self.assertNotIn(job["lease"], str(panel))
        self.assertEqual(local_ai.enqueue()["created"], 0)

    def test_invalid_output_cannot_finish_and_bounded_failure(self):
        self.source()
        local_ai.enqueue()
        job = local_ai.claim()
        with self.assertRaises(ValueError):
            self.finish(job, {"summary": "Inventado", "case_keys": ["inventado"]})
        for attempt in range(3):
            if attempt:
                task = AgentTask.objects.get(pk=job["id"])
                task.due_at = timezone.now() - timedelta(seconds=1)
                task.save()
                job = local_ai.claim()
            local_ai.finish(job["id"], job["lease"], error="invalid_output")
        self.assertIsNone(local_ai.claim())
        task.refresh_from_db()
        self.assertEqual(task.status, "blocked")
        self.assertEqual(task.output_payload["attempts"], 3)

    def test_expired_or_wrong_lease_and_stale_sources_are_rejected(self):
        source = self.source()
        local_ai.enqueue()
        job = local_ai.claim()
        with self.assertRaises(ValueError):
            local_ai.finish(job["id"], "wrong", error="timeout")
        source.status = "resolved"
        source.save()
        self.finish(job)
        task = AgentTask.objects.get(pk=job["id"])
        self.assertEqual(task.status, "cancelled")
        self.assertNotIn("result", task.output_payload)
        local_ai.enqueue()
        job = local_ai.claim()
        task = AgentTask.objects.get(pk=job["id"])
        task.due_at = timezone.now() - timedelta(seconds=1)
        task.save()
        with self.assertRaises(ValueError):
            self.finish(job)
        replacement = local_ai.claim()
        self.assertNotEqual(replacement["lease"], job["lease"])

    def test_disabled_does_not_enqueue_or_claim(self):
        self.source()
        local_ai.enqueue()
        local_ai.configure(enabled=False)
        self.assertEqual(local_ai.enqueue()["created"], 0)
        self.assertIsNone(local_ai.claim())

    def test_panel_requires_existing_permission_and_never_caches(self):
        url = reverse("orquestacion:dashboard") + "?section=local_ai"
        self.assertEqual(self.client.get(url).status_code, 302)
        user = get_user_model().objects.create_user(username="local-ai-reader", password="test")
        self.client.force_login(user)
        self.assertEqual(self.client.get(url).status_code, 403)
        user.is_superuser = True
        user.is_staff = True
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response["Cache-Control"])
        self.assertContains(response, "Mac-IA-02")

    def test_schedule_is_idempotent_and_only_pilot_is_toggled(self):
        from django_celery_beat.models import PeriodicTask
        other = PeriodicTask.objects.create(name="unrelated-existing", task="unrelated", enabled=False,
            interval=PeriodicTask.objects.get(name=local_ai.SCHEDULE).interval)
        local_ai.configure(enabled=True)
        local_ai.configure(enabled=True)
        self.assertEqual(PeriodicTask.objects.filter(name=local_ai.SCHEDULE).count(), 1)
        local_ai.configure(enabled=False)
        other.refresh_from_db()
        self.assertFalse(other.enabled)
        self.assertFalse(PeriodicTask.objects.get(name=local_ai.SCHEDULE).enabled)

    def test_count_whitelist_and_source_changes_cancel_before_claim(self):
        source = self.source(task_type="plan_purchase_chain_review", output_payload={
            "shortage_count": 2, "critical_shortage_count": 1, "estimated_shortage_amount": "9000",
            "supplier_name": "Privado", "recipe_ids": [1],
        })
        local_ai.enqueue()
        payload = AgentTask.objects.get(task_type=local_ai.TASK).input_payload
        self.assertEqual(payload["cases"][0]["counts"], {"shortage_count": 2, "critical_shortage_count": 1})
        self.assertNotIn("Privado", str(payload))
        source.output_payload["shortage_count"] = 4
        source.save()
        self.assertIsNone(local_ai.claim())
        self.assertEqual(AgentTask.objects.get(task_type=local_ai.TASK).status, "cancelled")
        source.output_payload["shortage_count"] = 2
        source.save()
        self.assertEqual(local_ai.enqueue()["created"], 1)
        self.assertIsNotNone(local_ai.claim())

    def test_capacity_and_incomplete_source_scan_are_bounded(self):
        AgentTask.objects.bulk_create([AgentTask(run=self.run, agent=self.agent, title="source",
            task_type="inventory_adjustment_authorization_guard", input_payload={"ajuste_id": i + 1}) for i in range(2001)])
        stats = local_ai.enqueue()
        self.assertTrue(stats["truncated"])
        self.assertEqual(stats["created"], 0)
        AgentTask.objects.filter(task_type__in=local_ai.SOURCES).order_by("id").first().delete()
        stats = local_ai.enqueue()
        self.assertFalse(stats["truncated"])
        self.assertEqual(stats["created"], 8)
        self.assertEqual(local_ai.enqueue()["created"], 0)
