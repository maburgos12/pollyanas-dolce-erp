from datetime import date
from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import TestCase, TransactionTestCase

from core.models import Sucursal
from horarios_especiales.models import HorarioEspecialDetalle, SolicitudHorarioEspecial
from horarios_especiales.services.requests import approve_request, cancel_request, create_request_from_rules, validate_request
from horarios_especiales.services.rules import resolve_approved_rule, validate_rules
from horarios_especiales.services.execution import execute_request
from horarios_especiales.services.effective_hours import read_effective_hours


class ScheduleRuleTests(TestCase):
    def setUp(self):
        self.branch = Sucursal.objects.create(codigo="RULEQA", nombre="Rule QA", activa=True)
        self.user = User.objects.create_user(username="rule_approver")

    def rule(self, kind="date", **kwargs):
        rule = {"kind": kind, "start_date": "2026-11-22", "closed_all_day": False,
                "time_windows": [{"open": "10:00", "close": "18:00"}]}
        if kind == "range":
            rule["end_date"] = "2026-11-30"
        if kind == "weekly":
            rule["weekdays"] = [6]
        return {**rule, **kwargs}

    def create(self, *rules):
        return create_request_from_rules(raw_text="explicit fixture", branch_ids=[self.branch.pk],
                                         rules=list(rules), actor=self.user)[0]

    def approve(self, obj):
        return approve_request(request_obj=obj, actor=self.user, expected_revision=obj.canonical_payload["revision"])

    def test_exact_preview_binding_and_cancelled_revision_cannot_reapprove(self):
        obj = self.create(self.rule())
        for revision in (None, "stale"):
            with self.assertRaises(ValueError):
                approve_request(request_obj=obj, actor=self.user, expected_revision=revision)
        self.approve(obj)
        self.assertEqual(obj.status, "APROBADO")
        with self.assertRaises(ValueError):
            self.approve(obj)
        self.assertEqual(obj.audit_entries.get(action="APPROVED").payload_json["revision"], obj.canonical_payload["revision"])
        cancel_request(request_obj=obj, actor=self.user)
        with self.assertRaises(ValueError):
            self.approve(obj)
        self.assertIsNone(resolve_approved_rule(self.branch, date(2026, 11, 22)))

    def test_modified_content_invalidates_revision_and_does_not_write_approval(self):
        obj = self.create(self.rule())
        obj.canonical_payload["rules"][0]["time_windows"][0]["open"] = "11:00"
        obj.save()
        with self.assertRaises(ValueError):
            self.approve(obj)
        obj.refresh_from_db()
        self.assertIsNone(obj.approved_at)

    def test_precedence_and_latest_approved_revision(self):
        exact = self.create(self.rule(closed_all_day=True, time_windows=[]))
        self.approve(exact)
        temporary = self.create(self.rule("range"))
        self.approve(temporary)
        weekly = self.create(self.rule("weekly", weekdays=[0, 6]))
        self.approve(weekly)
        self.assertEqual(resolve_approved_rule(self.branch, date(2026, 11, 22))["request_id"], exact.pk)
        self.assertEqual(resolve_approved_rule(self.branch, date(2026, 11, 23))["request_id"], temporary.pk)
        self.assertEqual(resolve_approved_rule(self.branch, date(2026, 12, 6))["request_id"], weekly.pk)
        latest = self.create(self.rule())
        self.approve(latest)
        self.assertEqual(resolve_approved_rule(self.branch, date(2026, 11, 22))["request_id"], latest.pk)
        cancel_request(request_obj=latest)
        self.assertEqual(resolve_approved_rule(self.branch, date(2026, 11, 22))["request_id"], exact.pk)

    def test_inclusive_period_weekdays_optional_end_and_multiple_dates(self):
        period = self.create(self.rule("range"))
        self.approve(period)
        for day in (22, 30):
            self.assertIsNotNone(resolve_approved_rule(self.branch, date(2026, 11, day)))
        self.assertIsNone(resolve_approved_rule(self.branch, date(2026, 12, 1)))
        weekly = self.create(self.rule("weekly", end_date="2026-12-06"))
        self.approve(weekly)
        self.assertIsNotNone(resolve_approved_rule(self.branch, date(2026, 12, 6)))
        self.assertIsNone(resolve_approved_rule(self.branch, date(2026, 12, 7)))
        self.assertIsNone(resolve_approved_rule(self.branch, date(2026, 12, 13)))
        dates = self.create(self.rule(start_date="2027-01-01"), self.rule(start_date="2027-01-05"))
        self.approve(dates)
        self.assertIsNone(resolve_approved_rule(self.branch, date(2027, 1, 2)))
        self.assertEqual(resolve_approved_rule(self.branch, date(2027, 1, 5))["request_id"], dates.pk)

    @patch("horarios_especiales.services.effective_hours._read_regular", return_value=({"status": "UNKNOWN", "windows": []}, None))
    def test_google_failure_does_not_revoke_erp_intent_or_maya_contract(self, _read):
        obj = self.create(self.rule())
        self.approve(obj)
        obj.status = "FALLIDO"
        obj.save()
        HorarioEspecialDetalle.objects.create(request=obj, sucursal=self.branch,
            target_date=date(2026, 11, 22), execution_status="FALLIDO")
        result = read_effective_hours("RULEQA", date(2026, 11, 22))
        self.assertEqual(result["effective"]["status"], "VERIFIED")
        self.assertEqual(result["effective"]["source"], "ERP_APPROVED_SPECIAL_HOURS")
        self.assertEqual(result["effective"]["revision"], obj.canonical_payload["revision"])
        self.assertEqual(result["branch_id"], str(self.branch.pk))
        self.assertEqual(result["timezone"], "America/Mazatlan")
        self.assertTrue(result["checked_at"])
        with self.assertRaises(ValueError):
            validate_request(request_obj=obj)

    def test_rules_not_silently_executed_as_one_day(self):
        obj = self.create(self.rule("weekly"))
        self.approve(obj)
        with patch("horarios_especiales.services.execution.GoogleBusinessProfilePublisher") as publisher:
            with self.assertRaises(ValueError):
                execute_request(request_obj=obj)
            publisher.assert_not_called()
        obj.refresh_from_db()
        self.assertEqual(obj.status, "APROBADO")
        self.assertIsNone(obj.executed_at)

    def test_invalid_rules_and_branch_ids_fail_before_any_write(self):
        invalid = [self.rule(start_date="2026-02-30"), self.rule(kind="unknown"),
                   self.rule("range", end_date=None), self.rule("range", end_date="2026-11-21"),
                   self.rule("weekly", weekdays=[True]), self.rule("weekly", weekdays=[7]),
                   self.rule("weekly", weekdays=[6, 6]), self.rule(closed_all_day="false"),
                   self.rule(time_windows=[{"open": "18:00", "close": "10:00"}]),
                   self.rule(time_windows=[{"open": "09:00", "close": "12:00"}, {"open": "11:00", "close": "13:00"}]),
                   self.rule(extra="ambiguous"), self.rule(weekdays=[0])]
        for rule in invalid:
            with self.subTest(rule=rule), self.assertRaises(ValueError):
                self.create(rule)
        for ids in ([self.branch.pk, self.branch.pk], [True], [999999], []):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                create_request_from_rules(raw_text="x", branch_ids=ids, rules=[self.rule()])
        with self.assertRaises(ValueError):
            validate_rules([self.rule()] * 32)
        self.assertFalse(SolicitudHorarioEspecial.objects.exists())

    def test_recomputed_payload_after_approval_is_not_approved_revision(self):
        from horarios_especiales.services.rules import rule_revision
        obj = self.create(self.rule())
        self.approve(obj)
        obj.canonical_payload["rules"][0]["time_windows"][0]["open"] = "11:00"
        obj.canonical_payload["revision"] = rule_revision(obj.canonical_payload)
        obj.save()
        self.assertEqual(resolve_approved_rule(self.branch, date(2026, 11, 22))["status"], "UNKNOWN")

    def test_api_requires_revision_and_rejects_unimplemented_publication(self):
        from rest_framework.test import APIClient
        self.user.is_superuser = True
        self.user.save()
        client = APIClient()
        client.force_authenticate(self.user)
        obj = self.create(self.rule())
        url = f"/api/integraciones/horarios-especiales/{obj.pk}/"
        self.assertEqual(client.post(url + "approve/", {}, format="json").status_code, 400)
        self.assertEqual(client.post(url + "approve/", {"expected_revision": obj.canonical_payload["revision"]}, format="json").status_code, 200)
        for action in ("validate", "execute", "retry"):
            self.assertEqual(client.post(url + action + "/", {}, format="json").status_code, 400)

    def test_api_rejects_nonobject_or_malformed_approval_body(self):
        import json
        from rest_framework.test import APIClient
        self.user.is_superuser = True
        self.user.save()
        client = APIClient()
        client.force_authenticate(self.user)
        obj = self.create(self.rule())
        url = f"/api/integraciones/horarios-especiales/{obj.pk}/approve/"
        for body in ([], "revision", None, {"expected_revision": "g" * 64},
                     {"expected_revision": "a" * 65}, {"expected_revision": []}):
            with self.subTest(body=body):
                response = client.generic("POST", url, json.dumps(body), content_type="application/json")
                self.assertEqual(response.status_code, 400)
        obj.refresh_from_db()
        self.assertIsNone(obj.approved_at)

    @patch("horarios_especiales.services.effective_hours._read_regular", return_value=({"status": "UNKNOWN", "windows": []}, None))
    def test_legacy_exact_date_and_conflicts_remain_authoritative(self, _read):
        from django.utils import timezone
        obj = self.create(self.rule(closed_all_day=True, time_windows=[]))
        self.approve(obj)
        legacy = SolicitudHorarioEspecial.objects.create(raw_command="legacy", status="APROBADO", approved_at=timezone.now())
        detail = HorarioEspecialDetalle.objects.create(request=legacy, sucursal=self.branch,
            target_date=date(2026, 11, 22), time_windows_json=[{"open": "08:00", "close": "19:00"}])
        effective = read_effective_hours("RULEQA", date(2026, 11, 22))["effective"]
        self.assertEqual(effective["windows"], detail.time_windows_json)
        self.assertFalse(effective["closed_all_day"])
        conflicting = SolicitudHorarioEspecial.objects.create(raw_command="legacy", status="APROBADO", approved_at=timezone.now())
        HorarioEspecialDetalle.objects.create(request=conflicting, sucursal=self.branch,
            target_date=date(2026, 11, 22), closed_all_day=True, time_windows_json=[])
        self.assertEqual(read_effective_hours("RULEQA", date(2026, 11, 22))["effective"]["status"], "UNKNOWN")


class ConcurrentScheduleApprovalTests(TransactionTestCase):
    def test_only_one_concurrent_approval_of_the_same_revision_succeeds(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections

        branch = Sucursal.objects.create(codigo="CONCURRENTQA", nombre="Concurrent QA", activa=True)
        obj, payload = create_request_from_rules(raw_text="concurrent fixture", branch_ids=[branch.pk],
            rules=[{"kind": "date", "start_date": "2026-11-22", "closed_all_day": True, "time_windows": []}])
        barrier = Barrier(2)

        def approve():
            close_old_connections()
            try:
                request = SolicitudHorarioEspecial.objects.get(pk=obj.pk)
                barrier.wait(timeout=5)
                try:
                    approve_request(request_obj=request, expected_revision=payload["revision"])
                    return "approved"
                except ValueError:
                    return "rejected"
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: approve(), range(2)))
        self.assertCountEqual(results, ["approved", "rejected"])
        self.assertEqual(obj.audit_entries.filter(action="APPROVED").count(), 1)
