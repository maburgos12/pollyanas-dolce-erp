from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase, TransactionTestCase

from core.models import Sucursal
from horarios_especiales.models import HorarioEspecialIntentoPublicacion, SucursalPlataformaExterna
from horarios_especiales.services.google_projection import execute_rules, projection_fields, branch_lock
from horarios_especiales.services.requests import approve_request, cancel_request, create_request_from_rules
from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher

TODAY = date(2026, 11, 22)
REGULAR = {"periods": [{"openDay": "MONDAY", "closeDay": "MONDAY", "openTime": {"hours": 8}, "closeTime": {"hours": 20}},
                       {"openDay": "SUNDAY", "closeDay": "SUNDAY", "openTime": {"hours": 9}, "closeTime": {"hours": 19}}]}


class MemoryGoogle:
    def __init__(self):
        self.locations = {}
        self.writes = []
        self.validations = []
        self.pending = ""
        self.mismatch = False
        self.fail = None
        self.after_patch = None

    def get_location(self, *, location_name):
        return deepcopy(self.locations.setdefault(location_name, {"regularHours": deepcopy(REGULAR)}))

    def patch_hours(self, *, location_name, fields, validate_only=False):
        if validate_only:
            self.validations.append(location_name)
            return {}
        # The backup must exist in the attempt before the first real network mutation.
        assert HorarioEspecialIntentoPublicacion.objects.filter(
            request_payload_json__name=location_name, response_payload_json__mutation_started=True,
        ).exists()
        if location_name == self.fail:
            raise RuntimeError("Google unavailable")
        self.writes.append((location_name, deepcopy(fields)))
        if not self.mismatch:
            self.locations[location_name].update(deepcopy(fields))
        if self.after_patch:
            callback, self.after_patch = self.after_patch, None
            callback()
        return {"name": location_name}

    def get_google_updated(self, *, location_name):
        return {"location": self.get_location(location_name=location_name), "pendingMask": self.pending}


def make_rule(kind="date", **overrides):
    result = {"kind": kind, "start_date": TODAY.isoformat(), "closed_all_day": False,
              "time_windows": [{"open": "10:30", "close": "18:00"}]}
    if kind == "weekly":
        result["weekdays"] = [6]
    if kind == "range":
        result["end_date"] = "2027-12-31"
    return {**result, **overrides}


def approve(branches, *rules):
    obj, payload = create_request_from_rules(raw_text="fixture", branch_ids=[b.pk for b in branches], rules=list(rules))
    approve_request(request_obj=obj, expected_revision=payload["revision"])
    return obj


class PublisherPayloadTests(SimpleTestCase):
    def test_time_of_day_and_unrelated_next_day_preserved(self):
        detail = SimpleNamespace(target_date=TODAY, closed_all_day=False,
                                 time_windows_json=[{"open": "10:30", "close": "18:00"}])
        periods = GoogleBusinessProfilePublisher._build_special_hour_periods(detail=detail)
        self.assertEqual(periods[0]["openTime"], {"hours": 10, "minutes": 30})
        next_day = {"startDate": {"year": 2026, "month": 11, "day": 23}, "closed": True}
        self.assertEqual(GoogleBusinessProfilePublisher._filter_existing_periods(
            existing_periods=periods + [next_day], detail=detail), [next_day])

    def test_mutation_requires_a_persisted_backup_attempt(self):
        client = Mock()
        publisher = GoogleBusinessProfilePublisher(client=client)
        with self.assertRaisesRegex(ValueError, "intento persistido"):
            publisher.publish_fields(location_name="locations/qa", current={},
                                     fields={"regularHours": REGULAR}, attempt=None)
        client.patch_hours.assert_not_called()

    def test_future_weekly_deferred_and_changed_weekday_preserves_other_days(self):
        row = ((1, 1, 1, 1), make_rule("weekly", start_date="2026-12-01"), "hash")
        fields, _, _, ongoing, _ = projection_fields(current={"regularHours": REGULAR}, rows=[row], today=TODAY, owned={})
        self.assertEqual(fields, {})
        self.assertTrue(ongoing)
        row[1]["start_date"] = TODAY.isoformat()
        fields, _, _, _, _ = projection_fields(current={"regularHours": REGULAR}, rows=[row], today=TODAY, owned={})
        self.assertIn(REGULAR["periods"][0], fields["regularHours"]["periods"])
        self.assertEqual(len(fields["regularHours"]["periods"]), 2)
        self.assertEqual(fields["regularHours"]["periods"][1]["openTime"], {"hours": 10, "minutes": 30})


class GoogleProjectionTests(TransactionTestCase):
    def setUp(self):
        self.branch = Sucursal.objects.create(codigo="GOOGLEQA", nombre="Google QA", activa=True)
        SucursalPlataformaExterna.objects.create(sucursal=self.branch, external_location_name="locations/qa")
        self.client = MemoryGoogle()
        self.publisher = GoogleBusinessProfilePublisher(client=self.client)

    def execute(self, obj, **kwargs):
        return execute_rules(request_obj=obj, today=TODAY, publisher=self.publisher, **kwargs)

    def test_duplicate_execution_reads_back_without_second_patch(self):
        obj = approve([self.branch], make_rule())
        first, second = self.execute(obj), self.execute(obj)
        self.assertTrue(first["public_confirmed"])
        self.assertTrue(second["public_confirmed"])
        self.assertEqual(len(self.client.writes), 1)
        self.assertEqual(len(self.client.validations), 1)
        attempt = obj.details.get().publication_attempts.order_by("pk").first()
        self.assertEqual(attempt.response_payload_json["before"], {"regularHours": REGULAR})
        self.assertEqual(obj.status, "APROBADO")
        with self.assertRaises(ValueError):
            cancel_request(request_obj=obj)

    def test_aggregate_publication_blocks_cancelling_other_covered_request_only(self):
        other = approve([self.branch], make_rule(start_date="2026-11-23"))
        future = approve([self.branch], make_rule("weekly", start_date="2027-12-01"))
        obj = approve([self.branch], make_rule(start_date="2026-11-24"))
        def cancellation_during_patch():
            with self.assertRaises(ValueError):
                cancel_request(request_obj=other)
        self.client.after_patch = cancellation_during_patch
        self.assertTrue(self.execute(obj)["public_confirmed"])
        attempt = obj.details.get().publication_attempts.get()
        self.assertIn({"request_id": other.pk, "revision": other.canonical_payload["revision"]},
                      attempt.error_payload_json["covered_revisions"])
        self.assertNotIn({"request_id": future.pk, "revision": future.canonical_payload["revision"]},
                         attempt.error_payload_json["covered_revisions"])
        self.assertFalse(other.details.exists())
        with self.assertRaises(ValueError):
            cancel_request(request_obj=other)
        cancel_request(request_obj=future)
        future.refresh_from_db()
        self.assertEqual(future.status, "CANCELADO")

    def test_request_owning_attempt_can_cancel_when_only_other_rules_were_published(self):
        other = approve([self.branch], make_rule())
        future = approve([self.branch], make_rule("weekly", start_date="2027-12-01"))
        self.assertEqual(self.execute(future)["details"][0]["status"], "PROGRAMMED")
        cancel_request(request_obj=future)
        self.assertEqual(future.status, "CANCELADO")
        with self.assertRaises(ValueError):
            cancel_request(request_obj=other)

    def test_aggregate_publication_records_weekly_contributor_before_patch(self):
        weekly = approve([self.branch], make_rule("weekly"))
        obj = approve([self.branch], make_rule(start_date="2026-11-24"))
        self.client.fail = "locations/qa"
        self.assertEqual(self.execute(obj)["failed"], 1)
        with self.assertRaises(ValueError):
            cancel_request(request_obj=weekly)
        evidence = obj.details.get().publication_attempts.get().error_payload_json["covered_revisions"]
        self.assertIn({"request_id": weekly.pk, "revision": weekly.canonical_payload["revision"]}, evidence)

    def test_failed_partial_retry_does_not_patch_successful_branch_twice(self):
        other = Sucursal.objects.create(codigo="GOOGLEQB", nombre="Google QB", activa=True)
        SucursalPlataformaExterna.objects.create(sucursal=other, external_location_name="locations/qb")
        obj = approve([self.branch, other], make_rule())
        self.client.fail = "locations/qb"
        first = self.execute(obj)
        self.assertEqual(first["failed"], 1)
        self.assertFalse(first["public_confirmed"])
        obj.refresh_from_db()
        self.assertEqual(obj.status, "APROBADO")
        self.client.fail = None
        self.assertTrue(self.execute(obj)["public_confirmed"])
        self.assertEqual([name for name, _ in self.client.writes], ["locations/qa", "locations/qb"])

    def test_readback_mismatch_and_pending_not_public_confirmation(self):
        obj = approve([self.branch], make_rule())
        self.client.mismatch = True
        result = self.execute(obj)
        self.assertEqual(result["details"][0]["status"], "READBACK_MISMATCH")
        self.assertFalse(result["public_confirmed"])
        self.client.mismatch = False
        self.client.pending = "specialHours"
        result = self.execute(obj)
        self.assertEqual(result["details"][0]["status"], "GOOGLE_PENDING")
        self.assertFalse(result["public_confirmed"])
        self.assertEqual(obj.details.get().execution_status, "PENDIENTE")

    def test_finite_weekly_retry_accepts_google_omitted_zeroes_and_end_date(self):
        obj = approve([self.branch], make_rule("weekly", end_date="2026-12-06"))
        self.assertTrue(self.execute(obj)["public_confirmed"])
        periods = self.client.locations["locations/qa"]["specialHours"]["specialHourPeriods"]
        for period in periods:
            period.pop("endDate")
            period["closeTime"].pop("minutes")
            period["closed"] = False
        result = self.execute(obj)
        self.assertTrue(result["public_confirmed"])
        self.assertEqual(len(self.client.writes), 1)

    def test_atomic_outer_transaction_fails_before_google_mutation(self):
        from django.db import transaction
        obj = approve([self.branch], make_rule())
        with transaction.atomic():
            result = self.execute(obj)
        self.assertIn("transaction.atomic", result["details"][0]["error"])
        self.assertEqual(self.client.writes, [])

    def test_google_public_verification_unavailable_is_only_api_accepted(self):
        obj = approve([self.branch], make_rule())
        self.client.get_google_updated = Mock(side_effect=RuntimeError("verification unavailable"))
        result = self.execute(obj)
        self.assertEqual(result["details"][0]["status"], "API_ACCEPTED")
        self.assertFalse(result["public_confirmed"])

    def test_weekly_manual_exception_conflicts_and_date_overrides_are_explicit(self):
        obj = approve([self.branch], make_rule("weekly"))
        manual = {"startDate": {"year": 2026, "month": 11, "day": 29}, "closed": True}
        self.client.locations["locations/qa"] = {"regularHours": REGULAR, "specialHours": {"specialHourPeriods": [manual]}}
        result = self.execute(obj)
        self.assertIn("MANUAL_EXCEPTION_CONFLICT", result["details"][0]["error"])
        self.assertEqual(self.client.writes, [])
        date_request = approve([self.branch], make_rule(start_date="2026-11-29"))
        result = self.execute(date_request)
        self.assertTrue(result["public_confirmed"])

    def test_bounded_range_and_roll_forward_with_durable_next_run(self):
        obj = approve([self.branch], make_rule("range"))
        result = self.execute(obj)
        self.assertEqual(len(self.client.writes[0][1]["specialHours"]["specialHourPeriods"]), 90)
        self.assertTrue(result["ongoing_projection"])
        self.assertFalse(result["public_confirmed"])
        self.assertEqual(result["details"][0]["status"], "PROGRAMMED")
        self.assertEqual(result["details"][0]["projection_receipt"], "GOOGLE_CONFIRMED")
        self.assertTrue(result["next_reconcile_at"])
        next_result = execute_rules(request_obj=obj, today=TODAY + timedelta(days=1), publisher=self.publisher)
        self.assertEqual(next_result["projection_end"], (TODAY + timedelta(days=90)).isoformat())
        periods = self.client.writes[-1][1]["specialHours"]["specialHourPeriods"]
        self.assertEqual(len(periods), 91)  # Past Google exception is preserved.

    def test_future_weekly_is_programmed_then_applied_on_start(self):
        obj = approve([self.branch], make_rule("weekly", start_date="2026-12-01"))
        result = self.execute(obj)
        self.assertEqual(result["details"][0]["status"], "PROGRAMMED")
        self.assertEqual(self.client.writes, [])
        self.assertFalse(result["public_confirmed"])
        result = execute_rules(request_obj=obj, today=date(2026, 12, 1), publisher=self.publisher)
        self.assertTrue(result["public_confirmed"])
        self.assertIn("regularHours", self.client.writes[0][1])

    def test_expired_request_does_not_publish_or_claim_success(self):
        obj = approve([self.branch], make_rule(start_date="2026-11-21"))
        result = self.execute(obj)
        self.assertTrue(result["projection_expired"])
        self.assertIsNone(result["next_reconcile_at"])
        self.assertFalse(result["public_confirmed"])
        self.assertEqual(result["success"], 0)
        self.assertEqual(self.client.locations, {})

    def test_new_approval_during_patch_requires_reconcile(self):
        obj = approve([self.branch], make_rule())
        self.client.after_patch = lambda: approve([self.branch], make_rule(closed_all_day=True, time_windows=[]))
        result = self.execute(obj)
        self.assertEqual(result["details"][0]["status"], "PENDING_RECONCILE")
        self.assertFalse(result["public_confirmed"])
        reconciled = self.execute(obj)
        self.assertFalse(reconciled["public_confirmed"])
        self.assertEqual(reconciled["details"][0]["status"], "SUPERSEDED")
        self.assertEqual(reconciled["details"][0]["projection_receipt"], "GOOGLE_CONFIRMED")
        periods = self.client.writes[-1][1]["specialHours"]["specialHourPeriods"]
        self.assertTrue(periods[0]["closed"])

    def test_finite_weekly_uses_dated_exceptions_and_cannot_cancel_running_mutation(self):
        obj = approve([self.branch], make_rule("weekly", end_date="2026-12-06"))
        def cancellation():
            with self.assertRaises(ValueError):
                cancel_request(request_obj=obj)
        self.client.after_patch = cancellation
        result = self.execute(obj)
        self.assertTrue(result["public_confirmed"])
        self.assertNotIn("regularHours", self.client.writes[0][1])
        self.assertEqual(len(self.client.writes[0][1]["specialHours"]["specialHourPeriods"]), 3)
        self.assertTrue(self.execute(obj)["public_confirmed"])


class GooglePublicationLockTests(TransactionTestCase):
    def test_second_worker_cannot_enter_same_branch(self):
        from concurrent.futures import ThreadPoolExecutor
        from django.db import close_old_connections
        def second():
            close_old_connections()
            try:
                with branch_lock(918273):
                    return "entered"
            except ValueError:
                return "busy"
            finally:
                close_old_connections()
        with branch_lock(918273), ThreadPoolExecutor(max_workers=1) as pool:
            self.assertEqual(pool.submit(second).result(timeout=5), "busy")
        with ThreadPoolExecutor(max_workers=1) as pool:
            self.assertEqual(pool.submit(second).result(timeout=5), "entered")

    def test_publication_lock_blocks_parallel_approval_without_losing_draft(self):
        from concurrent.futures import ThreadPoolExecutor
        from django.db import close_old_connections
        from horarios_especiales.models import SolicitudHorarioEspecial
        branch = Sucursal.objects.create(codigo="LOCKQA", nombre="Lock QA", activa=True)
        obj, payload = create_request_from_rules(raw_text="fixture", branch_ids=[branch.pk], rules=[make_rule()])
        def approval():
            close_old_connections()
            try:
                approve_request(request_obj=SolicitudHorarioEspecial.objects.get(pk=obj.pk), expected_revision=payload["revision"])
                return "approved"
            except ValueError:
                return "busy"
            finally:
                close_old_connections()
        with branch_lock(branch.pk), ThreadPoolExecutor(max_workers=1) as pool:
            self.assertEqual(pool.submit(approval).result(timeout=5), "busy")
        obj.refresh_from_db()
        self.assertEqual(obj.status, "BORRADOR")
        self.assertFalse(obj.audit_entries.filter(action="APPROVED").exists())
        approve_request(request_obj=obj, expected_revision=payload["revision"])
        self.assertEqual(obj.status, "APROBADO")
