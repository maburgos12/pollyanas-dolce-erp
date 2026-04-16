from __future__ import annotations

from datetime import date, datetime
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, SimpleTestCase
from django.urls import reverse

from core.models import AuditLog
from orquestacion.models import MemoryProposal, QualityFinding, RemediationProposal
from orquestacion.services.pointdailysale_guard import is_allowed_pointdailysale_path, scan_pointdailysale_usage
from orquestacion.services.protected_sales_reader_guard import (
    PROTECTED_SALES_READER_RULES,
    scan_protected_sales_reader_usage,
)
from orquestacion.services.quality_findings import (
    QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER,
    QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER,
    QUALITY_FINDING_CODE_SALES_PUBLICATION_GAP,
    ensure_remediation_proposal,
    record_quality_finding,
    sync_protected_sales_reader_findings,
    sync_sales_publication_gap_finding,
    sync_pointdailysale_guard_findings,
)
from orquestacion.services.sales_publication_guard import SalesPublicationGapScanResult, scan_sales_publication_gap


class PointDailySaleGuardTests(SimpleTestCase):
    def test_guard_detects_forbidden_file_and_respects_allowlist(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            allowed = root / "integraciones" / "views.py"
            allowed.parent.mkdir(parents=True, exist_ok=True)
            allowed.write_text(
                "from pos_bridge.models import PointDailySale\n"
                "latest = PointDailySale.objects.order_by('-sale_date').first()\n",
                encoding="utf-8",
            )

            forbidden = root / "api" / "bad_reader.py"
            forbidden.parent.mkdir(parents=True, exist_ok=True)
            forbidden.write_text(
                "from pos_bridge.models import PointDailySale\n"
                "rows = PointDailySale.objects.count()\n",
                encoding="utf-8",
            )

            result = scan_pointdailysale_usage(base_dir=root)

        self.assertTrue(result.has_violations)
        self.assertEqual(len(result.violations), 2)
        self.assertTrue(all(violation.relative_path == "api/bad_reader.py" for violation in result.violations))


class ProtectedSalesReaderGuardTests(SimpleTestCase):
    def test_guard_detects_forbidden_symbols_only_in_protected_paths(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            protected = root / "api" / "ai_gateway_services.py"
            protected.parent.mkdir(parents=True, exist_ok=True)
            protected.write_text(
                "from pos_bridge.models import PointDailySale\n"
                "latest = PointDailySale.objects.order_by('-sale_date').first()\n",
                encoding="utf-8",
            )

            agent_query = root / "pos_bridge" / "services" / "agent_query_service.py"
            agent_query.parent.mkdir(parents=True, exist_ok=True)
            agent_query.write_text(
                "from ventas.models import VentaHistorica\n"
                "rows = VentaHistorica.objects.count()\n",
                encoding="utf-8",
            )

            unrelated = root / "ventas" / "services" / "financials.py"
            unrelated.parent.mkdir(parents=True, exist_ok=True)
            unrelated.write_text(
                "from ventas.models import VentaHistorica\n"
                "rows = VentaHistorica.objects.count()\n",
                encoding="utf-8",
            )

            result = scan_protected_sales_reader_usage(base_dir=root)

        self.assertTrue(result.has_violations)
        self.assertEqual([violation.relative_path for violation in result.violations], ["api/ai_gateway_services.py", "api/ai_gateway_services.py"])
        self.assertTrue(all(violation.symbol == "PointDailySale" for violation in result.violations))

    def test_protected_paths_are_not_allowed_by_global_point_guard(self):
        for rule in PROTECTED_SALES_READER_RULES:
            self.assertFalse(
                is_allowed_pointdailysale_path(rule.relative_path),
                msg=f"{rule.relative_path} no debe quedar permitido por el guard global de PointDailySale.",
            )


class SalesPublicationGuardTests(SimpleTestCase):
    class _ChainStub:
        def __init__(self, value):
            self.value = value

        def filter(self, **_kwargs):
            return self

        def order_by(self, *_args):
            return self

        def values_list(self, *_args, **_kwargs):
            return self

        def only(self, *_args):
            return self

        def first(self):
            return self.value

    @patch("orquestacion.services.sales_publication_guard.PointSyncJob")
    @patch("orquestacion.services.sales_publication_guard._visible_cut_for")
    @patch("orquestacion.services.sales_publication_guard.FactVentaDiaria")
    @patch("orquestacion.services.sales_publication_guard.PointDailySale")
    def test_scan_sales_publication_gap_defers_while_sales_sync_is_running(
        self,
        point_daily_sale_mock,
        fact_venta_diaria_mock,
        visible_cut_mock,
        point_sync_job_mock,
    ):
        point_daily_sale_mock.objects = self._ChainStub(date(2026, 4, 11))
        fact_venta_diaria_mock.objects = self._ChainStub(date(2026, 4, 10))
        visible_cut_mock.return_value = date(2026, 4, 10)
        point_sync_job_mock.JOB_TYPE_SALES = "sales"
        point_sync_job_mock.STATUS_PENDING = "PENDING"
        point_sync_job_mock.STATUS_RUNNING = "RUNNING"
        point_sync_job_mock.objects = self._ChainStub(
            type(
                "SyncJob",
                (),
                {
                    "status": "RUNNING",
                    "started_at": datetime(2026, 4, 12, 9, 0, 0),
                    "finished_at": None,
                },
            )()
        )

        result = scan_sales_publication_gap(reference_date=date(2026, 4, 12))

        self.assertTrue(result.deferred_by_active_sync)
        self.assertFalse(result.has_gap)
        self.assertEqual(result.sync_job_status, "RUNNING")


class QualityFindingLoopTests(TestCase):
    def test_record_quality_finding_dedupes_and_reopens_resolved_items(self):
        first = record_quality_finding(
            code=QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER,
            category=QualityFinding.CATEGORY_ARCHITECTURE_VIOLATION,
            severity=QualityFinding.SEVERITY_HIGH,
            source_type=QualityFinding.SOURCE_GUARD,
            source_reference="api/bad_reader.py:1",
            statement="Uso directo no autorizado de PointDailySale fuera de la allowlist canónica del ERP.",
            evidence_refs=["api/bad_reader.py", "scripts/check_pointdailysale_usage.py"],
            details={"line_number": 1},
        )
        self.assertTrue(first.created)
        self.assertEqual(first.finding.detected_count, 1)

        first.finding.status = QualityFinding.STATUS_RESOLVED
        first.finding.save(update_fields=["status", "updated_at"])

        second = record_quality_finding(
            code=QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER,
            category=QualityFinding.CATEGORY_ARCHITECTURE_VIOLATION,
            severity=QualityFinding.SEVERITY_HIGH,
            source_type=QualityFinding.SOURCE_GUARD,
            source_reference="api/bad_reader.py:1",
            statement="Uso directo no autorizado de PointDailySale fuera de la allowlist canónica del ERP.",
            evidence_refs=["api/bad_reader.py", "scripts/check_pointdailysale_usage.py"],
            details={"line_number": 1},
        )

        self.assertFalse(second.created)
        self.assertTrue(second.reopened)
        self.assertEqual(second.finding.status, QualityFinding.STATUS_OPEN)
        self.assertEqual(second.finding.detected_count, 2)
        self.assertTrue(
            AuditLog.objects.filter(model="orquestacion.QualityFinding", action="UPDATE").exists()
        )

    def test_sync_guard_findings_creates_memory_and_remediation_and_resolves_on_clean_rerun(self):
        violation_result = scan_pointdailysale_usage(base_dir=self._build_repo_fixture(with_violation=True))
        first_summary = sync_pointdailysale_guard_findings(scan_result=violation_result)
        self.assertEqual(first_summary["findings_created"], 1)

        repeated_summary = sync_pointdailysale_guard_findings(scan_result=violation_result)
        self.assertGreaterEqual(repeated_summary["memory_created"] + repeated_summary["memory_updated"], 1)

        finding = QualityFinding.objects.get(code=QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER)
        remediation = RemediationProposal.objects.get(finding=finding)
        proposal = MemoryProposal.objects.get(source_type=MemoryProposal.SOURCE_QUALITY_GUARD)

        self.assertEqual(finding.detected_count, 2)
        self.assertEqual(proposal.status, MemoryProposal.STATUS_PROPOSED)
        self.assertEqual(remediation.status, RemediationProposal.STATUS_ACCEPTED)
        self.assertEqual(proposal.category, "architecture_guard_violation")
        self.assertNotEqual(proposal.status, MemoryProposal.STATUS_APPLIED)

        clean_result = scan_pointdailysale_usage(base_dir=self._build_repo_fixture(with_violation=False))
        resolved_summary = sync_pointdailysale_guard_findings(scan_result=clean_result)

        finding.refresh_from_db()
        remediation.refresh_from_db()
        self.assertEqual(resolved_summary["findings_resolved"], 1)
        self.assertEqual(finding.status, QualityFinding.STATUS_RESOLVED)
        self.assertEqual(remediation.status, RemediationProposal.STATUS_VALIDATED)

    def test_sync_protected_sales_reader_findings_creates_memory_and_resolves_on_clean_rerun(self):
        violation_result = scan_protected_sales_reader_usage(
            base_dir=self._build_protected_repo_fixture(with_violation=True)
        )
        first_summary = sync_protected_sales_reader_findings(scan_result=violation_result)
        self.assertEqual(first_summary["findings_created"], 1)

        repeated_summary = sync_protected_sales_reader_findings(scan_result=violation_result)
        self.assertGreaterEqual(repeated_summary["memory_created"] + repeated_summary["memory_updated"], 1)

        finding = QualityFinding.objects.get(code=QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER)
        remediation = RemediationProposal.objects.get(finding=finding)
        proposal = MemoryProposal.objects.get(source_type=MemoryProposal.SOURCE_QUALITY_GUARD)

        self.assertEqual(finding.detected_count, 2)
        self.assertEqual(remediation.status, RemediationProposal.STATUS_ACCEPTED)
        self.assertEqual(proposal.status, MemoryProposal.STATUS_PROPOSED)

        clean_result = scan_protected_sales_reader_usage(
            base_dir=self._build_protected_repo_fixture(with_violation=False)
        )
        resolved_summary = sync_protected_sales_reader_findings(scan_result=clean_result)

        finding.refresh_from_db()
        remediation.refresh_from_db()
        self.assertEqual(resolved_summary["findings_resolved"], 1)
        self.assertEqual(finding.status, QualityFinding.STATUS_RESOLVED)
        self.assertEqual(remediation.status, RemediationProposal.STATUS_VALIDATED)

    def test_sync_sales_publication_gap_finding_creates_non_blocking_runtime_finding_and_resolves(self):
        gap_result = SalesPublicationGapScanResult(
            reference_date=date(2026, 4, 12),
            target_date=date(2026, 4, 11),
            point_latest_date=date(2026, 4, 11),
            fact_latest_date=date(2026, 4, 9),
            visible_cut_date=date(2026, 4, 8),
            fact_lag_days=2,
            visible_lag_days=3,
            severity="high",
            is_blocking=False,
            reason="Gap de publicación visible detectado.",
            suggestion="Ejecutar refresh de analytics.",
            sync_job_status="",
            sync_job_started_at="",
            sync_job_finished_at="",
            deferred_by_active_sync=False,
        )

        first_summary = sync_sales_publication_gap_finding(gap_result=gap_result)
        finding = QualityFinding.objects.get(code=QUALITY_FINDING_CODE_SALES_PUBLICATION_GAP)
        remediation = RemediationProposal.objects.get(finding=finding)

        self.assertEqual(first_summary["findings_created"], 1)
        self.assertFalse(finding.is_blocking)
        self.assertEqual(remediation.status, RemediationProposal.STATUS_ACCEPTED)
        self.assertIsNone(finding.memory_proposal)

        clean_result = SalesPublicationGapScanResult(
            reference_date=gap_result.reference_date,
            target_date=gap_result.target_date,
            point_latest_date=gap_result.point_latest_date,
            fact_latest_date=gap_result.point_latest_date,
            visible_cut_date=gap_result.point_latest_date,
            fact_lag_days=0,
            visible_lag_days=0,
            severity="warning",
            is_blocking=False,
            reason="Sin gap de publicación visible.",
            suggestion="Sin acción.",
            sync_job_status="",
            sync_job_started_at="",
            sync_job_finished_at="",
            deferred_by_active_sync=False,
        )
        resolved_summary = sync_sales_publication_gap_finding(gap_result=clean_result)
        finding.refresh_from_db()
        remediation.refresh_from_db()
        self.assertEqual(resolved_summary["findings_resolved"], 1)
        self.assertEqual(finding.status, QualityFinding.STATUS_RESOLVED)
        self.assertEqual(remediation.status, RemediationProposal.STATUS_VALIDATED)

    def test_sync_sales_publication_gap_finding_skips_persist_when_gap_is_deferred(self):
        deferred_gap = SalesPublicationGapScanResult(
            reference_date=date(2026, 4, 12),
            target_date=date(2026, 4, 11),
            point_latest_date=date(2026, 4, 11),
            fact_latest_date=date(2026, 4, 10),
            visible_cut_date=date(2026, 4, 10),
            fact_lag_days=1,
            visible_lag_days=1,
            severity="warning",
            is_blocking=False,
            reason="Rezago diferido por sync activo.",
            suggestion="Esperar sync.",
            sync_job_status="RUNNING",
            sync_job_started_at="2026-04-12T09:00:00",
            sync_job_finished_at="",
            deferred_by_active_sync=True,
        )
        summary = sync_sales_publication_gap_finding(gap_result=deferred_gap)
        self.assertEqual(summary["violations"], 0)
        self.assertFalse(QualityFinding.objects.filter(code=QUALITY_FINDING_CODE_SALES_PUBLICATION_GAP).exists())

    def test_management_command_persists_findings_and_blocks_on_violation(self):
        with self.assertRaisesMessage(CommandError, "Quality guards encontraron violaciones arquitectónicas bloqueantes."):
            call_command(
                "run_quality_guards",
                base_dir=str(self._build_repo_fixture(with_violation=True)),
                stdout=StringIO(),
                stderr=StringIO(),
            )

        self.assertTrue(QualityFinding.objects.filter(code=QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER).exists())

    @patch("orquestacion.management.commands.run_quality_guards.run_quality_guards")
    def test_management_command_warns_on_publication_gap_without_blocking(self, mock_run_quality_guards):
        mock_run_quality_guards.return_value = self._build_mock_run_result()

        stdout = StringIO()
        call_command("run_quality_guards", stdout=stdout, stderr=StringIO())

        output = stdout.getvalue()
        self.assertIn("PublicationGap=1", output)
        self.assertIn("Quality guards OK.", output)

    def _build_repo_fixture(self, *, with_violation: bool) -> Path:
        tmpdir = TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        root = Path(tmpdir.name)
        allowed = root / "integraciones" / "views.py"
        allowed.parent.mkdir(parents=True, exist_ok=True)
        allowed.write_text(
            "from pos_bridge.models import PointDailySale\n"
            "latest = PointDailySale.objects.order_by('-sale_date').first()\n",
            encoding="utf-8",
        )
        if with_violation:
            forbidden = root / "api" / "bad_reader.py"
            forbidden.parent.mkdir(parents=True, exist_ok=True)
            forbidden.write_text(
                "from pos_bridge.models import PointDailySale\n"
                "rows = PointDailySale.objects.count()\n",
                encoding="utf-8",
            )
        else:
            safe_file = root / "api" / "safe_reader.py"
            safe_file.parent.mkdir(parents=True, exist_ok=True)
            safe_file.write_text("print('safe')\n", encoding="utf-8")
        return root

    def _build_protected_repo_fixture(self, *, with_violation: bool) -> Path:
        tmpdir = TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        root = Path(tmpdir.name)
        protected = root / "api" / "ai_gateway_services.py"
        protected.parent.mkdir(parents=True, exist_ok=True)
        if with_violation:
            protected.write_text(
                "from pos_bridge.models import PointDailySale\n"
                "total = PointDailySale.objects.count()\n",
                encoding="utf-8",
            )
        else:
            protected.write_text("print('safe gateway')\n", encoding="utf-8")
        return root

    def _build_mock_run_result(self):
        class MockPoint:
            violations = ()
            checked_files = 1
            has_violations = False

        class MockProtected:
            violations = ()
            checked_files = 1
            has_violations = False

        gap_result = SalesPublicationGapScanResult(
            reference_date=date(2026, 4, 12),
            target_date=date(2026, 4, 11),
            point_latest_date=date(2026, 4, 11),
            fact_latest_date=date(2026, 4, 10),
            visible_cut_date=date(2026, 4, 10),
            fact_lag_days=1,
            visible_lag_days=1,
            severity="warning",
            is_blocking=False,
            reason="Gap visible leve.",
            suggestion="refresh",
            sync_job_status="",
            sync_job_started_at="",
            sync_job_finished_at="",
            deferred_by_active_sync=False,
        )

        class MockRunResult:
            point_scan = MockPoint()
            protected_scan = MockProtected()
            publication_gap_scan = gap_result
            blocking_violations = 0
            has_blocking_violations = False

        return MockRunResult()


class QualityLoopViewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_user(username="quality_admin", password="pass123")
        self.reader = user_model.objects.create_user(username="quality_reader", password="pass123")
        admin_group, _ = Group.objects.get_or_create(name="ADMIN")
        reader_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.admin.groups.add(admin_group)
        self.reader.groups.add(reader_group)

        self.finding = record_quality_finding(
            code=QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER,
            category=QualityFinding.CATEGORY_ARCHITECTURE_VIOLATION,
            severity=QualityFinding.SEVERITY_HIGH,
            source_type=QualityFinding.SOURCE_GUARD,
            source_reference="api/bad_reader.py",
            statement="Uso directo no autorizado de PointDailySale fuera de la allowlist canónica del ERP.",
            evidence_refs=["api/bad_reader.py", "scripts/check_pointdailysale_usage.py"],
            details={"line_numbers": [1]},
        ).finding
        self.remediation = ensure_remediation_proposal(
            self.finding,
            target_files=["api/bad_reader.py"],
            suggested_tests=["./.venv/bin/python scripts/check_pointdailysale_usage.py"],
            suggested_fix="Usar sales_read_service en vez de PointDailySale directo.",
        )

    def test_quality_findings_list_allows_admin_and_denies_reader(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("orquestacion:quality_findings"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Loop de calidad y remediación")

        self.client.force_login(self.reader)
        denied = self.client.get(reverse("orquestacion:quality_findings"))
        self.assertEqual(denied.status_code, 403)

    def test_quality_finding_detail_allows_transition_and_rerun(self):
        self.client.force_login(self.admin)

        accept_response = self.client.post(
            reverse("orquestacion:quality_finding_detail", args=[self.finding.id]),
            {"action": "accept_remediation"},
        )
        self.assertEqual(accept_response.status_code, 302)
        self.remediation.refresh_from_db()
        self.assertEqual(self.remediation.status, RemediationProposal.STATUS_ACCEPTED)

        implement_response = self.client.post(
            reverse("orquestacion:quality_finding_detail", args=[self.finding.id]),
            {"action": "mark_implemented"},
        )
        self.assertEqual(implement_response.status_code, 302)
        self.remediation.refresh_from_db()
        self.assertEqual(self.remediation.status, RemediationProposal.STATUS_IMPLEMENTED)

        rerun_response = self.client.post(
            reverse("orquestacion:quality_finding_detail", args=[self.finding.id]),
            {"action": "rerun_guard"},
        )
        self.assertEqual(rerun_response.status_code, 302)
        self.finding.refresh_from_db()
        self.remediation.refresh_from_db()
        self.assertEqual(self.finding.status, QualityFinding.STATUS_RESOLVED)
        self.assertEqual(self.remediation.status, RemediationProposal.STATUS_VALIDATED)
