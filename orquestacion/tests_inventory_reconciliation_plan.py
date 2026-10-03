from datetime import date
from io import StringIO
from unittest.mock import patch

from django.conf import settings
from django.core.management import call_command
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.db import connection

from orquestacion.models import OrchestrationRun
from orquestacion.services.agent_runtime import run_agent_goal
from orquestacion import tests_inventory_reconciliation_runtime as fixtures
from pos_bridge.models import PointProduct
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditEvent
from reportes.services_inventory_audit_agent import InventoryAuditAgent


class ReconciliationPlanTests(TestCase):
    setUp = fixtures.InventoryReconciliationRuntimeTests.setUp
    goal = fixtures.InventoryReconciliationRuntimeTests.goal
    review = fixtures.InventoryReconciliationRuntimeTests.review
    save_case = fixtures.InventoryReconciliationRuntimeTests.save_case

    def plan(self, **metadata):
        with patch('requests.Session.request', side_effect=AssertionError('HTTP prohibido')), \
             patch.object(AuditStockHistoryService, 'reconcile', side_effect=AssertionError('No investigar todo el mes')), \
             patch.object(InventoryAuditAgent, 'investigate_case', side_effect=AssertionError('Reutilizar bitácoras')):
            return run_agent_goal(self.goal(metadata={'mode': 'plan_month', **metadata}), base_dir=settings.BASE_DIR)

    def clone(self, **changes):
        values = ProductInventoryAuditCase.objects.values().get(pk=self.case.pk)
        for field in ['id', 'created_at', 'updated_at']:
            values.pop(field)
        product = PointProduct.objects.create(external_id=str(PointProduct.objects.count()+900), name='Pastel vendido')
        values['product_id'] = product.pk
        values.update(changes)
        return ProductInventoryAuditCase.objects.create(**values)

    def test_plan_groups_global_source_block_once_and_keeps_independent_work(self):
        self.clone()
        self.audit_run.status = 'SOURCE_INCOMPLETE'
        self.audit_run.source_issues = [
            {'code': 'SOURCE_INCOMPLETE', 'message': 'WASTE_SYNC_COUNT_MISMATCH'},
            {'code': 'PRODUCT_RESOLVED_BY_SKU', 'message': 'Identidad encontrada'}]
        self.audit_run.save()
        plan = self.plan().observation['plan']
        self.assertEqual(plan['total_cases'], 2)
        self.assertEqual(len(plan['global_blockers']), 1)
        self.assertEqual(plan['global_blockers'][0]['affected_cases'], 2)
        self.assertEqual(len(plan['batch']), 2)
        self.assertFalse(plan['materialization_allowed'])

    def test_lots_are_bounded_deterministic_and_cursor_does_not_repeat(self):
        for _ in range(12):
            self.clone()
        a = self.plan().observation['plan']
        b = self.plan(after_case_id=a['next_after_case_id'], expected_plan_fingerprint=a['plan_fingerprint']).observation['plan']
        self.assertEqual(len(a['batch']), 10)
        self.assertEqual(len(b['batch']), 3)
        self.assertFalse(set(a['batch']) & set(b['batch']))
        self.assertEqual(a, self.plan().observation['plan'])

    def test_invalid_metadata_is_rejected_before_creating_runs(self):
        for metadata in ({'mode':'other'}, {'batch_size':11}, {'batch_size':True},
                         {'batch_size':0}, {'after_case_id':-1}, {'batch_size':'10'}, {'unexpected':1}):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                self.plan(**metadata)
        self.assertFalse(OrchestrationRun.objects.exists())

    def test_unchanged_review_is_reused_for_planning_not_claimed_as_live_verification(self):
        reviewed = self.review()
        plan = self.plan().observation['plan']
        self.assertEqual(plan['reused_review_count'], 1)
        self.assertEqual(plan['batch'], [])
        self.assertEqual(plan['reused_reviews'][0]['run_id'], reviewed.run_id)
        self.assertEqual(plan['reuse_scope'], 'RECORDED_CASE_AND_MONTH_ONLY')
        self.assertFalse(plan['live_sources_verified'])
        self.save_case(sales=9)
        self.assertEqual(self.plan().observation['plan']['batch'], [self.case.pk])

    def test_new_human_evidence_invalidates_stored_review_without_changing_case(self):
        self.review()
        ProductInventoryAuditEvent.objects.create(case=self.case, action='EXPLAIN', reason_code='DOCUMENT', notes='Folio recibido')
        self.assertEqual(self.plan().observation['plan']['batch'], [self.case.pk])

    def test_change_to_month_authority_invalidates_stored_review(self):
        self.review()
        self.audit_run.calculation_fingerprint = 'z'*64
        self.audit_run.save()
        self.assertEqual(self.plan().observation['plan']['batch'], [self.case.pk])

    def test_groups_are_prioritized_by_impact_and_missing_stays_pending(self):
        self.product.external_id = '999'
        self.product.save()
        self.save_case(investigation_summary={'missing':['Folio pendiente'], 'traceability_status':'COMPLETE'})
        self.clone(investigation_summary={'missing':['Otro folio']})
        self.clone(investigation_summary={})
        groups = self.plan().observation['plan']['groups']
        self.assertEqual(groups[0]['code'], 'REVIEW_DOCUMENTARY_EVIDENCE')
        self.assertEqual(groups[0]['affected_cases'], 2)
        self.assertFalse(self.plan().observation['closure_allowed'])

    def test_plan_is_scoped_to_anchor_month_and_sold_products(self):
        self.clone(product_id=PointProduct.objects.create(external_id='TOP', name='Topping chocolate').pk)
        plan = self.plan().observation['plan']
        self.assertEqual(plan['total_cases'], 1)
        self.assertEqual(plan['batch'], [self.case.pk])

    def test_second_plan_does_not_change_operational_cases_or_events(self):
        before = list(ProductInventoryAuditCase.objects.values())
        first = self.plan().observation
        second = self.plan().observation
        self.assertEqual(first, second)
        self.assertEqual(before, list(ProductInventoryAuditCase.objects.values()))
        self.assertFalse(ProductInventoryAuditEvent.objects.exists())

    def test_cli_exposes_plan_without_new_goal_or_seed(self):
        output = StringIO()
        call_command('run_agent_goal', goal='reconciliation_guard', event_id=self.case.pk,
                     entity_type='ProductInventoryAuditCase', plan_month=True, batch_size=3, stdout=output)
        self.assertIn('plan_month', output.getvalue())
        self.assertIn('total_cases', output.getvalue())

    def test_history_remainder_is_not_hidden_by_balanced_case(self):
        self.product.external_id = '999'
        self.product.save()
        self.save_case(source_trace={'opening':[1], 'closing':[2], 'point_history':{
            'coverage_status':'COMPLETE', 'unexplained_remainder':'5'}})
        self.assertEqual(self.plan().observation['plan']['groups'][0]['code'], 'REVIEW_STOCK_REMAINDER')

    def test_known_document_does_not_hide_unknown_or_remainder(self):
        for history, expected in (({'unknown_movement_ids':[123]}, 'REVIEW_UNKNOWN_STOCK_MOVEMENTS'),
                                  ({'unexplained_remainder':'5'}, 'REVIEW_STOCK_REMAINDER')):
            with self.subTest(expected=expected):
                self.save_case(source_trace={'point_history':history})
                self.assertEqual(self.plan().observation['plan']['groups'][0]['code'], expected)

    def test_plan_cursor_resets_when_evidence_changes(self):
        self.clone()
        a = self.plan(batch_size=1).observation['plan']
        self.save_case(sales=1)
        b = self.plan(batch_size=1, after_case_id=a['next_after_case_id'],
                      expected_plan_fingerprint=a['plan_fingerprint']).observation['plan']
        self.assertTrue(b['cursor_reset_due_to_changes'])

    def test_cli_rejects_cursor_without_plan_mode(self):
        from django.core.management import CommandError
        with self.assertRaises(CommandError):
            call_command('run_agent_goal', goal='reconciliation_guard', event_id=self.case.pk,
                         entity_type='ProductInventoryAuditCase', after_case_id=3)
        self.assertFalse(OrchestrationRun.objects.exists())

    def test_group_missing_sample_is_bounded_and_all_ids_retained(self):
        for _ in range(12):
            self.clone(investigation_summary={'missing':['Folio exacto pendiente']})
        group = next(group for group in self.plan().observation['plan']['groups']
                     if group['code'] == 'REVIEW_DOCUMENTARY_EVIDENCE')
        self.assertEqual(group['affected_cases'], 12)
        self.assertEqual(len(group['case_ids']), 12)
        self.assertLessEqual(len(group['stored_missing']), 10)

    def test_month_signature_is_calculated_once_not_per_case(self):
        for _ in range(11):
            self.clone()
        from orquestacion.services import inventory_reconciliation_plan as planner
        self.assertTrue(hasattr(planner, 'recorded_month_signature'))
        with patch.object(planner, 'recorded_month_signature', wraps=planner.recorded_month_signature) as signature:
            self.plan()
        self.assertEqual(signature.call_count, 1)

    def test_plan_query_count_does_not_grow_per_case(self):
        with CaptureQueriesContext(connection) as small:
            self.plan()
        for _ in range(20):
            self.clone()
        with CaptureQueriesContext(connection) as large:
            self.plan()
        self.assertLessEqual(len(large), len(small)+1)

    def test_new_commercial_differences_precede_large_generic_history_group(self):
        for _ in range(12):
            self.clone()
        urgent = self.clone(sales=3, source_trace={'opening':[1], 'closing':[2], 'point_history':{
            'aggregate_comparison':{'sales':{'aggregate':'3','point_history':'2'}}}})
        plan = self.plan(batch_size=1).observation['plan']
        self.assertEqual(plan['groups'][0]['code'], 'REVIEW_COMMERCIAL_STOCK_EFFECT')
        self.assertEqual(plan['batch'], [urgent.pk])
