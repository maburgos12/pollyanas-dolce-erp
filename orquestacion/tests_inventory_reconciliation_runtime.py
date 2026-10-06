from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.conf import settings
from django.test import TestCase
from django.utils import timezone

from core.models import Notificacion
from orquestacion.models import AgentDefinition, AgentSuggestion, AgentTask, OrchestrationRun
from orquestacion.services.agent_runtime import Goal, build_agent_context, run_agent_goal
from pos_bridge.models import PointBranch, PointProduct, PointProductHistoryImport, PointProductHistoryRow
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditRun


SKILL = '.agent/skills/42-domain-inventory/skill-point-inventory-reconciliation/SKILL.md'


class EmptyHistoryClient:
    def login(self):
        pass

    def get_stock_history(self, product_id, branch_id, *, movements=500):
        return []


class InventoryReconciliationRuntimeTests(TestCase):
    def setUp(self):
        self.agent = AgentDefinition.objects.create(
            code='agente_conciliacion', name='Agente de Conciliación', domain='conciliacion',
            status='active', supported_goal_types_json=['reconciliation_guard'],
            context_files_json=['.agent/skills/60-automation-ops/skill-agent-runtime-foundation/SKILL.md'],
        )
        self.branch = PointBranch.objects.create(external_id='13', name='Guamúchil')
        self.product = PointProduct.objects.create(external_id='1', sku='0001', name='Pay de Queso Grande')
        self.month = date(2026, 9, 1)
        self.audit_run = ProductInventoryAuditRun.objects.create(
            month=self.month, status='READY', calculation_fingerprint='a' * 64)
        self.case = ProductInventoryAuditCase.objects.create(
            run=self.audit_run, month=self.month, branch=self.branch, product=self.product,
            opening_point=10, expected_closing=10, point_closing=10, difference=0,
            production=0, sales=0, waste=0, transfer_in=0, transfer_out=0,
            conversion_in=0, conversion_out=0, identified_adjustment=0,
            movement_status='BALANCED', calculation_fingerprint='b' * 64, rebuilt_at=timezone.now(),
            source_trace={'opening': [1], 'closing': [2]},
        )

    def goal(self, **changes):
        values = dict(goal_type='reconciliation_guard', objective='Revisar expediente sin alterar datos',
                      entity_type='ProductInventoryAuditCase', entity_id=self.case.pk)
        values.update(changes)
        return Goal(**values)

    def review(self, **changes):
        with patch('requests.Session.request', side_effect=AssertionError('HTTP prohibido')):
            return run_agent_goal(self.goal(**changes), base_dir=settings.BASE_DIR)

    def complete_history(self):
        AuditStockHistoryService(client=EmptyHistoryClient()).capture(self.branch, self.product, self.month)

    def save_case(self, **changes):
        for key, value in changes.items():
            setattr(self.case, key, value)
        self.case.save()

    def test_registered_goal_loads_skill_and_records_read_only_decision(self):
        before = ProductInventoryAuditCase.objects.values().get(pk=self.case.pk)
        result = self.review()
        self.assertEqual(result.status, 'success')
        self.assertEqual(result.decision, 'complete_review')
        self.assertIn(SKILL, result.context.loaded_files)
        self.assertFalse(result.observation['closure_allowed'])
        self.assertEqual(result.observation['skill']['version'], '1')
        self.assertEqual(before, ProductInventoryAuditCase.objects.values().get(pk=self.case.pk))
        run = OrchestrationRun.objects.get(pk=result.run_id)
        self.assertIn('Revisión', run.result_summary_json['message'])
        self.assertTrue(run.loop_checkpoints.filter(phase='observe').exists())
        self.assertTrue(run.loop_checkpoints.filter(phase='verify').exists())

    def test_complete_history_is_not_recaptured_even_without_physical_count(self):
        self.complete_history()
        with patch.object(AuditStockHistoryService, 'capture', side_effect=AssertionError('No recapturar')):
            result = self.review()
        self.assertEqual(result.observation['point_history']['coverage_status'], 'COMPLETE')
        self.assertFalse(result.observation['next_step']['point_http_allowed'])
        self.assertEqual(result.observation['physical_status'], 'NOT_AVAILABLE')
        self.assertFalse(result.observation['closure_allowed'])

    def test_global_source_failure_prevents_capture_for_old_balanced_case(self):
        self.complete_history()
        self.audit_run.status = 'SOURCE_INCOMPLETE'
        self.audit_run.source_issues = [{'code': 'SOURCE_INCOMPLETE', 'message': 'WASTE_SYNC_COUNT_MISMATCH'}]
        self.audit_run.save()
        result = self.review()
        self.assertEqual(result.observation['next_step']['code'], 'REVIEW_MONTH_SOURCE_AUTHORITY')
        self.assertFalse(result.observation['source_authoritative'])
        self.assertEqual(result.observation['case_balance_status'], 'BALANCED')

    def test_commercial_sales_are_not_replaced_by_stock(self):
        self.complete_history()
        self.save_case(sales=Decimal('14'), source_trace={
            'opening': [1], 'closing': [2],
            'point_history': {'aggregate_comparison': {'sales': {'aggregate': '14', 'point_history': '-1'}}}})
        result = self.review()
        self.assertEqual(result.observation['next_step']['code'], 'REVIEW_COMMERCIAL_STOCK_EFFECT')
        self.assertEqual(Decimal(result.observation['commercial_sales']), Decimal('14'))
        self.case.refresh_from_db()
        self.assertEqual(self.case.sales, Decimal('14'))

    def test_missing_document_wins_over_complete_traceability_label(self):
        from pos_bridge.models import PointTransferLine
        transfer = PointTransferLine.objects.create(
            origin_branch=self.branch, destination_branch=self.branch,
            transfer_external_id='38657', detail_external_id='540006', source_hash='c' * 64,
            registered_at=timezone.now(), sent_at=timezone.now(), received_at=timezone.now(),
            item_name=self.product.name, item_code=self.product.sku,
            sent_quantity=1, received_quantity=1, is_received=True, is_finalized=False)
        self.save_case(source_trace={'opening': [1], 'closing': [2], 'transfers': [transfer.pk]})
        result = self.review()
        self.assertEqual(result.observation['traceability_status'], 'PENDING')
        self.assertIn('38657/540006', ' '.join(result.observation['missing']))
        self.assertFalse(result.observation['closure_allowed'])

    def test_dated_verified_notes_are_loaded_only_for_matching_subject(self):
        result = self.review()
        notes = result.observation['known_findings']
        self.assertTrue(notes)
        self.assertEqual(notes[0]['case_reference'], 3893)
        self.assertIn('38681/540391', str(notes))
        self.assertTrue(notes[0]['historical_evidence_only'])
        self.branch.external_id = '8'
        self.branch.save()
        self.assertEqual(self.review().observation['known_findings'], [])

    def test_second_review_has_no_operational_rows_or_notifications(self):
        self.complete_history()
        before = list(ProductInventoryAuditCase.objects.values())
        counts = (PointProductHistoryImport.objects.count(), PointProductHistoryRow.objects.count(),
                  Notificacion.objects.count(), AgentSuggestion.objects.count())
        first = self.review()
        second = self.review()
        self.assertNotEqual(first.run_id, second.run_id)  # Explicit reviews have separate audit trails.
        self.assertEqual(first.observation, second.observation)
        self.assertEqual(before, list(ProductInventoryAuditCase.objects.values()))
        self.assertEqual(counts, (PointProductHistoryImport.objects.count(), PointProductHistoryRow.objects.count(),
                                 Notificacion.objects.count(), AgentSuggestion.objects.count()))

    def test_canonical_coverage_findings_retain_exact_identity_and_second_capture_proof(self):
        subjects = (
            ('5', '118', 2251, 76, '4+28-30-1=1', [28605464, 28797569]),
            ('1', '445', 3371, 508, '7+171-169-1=8', [28607214, 28799418]),
            ('1', '917', 3372, 509, '1+44-44=1', [28607215, 28799419]),
            ('4', '818', 3432, 531, '1+32-31=2', [28607482, 28799695]),
            ('2', '169', 2443, 138, '73-58=15', [28605103, 28797190]),
            ('3', '169', 2835, 259, '46+200-131=115', [28606447, 28798606]),
            ('6', '170', 3033, 339, '84+260-152=192', [28606784, 28798961]),
            ('1', '169', 3218, 402, '101+300-331=70', [28607119, 28799314]),
            ('1', '1001', 3220, 404, '16+11-23=4', [28607124, 28799319]),
            ('1', '1044', 3413, 522, '17+21-65-29+140=84', [28607236, 28799444]),
            ('4', '169', 3422, 524, '96+100-77=119', [28607455, 28799668]),
            ('4', '1044', 3600, 584, '3+30-10-10=13', [28607572, 28799798]),
            ('7', '169', 3608, 585, '111+100-35=176', [28607791, 28800022]),
            ('13', '170', 3799, 649, '162-28=134', [28606112, 28798253]),
            ("3","170",2836,260,"120+160-177=103",[28606448,28798607]),
            ("2","1005",2464,147,"18-15=3",[28605166,28797262]),
            ("5","169",2238,67,"114+100-86=128",[28605439,28797544]),
            ("1","267",3271,442,"100+12-11=101",[28607225,28799431]),
            ("11","1044",2829,258,"3+12-1=14",[28605892,28798028]),
            ("13","169",3798,648,"45-10=35",[28606111,28798252]),
            ("5","403",2296,92,"0+10-1=9",[28605558,28797677]),
            ("2","664",2478,159,"2+10-1=11",[28605188,28797284]),
            ("1","404",3278,448,"88-9=79",[28607234,28799441]),
            ("2","660",2471,155,"2+10-2=10",[28605180,28797276]),
        )
        for branch, product, case_id, import_id, equation, snapshots in subjects:
            with self.subTest(case=case_id):
                self.branch.external_id = branch
                self.branch.save()
                self.product.external_id = product
                self.product.save()
                with patch.object(AuditStockHistoryService, 'capture', side_effect=AssertionError('No captura')):
                    result = self.review()
                note = next((item for item in result.observation['known_findings']
                             if item['case_reference'] == case_id), None)
                self.assertIsNotNone(note)
                self.assertEqual(note['canonical_import_id'], import_id)
                self.assertEqual(note['documentary_equation'], equation)
                self.assertEqual(note['snapshot_ids'], snapshots)
                self.assertEqual(note['history_coverage_at_observation'], 'COMPLETE')
                self.assertEqual(note['second_capture'], {
                    'http': 0, 'new_rows': 0, 'new_imports': 0, 'duplicates': 0, 'new_notices': 0})
                self.assertFalse(note['projection_rebuilt'])
                self.assertTrue(note['historical_evidence_only'])
                # A historical COMPLETE finding must not falsify a missing live canonical import.
                self.assertEqual(result.observation['point_history']['coverage_status'], 'MISSING')
                self.assertFalse(result.observation['closure_allowed'])

    def test_loaded_continuity_distinguishes_report_from_conversion_execution(self):
        context = build_agent_context(self.goal(), base_dir=settings.BASE_DIR)
        for token in ('Reporte1082', 'no es folio de ejecución',
                      'f7c08d840141db5a21c0af6286c606591df99f525941e4f39fa8c6844684a833'):
            self.assertTrue(token in context.context_markdown, f'Falta evidencia {token}')

    def test_loaded_procedure_documents_history_limits_without_automatic_expansion(self):
        context = build_agent_context(self.goal(), base_dir=settings.BASE_DIR)
        for token in ('5/10/15/50/100/300/500', '101 no es un límite válido',
                      'Una consulta corta que no cruza el corte no prueba la frontera'):
            self.assertTrue(token in context.context_markdown, f'Falta contrato {token}')

    def test_loaded_context_keeps_cancelled_waste_stock_timeline_unresolved(self):
        context = build_agent_context(self.goal(), base_dir=settings.BASE_DIR)
        for token in ('1680267', '1680271', '4→0→4', 'crédito ficticio',
                      'Bamoa6COMPLETE', 'no fabricar fetched_movement_ids'):
            self.assertTrue(token in context.context_markdown, f'Falta evidencia {token}')

    def test_publish_is_rejected_before_audit_writes(self):
        with self.assertRaisesMessage(ValueError, 'solo revisión'):
            self.review(requested_action='publish_if_safe')
        self.assertFalse(OrchestrationRun.objects.exists())
        self.assertFalse(AgentTask.objects.exists())

    def test_wrong_agent_or_entity_is_rejected(self):
        for change in ({'agent_code': 'director_operativo'}, {'entity_type': 'EventoVenta'},
                       {'entity_id': 2147483647}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.review(**change)
        self.assertFalse(OrchestrationRun.objects.exists())

    def test_inactive_agent_is_rejected(self):
        self.agent.status = 'paused'
        self.agent.save()
        with self.assertRaisesMessage(ValueError, 'activo'):
            self.review()
        self.assertFalse(OrchestrationRun.objects.exists())

    def test_missing_context_is_rejected_before_audit_writes(self):
        with TemporaryDirectory() as empty_root:
            with self.assertRaisesMessage(ValueError, 'contexto'):
                run_agent_goal(self.goal(), base_dir=Path(empty_root))
        self.assertFalse(OrchestrationRun.objects.exists())

    def test_catalog_declares_skill_and_existing_goal(self):
        from orquestacion.catalog import AGENTS
        definition = next(agent for agent in AGENTS if agent['code'] == 'agente_conciliacion')
        self.assertIn('reconciliation_guard', definition['supported_goal_types_json'])
        self.assertIn(SKILL, definition['context_files_json'])
        self.assertIn(SKILL, build_agent_context(self.goal(), base_dir=settings.BASE_DIR).files_in_order)

    def test_declared_external_tools_do_not_create_memory_proposals(self):
        from orquestacion.models import MemoryProposal
        self.agent.allowed_tools_json = ['api.unknown_binding']
        self.agent.save()
        self.review()
        self.review()
        self.assertEqual(MemoryProposal.objects.count(), 0)
        self.assertEqual(AgentSuggestion.objects.count(), 0)

    def test_incomplete_history_candidate_requires_all_gates(self):
        from pos_bridge.services.audit_stock_history_service import PointHistoryReconciliation
        self.product.external_id = '987654'
        self.product.save()
        self.save_case(difference=1, movement_status='NEEDS_EXPLANATION')
        with patch.object(AuditStockHistoryService, 'reconcile', return_value=PointHistoryReconciliation('INCOMPLETE')):
            self.assertTrue(self.review().observation['next_step']['history_capture_candidate'])
            self.save_case(source_trace={'closing': [2]})
            self.assertFalse(self.review().observation['next_step']['history_capture_candidate'])
            self.save_case(source_trace={'opening': [1], 'closing': [2]}, difference=0)
            self.assertFalse(self.review().observation['next_step']['history_capture_candidate'])

    def test_stock_remainder_is_not_hidden_by_balanced_projection(self):
        from pos_bridge.services.audit_stock_history_service import PointHistoryReconciliation
        with patch.object(AuditStockHistoryService, 'reconcile', return_value=PointHistoryReconciliation(
                'COMPLETE', identified_adjustment=Decimal('1'))):
            result = self.review()
        self.assertEqual(result.observation['next_step']['code'], 'REVIEW_STOCK_REMAINDER')
        self.assertEqual(result.observation['traceability_status'], 'PENDING')

    def test_unknown_history_never_recommends_capture(self):
        from pos_bridge.services.audit_stock_history_service import PointHistoryReconciliation
        with patch.object(AuditStockHistoryService, 'reconcile', return_value=PointHistoryReconciliation(
                'INCOMPLETE', unknown_movement_ids=(123,))):
            result = self.review()
        self.assertFalse(result.observation['next_step']['history_capture_candidate'])
        self.assertIn('123', str(result.observation['missing']))

    def test_missing_boundaries_are_not_zero_evidence(self):
        self.save_case(movement_status='SOURCE_INCOMPLETE', source_trace={})
        result = self.review()
        self.assertIsNone(result.observation['point_history']['unexplained_remainder'])
        self.assertFalse(result.observation['next_step']['history_capture_candidate'])

    def test_ready_case_without_boundary_references_is_not_proof_of_zero(self):
        self.save_case(source_trace={})
        result = self.review()
        self.assertIsNone(result.observation['point_history']['unexplained_remainder'])

    def test_verified_historical_notes_cannot_authorize_capture(self):
        from pos_bridge.services.audit_stock_history_service import PointHistoryReconciliation
        self.save_case(difference=1, movement_status='NEEDS_EXPLANATION')
        with patch.object(AuditStockHistoryService, 'reconcile', return_value=PointHistoryReconciliation('INCOMPLETE')):
            result = self.review()
        self.assertFalse(result.observation['next_step']['history_capture_candidate'])

    def test_ready_run_identity_resolution_facts_are_not_source_errors(self):
        self.audit_run.source_issues = [{'code': 'PRODUCT_RESOLVED_BY_SKU', 'message': 'Hecho de resolución'}]
        self.audit_run.save()
        self.assertTrue(self.review().observation['source_authoritative'])

    def test_context_loads_portable_detailed_evidence(self):
        result = self.review()
        self.assertIn('references/evidencias/auditoria-merma-matriz-20261003.md',
                      '\n'.join(result.context.loaded_files))
        self.assertIn('1683114', result.context.context_markdown)
        self.assertIn('38598/539340', result.context.context_markdown)
        self.assertIn('El rendimiento de rebanadas depende de la presentación', result.context.context_markdown)
        self.assertIn('1677688 entrada Rebanada12', result.context.context_markdown)
        self.assertIn('23/23 destinos de producto', result.context.context_markdown)

    def test_existing_management_command_can_review_inventory_entity(self):
        from django.core.management import call_command
        from io import StringIO
        output = StringIO()
        with patch('requests.Session.request', side_effect=AssertionError('HTTP prohibido')):
            call_command('run_agent_goal', goal='reconciliation_guard', event_id=self.case.pk,
                         entity_type='ProductInventoryAuditCase', stdout=output)
        self.assertIn('complete_review', output.getvalue())
        self.assertIn('REVIEW_', output.getvalue())
