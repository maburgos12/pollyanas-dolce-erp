"""Plan from recorded evidence, not a live-source verifier or an executor."""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
import hashlib
import json

from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import F

from orquestacion.models import OrchestrationRun
from reportes.models import ProductInventoryAuditCase
from reportes.services_inventory_audit_report import _case_quantity, case_balance_status


def digest(value):
    return hashlib.sha256(json.dumps(value, cls=DjangoJSONEncoder, sort_keys=True,
                                    ensure_ascii=False).encode()).hexdigest()


def recorded_month_signature(run):
    return digest({field.attname: getattr(run, field.attname) for field in run._meta.concrete_fields})


def recorded_state_signature(case, notes, *, month_signature=None):
    """Only recorded case/month/events; never claim mutable external sources unchanged."""
    case_values = {field.attname: getattr(case, field.attname) for field in case._meta.concrete_fields}
    events = [{field.attname: (str(getattr(event, field.attname)) if field.get_internal_type() == 'FileField'
                              else getattr(event, field.attname)) for field in event._meta.concrete_fields}
              for event in case.events.all()]
    return digest({'contract': 1, 'case': case_values,
                   'month': month_signature or recorded_month_signature(case.run),
                   'events': events, 'notes': notes})


def validate_plan_metadata(metadata):
    if not isinstance(metadata, dict):
        raise ValueError('Metadatos de revisión inválidos.')
    if not metadata:
        return
    if metadata.get('mode') != 'plan_month':
        raise ValueError('Modo de conciliación inválido.')
    if set(metadata) - {'mode', 'batch_size', 'after_case_id', 'expected_plan_fingerprint'}:
        raise ValueError('Opciones de planificación desconocidas.')
    size, after = metadata.get('batch_size', 10), metadata.get('after_case_id', 0)
    if type(size) is not int or not 1 <= size <= 10 or type(after) is not int or after < 0:
        raise ValueError('Plan requiere lote de 1 a 10 y cursor no negativo.')
    fingerprint = metadata.get('expected_plan_fingerprint', '')
    if (not isinstance(fingerprint, str) or
            (fingerprint and (len(fingerprint) != 64 or any(c not in '0123456789abcdef' for c in fingerprint))) or
            (after and not fingerprint)):
        raise ValueError('Reanudar el lote requiere la huella exacta del plan anterior.')


def _group_code(case, notes):
    history = (case.source_trace or {}).get('point_history', {})
    comparison = history.get('aggregate_comparison', {}).get('sales', {})
    stock_sales = comparison.get('point_history', history.get('sales'))
    try:
        if stock_sales is not None and _case_quantity(case, 'sales') != Decimal(str(stock_sales)):
            return 'REVIEW_COMMERCIAL_STOCK_EFFECT'
    except (InvalidOperation, TypeError):
        return 'REVIEW_EXISTING_HISTORY'
    if history.get('unknown_movement_ids'):
        return 'REVIEW_UNKNOWN_STOCK_MOVEMENTS'
    try:
        if history.get('unexplained_remainder') is not None and Decimal(str(history['unexplained_remainder'])) != 0:
            return 'REVIEW_STOCK_REMAINDER'
    except InvalidOperation:
        return 'REVIEW_EXISTING_HISTORY'
    if any(note['month'] == case.month.isoformat() and
           note['branch_external_id'] == case.branch.external_id and
           note['product_external_id'] == case.product.external_id for note in notes):
        return 'REVIEW_STORED_VERIFIED_DOCUMENTS'
    if (case.opening_point < 0 or case.point_closing < 0):
        return 'REVIEW_INHERITED_NEGATIVE_STOCK'
    if history.get('coverage_status') != 'COMPLETE':
        # Missing is not zero; this is a review queue, never permission to capture.
        if case.investigation_summary.get('missing'):
            return 'REVIEW_DOCUMENTARY_EVIDENCE'
        return 'REVIEW_EXISTING_HISTORY'
    if case.investigation_summary.get('missing') or case.investigation_summary.get('discrepancies'):
        return 'REVIEW_DOCUMENTARY_EVIDENCE'
    if case.difference != 0:
        return 'REVIEW_STOCK_REMAINDER'
    return 'REVIEW_APPROVAL_AND_PHYSICAL_COUNT'


def observe_month_plan(anchor, goal, notes):
    cases = list(ProductInventoryAuditCase.objects.sold_products().filter(run=anchor.run, month=anchor.month)
                 .select_related('branch', 'product').prefetch_related('events').order_by('pk'))
    month_signature = recorded_month_signature(anchor.run)
    for case in cases:
        # Do not repeat a potentially large monthly source_issues JSON in every SQL row.
        case.run = anchor.run
    ids = [case.pk for case in cases]
    signatures = {case.pk: recorded_state_signature(case, notes, month_signature=month_signature) for case in cases}
    # PostgreSQL DISTINCT ON selects a single latest individual review per subject.
    previous = (OrchestrationRun.objects.filter(status='success',
                result_summary_json__observation__case_id__in=ids,
                result_summary_json__observation__month=anchor.month.isoformat(),
                result_summary_json__observation__recorded_state_signature__isnull=False)
                .annotate(subject=F('result_summary_json__observation__case_id'))
                .order_by('subject', '-pk').distinct('subject')
                .values('pk', 'result_summary_json', 'subject'))
    reused = {}
    for row in previous:
        observation = row['result_summary_json']['observation']
        case_id = observation['case_id']
        if observation.get('recorded_state_signature') == signatures.get(case_id):
            reused[case_id] = {'case_id': case_id, 'run_id': row['pk']}
    grouped = defaultdict(list)
    for case in cases:
        grouped[_group_code(case, notes)].append(case)
    groups = []
    queue = []
    priority = {'REVIEW_COMMERCIAL_STOCK_EFFECT': 0, 'REVIEW_STOCK_REMAINDER': 1,
                'REVIEW_UNKNOWN_STOCK_MOVEMENTS': 1, 'REVIEW_STORED_VERIFIED_DOCUMENTS': 2,
                'REVIEW_INHERITED_NEGATIVE_STOCK': 2, 'REVIEW_EXISTING_HISTORY': 3,
                'REVIEW_DOCUMENTARY_EVIDENCE': 3, 'REVIEW_APPROVAL_AND_PHYSICAL_COUNT': 4}
    for code, members in sorted(grouped.items(), key=lambda item: (
            priority[item[0]], -len(item[1]), item[0])):
        members.sort(key=lambda case: (case.attention_level != 'HIGH', case.pk))
        groups.append({'code': code, 'affected_cases': len(members),
                       'priority': priority[code],
                       'case_ids': [case.pk for case in members],
                       'stored_missing': [{'case_id': case.pk, 'missing': case.investigation_summary.get('missing', [])}
                                          for case in members[:10] if case.investigation_summary.get('missing')],
                       'documentary_sample_limit': 10,
                       'requires_individual_verification': True,
                       'send_notifications': False})
        queue.extend(case.pk for case in members if case.pk not in reused)
    source_errors = [issue for issue in anchor.run.source_issues if issue.get('code') == 'SOURCE_INCOMPLETE']
    blocked = anchor.run.status != 'READY' or bool(source_errors)
    blockers = []
    if blocked:
        blockers = [{'code': 'REVIEW_MONTH_SOURCE_AUTHORITY', 'affected_cases': len(cases),
                     'messages': list(dict.fromkeys(issue['message'] for issue in source_errors)),
                     'requires_separate_authorization': True, 'send_notifications': False}]
    fingerprint = digest({'signatures': signatures, 'reused': reused, 'queue': queue, 'blockers': blockers})
    after = goal.metadata.get('after_case_id', 0)
    changed = bool(after and goal.metadata['expected_plan_fingerprint'] != fingerprint)
    if after and not changed and after not in queue:
        raise ValueError('Cursor no pertenece al plan solicitado.')
    start = 0 if changed or not after else queue.index(after) + 1
    batch = queue[start:start + goal.metadata.get('batch_size', 10)]
    plan = {'total_cases': len(cases), 'global_blockers': blockers, 'groups': groups,
            'batch': batch, 'remaining_review_candidates': len(queue) - start - len(batch),
            'next_after_case_id': batch[-1] if batch else 0, 'plan_fingerprint': fingerprint,
            'cursor_reset_due_to_changes': changed,
            'reused_review_count': len(reused), 'reused_reviews': list(reused.values()),
            'reuse_scope': 'RECORDED_CASE_AND_MONTH_ONLY', 'live_sources_verified': False,
            'materialization_allowed': False, 'closure_allowed': False,
            'progress': {'recorded_balance_status': dict(Counter(case_balance_status(case) for case in cases)),
                         'recorded_physical_status': dict(Counter(case.physical_status for case in cases)),
                         'review_candidates': len(queue)},
            'notice': 'Plan con evidencia registrada, no verificación de fuentes vivas. '
                      'No repetir solicitudes ni HTTP; cambios externos requieren comprobación explícita.'}
    reason = (f"Plan mensual: {len(cases)} expedientes, {len(blockers)} bloqueo(s) global(es), "
              f"{len(groups)} grupos y {len(batch)} revisiones propuestas; "
              f"{len(reused)} revisiones registradas reutilizadas.")
    return anchor, {'mode': 'plan_month', 'case_id': anchor.pk, 'month': anchor.month.isoformat(),
                    'plan': plan, 'closure_allowed': False,
                    'next_step': {'code': 'REVIEW_MONTH_SOURCE_AUTHORITY' if blocked else 'REVIEW_PRIORITY_BATCH',
                                  'reason': reason, 'point_http_allowed': False,
                                  'history_capture_candidate': False, 'requires_separate_execution': True}}, []
