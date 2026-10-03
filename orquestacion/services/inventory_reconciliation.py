"""Bounded, offline investigation for the existing reconciliation agent.

Only orchestration records are written by the caller. This module never captures,
materializes, synchronizes, approves or notifies an inventory case.
"""
import json

from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from reportes.models import ProductInventoryAuditCase
from reportes.services_inventory_audit_agent import InventoryAuditAgent
from reportes.services_inventory_audit_report import _case_quantity, case_balance_status
from orquestacion.services.inventory_reconciliation_plan import (
    validate_plan_metadata, recorded_state_signature, observe_month_plan,
)


SKILL_ROOT = '.agent/skills/42-domain-inventory/skill-point-inventory-reconciliation'
CONTEXT_FILES = [f'{SKILL_ROOT}/{name}' for name in (
    'SKILL.md', 'references/procedimiento.md', 'references/septiembre-2026.md',
    'references/hallazgos.json',
)]
CONTEXT_FILES += [f'{SKILL_ROOT}/references/evidencias/{name}' for name in (
    'auditoria-merma-matriz-20261003.md', 'auditoria-ciruela-identidad-20261003.md',
    'auditoria-finalizacion-3911-20261003.md', 'auditoria-devoluciones-3893-20261003.md',
)]


def validate_review(goal, agent, context):
    """Fail before creating runs/tasks if the bounded review cannot be performed."""
    if goal.requested_action != 'review':
        raise ValueError('Conciliación admite solo revisión; no publica ni sincroniza.')
    if agent.code != 'agente_conciliacion' or agent.status != 'active':
        raise ValueError('La revisión requiere el agente de conciliación activo.')
    if goal.entity_type != 'ProductInventoryAuditCase' or not goal.entity_id:
        raise ValueError('Se requiere un expediente ProductInventoryAuditCase exacto.')
    if not ProductInventoryAuditCase.objects.filter(pk=goal.entity_id).exists():
        raise ValueError('El expediente solicitado no existe.')
    validate_plan_metadata(goal.metadata)
    if any(path not in context.loaded_files for path in CONTEXT_FILES):
        raise ValueError('Falta contexto obligatorio de la habilidad de conciliación.')
    # Read the same bytes loaded into the audited context, not a second mutable file.
    marker = f'<!-- {SKILL_ROOT}/references/hallazgos.json -->\n'
    try:
        raw = context.context_markdown.split(marker, 1)[1].split('\n<!-- ', 1)[0]
        notes = json.loads(raw)
        if notes['version'] != '1' or not isinstance(notes['findings'], list):
            raise ValueError('Versión o formato inválido')
        for note in notes['findings']:
            if not all(key in note for key in ('month', 'branch_external_id',
                    'product_external_id', 'case_reference', 'observed_at', 'note')):
                raise ValueError('Hallazgo incompleto')
    except (IndexError, KeyError, TypeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError('El contexto de hallazgos no es válido.') from exc
    return notes['findings']


def observe_review(goal, *, notes):
    case = ProductInventoryAuditCase.objects.select_related('run', 'branch', 'product').get(pk=goal.entity_id)
    if goal.metadata.get('mode') == 'plan_month':
        return observe_month_plan(case, goal, notes)
    history = AuditStockHistoryService().reconcile(case.branch, case.product, case.month)
    investigation = InventoryAuditAgent().investigate_case(case).summary
    commercial = _case_quantity(case, 'sales')
    opening = _case_quantity(case, 'opening_point')
    closing = _case_quantity(case, 'point_closing')
    trace = case.source_trace or {}
    bounds_known = (opening is not None and closing is not None
                    and bool(trace.get('opening')) and bool(trace.get('closing')))
    history_payload = history.as_dict(opening=opening, point_closing=closing) if bounds_known else {
        'coverage_status': history.coverage_status,
        'unknown_movement_ids': list(history.unknown_movement_ids),
        'unexplained_remainder': None,
    }
    issues = list(case.run.source_issues or [])
    authoritative = (case.run.status == 'READY' and case_balance_status(case) != 'SOURCE_INCOMPLETE'
                     and not any(issue.get('code') == 'SOURCE_INCOMPLETE' for issue in issues))
    missing = list(investigation.get('missing') or [])
    if not bounds_known:
        missing.append('Apertura y cierre requieren referencias documentales; valores numéricos no prueban captura.')
    if not authoritative:
        missing.extend(issue['message'] for issue in issues if issue.get('code') == 'SOURCE_INCOMPLETE')
        missing.append('Autoridad de las fuentes mensuales pendiente; un saldo antiguo no valida fuentes actuales.')
    findings = [{**note, 'historical_evidence_only': True} for note in notes
                if note['month'] == case.month.isoformat()
                and note['branch_external_id'] == case.branch.external_id
                and note['product_external_id'] == case.product.external_id]
    mismatch = (history.coverage_status != 'MISSING' and commercial is not None
                and commercial != history.sales)
    if mismatch:
        missing.append(f'Venta comercial {commercial} frente a efecto de stock {history.sales}: relación no comprobada.')
    unknown = bool(history.unknown_movement_ids)
    if unknown:
        missing.append(f'Movimientos de stock desconocidos: {list(history.unknown_movement_ids)}.')
    remainder = (history.unexplained_remainder(opening, closing)
                 if bounds_known and history.coverage_status != 'MISSING' else None)
    if remainder is not None and remainder != 0:
        missing.append(f'Remanente histórico {remainder}; una proyección anterior no lo explica.')
    if history.coverage_status != 'COMPLETE':
        missing.append(f'Cobertura histórica {history.coverage_status}; no equivale a cierre comprobado.')
    eligible = (authoritative and bounds_known and bool(trace.get('opening')) and bool(trace.get('closing'))
                and history.coverage_status == 'INCOMPLETE' and not unknown and not mismatch
                and not findings
                and commercial is not None and case.difference != 0
                and ProductInventoryAuditCase.objects.sold_products().filter(pk=case.pk).exists()
                and history.unexplained_remainder(opening, closing) == 0)
    if not authoritative:
        code = 'REVIEW_MONTH_SOURCE_AUTHORITY'
    elif mismatch:
        code = 'REVIEW_COMMERCIAL_STOCK_EFFECT'
    elif remainder is not None and remainder != 0:
        code = 'REVIEW_STOCK_REMAINDER'
    elif findings:
        code = 'REVIEW_STORED_VERIFIED_DOCUMENTS'
    elif unknown:
        code = 'REVIEW_UNKNOWN_STOCK_MOVEMENTS'
    elif eligible:
        code = 'VERIFY_HISTORY_COVERAGE'
    elif history.coverage_status != 'COMPLETE':
        code = 'REVIEW_EXISTING_HISTORY'
    elif (opening is not None and opening < 0) or (closing is not None and closing < 0):
        code = 'REVIEW_INHERITED_NEGATIVE_STOCK'
    elif missing or investigation.get('discrepancies'):
        code = 'REVIEW_DOCUMENTARY_EVIDENCE'
    else:
        code = 'REVIEW_APPROVAL_AND_PHYSICAL_COUNT'
    reasons = {
        'REVIEW_MONTH_SOURCE_AUTHORITY': 'Revisar autoridad de fuentes mensuales antes de recalcular.',
        'REVIEW_COMMERCIAL_STOCK_EFFECT': 'Conservar ventas comerciales y comprobar su efecto de stock.',
        'REVIEW_STOCK_REMAINDER': 'Investigar el remanente con los movimientos guardados, sin compensaciones.',
        'REVIEW_STORED_VERIFIED_DOCUMENTS': 'Reutilizar documentos ya verificados antes de pedir o descargar evidencia.',
        'REVIEW_UNKNOWN_STOCK_MOVEMENTS': 'Identificar movimientos desconocidos con su documento original.',
        'VERIFY_HISTORY_COVERAGE': 'Falta acreditar cobertura posterior al cierre; preparar verificación acotada separada.',
        'REVIEW_EXISTING_HISTORY': 'Revisar límites y evidencia guardada antes de proponer captura.',
        'REVIEW_INHERITED_NEGATIVE_STOCK': 'Rastrear negativos heredados; saldo aritmético no demuestra existencia física.',
        'REVIEW_DOCUMENTARY_EVIDENCE': 'Atender documentos pendientes sin convertirlos en pérdidas ni otro retorno.',
        'REVIEW_APPROVAL_AND_PHYSICAL_COUNT': 'Verificar aprobación independiente y conteo físico aplicable antes de cierre.',
    }
    next_step = {'code': code, 'point_http_allowed': False,
                 'reason': reasons[code],
                 'history_capture_candidate': eligible,
                 'requires_separate_execution': True}
    if eligible:
        next_step['capture_constraints'] = {'max_batch': 10, 'force': False,
            'canonical_only': True, 'deduplicate_by': 'FK_Movimiento',
            'session_lock': 'point_account_session_lock', 'wait': False}
    observation = {
        'skill': {'id': 'point-inventory-reconciliation', 'version': '1'},
        'case_id': case.pk, 'month': case.month.isoformat(),
        'recorded_state_signature': recorded_state_signature(case, notes),
        'branch_external_id': case.branch.external_id, 'product_external_id': case.product.external_id,
        'case_balance_status': case_balance_status(case), 'difference': str(case.difference),
        'source_authoritative': authoritative, 'source_issues': issues,
        'source_calculation_fingerprint': case.run.calculation_fingerprint,
        'source_updated_at': case.run.updated_at.isoformat(),
        'commercial_sales': str(commercial) if commercial is not None else None,
        'stock_sales': str(history.sales) if history.coverage_status != 'MISSING' else None,
        'point_history': history_payload, 'physical_status': case.physical_status,
        'traceability_status': 'PENDING' if missing or findings or investigation.get('discrepancies') else
            investigation.get('traceability_status', 'PENDING'),
        'missing': list(dict.fromkeys(missing)), 'known_findings': findings,
        'investigation': investigation, 'next_step': next_step, 'closure_allowed': False,
    }
    return case, observation, []  # Review completion is not inventory approval/closure.
