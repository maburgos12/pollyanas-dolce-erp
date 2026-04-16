from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from typing import Iterable

from django.utils import timezone

from core.models import AuditLog
from orquestacion.models import MemoryProposal, QualityFinding, RemediationProposal
from orquestacion.services.memory_proposals import MemoryProposalResult, propose_memory_update
from orquestacion.services.pointdailysale_guard import (
    POINT_DAILY_SALE_SUGGESTION,
    GuardScanResult,
    GuardViolation,
)
from orquestacion.services.protected_sales_reader_guard import (
    ProtectedSalesReaderScanResult,
    ProtectedSalesReaderViolation,
)
from orquestacion.services.sales_publication_guard import SalesPublicationGapScanResult

QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER = "direct_pointdailysale_reader"
QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER = "protected_raw_sales_reader"
QUALITY_FINDING_CODE_SALES_PUBLICATION_GAP = "sales_publication_gap"
QUALITY_FINDING_CATEGORY_ARCHITECTURE_VIOLATION = QualityFinding.CATEGORY_ARCHITECTURE_VIOLATION
REMEDIATION_TYPE_CANONICAL_SALES_READER = "canonical_sales_reader"
REMEDIATION_TYPE_SALES_PUBLICATION_CATCHUP = "sales_publication_catchup"


@dataclass(frozen=True)
class QualityFindingResult:
    finding: QualityFinding
    created: bool
    reopened: bool = False


def record_quality_finding(
    *,
    code: str,
    category: str,
    severity: str,
    source_type: str,
    source_reference: str,
    statement: str,
    evidence_refs: Iterable[str],
    details: dict | None = None,
    is_blocking: bool = True,
) -> QualityFindingResult:
    normalized_evidence = _normalize_list(evidence_refs)
    if not normalized_evidence:
        raise ValueError("QualityFinding requiere al menos una evidencia.")
    normalized_statement = _normalize_text(statement)
    if not normalized_statement:
        raise ValueError("QualityFinding requiere statement.")

    finding_key = _build_finding_key(code=code, source_reference=source_reference)
    now = timezone.now()
    finding, created = QualityFinding.objects.get_or_create(
        finding_key=finding_key,
        defaults={
            "code": code,
            "category": category,
            "severity": severity,
            "status": QualityFinding.STATUS_OPEN,
            "source_type": source_type,
            "source_reference": source_reference,
            "statement": normalized_statement,
            "evidence_refs_json": normalized_evidence,
            "detected_count": 1,
            "first_seen_at": now,
            "last_seen_at": now,
            "is_blocking": is_blocking,
            "details_json": dict(details or {}),
        },
    )
    reopened = False
    if created:
        _log_quality_audit("CREATE", finding, actor=None, payload={"created": True})
        return QualityFindingResult(finding=finding, created=True, reopened=False)

    update_fields = ["detected_count", "last_seen_at", "updated_at"]
    finding.detected_count += 1
    finding.last_seen_at = now
    finding.evidence_refs_json = _merge_lists(finding.evidence_refs_json, normalized_evidence)
    update_fields.append("evidence_refs_json")
    if finding.statement != normalized_statement:
        finding.statement = normalized_statement
        update_fields.append("statement")
    if finding.severity != severity:
        finding.severity = severity
        update_fields.append("severity")
    if finding.is_blocking != is_blocking:
        finding.is_blocking = is_blocking
        update_fields.append("is_blocking")
    if details:
        finding.details_json = {**(finding.details_json or {}), **details}
        update_fields.append("details_json")
    if finding.status == QualityFinding.STATUS_RESOLVED:
        finding.status = QualityFinding.STATUS_OPEN
        finding.resolved_at = None
        reopened = True
        update_fields.extend(["status", "resolved_at"])
    finding.save(update_fields=update_fields)
    _log_quality_audit(
        "UPDATE",
        finding,
        actor=None,
        payload={"created": False, "reopened": reopened, "detected_count": finding.detected_count},
    )
    return QualityFindingResult(finding=finding, created=False, reopened=reopened)


def ensure_remediation_proposal(
    finding: QualityFinding,
    *,
    target_files: Iterable[str],
    suggested_tests: Iterable[str],
    suggested_fix: str,
    remediation_type: str = REMEDIATION_TYPE_CANONICAL_SALES_READER,
    risk_level: str = RemediationProposal.RISK_LOW,
) -> RemediationProposal:
    remediation_key = _build_remediation_key(finding=finding, remediation_type=remediation_type)
    proposal, created = RemediationProposal.objects.get_or_create(
        remediation_key=remediation_key,
        defaults={
            "finding": finding,
            "remediation_type": remediation_type,
            "summary": f"Corregir {finding.code} en {finding.source_reference}",
            "suggested_fix": _normalize_text(suggested_fix),
            "target_files_json": _normalize_list(target_files),
            "suggested_tests_json": _normalize_list(suggested_tests),
            "risk_level": risk_level,
            "details_json": {
                "finding_code": finding.code,
                "category": finding.category,
            },
        },
    )
    if created:
        _log_quality_audit(
            "CREATE_REMEDIATION",
            finding,
            actor=None,
            payload={"remediation_key": proposal.remediation_key, "status": proposal.status},
        )
        return proposal

    proposal.summary = f"Corregir {finding.code} en {finding.source_reference}"
    proposal.suggested_fix = _normalize_text(suggested_fix)
    proposal.target_files_json = _merge_lists(proposal.target_files_json, target_files)
    proposal.suggested_tests_json = _merge_lists(proposal.suggested_tests_json, suggested_tests)
    if proposal.status == RemediationProposal.STATUS_VALIDATED and finding.status != QualityFinding.STATUS_RESOLVED:
        proposal.status = RemediationProposal.STATUS_IMPLEMENTED
        proposal.validated_at = None
    proposal.save(
        update_fields=[
            "summary",
            "suggested_fix",
            "target_files_json",
            "suggested_tests_json",
            "status",
            "validated_at",
            "updated_at",
        ]
    )
    return proposal


def accept_remediation_proposal(remediation: RemediationProposal, *, actor) -> RemediationProposal:
    remediation.status = RemediationProposal.STATUS_ACCEPTED
    remediation.save(update_fields=["status", "updated_at"])
    _log_remediation_audit("ACCEPT_REMEDIATION", remediation, actor=actor, payload={"status": remediation.status})
    return remediation


def mark_remediation_implemented(remediation: RemediationProposal, *, actor) -> RemediationProposal:
    remediation.status = RemediationProposal.STATUS_IMPLEMENTED
    remediation.validated_at = None
    remediation.save(update_fields=["status", "validated_at", "updated_at"])
    _log_remediation_audit("IMPLEMENT_REMEDIATION", remediation, actor=actor, payload={"status": remediation.status})
    return remediation


def reject_remediation_proposal(remediation: RemediationProposal, *, actor, reason: str) -> RemediationProposal:
    remediation.status = RemediationProposal.STATUS_REJECTED
    remediation.details_json = {
        **(remediation.details_json or {}),
        "rejection_reason": _normalize_text(reason),
    }
    remediation.save(update_fields=["status", "details_json", "updated_at"])
    _log_remediation_audit(
        "REJECT_REMEDIATION",
        remediation,
        actor=actor,
        payload={"status": remediation.status, "reason": remediation.details_json.get("rejection_reason", "")},
    )
    return remediation


def maybe_create_memory_proposal_for_finding(finding: QualityFinding) -> MemoryProposalResult | None:
    if finding.category != QUALITY_FINDING_CATEGORY_ARCHITECTURE_VIOLATION:
        return None
    if finding.code not in {
        QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER,
        QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER,
    }:
        return None
    if finding.detected_count < 2:
        return None
    detector_ref = (
        "scripts/check_protected_sales_readers.py"
        if finding.code == QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER
        else "scripts/check_pointdailysale_usage.py"
    )
    evidence_refs = _normalize_list(
        [
            *list(finding.evidence_refs_json or []),
            detector_ref,
            "docs/CANONICIDAD_VENTAS_ERP.md",
        ]
    )
    result = propose_memory_update(
        section=MemoryProposal.SECTION_GAP,
        summary="Uso directo recurrente de PointDailySale fuera de la allowlist",
        statement=_memory_statement_for_finding(finding),
        source_type=MemoryProposal.SOURCE_QUALITY_GUARD,
        source_reference=f"quality_guard:{finding.code}:{finding.source_reference}",
        confidence_score=0.99,
        evidence_refs=evidence_refs,
    )
    if finding.memory_proposal_id != result.proposal.id:
        finding.memory_proposal = result.proposal
        finding.save(update_fields=["memory_proposal", "updated_at"])
    return result


def sync_protected_sales_reader_findings(*, scan_result: ProtectedSalesReaderScanResult) -> dict[str, object]:
    active_keys: set[str] = set()
    created = 0
    updated = 0
    reopened = 0
    memory_created = 0
    memory_updated = 0

    grouped_violations: dict[str, list[ProtectedSalesReaderViolation]] = {}
    for violation in scan_result.violations:
        grouped_violations.setdefault(violation.relative_path, []).append(violation)

    for relative_path, grouped in grouped_violations.items():
        line_numbers = sorted({violation.line_number for violation in grouped})
        symbols = sorted({violation.symbol for violation in grouped})
        finding_result = record_quality_finding(
            code=QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER,
            category=QUALITY_FINDING_CATEGORY_ARCHITECTURE_VIOLATION,
            severity=QualityFinding.SEVERITY_HIGH,
            source_type=QualityFinding.SOURCE_GUARD,
            source_reference=relative_path,
            statement=grouped[0].reason,
            evidence_refs=[relative_path, "scripts/check_protected_sales_readers.py"],
            details={
                "line_numbers": line_numbers,
                "symbols": symbols,
                "suggestion": grouped[0].suggestion,
            },
            is_blocking=True,
        )
        active_keys.add(finding_result.finding.finding_key)
        created += int(finding_result.created)
        updated += int(not finding_result.created)
        reopened += int(finding_result.reopened)

        remediation = ensure_remediation_proposal(
            finding_result.finding,
            target_files=[relative_path],
            suggested_tests=[
                "./.venv/bin/python scripts/check_protected_sales_readers.py",
                "./scripts/run_tests_local.sh orquestacion.tests_quality_loop",
            ],
            suggested_fix=f"{grouped[0].suggestion} Archivo protegido afectado: {relative_path}.",
        )
        if remediation.status == RemediationProposal.STATUS_PROPOSED:
            remediation.status = RemediationProposal.STATUS_ACCEPTED
            remediation.save(update_fields=["status", "updated_at"])

        memory_result = maybe_create_memory_proposal_for_finding(finding_result.finding)
        if memory_result is not None:
            if memory_result.created:
                memory_created += 1
            else:
                memory_updated += 1

    resolved = _resolve_missing_guard_findings(
        code=QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER,
        source_type=QualityFinding.SOURCE_GUARD,
        active_keys=active_keys,
        source="protected_sales_guard_rerun",
    )

    return {
        "checked_files": scan_result.checked_files,
        "violations": len(scan_result.violations),
        "findings_created": created,
        "findings_updated": updated,
        "findings_reopened": reopened,
        "findings_resolved": resolved,
        "memory_created": memory_created,
        "memory_updated": memory_updated,
    }


def sync_sales_publication_gap_finding(*, gap_result: SalesPublicationGapScanResult) -> dict[str, object]:
    active_keys: set[str] = set()
    created = 0
    updated = 0
    reopened = 0
    if gap_result.has_gap and gap_result.target_date:
        source_reference = f"sales_publication_gap:{gap_result.target_date.isoformat()}"
        finding_result = record_quality_finding(
            code=QUALITY_FINDING_CODE_SALES_PUBLICATION_GAP,
            category=QualityFinding.CATEGORY_PUBLICATION_GAP,
            severity=(
                QualityFinding.SEVERITY_HIGH
                if gap_result.severity == "high"
                else QualityFinding.SEVERITY_WARNING
            ),
            source_type=QualityFinding.SOURCE_RUNTIME,
            source_reference=source_reference,
            statement=gap_result.reason,
            evidence_refs=[
                "reportes/sales_dashboard_freshness.py",
                "reportes/analytics_service.py",
                "reportes/dashboard_sales_dataset.py",
            ],
            details={
                "reference_date": gap_result.reference_date.isoformat(),
                "target_date": gap_result.target_date.isoformat(),
                "point_latest_date": gap_result.point_latest_date.isoformat() if gap_result.point_latest_date else "",
                "fact_latest_date": gap_result.fact_latest_date.isoformat() if gap_result.fact_latest_date else "",
                "visible_cut_date": gap_result.visible_cut_date.isoformat() if gap_result.visible_cut_date else "",
                "fact_lag_days": gap_result.fact_lag_days,
                "visible_lag_days": gap_result.visible_lag_days,
                "suggestion": gap_result.suggestion,
            },
            is_blocking=gap_result.is_blocking,
        )
        active_keys.add(finding_result.finding.finding_key)
        created += int(finding_result.created)
        updated += int(not finding_result.created)
        reopened += int(finding_result.reopened)
        remediation = ensure_remediation_proposal(
            finding_result.finding,
            target_files=[
                "reportes/analytics_service.py",
                "reportes/sales_dashboard_freshness.py",
                "reportes/dashboard_sales_dataset.py",
            ],
            suggested_tests=[
                f"./.venv/bin/python manage.py refresh_analytics_layer --date {gap_result.target_date.isoformat()} --lookback-days {max(gap_result.fact_lag_days, gap_result.visible_lag_days, 2) + 1}",
                "./.venv/bin/python manage.py run_quality_guards",
            ],
            suggested_fix=gap_result.suggestion,
            remediation_type=REMEDIATION_TYPE_SALES_PUBLICATION_CATCHUP,
            risk_level=RemediationProposal.RISK_MEDIUM,
        )
        if remediation.status == RemediationProposal.STATUS_PROPOSED:
            remediation.status = RemediationProposal.STATUS_ACCEPTED
            remediation.save(update_fields=["status", "updated_at"])

    resolved = _resolve_missing_guard_findings(
        code=QUALITY_FINDING_CODE_SALES_PUBLICATION_GAP,
        source_type=QualityFinding.SOURCE_RUNTIME,
        active_keys=active_keys,
        source="sales_publication_guard_rerun",
    )
    return {
        "violations": 1 if gap_result.has_gap else 0,
        "findings_created": created,
        "findings_updated": updated,
        "findings_reopened": reopened,
        "findings_resolved": resolved,
        "target_date": gap_result.target_date.isoformat() if gap_result.target_date else "",
    }


def sync_pointdailysale_guard_findings(*, scan_result: GuardScanResult) -> dict[str, object]:
    active_keys: set[str] = set()
    created = 0
    updated = 0
    reopened = 0
    memory_created = 0
    memory_updated = 0

    grouped_violations: dict[str, list[GuardViolation]] = {}
    for violation in scan_result.violations:
        grouped_violations.setdefault(violation.relative_path, []).append(violation)

    for relative_path, grouped in grouped_violations.items():
        line_numbers = sorted({violation.line_number for violation in grouped})
        source_reference = relative_path
        finding_result = record_quality_finding(
            code=QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER,
            category=QUALITY_FINDING_CATEGORY_ARCHITECTURE_VIOLATION,
            severity=QualityFinding.SEVERITY_HIGH,
            source_type=QualityFinding.SOURCE_GUARD,
            source_reference=source_reference,
            statement=grouped[0].reason,
            evidence_refs=[
                relative_path,
                "scripts/check_pointdailysale_usage.py",
            ],
            details={
                "line_numbers": line_numbers,
                "suggestion": grouped[0].suggestion,
            },
            is_blocking=True,
        )
        active_keys.add(finding_result.finding.finding_key)
        created += int(finding_result.created)
        updated += int(not finding_result.created)
        reopened += int(finding_result.reopened)

        remediation = ensure_remediation_proposal(
            finding_result.finding,
            target_files=[relative_path],
            suggested_tests=[
                "./.venv/bin/python scripts/check_pointdailysale_usage.py",
                "./scripts/run_tests_local.sh orquestacion.tests_quality_loop",
            ],
            suggested_fix=(
                f"{POINT_DAILY_SALE_SUGGESTION} Archivo afectado: {relative_path}."
            ),
        )
        if remediation.status == RemediationProposal.STATUS_PROPOSED:
            remediation.status = RemediationProposal.STATUS_ACCEPTED
            remediation.save(update_fields=["status", "updated_at"])

        memory_result = maybe_create_memory_proposal_for_finding(finding_result.finding)
        if memory_result is not None:
            if memory_result.created:
                memory_created += 1
            else:
                memory_updated += 1

    resolved = _resolve_missing_guard_findings(
        code=QUALITY_FINDING_CODE_DIRECT_POINTDAILYSALE_READER,
        source_type=QualityFinding.SOURCE_GUARD,
        active_keys=active_keys,
        source="guard_rerun",
    )

    return {
        "checked_files": scan_result.checked_files,
        "violations": len(scan_result.violations),
        "findings_created": created,
        "findings_updated": updated,
        "findings_reopened": reopened,
        "findings_resolved": resolved,
        "memory_created": memory_created,
        "memory_updated": memory_updated,
    }


def _build_finding_key(*, code: str, source_reference: str) -> str:
    raw = f"{code}||{source_reference.strip().lower()}"
    return f"qf-{sha1(raw.encode('utf-8')).hexdigest()[:20]}"


def _build_remediation_key(*, finding: QualityFinding, remediation_type: str) -> str:
    raw = f"{finding.finding_key}||{remediation_type}"
    return f"rem-{sha1(raw.encode('utf-8')).hexdigest()[:20]}"


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_list(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidate = str(value or "").strip()
        if not candidate or candidate in seen:
            continue
        normalized.append(candidate)
        seen.add(candidate)
    return normalized


def _merge_lists(existing: Iterable[str], new_values: Iterable[str]) -> list[str]:
    return _normalize_list([*existing, *new_values])


def _memory_statement_for_finding(finding: QualityFinding) -> str:
    if finding.code == QUALITY_FINDING_CODE_PROTECTED_RAW_SALES_READER:
        return (
            "El guard arquitectonico detecto un lector crudo de ventas dentro de un archivo protegido "
            "por la politica canónica; gateway, datasets visibles y consultas operativas deben consumir "
            "ventas/services/sales_read_service.py o ventas/services/sales_canonical_source.py."
        )
    return (
        "El guard arquitectonico detecto uso directo recurrente de PointDailySale fuera de la allowlist "
        "canonica; estas lecturas deben pasar por ventas/services/sales_read_service.py o "
        "ventas/services/sales_canonical_source.py."
    )


def _resolve_missing_guard_findings(
    *,
    code: str,
    source_type: str,
    active_keys: set[str],
    source: str,
) -> int:
    resolved = 0
    for finding in QualityFinding.objects.filter(
        code=code,
        status=QualityFinding.STATUS_OPEN,
        source_type=source_type,
    ):
        if finding.finding_key in active_keys:
            continue
        finding.status = QualityFinding.STATUS_RESOLVED
        finding.resolved_at = timezone.now()
        finding.save(update_fields=["status", "resolved_at", "updated_at"])
        _log_quality_audit("RESOLVE", finding, actor=None, payload={"source": source})
        finding.remediation_proposals.exclude(status=RemediationProposal.STATUS_REJECTED).update(
            status=RemediationProposal.STATUS_VALIDATED,
            validated_at=finding.resolved_at,
            updated_at=finding.resolved_at,
        )
        resolved += 1
    return resolved


def _log_quality_audit(action: str, finding: QualityFinding, *, actor, payload: dict) -> None:
    AuditLog.objects.create(
        user=actor,
        action=action,
        model="orquestacion.QualityFinding",
        object_id=str(finding.id or finding.finding_key),
        payload={
            "finding_key": finding.finding_key,
            "code": finding.code,
            "status": finding.status,
            **payload,
        },
    )


def _log_remediation_audit(action: str, remediation: RemediationProposal, *, actor, payload: dict) -> None:
    AuditLog.objects.create(
        user=actor,
        action=action,
        model="orquestacion.RemediationProposal",
        object_id=str(remediation.id or remediation.remediation_key),
        payload={
            "remediation_key": remediation.remediation_key,
            "finding_key": remediation.finding.finding_key,
            "status": remediation.status,
            **payload,
        },
    )
