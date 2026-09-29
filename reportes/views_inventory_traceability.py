from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal
from html import escape
from pathlib import Path
from uuid import uuid4

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.db import models, transaction
from django.http import FileResponse, Http404, HttpRequest, HttpResponse, JsonResponse
from django.middleware.csrf import get_token
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.text import get_valid_filename
from django.views.decorators.http import require_GET, require_POST

from core.access import ACCESS_MANAGE, can_view_reportes, get_module_access
from mantenimiento.evidence_validation import (
    EvidenceValidationError,
    validate_evidence_files,
)
from pos_bridge.models import (
    PointConversionLine,
    PointDailySale,
    PointHistoricalInventoryClosingLine,
    PointOpenTransferSnapshotMember,
    PointProductionLine,
    PointTransferLine,
    PointWasteLine,
)
from pos_bridge.services.branch_inventory_traceability_service import (
    canonical_point_branch_identity,
)
from reportes.models import (
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)

MAX_EVIDENCE_SIZE = 10 * 1024 * 1024
MAX_NOTES_LENGTH = 4000

MOVEMENT_STATUS_LABELS = {
    ProductInventoryAuditCase.MovementStatus.BALANCED: "Conciliado",
    ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION: "Requiere explicación",
    ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL: "Pendiente de aprobación",
    ProductInventoryAuditCase.MovementStatus.RESOLVED: "Resuelto y aprobado",
    ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE: "Fuente incompleta",
}

TRACE_SOURCE_LABELS = {
    "opening": "Inventario inicial Point",
    "production": "Producción",
    "transfers": "Transferencias",
    "conversions": "Conversiones",
    "sales": "Ventas",
    "waste": "Merma",
    "adjustments": "Ajustes identificados",
    "closing": "Cierre Point",
}

BALANCE_SEQUENCE = (
    ("opening_point", "Inventario inicial Point", "+"),
    ("production", "Producción", "+"),
    ("transfer_in", "Transferencias recibidas", "+"),
    ("conversion_in", "Conversiones de entrada", "+"),
    ("sales", "Ventas", "−"),
    ("waste", "Merma", "−"),
    ("transfer_out", "Transferencias enviadas", "−"),
    ("conversion_out", "Conversiones de salida", "−"),
    ("identified_adjustment", "Ajustes identificados", "±"),
    ("expected_closing", "Inventario esperado", "="),
    ("point_closing", "Cierre Point", "↔"),
    ("difference", "Diferencia", "="),
)

EXCEPTION_STATUSES = (
    ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
    ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
    ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
)
RECONCILED_STATUSES = (
    ProductInventoryAuditCase.MovementStatus.BALANCED,
    ProductInventoryAuditCase.MovementStatus.RESOLVED,
)

REASON_LABELS = {
    "TRANSFER_PENDING": "Transferencia pendiente o desfasada",
    "CONVERSION": "Conversión o producto rebanado",
    "WASTE": "Merma registrada o pendiente",
    "PRODUCTION": "Producción por aclarar",
    "SALE": "Venta por aclarar",
    "STOCK_ELSEWHERE": "Producto localizado en otra ubicación",
    "OTHER": "Otra causa comprobable",
    "PHYSICAL_EVIDENCE": "Evidencia física",
    "REVIEWED": "Explicación revisada",
    "INSUFFICIENT": "Evidencia insuficiente",
    "CUSTODY_REVIEW": "Revisión de custodia",
}
EXPLANATION_REASON_CODES = (
    "TRANSFER_PENDING",
    "CONVERSION",
    "WASTE",
    "PRODUCTION",
    "SALE",
    "STOCK_ELSEWHERE",
    "OTHER",
)
REVIEW_REASON_CODES = {
    ProductInventoryAuditEvent.Action.APPROVE: "REVIEWED",
    ProductInventoryAuditEvent.Action.REJECT: "INSUFFICIENT",
}


def _require_report_access(request: HttpRequest) -> None:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")


def _has_global_custody_access(user) -> bool:
    return bool(
        user.is_superuser
        or get_module_access(user, "reportes") == ACCESS_MANAGE
    )


def _require_case_custody(user, case: ProductInventoryAuditCase) -> None:
    if _has_global_custody_access(user):
        return
    erp_branch_id = case.branch.erp_branch_id
    profile = getattr(user, "userprofile", None)
    if erp_branch_id is None or profile is None or profile.sucursal_id != erp_branch_id:
        raise PermissionDenied(
            "El caso pertenece a una ubicación fuera de tu custodia autorizada."
        )


def _wants_json(request: HttpRequest) -> bool:
    return (
        "application/json" in request.headers.get("Accept", "")
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    )


def _wants_html(request: HttpRequest) -> bool:
    return "text/html" in request.headers.get("Accept", "")


def _possible_cause(case: ProductInventoryAuditCase) -> str:
    issue_codes = set(case.issue_codes if isinstance(case.issue_codes, list) else [])
    if "TRANSFER_QUANTITY_MISMATCH" in issue_codes:
        return "Transferencia por conciliar"
    if issue_codes.intersection(
        {"MISSING_CONVERSION_ORIGIN", "CONVERSION_ORIGIN_UNRESOLVED"}
    ):
        return "Origen de conversión por identificar"
    if "MISSING_CONVERSION_DESTINATION" in issue_codes:
        return "Destino de conversión por identificar"
    if "CONVERSION_EQUIVALENCE_MISMATCH" in issue_codes:
        return "La conversión no coincide con la equivalencia aprobada"
    if "NON_DERIVED_CONVERSION" in issue_codes:
        return "Conversión sin equivalencia aprobada para este producto"
    if case.movement_status == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE:
        return "Falta evidencia de una fuente del mes"
    if case.movement_status == ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL:
        return "Hay una explicación esperando revisión"
    if case.movement_status == ProductInventoryAuditCase.MovementStatus.RESOLVED:
        return "Diferencia explicada y aprobada"
    if case.difference and (case.conversion_in or case.conversion_out):
        return "Conversión registrada; revisar trazabilidad"
    if case.difference and case.waste:
        return "Merma registrada; revisar fecha, cantidad y ubicación"
    if case.difference and (case.transfer_in or case.transfer_out):
        return "Transferencia por conciliar"
    if case.difference and case.production:
        return "Producción registrada; revisar fecha y ubicación"
    if case.difference > 0:
        return "Point reportó más producto que el saldo esperado"
    if case.difference < 0:
        return "Point reportó menos producto que el saldo esperado"
    return "Sin diferencia"


def _case_status_context(case: ProductInventoryAuditCase) -> dict[str, str]:
    return {
        "point": (
            "Cierre protegido"
            if case.point_closing_status == ProductInventoryAuditCase.PointClosingStatus.PROTECTED
            else "Cierre disponible"
        ),
        "movement": MOVEMENT_STATUS_LABELS.get(case.movement_status, "Por revisar"),
        "physical": case.get_physical_status_display(),
    }


def _format_evidence_date(value) -> str:
    if value is None:
        return "Sin fecha informada"
    if hasattr(value, "date"):
        if timezone.is_aware(value):
            value = timezone.localtime(value)
        value = value.date()
    return value.strftime("%d/%m/%Y")


def _evidence_row(
    *,
    source,
    source_id,
    date_value,
    location,
    quantity,
    actor,
    reference,
    source_quantity=None,
    source_quantity_label=None,
) -> dict[str, object]:
    return {
        "source": TRACE_SOURCE_LABELS[source],
        "source_id": source_id,
        "date": _format_evidence_date(date_value),
        "location": location,
        "quantity": quantity,
        "actor": actor or "No informado por Point",
        "reference": reference or f"Point #{source_id}",
        "source_quantity": source_quantity,
        "source_quantity_label": source_quantity_label,
        "quantity_is_text": isinstance(quantity, str),
        "available": True,
    }


def _missing_evidence_row(source: str, source_id: int) -> dict[str, object]:
    return {
        "source": TRACE_SOURCE_LABELS.get(source, "Movimiento"),
        "source_id": source_id,
        "available": False,
        "message": "Evidencia ya no disponible en la fuente",
    }


def _reason_label(reason_code: str) -> str:
    return REASON_LABELS.get(reason_code, "Causa registrada")


def _source_evidence_by_step(
    case: ProductInventoryAuditCase,
) -> tuple[
    dict[str, list[dict[str, object]]],
    list[dict[str, object]],
]:
    trace = case.source_trace if isinstance(case.source_trace, dict) else {}
    evidence = {field: [] for field, _label, _operator in BALANCE_SEQUENCE}
    undirected_evidence: list[dict[str, object]] = []

    def ids_for(source):
        values = trace.get(source, [])
        return [value for value in values if type(value) is int and value > 0]

    def impact_for(source, source_id):
        impacts = trace.get(f"{source}_impacts", {})
        if not isinstance(impacts, dict):
            return None
        raw_value = impacts.get(str(source_id), impacts.get(source_id))
        if raw_value is None:
            return None
        try:
            return Decimal(str(raw_value))
        except (ArithmeticError, ValueError):
            return None

    closing_ids = set(ids_for("opening") + ids_for("closing"))
    closings = {
        row.pk: row
        for row in PointHistoricalInventoryClosingLine.objects.filter(pk__in=closing_ids)
        .select_related("closing", "branch")
    }
    sales = {
        row.pk: row
        for row in PointDailySale.objects.filter(
            pk__in=ids_for("sales")
        ).select_related("branch")
    }
    production = {
        row.pk: row
        for row in PointProductionLine.objects.filter(
            pk__in=ids_for("production")
        ).select_related("branch")
    }
    waste = {
        row.pk: row
        for row in PointWasteLine.objects.filter(
            pk__in=ids_for("waste")
        ).select_related("branch")
    }
    transfer_ids = set(
        ids_for("transfers")
        + ids_for("transfer_in")
        + ids_for("transfer_out")
    )
    transfers = {
        row.pk: row
        for row in PointTransferLine.objects.filter(pk__in=transfer_ids).select_related(
            "origin_branch", "destination_branch"
        )
    }
    snapshot_transfer_ids = set(
        ids_for("open_transfer_snapshot_in")
        + ids_for("open_transfer_snapshot_out")
    )
    snapshot_transfers = {
        row.pk: row
        for row in PointOpenTransferSnapshotMember.objects.filter(
            pk__in=snapshot_transfer_ids
        ).select_related("snapshot")
    }
    conversion_ids = set(
        ids_for("conversions")
        + ids_for("conversion_in")
        + ids_for("conversion_out")
    )
    conversions = {
        row.pk: row
        for row in PointConversionLine.objects.filter(
            pk__in=conversion_ids
        ).select_related("branch")
    }
    source_steps = {
        "opening": "opening_point",
        "production": "production",
        "sales": "sales",
        "waste": "waste",
        "adjustments": "identified_adjustment",
        "closing": "point_closing",
    }

    for source in (
        "opening",
        "production",
        "sales",
        "waste",
        "adjustments",
        "closing",
    ):
        for source_id in ids_for(source):
            step = source_steps.get(source)
            if source in {"opening", "closing"}:
                row = closings.get(source_id)
                if row:
                    evidence[step].append(
                        _evidence_row(
                            source=source,
                            source_id=source_id,
                            date_value=row.closing.operational_date,
                            location=row.branch.name,
                            quantity=row.stock,
                            actor=(row.evidence or {}).get("actor"),
                            reference=f"Point #{source_id}",
                        )
                    )
                    continue
            elif source == "sales":
                row = sales.get(source_id)
                if row:
                    evidence[step].append(
                        _evidence_row(
                            source=source,
                            source_id=source_id,
                            date_value=row.sale_date,
                            location=row.branch.name,
                            quantity=row.quantity,
                            actor=None,
                            reference=f"Point #{source_id}",
                        )
                    )
                    continue
            elif source == "production":
                row = production.get(source_id)
                if row:
                    evidence[step].append(
                        _evidence_row(
                            source=source,
                            source_id=source_id,
                            date_value=row.production_date,
                            location=row.branch.name,
                            quantity=row.produced_quantity,
                            actor=row.responsible,
                            reference=row.production_external_id or f"Point #{source_id}",
                        )
                    )
                    continue
            elif source == "waste":
                row = waste.get(source_id)
                if row:
                    evidence[step].append(
                        _evidence_row(
                            source=source,
                            source_id=source_id,
                            date_value=row.movement_at,
                            location=row.branch.name,
                            quantity=row.quantity,
                            actor=row.responsible,
                            reference=row.movement_external_id or f"Point #{source_id}",
                        )
                    )
                    continue
            if step is not None:
                evidence[step].append(_missing_evidence_row(source, source_id))

    for trace_bucket, step in (
        ("transfer_in", "transfer_in"),
        ("transfer_out", "transfer_out"),
    ):
        incoming = step == "transfer_in"
        for source_id in ids_for(trace_bucket):
            row = transfers.get(source_id)
            if row is None:
                evidence[step].append(_missing_evidence_row("transfers", source_id))
                continue
            evidence[step].append(
                _evidence_row(
                    source="transfers",
                    source_id=source_id,
                    date_value=(row.received_at if incoming else row.sent_at)
                    or row.registered_at,
                    location=(
                        f"{row.origin_branch.name} → {row.destination_branch.name}"
                    ),
                    quantity=(
                        row.received_quantity if incoming else row.sent_quantity
                    ),
                    actor=(row.received_by if incoming else row.sent_by)
                    or row.requested_by,
                    reference=row.transfer_external_id or f"Point #{source_id}",
                )
            )

    for trace_bucket, step in (
        ("open_transfer_snapshot_in", "transfer_in"),
        ("open_transfer_snapshot_out", "transfer_out"),
    ):
        incoming = step == "transfer_in"
        for source_id in ids_for(trace_bucket):
            row = snapshot_transfers.get(source_id)
            if row is None:
                evidence[step].append(_missing_evidence_row("transfers", source_id))
                continue
            evidence[step].append(
                _evidence_row(
                    source="transfers",
                    source_id=source_id,
                    date_value=(row.received_at if incoming else row.sent_at)
                    or row.registered_at,
                    location=(
                        f"{row.origin_branch_name} → {row.destination_branch_name}"
                    ),
                    quantity=(
                        row.received_quantity if incoming else row.sent_quantity
                    ),
                    actor=(row.received_by if incoming else row.sent_by)
                    or row.requested_by,
                    reference=(
                        f"{row.transfer_external_id} · "
                        f"cierre inmutable #{row.snapshot_id}"
                    ),
                )
            )

    for trace_bucket, step in (
        ("conversion_in", "conversion_in"),
        ("conversion_out", "conversion_out"),
    ):
        for source_id in ids_for(trace_bucket):
            row = conversions.get(source_id)
            if row is None:
                evidence[step].append(_missing_evidence_row("conversions", source_id))
                continue
            applied_impact = impact_for(trace_bucket, source_id)
            evidence[step].append(
                _evidence_row(
                    source="conversions",
                    source_id=source_id,
                    date_value=row.movement_at,
                    location=row.branch.name,
                    quantity=(
                        applied_impact
                        if applied_impact is not None
                        else "Impacto no conservado"
                    ),
                    actor=(row.raw_payload or {}).get("responsable"),
                    reference=row.movement_external_id or f"Point #{source_id}",
                    source_quantity=(
                        row.quantity if trace_bucket == "conversion_out" else None
                    ),
                    source_quantity_label=(
                        "Cantidad registrada en Point (producto destino)"
                        if trace_bucket == "conversion_out"
                        else None
                    ),
                )
            )

    if "transfer_in" not in trace and "transfer_out" not in trace:
        for source_id in ids_for("transfers"):
            row = transfers.get(source_id)
            if row is None:
                undirected_evidence.append(
                    _missing_evidence_row("transfers", source_id)
                )
                continue
            undirected_evidence.append(
                _evidence_row(
                    source="transfers",
                    source_id=source_id,
                    date_value=row.received_at or row.sent_at or row.registered_at,
                    location=f"{row.origin_branch.name} → {row.destination_branch.name}",
                    quantity=(
                        f"Enviada {row.sent_quantity} · "
                        f"recibida {row.received_quantity}"
                    ),
                    actor=row.received_by or row.sent_by or row.requested_by,
                    reference=row.transfer_external_id or f"Point #{source_id}",
                )
            )

    if "conversion_in" not in trace and "conversion_out" not in trace:
        for source_id in ids_for("conversions"):
            row = conversions.get(source_id)
            if row is None:
                undirected_evidence.append(
                    _missing_evidence_row("conversions", source_id)
                )
                continue
            undirected_evidence.append(
                _evidence_row(
                    source="conversions",
                    source_id=source_id,
                    date_value=row.movement_at,
                    location=row.branch.name,
                    quantity=row.quantity,
                    actor=(row.raw_payload or {}).get("responsable"),
                    reference=row.movement_external_id or f"Point #{source_id}",
                )
            )

    return evidence, undirected_evidence


def _parse_month(raw_month: str) -> date:
    try:
        parsed = date.fromisoformat(f"{raw_month}-01")
    except (TypeError, ValueError):
        raise ValueError("El mes debe tener formato AAAA-MM.") from None
    if parsed.strftime("%Y-%m") != raw_month:
        raise ValueError("El mes debe tener formato AAAA-MM.")
    return parsed


def _case_payload(case: ProductInventoryAuditCase) -> dict[str, object]:
    return {
        "id": case.pk,
        "month": case.month.strftime("%Y-%m"),
        "branch": {"id": case.branch_id, "name": case.branch.name},
        "product": {
            "id": case.product_id,
            "sku": case.product.sku,
            "name": case.product.name,
        },
        "movement_status": case.movement_status,
        "point_closing_status": case.point_closing_status,
        "physical_status": case.physical_status,
        "difference": str(case.difference),
    }


def _case_fragment(case: ProductInventoryAuditCase) -> str:
    anchor = f"inventory-audit-case-{case.pk}"
    status_label = MOVEMENT_STATUS_LABELS.get(case.movement_status, "Por revisar")
    return (
        f'<article id="{anchor}" data-branch-id="{case.branch_id}" '
        f'data-movement-status="{escape(case.movement_status)}">'
        f"<strong>{escape(case.product.name)}</strong> · "
        f"{escape(case.branch.name)} · "
        f'<span data-audit-status>{escape(status_label)}</span>'
        "</article>"
    )


def _case_url(case: ProductInventoryAuditCase) -> str:
    return (
        f"{reverse('reportes:inventory_audit_case', args=[case.pk])}"
        f"#inventory-audit-case-{case.pk}"
    )


def _action_response(
    request: HttpRequest,
    *,
    case: ProductInventoryAuditCase,
    ok: bool,
    message: str,
    status: int = 200,
    fields: dict[str, str] | None = None,
) -> HttpResponse:
    target = f"#inventory-audit-case-{case.pk}"
    if _wants_json(request):
        payload: dict[str, object] = {
            "ok": ok,
            "toast": {
                "type": "success" if ok else "error",
                "message": message,
                "persistent": not ok,
            },
            "target": target,
            "html": (
                _case_fragment(case)
                if ok
                else _action_form_fragment(
                    request,
                    case=case,
                    message=message,
                    fields=fields or {},
                )
            ),
        }
        if fields is not None:
            payload["fields"] = fields
        if ok:
            payload["redirect"] = _case_url(case)
            payload["reload"] = True
        return JsonResponse(payload, status=status)

    if ok:
        messages.success(request, message)
        return redirect(_case_url(case))

    return HttpResponse(
        _action_form_fragment(
            request,
            case=case,
            message=message,
            fields=fields or {},
        ),
        status=status,
    )


def _action_form_fragment(
    request: HttpRequest,
    *,
    case: ProductInventoryAuditCase,
    message: str,
    fields: dict[str, str],
) -> str:
    raw_reason_code = fields.get("reason_code", "")
    notes = escape(fields.get("notes", ""))
    action_url = escape(request.path)
    csrf_token = escape(get_token(request))
    evidence_field = ""
    is_explanation = (
        request.resolver_match
        and request.resolver_match.url_name == "inventory_audit_explain"
    )
    if is_explanation:
        evidence_field = (
            '<label>Evidencia opcional '
            '<input type="file" name="evidence" accept=".pdf,.jpg,.jpeg,.png,.webp">'
            "</label>"
        )
        if raw_reason_code and raw_reason_code not in EXPLANATION_REASON_CODES:
            options = '<option value="" selected>Causa registrada</option>'
        else:
            options = '<option value="">Selecciona una causa</option>'
        options += "".join(
            f'<option value="{code}"'
            f'{" selected" if raw_reason_code == code else ""}>'
            f"{escape(REASON_LABELS[code])}</option>"
            for code in EXPLANATION_REASON_CODES
        )
        reason_field = f'<select name="reason_code" required>{options}</select>'
    else:
        safe_review_code = (
            raw_reason_code if raw_reason_code in {"REVIEWED", "INSUFFICIENT"} else ""
        )
        reason_field = (
            '<select name="reason_code" required>'
            f'<option value="{escape(safe_review_code)}" selected>'
            f"{escape(_reason_label(safe_review_code))}</option></select>"
        )
    return (
        f'<article id="inventory-audit-case-{case.pk}">'
        '<section id="inventory-audit-action-error" role="alert">'
        f"<p>{escape(message)}</p>"
        f'<form method="post" action="{action_url}" enctype="multipart/form-data" '
        'data-async-action>'
        f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}">'
        '<label>Causa '
        f"{reason_field}</label>"
        '<label>Notas '
        f'<textarea name="notes" maxlength="{MAX_NOTES_LENGTH}">{notes}</textarea>'
        "</label>"
        f"{evidence_field}"
        '<button type="submit">Reintentar</button>'
        "</form></section></article>"
    )


def _locked_case(pk: int) -> ProductInventoryAuditCase:
    try:
        return (
            ProductInventoryAuditCase.objects.select_for_update()
            .select_related("branch", "product", "run")
            .get(pk=pk)
        )
    except ProductInventoryAuditCase.DoesNotExist:
        raise Http404("Caso de auditoría no encontrado.") from None


def _latest_explanation(
    case: ProductInventoryAuditCase,
) -> ProductInventoryAuditEvent | None:
    prefetched = getattr(case, "prefetched_events", None)
    latest = prefetched[-1] if prefetched else None
    if prefetched is None:
        latest = case.events.order_by("-created_at", "-id").first()
    if latest is None or latest.action != ProductInventoryAuditEvent.Action.EXPLAIN:
        return None
    return latest


def _posted_fields(request: HttpRequest) -> dict[str, str]:
    return {
        "reason_code": request.POST.get("reason_code", ""),
        "notes": request.POST.get("notes", ""),
    }


def _validate_evidence(uploaded) -> str | None:
    if uploaded is None:
        return None
    if uploaded.size > MAX_EVIDENCE_SIZE:
        raise EvidenceValidationError(
            [f"{Path(uploaded.name or 'Archivo').name}: excede el límite de 10 MB."]
        )
    validate_evidence_files([uploaded])
    return Path(uploaded.name).suffix.lower()


def _store_evidence(event, uploaded, suffix: str) -> dict[str, object]:
    digest = hashlib.sha256()
    for chunk in uploaded.chunks():
        digest.update(chunk)
    uploaded.seek(0)
    event.evidence.save(f"{uuid4()}{suffix}", uploaded, save=False)
    original_name = get_valid_filename(Path(uploaded.name).name) or "evidencia"
    return {
        "evidence_original_name": original_name,
        "evidence_sha256": digest.hexdigest(),
        "evidence_size": uploaded.size,
        "evidence_content_type": uploaded.content_type,
    }


@login_required
@require_GET
def dashboard(request: HttpRequest) -> HttpResponse:
    _require_report_access(request)
    render_html = _wants_html(request)
    raw_month = (request.GET.get("month") or "").strip()
    if raw_month:
        try:
            selected_month = _parse_month(raw_month)
        except ValueError as exc:
            if render_html:
                return HttpResponse(
                    "<h1>Mes no válido</h1>"
                    f"<p>{escape(str(exc))}</p>",
                    status=400,
                    content_type="text/html; charset=utf-8",
                )
            return JsonResponse({"ok": False, "error": str(exc)}, status=400)
        run = ProductInventoryAuditRun.objects.filter(month=selected_month).first()
    else:
        run = ProductInventoryAuditRun.objects.order_by("-month").first()

    cases = []
    branches = []
    selected_branch = (request.GET.get("branch") or "").strip()
    selected_status = (request.GET.get("status") or "").strip()
    selected_tab = (request.GET.get("tab") or "exceptions").strip()
    if selected_tab not in {"exceptions", "balanced"}:
        selected_tab = "exceptions"
    kpis = {"balanced": 0, "pending": 0, "pending_approval": 0}
    tab_counts = {"exceptions": 0, "balanced": 0}
    page_obj = None
    result_count = 0
    page_size = 100 if request.GET.get("page_size") == "100" else 50
    if run is not None:
        branch_aliases, _branch_objects = canonical_point_branch_identity()
        month_cases = ProductInventoryAuditCase.objects.filter(
            run=run,
            month=run.month,
            branch_id__in=set(branch_aliases.values()),
        )
        branches = list(
            month_cases.order_by("branch__name")
            .values("branch_id", "branch__name")
            .distinct()
        )
        if selected_branch:
            try:
                month_cases = month_cases.filter(branch_id=int(selected_branch))
            except ValueError:
                selected_branch = ""
        totals = month_cases.aggregate(
            balanced=models.Count(
                "id",
                filter=models.Q(
                    movement_status=ProductInventoryAuditCase.MovementStatus.BALANCED
                ),
            ),
            pending=models.Count(
                "id",
                filter=models.Q(
                    movement_status__in=(
                        ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
                        ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
                    )
                ),
            ),
            pending_approval=models.Count(
                "id",
                filter=models.Q(
                    movement_status=ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
                ),
            ),
        )
        kpis = {name: int(value or 0) for name, value in totals.items()}
        case_queryset = month_cases
        if selected_status in ProductInventoryAuditCase.MovementStatus.values:
            case_queryset = case_queryset.filter(movement_status=selected_status)
        elif selected_status:
            selected_status = ""
        tab_totals = case_queryset.aggregate(
            exceptions=models.Count(
                "id", filter=models.Q(movement_status__in=EXCEPTION_STATUSES)
            ),
            balanced=models.Count(
                "id", filter=models.Q(movement_status__in=RECONCILED_STATUSES)
            ),
        )
        tab_counts = {name: int(value or 0) for name, value in tab_totals.items()}
        if render_html or "tab" in request.GET:
            case_queryset = case_queryset.filter(
                movement_status__in=(
                    EXCEPTION_STATUSES
                    if selected_tab == "exceptions"
                    else RECONCILED_STATUSES
                )
            )
        ordered_cases = case_queryset.select_related("branch", "product").order_by(
            models.Case(
                models.When(
                    movement_status=ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
                    then=0,
                ),
                models.When(
                    movement_status=ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
                    then=1,
                ),
                models.When(
                    movement_status=ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
                    then=2,
                ),
                models.When(
                    movement_status=ProductInventoryAuditCase.MovementStatus.RESOLVED,
                    then=3,
                ),
                default=4,
                output_field=models.IntegerField(),
            ),
            "branch__name",
            "product__name",
            "id",
        )
        paginator = Paginator(ordered_cases, page_size)
        page_obj = paginator.get_page(request.GET.get("page"))
        result_count = paginator.count
        cases = list(page_obj.object_list)
        for case in cases:
            case.ui_possible_cause = _possible_cause(case)
            case.ui_movement_status = MOVEMENT_STATUS_LABELS.get(
                case.movement_status, "Por revisar"
            )

    if render_html:
        pagination_params = request.GET.copy()
        pagination_params.pop("page", None)
        return render(
            request,
            "reportes/auditoria_inventario.html",
            {
                "run": run,
                "cases": cases,
                "branches": branches,
                "selected_month": run.month.strftime("%Y-%m") if run else raw_month,
                "selected_branch": selected_branch,
                "selected_status": selected_status,
                "selected_tab": selected_tab,
                "status_options": ProductInventoryAuditCase.MovementStatus.choices,
                "kpis": kpis,
                "tab_counts": tab_counts,
                "page_obj": page_obj,
                "page_size": page_size,
                "result_count": result_count,
                "pagination_query": pagination_params.urlencode(),
            },
        )
    return JsonResponse(
        {
            "ok": True,
            "run": (
                {
                    "id": run.pk,
                    "month": run.month.strftime("%Y-%m"),
                    "status": run.status,
                }
                if run is not None
                else None
            ),
            "cases": [_case_payload(case) for case in cases],
            "pagination": {
                "page": page_obj.number if page_obj else 1,
                "pages": page_obj.paginator.num_pages if page_obj else 0,
                "total": result_count,
                "page_size": page_size,
            },
        }
    )


@login_required
@require_GET
def case_detail(request: HttpRequest, pk: int) -> HttpResponse:
    _require_report_access(request)
    try:
        event_queryset = ProductInventoryAuditEvent.objects.select_related(
            "actor", "related_event", "related_event__actor"
        ).order_by("created_at", "id")
        case = (
            ProductInventoryAuditCase.objects.select_related("branch", "product", "run")
            .get(pk=pk)
        )
    except ProductInventoryAuditCase.DoesNotExist:
        raise Http404("Caso de auditoría no encontrado.") from None
    related_branch_ids = [case.branch_id]
    if case.branch.erp_branch_id is not None:
        related_branch_ids = list(
            case.branch.__class__.objects.filter(
                erp_branch_id=case.branch.erp_branch_id
            ).values_list("id", flat=True)
        )
    related_events = list(
        event_queryset.filter(
            case__month=case.month,
            case__product_id=case.product_id,
            case__branch_id__in=related_branch_ids,
        )
    )
    case.prefetched_events = [
        event for event in related_events if event.case_id == case.id
    ]
    payload = _case_payload(case)
    payload["events"] = [
        {
            "id": event.pk,
            "action": event.action,
            "reason_code": event.reason_code,
            "notes": event.notes,
            "actor_id": event.actor_id,
            "related_event_id": event.related_event_id,
            "created_at": event.created_at.isoformat(),
            "evidence_download_url": (
                reverse(
                    "reportes:inventory_audit_evidence",
                    args=[event.case_id, event.pk],
                )
                if event.evidence
                else None
            ),
        }
        for event in related_events
    ]
    if _wants_html(request):
        has_custody = True
        try:
            _require_case_custody(request.user, case)
        except PermissionDenied:
            has_custody = False
        latest_explanation = _latest_explanation(case)
        source_evidence, undirected_evidence = _source_evidence_by_step(case)
        events = related_events
        for event in events:
            event.ui_reason_label = _reason_label(event.reason_code)
        if latest_explanation is not None:
            latest_explanation.ui_reason_label = _reason_label(
                latest_explanation.reason_code
            )
        return render(
            request,
            "reportes/auditoria_inventario_caso.html",
            {
                "case": case,
                "status": _case_status_context(case),
                "possible_cause": _possible_cause(case),
                "balance_steps": [
                    {
                        "number": index,
                        "field": field,
                        "label": label,
                        "operator": operator,
                        "value": getattr(case, field),
                        "evidence": source_evidence[field],
                    }
                    for index, (field, label, operator) in enumerate(
                        BALANCE_SEQUENCE, start=1
                    )
                ],
                "events": events,
                "undirected_evidence": undirected_evidence,
                "latest_explanation": latest_explanation,
                "can_explain": (
                    has_custody
                    and request.user.has_perm(
                        "reportes.change_productinventoryauditcase"
                    )
                    and case.movement_status
                    == ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION
                ),
                "can_review": (
                    has_custody
                    and request.user.has_perm(
                        "reportes.approve_product_inventory_audit"
                    )
                    and case.movement_status
                    == ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
                    and latest_explanation is not None
                    and latest_explanation.actor_id != request.user.pk
                ),
            },
        )
    return JsonResponse({"ok": True, "case": payload})


@login_required
@require_POST
def explain_case(request: HttpRequest, pk: int) -> HttpResponse:
    _require_report_access(request)
    if not request.user.has_perm("reportes.change_productinventoryauditcase"):
        raise PermissionDenied("No tienes permisos para explicar este caso.")

    fields = _posted_fields(request)
    reason_code = fields["reason_code"].strip()
    notes = fields["notes"].strip()
    event = None
    try:
        with transaction.atomic():
            case = _locked_case(pk)
            _require_case_custody(request.user, case)
            if (
                reason_code not in EXPLANATION_REASON_CODES
                or not notes
                or len(notes) > MAX_NOTES_LENGTH
            ):
                return _action_response(
                    request,
                    case=case,
                    ok=False,
                    message=(
                        "Indica una causa y una explicación de hasta "
                        f"{MAX_NOTES_LENGTH} caracteres antes de guardar."
                    ),
                    status=400,
                    fields=fields,
                )
            if (
                case.movement_status
                != ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION
            ):
                return _action_response(
                    request,
                    case=case,
                    ok=False,
                    message="El caso ya cambió de estado. Recarga antes de continuar.",
                    status=409,
                    fields=fields,
                )
            uploaded = request.FILES.get("evidence")
            try:
                suffix = _validate_evidence(uploaded)
            except EvidenceValidationError as exc:
                return _action_response(
                    request,
                    case=case,
                    ok=False,
                    message=str(exc),
                    status=400,
                    fields=fields,
                )
            event = ProductInventoryAuditEvent(
                case=case,
                action=ProductInventoryAuditEvent.Action.EXPLAIN,
                reason_code=reason_code,
                notes=notes,
                actor=request.user,
                metadata={"custody_branch_id": case.branch_id},
            )
            if uploaded is not None and suffix is not None:
                event.metadata.update(_store_evidence(event, uploaded, suffix))
            event.save()
            case.movement_status = ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
            case.save(update_fields=["movement_status", "updated_at"])
            return _action_response(
                request,
                case=case,
                ok=True,
                message="La explicación quedó pendiente de aprobación.",
            )
    except Exception:
        if event is not None and event.evidence:
            event.evidence.storage.delete(event.evidence.name)
        raise


def _review_case(
    request: HttpRequest,
    *,
    pk: int,
    action: str,
) -> HttpResponse:
    _require_report_access(request)
    if not request.user.has_perm("reportes.approve_product_inventory_audit"):
        raise PermissionDenied("No tienes permisos para revisar este caso.")

    fields = _posted_fields(request)
    reason_code = REVIEW_REASON_CODES[action]
    fields["reason_code"] = reason_code
    notes = fields["notes"].strip()
    with transaction.atomic():
        case = _locked_case(pk)
        _require_case_custody(request.user, case)
        if len(notes) > MAX_NOTES_LENGTH or (
            action == ProductInventoryAuditEvent.Action.REJECT and not notes
        ):
            return _action_response(
                request,
                case=case,
                ok=False,
                message=(
                    "Indica el motivo del rechazo antes de continuar."
                    if action == ProductInventoryAuditEvent.Action.REJECT and not notes
                    else "El comentario no puede exceder el límite permitido."
                ),
                status=400,
                fields=fields,
            )
        if (
            case.movement_status
            != ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
        ):
            return _action_response(
                request,
                case=case,
                ok=False,
                message="El caso ya cambió de estado. Recarga antes de continuar.",
                status=409,
                fields=fields,
            )
        explanation = _latest_explanation(case)
        if explanation is None or explanation.actor_id is None:
            return _action_response(
                request,
                case=case,
                ok=False,
                message="El caso no tiene una explicación vigente para revisar.",
                status=409,
                fields=fields,
            )
        if explanation.actor_id == request.user.pk:
            return _action_response(
                request,
                case=case,
                ok=False,
                message="La persona que explicó el caso no puede aprobarlo ni rechazarlo.",
                status=409,
                fields=fields,
            )

        ProductInventoryAuditEvent.objects.create(
            case=case,
            action=action,
            reason_code=reason_code,
            notes=notes,
            actor=request.user,
            related_event=explanation,
            metadata={"custody_branch_id": case.branch_id},
        )
        if action == ProductInventoryAuditEvent.Action.APPROVE:
            case.movement_status = ProductInventoryAuditCase.MovementStatus.RESOLVED
            success_message = "La explicación fue aprobada y el caso quedó resuelto."
        else:
            case.movement_status = ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION
            success_message = "La explicación fue rechazada; el caso requiere una nueva explicación."
        case.save(update_fields=["movement_status", "updated_at"])
        return _action_response(
            request,
            case=case,
            ok=True,
            message=success_message,
        )


@login_required
@require_POST
def approve_case(request: HttpRequest, pk: int) -> HttpResponse:
    return _review_case(
        request,
        pk=pk,
        action=ProductInventoryAuditEvent.Action.APPROVE,
    )


@login_required
@require_POST
def reject_case(request: HttpRequest, pk: int) -> HttpResponse:
    return _review_case(
        request,
        pk=pk,
        action=ProductInventoryAuditEvent.Action.REJECT,
    )


@login_required
@require_GET
def download_evidence(request: HttpRequest, pk: int, event_id: int) -> HttpResponse:
    _require_report_access(request)
    try:
        case = ProductInventoryAuditCase.objects.select_related("branch").get(pk=pk)
    except ProductInventoryAuditCase.DoesNotExist:
        raise Http404("Caso de auditoría no encontrado.") from None
    _require_case_custody(request.user, case)
    try:
        event = case.events.get(
            pk=event_id,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
        )
    except ProductInventoryAuditEvent.DoesNotExist:
        raise Http404("Evidencia no encontrada.") from None
    name = event.evidence.name if event.evidence else ""
    if not name or Path(name).name != name:
        raise Http404("Evidencia no encontrada.")
    storage = event.evidence.storage
    if not storage.exists(name):
        raise Http404("Archivo no disponible.")
    filename = event.metadata.get("evidence_original_name") or "evidencia"
    filename = get_valid_filename(Path(str(filename)).name) or "evidencia"
    response = FileResponse(
        event.evidence.open("rb"),
        as_attachment=True,
        filename=filename,
    )
    response["X-Content-Type-Options"] = "nosniff"
    response["Cache-Control"] = "private, no-store"
    return response
