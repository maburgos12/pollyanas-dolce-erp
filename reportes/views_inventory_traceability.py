from __future__ import annotations

import hashlib
from datetime import date
from html import escape
from pathlib import Path
from uuid import uuid4

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import FileResponse, Http404, HttpRequest, HttpResponse, JsonResponse
from django.middleware.csrf import get_token
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.text import get_valid_filename
from django.views.decorators.http import require_GET, require_POST

from core.access import ACCESS_MANAGE, can_view_reportes, get_module_access
from mantenimiento.evidence_validation import (
    EvidenceValidationError,
    validate_evidence_files,
)
from reportes.models import (
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)

MAX_EVIDENCE_SIZE = 10 * 1024 * 1024
MAX_NOTES_LENGTH = 4000


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
    return (
        f'<article id="{anchor}" data-branch-id="{case.branch_id}" '
        f'data-movement-status="{escape(case.movement_status)}">'
        f"<strong>{escape(case.product.name)}</strong> · "
        f"{escape(case.branch.name)} · "
        f'<span data-audit-status>{escape(case.movement_status)}</span>'
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
    reason_code = escape(fields.get("reason_code", ""))
    notes = escape(fields.get("notes", ""))
    action_url = escape(request.path)
    csrf_token = escape(get_token(request))
    evidence_field = ""
    if (
        request.resolver_match
        and request.resolver_match.url_name == "inventory_audit_explain"
    ):
        evidence_field = (
            '<label>Evidencia opcional '
            '<input type="file" name="evidence" accept=".pdf,.jpg,.jpeg,.png,.webp">'
            "</label>"
        )
    return (
        f'<article id="inventory-audit-case-{case.pk}">'
        '<section id="inventory-audit-action-error" role="alert">'
        f"<p>{escape(message)}</p>"
        f'<form method="post" action="{action_url}" enctype="multipart/form-data" '
        'data-async-action>'
        f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}">'
        '<label>Causa '
        f'<input name="reason_code" maxlength="80" value="{reason_code}"></label>'
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
    raw_month = (request.GET.get("month") or "").strip()
    if raw_month:
        try:
            selected_month = _parse_month(raw_month)
        except ValueError as exc:
            return JsonResponse({"ok": False, "error": str(exc)}, status=400)
        run = ProductInventoryAuditRun.objects.filter(month=selected_month).first()
    else:
        run = ProductInventoryAuditRun.objects.order_by("-month").first()

    cases = []
    if run is not None:
        cases = list(
            ProductInventoryAuditCase.objects.filter(run=run, month=run.month)
            .select_related("branch", "product")
            .order_by("branch__name", "product__name", "id")
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
        }
    )


@login_required
@require_GET
def case_detail(request: HttpRequest, pk: int) -> HttpResponse:
    _require_report_access(request)
    try:
        case = (
            ProductInventoryAuditCase.objects.select_related("branch", "product", "run")
            .prefetch_related("events")
            .get(pk=pk)
        )
    except ProductInventoryAuditCase.DoesNotExist:
        raise Http404("Caso de auditoría no encontrado.") from None
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
                    args=[case.pk, event.pk],
                )
                if event.evidence
                else None
            ),
        }
        for event in case.events.all()
    ]
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
                not reason_code
                or not notes
                or len(reason_code) > 80
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
    reason_code = fields["reason_code"].strip()
    notes = fields["notes"].strip()
    with transaction.atomic():
        case = _locked_case(pk)
        _require_case_custody(request.user, case)
        if (
            not reason_code
            or len(reason_code) > 80
            or len(notes) > MAX_NOTES_LENGTH
        ):
            return _action_response(
                request,
                case=case,
                ok=False,
                message="Indica una causa para registrar la revisión.",
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
