from __future__ import annotations

from datetime import date
from html import escape

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect
from django.urls import reverse
from django.views.decorators.http import require_GET, require_POST

from core.access import can_view_reportes
from reportes.models import (
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)


def _require_report_access(request: HttpRequest) -> None:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")


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
            "html": _case_fragment(case),
        }
        if fields is not None:
            payload["fields"] = fields
        return JsonResponse(payload, status=status)

    if ok:
        messages.success(request, message)
        return redirect(_case_url(case))

    preserved = fields or {}
    body = (
        f'<section id="inventory-audit-action-error" role="alert">'
        f"<p>{escape(message)}</p>"
        f'<input name="reason_code" value="{escape(preserved.get("reason_code", ""))}">'
        f'<textarea name="notes">{escape(preserved.get("notes", ""))}</textarea>'
        "</section>"
    )
    return HttpResponse(body, status=status)


def _locked_case(pk: int) -> ProductInventoryAuditCase:
    try:
        return (
            ProductInventoryAuditCase.objects.select_for_update()
            .select_related("branch", "product", "run")
            .get(pk=pk)
        )
    except ProductInventoryAuditCase.DoesNotExist:
        raise Http404("Caso de auditoría no encontrado.") from None


def _latest_explanation(case: ProductInventoryAuditCase) -> ProductInventoryAuditEvent | None:
    latest = case.events.select_related("actor").order_by("-created_at", "-id").first()
    if latest is None or latest.action != ProductInventoryAuditEvent.Action.EXPLAIN:
        return None
    return latest


def _posted_fields(request: HttpRequest) -> dict[str, str]:
    return {
        "reason_code": request.POST.get("reason_code", ""),
        "notes": request.POST.get("notes", ""),
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
            .prefetch_related("events__actor")
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
    with transaction.atomic():
        case = _locked_case(pk)
        if not reason_code or not notes or len(reason_code) > 80:
            return _action_response(
                request,
                case=case,
                ok=False,
                message="Indica una causa y una explicación antes de guardar.",
                status=400,
                fields=fields,
            )
        if case.movement_status != ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION:
            return _action_response(
                request,
                case=case,
                ok=False,
                message="El caso ya cambió de estado. Recarga antes de continuar.",
                status=409,
                fields=fields,
            )
        ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code=reason_code,
            notes=notes,
            evidence=request.FILES.get("evidence"),
            actor=request.user,
            metadata={"custody_branch_id": case.branch_id},
        )
        case.movement_status = ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
        case.save(update_fields=["movement_status", "updated_at"])
        return _action_response(
            request,
            case=case,
            ok=True,
            message="La explicación quedó pendiente de aprobación.",
        )


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
        if not reason_code or len(reason_code) > 80:
            return _action_response(
                request,
                case=case,
                ok=False,
                message="Indica una causa para registrar la revisión.",
                status=400,
                fields=fields,
            )
        if case.movement_status != ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL:
            return _action_response(
                request,
                case=case,
                ok=False,
                message="El caso ya cambió de estado. Recarga antes de continuar.",
                status=409,
                fields=fields,
            )
        explanation = _latest_explanation(case)
        if explanation is None:
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
