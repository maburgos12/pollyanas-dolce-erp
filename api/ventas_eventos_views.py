from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.db import transaction
from django.db.models import Sum
from django.shortcuts import get_object_or_404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status

from core.access import ROLE_ADMIN, ROLE_COMPRAS, ROLE_DG, ROLE_PRODUCCION, ROLE_VENTAS, ROLE_LECTURA, has_any_role
from core.branch_catalog import EXCLUDED_BRANCH_CODES, eligible_sales_event_branch_qs
from core.models import Sucursal
from django.utils import timezone
from recetas.models import Receta
from ventas.models import (
    EventoVenta,
    EventoVentaApproval,
    EventoVentaAdjustment,
    EventoVentaCapacityRule,
    EventoVentaExecutionMetric,
    EventoVentaForecast,
    EventoVentaFinancial,
    EventoVentaInputRequirement,
    EventoVentaProductionPlan,
    EventoVentaProductionLine,
    EventoVentaPurchaseRequirement,
    EventoVentaProducto,
    EventoVentaSucursal,
)
from ventas.services.audit import log_evento_change
from ventas.services.financials import build_financials
from ventas.services.forecasting import generate_event_forecast
from ventas.services.postmortem import build_postmortem
from ventas.services.production import generate_production_plan
from ventas.services.requirements import build_input_requirements, build_purchase_requirements


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _can_view(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_VENTAS, ROLE_PRODUCCION, ROLE_COMPRAS, ROLE_LECTURA)


def _can_manage(user) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_VENTAS)


def _can_approve(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN)


def _event_payload(event: EventoVenta) -> dict:
    return {
        "id": event.id,
        "code": event.code,
        "name": event.name,
        "event_type": event.event_type,
        "main_date": event.main_date,
        "analysis_start_date": event.analysis_start_date,
        "analysis_end_date": event.analysis_end_date,
        "status": event.status,
        "priority": event.priority,
        "scenario_focus": event.scenario_focus,
        "version": event.version,
    }


class SalesEventListCreateView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)

        status_filter = (request.GET.get("status") or "").strip().upper()
        q = (request.GET.get("q") or "").strip()
        event_type = (request.GET.get("event_type") or "").strip()

        qs = EventoVenta.objects.all()
        if status_filter:
            qs = qs.filter(status=status_filter)
        if event_type:
            qs = qs.filter(event_type__icontains=event_type)
        if q:
            qs = qs.filter(name__icontains=q)

        data = [_event_payload(ev) for ev in qs[:200]]
        return Response({"results": data})

    def post(self, request):
        if not _can_manage(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)

        payload = request.data or {}
        name = str(payload.get("name") or "").strip()
        if not name:
            return Response({"detail": "El nombre es requerido."}, status=status.HTTP_400_BAD_REQUEST)

        main_date = _parse_iso_date(payload.get("main_date"))
        analysis_start = _parse_iso_date(payload.get("analysis_start_date")) or main_date
        analysis_end = _parse_iso_date(payload.get("analysis_end_date")) or main_date
        if not main_date:
            return Response({"detail": "main_date es requerido."}, status=status.HTTP_400_BAD_REQUEST)

        branch_ids = payload.get("branch_ids") or []
        product_ids = payload.get("product_ids") or []

        with transaction.atomic():
            event = EventoVenta.objects.create(
                name=name,
                event_type=str(payload.get("event_type") or "").strip(),
                main_date=main_date,
                analysis_start_date=analysis_start,
                analysis_end_date=analysis_end,
                objective_type=str(payload.get("objective_type") or "").strip(),
                objective_notes=str(payload.get("objective_notes") or "").strip(),
                approval_deadline=_parse_iso_date(payload.get("approval_deadline")),
                priority=str(payload.get("priority") or EventoVenta.PRIORIDAD_MEDIA),
                scenario_focus=str(payload.get("scenario_focus") or EventoVenta.SCENARIO_BASE),
                conservative_pct=Decimal(str(payload.get("conservative_pct") or "0.90")),
                aggressive_pct=Decimal(str(payload.get("aggressive_pct") or "1.10")),
                created_by=request.user,
            )

            for branch in eligible_sales_event_branch_qs().filter(id__in=branch_ids):
                EventoVentaSucursal.objects.create(sales_event=event, branch=branch)
            for product in Receta.objects.filter(id__in=product_ids):
                EventoVentaProducto.objects.create(sales_event=event, product=product)

            log_evento_change(event, "EventoVenta", event.id, "CREATE", new_data={"name": event.name}, actor=request.user)

        return Response(_event_payload(event), status=status.HTTP_201_CREATED)


class SalesEventDetailView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)

        event = get_object_or_404(EventoVenta, pk=event_id)
        data = _event_payload(event)
        data["branches"] = list(
            EventoVentaSucursal.objects.filter(
                sales_event=event,
                is_active=True,
                branch__activa=True,
            )
            .exclude(branch__codigo__in=EXCLUDED_BRANCH_CODES)
            .values_list("branch_id", flat=True)
        )
        data["products"] = list(EventoVentaProducto.objects.filter(sales_event=event, is_active=True).values_list("product_id", flat=True))
        data["capacity_rules"] = [
            {
                "id": row.id,
                "capacity_date": row.capacity_date,
                "product_id": row.product_id,
                "max_production_qty": row.max_production_qty,
                "notes": row.notes,
            }
            for row in EventoVentaCapacityRule.objects.filter(sales_event=event, is_active=True).order_by("capacity_date", "product_id", "id")
        ]
        return Response(data)

    def patch(self, request, event_id: int):
        if not _can_manage(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)

        event = get_object_or_404(EventoVenta, pk=event_id)
        payload = request.data or {}

        if "name" in payload:
            event.name = str(payload.get("name") or event.name).strip()
        if "event_type" in payload:
            event.event_type = str(payload.get("event_type") or event.event_type).strip()
        if "main_date" in payload:
            event.main_date = _parse_iso_date(payload.get("main_date")) or event.main_date
        if "analysis_start_date" in payload:
            event.analysis_start_date = _parse_iso_date(payload.get("analysis_start_date")) or event.analysis_start_date
        if "analysis_end_date" in payload:
            event.analysis_end_date = _parse_iso_date(payload.get("analysis_end_date")) or event.analysis_end_date
        if "objective_type" in payload:
            event.objective_type = str(payload.get("objective_type") or event.objective_type).strip()
        if "objective_notes" in payload:
            event.objective_notes = str(payload.get("objective_notes") or event.objective_notes).strip()
        if "approval_deadline" in payload:
            event.approval_deadline = _parse_iso_date(payload.get("approval_deadline"))
        if "priority" in payload:
            event.priority = str(payload.get("priority") or event.priority)
        if "scenario_focus" in payload:
            event.scenario_focus = str(payload.get("scenario_focus") or event.scenario_focus)
        if "conservative_pct" in payload:
            event.conservative_pct = Decimal(str(payload.get("conservative_pct") or event.conservative_pct))
        if "aggressive_pct" in payload:
            event.aggressive_pct = Decimal(str(payload.get("aggressive_pct") or event.aggressive_pct))
        event.save()

        if "branch_ids" in payload:
            EventoVentaSucursal.objects.filter(sales_event=event).delete()
            for branch in eligible_sales_event_branch_qs().filter(id__in=payload.get("branch_ids") or []):
                EventoVentaSucursal.objects.create(sales_event=event, branch=branch)
        if "product_ids" in payload:
            EventoVentaProducto.objects.filter(sales_event=event).delete()
            for product in Receta.objects.filter(id__in=payload.get("product_ids") or []):
                EventoVentaProducto.objects.create(sales_event=event, product=product)

        log_evento_change(event, "EventoVenta", event.id, "UPDATE", new_data={"name": event.name}, actor=request.user)
        return Response(_event_payload(event))


class SalesEventGenerateForecastView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not _can_manage(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        result = generate_event_forecast(event, request.user)
        return Response(result)


class SalesEventForecastListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        rows = EventoVentaForecast.objects.filter(sales_event_id=event_id).select_related("branch", "product")[:2000]
        data = [
            {
                "date": row.forecast_date,
                "branch": row.branch.codigo,
                "product": row.product.nombre,
                "base": row.base_demand,
                "uplift": row.event_uplift,
                "trend": row.trend_adjustment,
                "final": row.final_forecast,
                "confidence": row.confidence_score,
                "explanation": row.explanation_json,
            }
            for row in rows
        ]
        return Response({"results": data})


class SalesEventSubmitApprovalView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not _can_manage(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        EventoVentaApproval.objects.create(
            sales_event=event,
            approval_stage=EventoVentaApproval.STAGE_DIRECCION,
            role_required="DG",
        )
        event.status = EventoVenta.STATUS_PENDIENTE_DG
        event.save(update_fields=["status", "updated_at"])
        return Response({"status": event.status})


class SalesEventApproveView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not _can_approve(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        event.status = EventoVenta.STATUS_APROBADO
        event.approved_by = request.user
        event.approved_at = timezone.now()
        event.save(update_fields=["status", "approved_by", "approved_at", "updated_at"])
        return Response({"status": event.status})


class SalesEventRejectView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not _can_approve(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        event.status = EventoVenta.STATUS_RECHAZADO
        event.rejected_by = request.user
        event.rejected_at = timezone.now()
        event.save(update_fields=["status", "rejected_by", "rejected_at", "updated_at"])
        return Response({"status": event.status})


class SalesEventSendToProductionView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not has_any_role(request.user, ROLE_ADMIN, ROLE_PRODUCCION):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        result = generate_production_plan(event)
        return Response(result)


class SalesEventProductionPlanView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        rows = EventoVentaProductionLine.objects.filter(production_plan__sales_event_id=event_id).select_related("product")[:2000]
        data = [
            {
                "date": row.production_day,
                "product": row.product.nombre,
                "required": row.required_qty,
                "planned": row.planned_qty,
                "net": row.net_qty_to_produce,
                "stock": row.existing_finished_stock,
                "capacity_limit": row.capacity_limit_qty,
                "capacity_gap": row.capacity_gap_qty,
                "constraint_reason": row.constraint_reason,
            }
            for row in rows
        ]
        return Response({"results": data})


class SalesEventCapacityRulesView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        rows = EventoVentaCapacityRule.objects.filter(sales_event_id=event_id, is_active=True).select_related("product")[:500]
        data = [
            {
                "id": row.id,
                "capacity_date": row.capacity_date,
                "product_id": row.product_id,
                "product": row.product.nombre if row.product_id else "",
                "max_production_qty": row.max_production_qty,
                "notes": row.notes,
            }
            for row in rows
        ]
        return Response({"results": data})

    def post(self, request, event_id: int):
        if not has_any_role(request.user, ROLE_ADMIN, ROLE_PRODUCCION):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        payload = request.data or {}
        raw_limit = str(payload.get("max_production_qty") or "").strip()
        if not raw_limit:
            return Response({"detail": "max_production_qty es requerido."}, status=status.HTTP_400_BAD_REQUEST)
        rule = EventoVentaCapacityRule.objects.create(
            sales_event=event,
            capacity_date=_parse_iso_date(payload.get("capacity_date")),
            product_id=payload.get("product_id") or None,
            max_production_qty=Decimal(raw_limit),
            notes=str(payload.get("notes") or "").strip(),
        )
        log_evento_change(
            event,
            "EventoVentaCapacityRule",
            rule.id,
            "CREATE",
            new_data={
                "capacity_date": rule.capacity_date.isoformat() if rule.capacity_date else "",
                "product_id": rule.product_id,
                "max_production_qty": str(rule.max_production_qty),
                "notes": rule.notes,
            },
            actor=request.user,
        )
        return Response(
            {
                "id": rule.id,
                "capacity_date": rule.capacity_date,
                "product_id": rule.product_id,
                "max_production_qty": rule.max_production_qty,
                "notes": rule.notes,
            },
            status=status.HTTP_201_CREATED,
        )


class SalesEventCapacityRuleDetailView(APIView):
    permission_classes = [IsAuthenticated]

    def delete(self, request, event_id: int, rule_id: int):
        if not has_any_role(request.user, ROLE_ADMIN, ROLE_PRODUCCION):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        rule = get_object_or_404(EventoVentaCapacityRule, pk=rule_id, sales_event=event)
        old_data = {
            "capacity_date": rule.capacity_date.isoformat() if rule.capacity_date else "",
            "product_id": rule.product_id,
            "max_production_qty": str(rule.max_production_qty),
            "notes": rule.notes,
        }
        rule.delete()
        log_evento_change(event, "EventoVentaCapacityRule", rule_id, "DELETE", old_data=old_data, actor=request.user)
        return Response(status=status.HTTP_204_NO_CONTENT)


class SalesEventConfirmProductionView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not has_any_role(request.user, ROLE_ADMIN, ROLE_PRODUCCION):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        updated = EventoVentaProductionPlan.objects.filter(sales_event=event).update(
            status=EventoVentaProductionPlan.STATUS_CONFIRMADO,
            approved_by_production=request.user,
            approved_at=timezone.now(),
        )
        event.status = EventoVenta.STATUS_VALIDADO_PROD
        event.save(update_fields=["status", "updated_at"])
        return Response({"status": event.status, "updated_plans": updated})


class SalesEventAdjustmentsView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not _can_approve(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        payload = request.data or {}

        adjustment = EventoVentaAdjustment.objects.create(
            sales_event=event,
            branch_id=payload.get("branch_id"),
            product_id=payload.get("product_id"),
            field_name=str(payload.get("field_name") or "").strip(),
            old_value=str(payload.get("old_value") or ""),
            new_value=str(payload.get("new_value") or ""),
            adjustment_reason=str(payload.get("adjustment_reason") or ""),
            adjusted_by=request.user,
        )
        return Response({"id": adjustment.id})


class SalesEventInputRequirementsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        rows = EventoVentaInputRequirement.objects.filter(sales_event_id=event_id).select_related("input_item")[:2000]
        data = [
            {
                "input": row.input_item.nombre,
                "required": row.required_qty,
                "on_hand": row.on_hand_qty,
                "shortage": row.net_shortage_qty,
                "risk": row.risk_level,
                "required_by": row.required_by_date,
            }
            for row in rows
        ]
        return Response({"results": data})


class SalesEventSendToPurchasesView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not has_any_role(request.user, ROLE_ADMIN, ROLE_COMPRAS):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        build_input_requirements(event)
        result = build_purchase_requirements(event)
        return Response(result)


class SalesEventPurchaseRequirementsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        rows = EventoVentaPurchaseRequirement.objects.filter(sales_event_id=event_id).select_related("input_requirement__input_item")[:2000]
        data = [
            {
                "input": row.input_requirement.input_item.nombre,
                "qty": row.suggested_purchase_qty,
                "deadline": row.purchase_deadline,
                "cost": row.estimated_cost,
            }
            for row in rows
        ]
        return Response({"results": data})


class SalesEventFinancialSummaryView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        if not EventoVentaFinancial.objects.filter(sales_event_id=event_id).exists():
            build_financials(event)
        rows = EventoVentaFinancial.objects.filter(sales_event_id=event_id)
        data = [
            {
                "scenario": row.scenario,
                "sales": row.estimated_sales,
                "cogs": row.estimated_cogs,
                "profit": row.estimated_gross_profit,
                "margin": row.estimated_margin,
            }
            for row in rows
        ]
        return Response({"results": data})


class SalesEventExecutionDashboardView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        qs = EventoVentaForecast.objects.filter(sales_event_id=event_id)
        totals = qs.aggregate(total=Sum("final_forecast"))
        by_branch = (
            qs.values("branch__codigo")
            .annotate(total=Sum("final_forecast"))
            .order_by("-total")
        )
        return Response({
            "total_forecast": totals.get("total") or 0,
            "by_branch": list(by_branch),
        })


class SalesEventCloseView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, event_id: int):
        if not _can_manage(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        event.status = EventoVenta.STATUS_CERRADO
        event.save(update_fields=["status", "updated_at"])
        return Response({"status": event.status})


class SalesEventPostmortemView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, event_id: int):
        if not _can_view(request.user):
            return Response({"detail": "Sin permiso."}, status=status.HTTP_403_FORBIDDEN)
        event = get_object_or_404(EventoVenta, pk=event_id)
        result = build_postmortem(event)
        rows = list(
            EventoVentaExecutionMetric.objects.filter(sales_event=event)
            .select_related("branch", "product")
            .order_by("-metric_date")[:500]
        )
        result["rows"] = [
            {
                "date": row.metric_date,
                "branch": row.branch.codigo if row.branch_id else "",
                "product": row.product.nombre if row.product_id else "",
                "forecast_qty": row.forecast_qty,
                "actual_qty": row.actual_qty,
                "variance_qty": row.variance_qty,
                "actual_sales": row.actual_sales,
            }
            for row in rows
        ]
        return Response(result)
