from __future__ import annotations

import logging
from datetime import date, timedelta
from zoneinfo import ZoneInfo
import re

from django.utils import timezone

from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import ROLE_ADMIN, ROLE_DG, ROLE_VENTAS, has_any_role
from horarios_especiales.models import SolicitudHorarioEspecial
from horarios_especiales.services.command_parser import build_preview_from_command
from horarios_especiales.services.execution import execute_request
from horarios_especiales.services.requests import (
    approve_request,
    cancel_request,
    create_request_from_text,
    validate_request,
)
from horarios_especiales.tasks import execute_special_hours_request_task

from .special_hours_serializers import SpecialHoursActionSerializer, SpecialHoursApprovalSerializer, SpecialHoursPreviewSerializer


def _can_manage_special_hours(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_VENTAS)


def _can_approve_special_hours(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN)


def _serialize_request(obj: SolicitudHorarioEspecial) -> dict:
    details = list(
        obj.details.select_related("sucursal").order_by("target_date", "sucursal__codigo").values(
            "id",
            "sucursal_id",
            "sucursal__codigo",
            "sucursal__nombre",
            "target_date",
            "closed_all_day",
            "time_windows_json",
            "execution_status",
            "validation_errors_json",
        )
    )
    return {
        "id": obj.id,
        "request_code": obj.request_code,
        "status": obj.status,
        "source_channel": obj.source_channel,
        "reason": obj.reason,
        "raw_command": obj.raw_command,
        "canonical_payload": obj.canonical_payload,
        "execution_summary_json": obj.execution_summary_json,
        "requested_by": getattr(obj.requested_by, "username", ""),
        "approved_by": getattr(obj.approved_by, "username", ""),
        "executed_by": getattr(obj.executed_by, "username", ""),
        "approved_at": obj.approved_at,
        "executed_at": obj.executed_at,
        "created_at": obj.created_at,
        "updated_at": obj.updated_at,
        "details": details,
    }


logger = logging.getLogger(__name__)


class PublicBranchHoursView(APIView):
    """Public business hours only; never fetch the website's regular schedule."""
    authentication_classes = []
    permission_classes = [AllowAny]

    def get(self, request):
        from horarios_especiales.services.effective_hours import read_effective_hours
        from core.models import sucursales_operativas

        today = timezone.now().astimezone(ZoneInfo("America/Mazatlan")).date()
        raw_date = request.query_params.get("target_date", today.isoformat())
        codes = request.query_params.get("branch_codes", "").split(",")
        try:
            target = date.fromisoformat(raw_date)
            if (raw_date != target.isoformat() or not today - timedelta(days=1) <= target <= today + timedelta(days=366)
                    or not 1 <= len(codes) <= 10 or len(set(codes)) != len(codes)
                    or any(not re.fullmatch(r"[A-Z][A-Z0-9_]{0,39}", code) for code in codes)):
                raise ValueError
            branches = list(sucursales_operativas(reference_date=target).filter(codigo__in=codes))
            if {branch.codigo for branch in branches} != set(codes):
                raise ValueError
            rows = []
            for branch in branches:
                payload = read_effective_hours(branch.codigo, target, include_regular=False)
                effective = payload["effective"]
                rows.append({"branch_code": branch.codigo, "branch_name": branch.nombre,
                    "target_date": raw_date, "timezone": "America/Mazatlan", "checked_at": payload["checked_at"],
                    "status": ("NO_OVERRIDE" if effective.get("reason") == "SPECIAL_HOURS_NOT_CONFIRMED" else effective["status"]),
                    "source": effective.get("source"), "revision": effective.get("revision"),
                    "closed_all_day": effective.get("closed_all_day"), "windows": effective["windows"]})
        except (ValueError, OverflowError):
            return Response({"detail": "Se requieren códigos exactos (máximo 10) y fecha ISO entre ayer y 366 días."}, status=400)
        return Response({"target_date": raw_date, "timezone": "America/Mazatlan", "branches": rows},
                        headers={"Cache-Control": "no-store"})


class SpecialHoursEffectiveView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        from horarios_especiales.services.effective_hours import read_effective_hours

        name = request.query_params.get("branch_name", "").strip()
        raw_date = request.query_params.get("target_date", "")
        try:
            if not name or len(name) > 120 or len(raw_date) != 10:
                raise ValueError("Invalid input")
            target_date = date.fromisoformat(raw_date)
            if target_date.isoformat() != raw_date:
                raise ValueError("Invalid date")
            payload = read_effective_hours(name, target_date)
        except ValueError:
            return Response({"detail": "Se requiere sucursal operativa única y fecha ISO válida."}, status=400)
        return Response(payload)


class SpecialHoursPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not _can_manage_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para gestionar horarios especiales.")
        serializer = SpecialHoursPreviewSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        preview = build_preview_from_command(serializer.validated_data["text"])
        payload = dict(preview.canonical_payload)
        payload["reason"] = serializer.validated_data.get("reason") or ""
        payload["source_channel"] = serializer.validated_data.get("source_channel") or "API"
        return Response(
            {
                "preview": payload,
                "validation_errors": payload.get("validation_errors") or [],
            },
            status=status.HTTP_200_OK,
        )


class SpecialHoursListCreateView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not _can_manage_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para consultar horarios especiales.")
        qs = SolicitudHorarioEspecial.objects.select_related("requested_by", "approved_by", "executed_by").order_by("-created_at")
        status_filter = (request.GET.get("status") or "").strip().upper()
        if status_filter in {choice[0] for choice in SolicitudHorarioEspecial.STATUS_CHOICES}:
            qs = qs.filter(status=status_filter)
        return Response({"rows": [_serialize_request(obj) for obj in qs[:100]]}, status=status.HTTP_200_OK)

    def post(self, request):
        if not _can_manage_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para crear horarios especiales.")
        serializer = SpecialHoursPreviewSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        request_obj, payload = create_request_from_text(
            raw_text=serializer.validated_data["text"],
            actor=request.user,
            reason=serializer.validated_data.get("reason") or "",
            source_channel=serializer.validated_data.get("source_channel") or SolicitudHorarioEspecial.SOURCE_API,
        )
        return Response(
            {
                "request": _serialize_request(request_obj),
                "validation_errors": payload.get("validation_errors") or [],
            },
            status=status.HTTP_201_CREATED,
        )


class SpecialHoursDetailView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, request_id: int):
        if not _can_manage_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para consultar horarios especiales.")
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        return Response({"request": _serialize_request(obj)}, status=status.HTTP_200_OK)


class SpecialHoursValidateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_manage_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para validar horarios especiales.")
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        try:
            errors = validate_request(request_obj=obj, actor=request.user)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=400)
        obj.refresh_from_db()
        return Response({"request": _serialize_request(obj), "validation_errors": errors}, status=status.HTTP_200_OK)


class SpecialHoursApproveView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_approve_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para aprobar horarios especiales.")
        serializer = SpecialHoursApprovalSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        try:
            approve_request(request_obj=obj, actor=request.user, expected_revision=serializer.validated_data.get("expected_revision"))
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=400)
        obj.refresh_from_db()
        return Response({"request": _serialize_request(obj)}, status=status.HTTP_200_OK)


class SpecialHoursExecuteView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_approve_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para ejecutar horarios especiales.")
        serializer = SpecialHoursActionSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        if obj.cancelled_at or not obj.approved_at or obj.status not in {"APROBADO", "EJECUTADO", "FALLIDO"}:
            return Response({"detail": "La solicitud debe estar aprobada y no cancelada."}, status=400)
        if serializer.validated_data.get("async_execute", True):
            try:
                task = execute_special_hours_request_task.delay(request_id=obj.id, actor_id=request.user.id)
            except Exception as exc:
                logger.warning("[horarios] No se pudo encolar la ejecución de %s: %s", obj.id, exc)
                return Response(
                    {
                        "status": "unavailable",
                        "detail": "La cola de tareas no responde; no se encoló la ejecución. Reintenta en unos minutos.",
                        "request": _serialize_request(obj),
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            return Response(
                {
                    "status": "queued",
                    "task_id": task.id,
                    "request": _serialize_request(obj),
                },
                status=status.HTTP_202_ACCEPTED,
            )
        try:
            summary = execute_request(request_obj=obj, actor=request.user)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=400)
        obj.refresh_from_db()
        return Response({"request": _serialize_request(obj), "summary": summary}, status=status.HTTP_200_OK)


class SpecialHoursRetryView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_approve_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para reintentar horarios especiales.")
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        if obj.cancelled_at or not obj.approved_at or obj.status not in {"APROBADO", "EJECUTADO", "FALLIDO"}:
            return Response({"detail": "La solicitud debe estar aprobada y no cancelada."}, status=400)
        try:
            task = execute_special_hours_request_task.delay(request_id=obj.id, actor_id=request.user.id)
        except Exception as exc:
            logger.warning("[horarios] No se pudo encolar el reintento de %s: %s", obj.id, exc)
            return Response(
                {
                    "status": "unavailable",
                    "detail": "La cola de tareas no responde; la solicitud quedó como estaba. Reintenta en unos minutos.",
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return Response({"status": "queued", "task_id": task.id}, status=status.HTTP_202_ACCEPTED)


class SpecialHoursCancelView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_approve_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para cancelar horarios especiales.")
        serializer = SpecialHoursActionSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        try:
            cancel_request(request_obj=obj, actor=request.user, reason=serializer.validated_data.get("comment") or "")
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=400)
        obj.refresh_from_db()
        return Response({"request": _serialize_request(obj)}, status=status.HTTP_200_OK)
