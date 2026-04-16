from __future__ import annotations

from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
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

from .special_hours_serializers import SpecialHoursActionSerializer, SpecialHoursPreviewSerializer


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
        errors = validate_request(request_obj=obj, actor=request.user)
        obj.refresh_from_db()
        return Response({"request": _serialize_request(obj), "validation_errors": errors}, status=status.HTTP_200_OK)


class SpecialHoursApproveView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_approve_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para aprobar horarios especiales.")
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        approve_request(request_obj=obj, actor=request.user)
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
        if serializer.validated_data.get("async_execute", True):
            task = execute_special_hours_request_task.delay(request_id=obj.id, actor_id=request.user.id)
            return Response(
                {
                    "status": "queued",
                    "task_id": task.id,
                    "request": _serialize_request(obj),
                },
                status=status.HTTP_202_ACCEPTED,
            )
        summary = execute_request(request_obj=obj, actor=request.user)
        obj.refresh_from_db()
        return Response({"request": _serialize_request(obj), "summary": summary}, status=status.HTTP_200_OK)


class SpecialHoursRetryView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_approve_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para reintentar horarios especiales.")
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        obj.status = SolicitudHorarioEspecial.STATUS_APROBADO
        obj.save(update_fields=["status", "updated_at"])
        task = execute_special_hours_request_task.delay(request_id=obj.id, actor_id=request.user.id)
        return Response({"status": "queued", "task_id": task.id}, status=status.HTTP_202_ACCEPTED)


class SpecialHoursCancelView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, request_id: int):
        if not _can_approve_special_hours(request.user):
            raise PermissionDenied("No tienes permisos para cancelar horarios especiales.")
        serializer = SpecialHoursActionSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        obj = get_object_or_404(SolicitudHorarioEspecial, id=request_id)
        cancel_request(request_obj=obj, actor=request.user, reason=serializer.validated_data.get("comment") or "")
        obj.refresh_from_db()
        return Response({"request": _serialize_request(obj)}, status=status.HTTP_200_OK)

