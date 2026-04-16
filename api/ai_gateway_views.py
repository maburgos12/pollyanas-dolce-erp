from __future__ import annotations

from django.core.exceptions import PermissionDenied
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .ai_gateway_serializers import (
    AIToolApprovalDecisionSerializer,
    AIToolApprovalRequestSerializer,
    AIToolInvokeSerializer,
)
from .ai_gateway_services import (
    build_gateway_manifest,
    build_gateway_openapi_spec,
    decide_tool_approval,
    execute_approved_tool,
    get_tool_definition,
    invoke_tool,
    list_allowed_tools,
    list_pending_approvals,
    request_tool_approval,
)


def _tool_endpoints(request, tool_key: str, requires_approval: bool) -> dict[str, str]:
    endpoints = {
        "detail_path": request.build_absolute_uri(f"/api/ai-gateway/tools/{tool_key}/"),
        "invoke_path": request.build_absolute_uri(f"/api/ai-gateway/tools/{tool_key}/invoke/"),
    }
    if requires_approval:
        endpoints["request_approval_path"] = request.build_absolute_uri(f"/api/ai-gateway/tools/{tool_key}/request-approval/")
    return endpoints


def _attach_endpoints(request, tool: dict) -> dict:
    enriched = dict(tool)
    enriched["endpoints"] = _tool_endpoints(request, tool["key"], bool(tool.get("requires_approval")))
    return enriched


class AIGatewayToolsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        tools = list_allowed_tools(request.user)
        tools = [_attach_endpoints(request, tool) for tool in tools]
        return Response(
            {
                "tools": tools,
                "count": len(tools),
            },
            status=status.HTTP_200_OK,
        )


class AIGatewayToolDetailView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, tool_key: str):
        try:
            tool = get_tool_definition(user=request.user, tool_key=tool_key)
        except PermissionDenied as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_403_FORBIDDEN)
        return Response(_attach_endpoints(request, tool), status=status.HTTP_200_OK)


class AIGatewayManifestView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        manifest = build_gateway_manifest(user=request.user)
        manifest["gateway"]["manifest_path"] = request.build_absolute_uri("/api/ai-gateway/manifest/")
        manifest["auth"]["me_path"] = request.build_absolute_uri("/api/auth/me/")
        manifest["auth"]["token_path"] = request.build_absolute_uri("/api/auth/token/")
        manifest["approval_workflow"]["list_path"] = request.build_absolute_uri("/api/ai-gateway/approvals/")
        manifest["approval_workflow"]["decision_path_template"] = request.build_absolute_uri(
            "/api/ai-gateway/approvals/{suggestion_id}/{decision}/"
        )
        manifest["approval_workflow"]["execute_path_template"] = request.build_absolute_uri(
            "/api/ai-gateway/approvals/{suggestion_id}/execute/"
        )
        manifest["tools"] = [_attach_endpoints(request, tool) for tool in manifest["tools"]]
        manifest["count"] = len(manifest["tools"])
        return Response(manifest, status=status.HTTP_200_OK)


class AIGatewayOpenAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        tool_keys_raw = request.query_params.getlist("tool_keys")
        if len(tool_keys_raw) == 1 and "," in tool_keys_raw[0]:
            requested_tool_keys = {chunk.strip() for chunk in tool_keys_raw[0].split(",") if chunk.strip()}
        else:
            requested_tool_keys = {chunk.strip() for chunk in tool_keys_raw if chunk.strip()}
        try:
            spec = build_gateway_openapi_spec(
                user=request.user,
                request=request,
                profile=request.query_params.get("profile") or "",
                requested_tool_keys=requested_tool_keys or None,
            )
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        return Response(spec, status=status.HTTP_200_OK)


class AIGatewayToolInvokeView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, tool_key: str):
        serializer = AIToolInvokeSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        try:
            result = invoke_tool(
                user=request.user,
                tool_key=tool_key,
                arguments=serializer.validated_data.get("arguments") or {},
            )
        except PermissionDenied as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_403_FORBIDDEN)
        return Response(result, status=status.HTTP_200_OK)


class AIGatewayApprovalRequestView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, tool_key: str):
        serializer = AIToolApprovalRequestSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        try:
            result = request_tool_approval(
                user=request.user,
                tool_key=tool_key,
                arguments=serializer.validated_data.get("arguments") or {},
                summary=serializer.validated_data.get("summary") or "",
                rationale=serializer.validated_data.get("rationale") or "",
            )
        except PermissionDenied as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_403_FORBIDDEN)
        return Response(result, status=status.HTTP_201_CREATED)


class AIGatewayApprovalListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            result = list_pending_approvals(user=request.user)
        except PermissionDenied as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_403_FORBIDDEN)
        return Response(result, status=status.HTTP_200_OK)


class AIGatewayApprovalDecisionView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, suggestion_id: int, decision: str):
        serializer = AIToolApprovalDecisionSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        decision_normalized = (decision or "").strip().lower()
        if decision_normalized not in {"approve", "reject"}:
            return Response({"detail": "decision inválida. Usa approve o reject."}, status=status.HTTP_400_BAD_REQUEST)
        try:
            result = decide_tool_approval(
                user=request.user,
                suggestion_id=suggestion_id,
                approve=(decision_normalized == "approve"),
                comment=serializer.validated_data.get("comment") or "",
            )
        except PermissionDenied as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_403_FORBIDDEN)
        return Response(result, status=status.HTTP_200_OK)


class AIGatewayApprovalExecuteView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, suggestion_id: int):
        try:
            result = execute_approved_tool(user=request.user, suggestion_id=suggestion_id)
        except PermissionDenied as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_403_FORBIDDEN)
        return Response(result, status=status.HTTP_200_OK)
