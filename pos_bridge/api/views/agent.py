from __future__ import annotations

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from pos_bridge.api.permissions import IsPosAdminUser
from pos_bridge.api.serializers.sync_jobs import AgentQuerySerializer, AgentResponseSerializer
from pos_bridge.services.agent_query_service import PosAgentQueryService


class AgentQueryView(APIView):
    permission_classes = [IsPosAdminUser]

    def post(self, request):
        serializer = AgentQuerySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        service = PosAgentQueryService()
        result = service.process_query(
            query=serializer.validated_data["query"],
            user=request.user,
            context=serializer.validated_data.get("context", {}),
        )
        return Response(AgentResponseSerializer(result).data, status=status.HTTP_200_OK)
