from django.contrib.auth import authenticate
from rest_framework import serializers, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from api.public_views import _auth_public_client, _log_access
from integraciones.models import PublicApiClient
from logistica.services_domicilio_assignment import (
    repartidores_disponibles_queryset,
)
from rrhh.services_identidad import nombre_operativo_usuario


class DriverIdentityLoginSerializer(serializers.Serializer):
    username = serializers.CharField(
        min_length=1,
        max_length=150,
        trim_whitespace=True,
    )
    password = serializers.CharField(
        min_length=1,
        max_length=128,
        trim_whitespace=False,
        write_only=True,
    )


def _authorize_driver_identity(api_client, request):
    if api_client.has_capability(
        PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
    ):
        return None
    _log_access(api_client, request, status.HTTP_403_FORBIDDEN)
    return Response(
        {"detail": "Integración no autorizada para identidad de reparto."},
        status=status.HTTP_403_FORBIDDEN,
    )


def _eligible_driver_for_client(*, api_client, user_id: int):
    return (
        repartidores_disponibles_queryset()
        .filter(
            user_id=user_id,
            user__groups__name__iexact="repartidor",
            api_clients_logistica_autorizados=api_client,
        )
        .distinct()
        .first()
    )


def _identity_payload(driver):
    name = nombre_operativo_usuario(driver.user)
    return {
        "user": {
            "id": driver.user_id,
            "username": driver.user.get_username(),
            "full_name": name,
        },
        "driver": {
            "id": driver.id,
            "name": name,
        },
    }


class PublicDriverIdentityLoginView(APIView):
    permission_classes = [AllowAny]
    authentication_classes = []

    def post(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_driver_identity(api_client, request)
        if capability_error:
            return capability_error

        serializer = DriverIdentityLoginSerializer(data=request.data)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_401_UNAUTHORIZED)
            return Response(
                {"detail": "No fue posible validar las credenciales de reparto."},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        user = authenticate(
            request=request,
            username=serializer.validated_data["username"],
            password=serializer.validated_data["password"],
        )
        driver = (
            _eligible_driver_for_client(api_client=api_client, user_id=user.id)
            if user is not None
            else None
        )
        if driver is None:
            _log_access(api_client, request, status.HTTP_401_UNAUTHORIZED)
            return Response(
                {"detail": "No fue posible validar las credenciales de reparto."},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(_identity_payload(driver), status=status.HTTP_200_OK)


class PublicDriverIdentityStatusView(APIView):
    permission_classes = [AllowAny]
    authentication_classes = []

    def get(self, request, erp_user_id: int):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_driver_identity(api_client, request)
        if capability_error:
            return capability_error

        driver = _eligible_driver_for_client(
            api_client=api_client,
            user_id=erp_user_id,
        )
        if driver is None:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            return Response(
                {"detail": "Cuenta de reparto no disponible."},
                status=status.HTTP_404_NOT_FOUND,
            )
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(_identity_payload(driver), status=status.HTTP_200_OK)
