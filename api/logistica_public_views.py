from __future__ import annotations

from django.http import Http404
from rest_framework import serializers, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from api.public_views import _auth_public_client, _log_access
from integraciones.models import PublicApiClient
from logistica.services_domicilio_assignment import (
    DomicilioAssignmentError,
    assign_domicilio,
    get_owned_domicilio_or_404,
    list_repartidores_disponibles_minimal,
    repartidores_disponibles_queryset,
    unidades_disponibles_queryset,
)
from logistica.models import SolicitudDomicilio
from logistica.services_domicilio_status import (
    DomicilioStatusError,
    update_domicilio_status,
)
from logistica.services_domicilio_location import (
    DomicilioLocationError,
    record_domicilio_location,
)


class ExternalActorSerializer(serializers.Serializer):
    id = serializers.CharField(max_length=64, trim_whitespace=True)
    nombre = serializers.CharField(max_length=120, trim_whitespace=True)


class PublicDomicilioAssignmentSerializer(serializers.Serializer):
    repartidor_id = serializers.IntegerField(min_value=1)
    unidad_id = serializers.IntegerField(min_value=1, required=False)
    actor = ExternalActorSerializer()

    def validate_repartidor_id(self, value):
        if isinstance(self.initial_data.get("repartidor_id"), bool):
            raise serializers.ValidationError("Debe ser un entero positivo.")
        return value


class PublicDomicilioStatusSerializer(serializers.Serializer):
    repartidor_id = serializers.IntegerField(min_value=1)
    actor = ExternalActorSerializer()
    estatus = serializers.ChoiceField(
        choices=[
            SolicitudDomicilio.ESTATUS_EN_RUTA,
            SolicitudDomicilio.ESTATUS_ENTREGADO,
            SolicitudDomicilio.ESTATUS_INCIDENCIA,
        ]
    )
    operation_id = serializers.UUIDField()
    motivo = serializers.CharField(
        max_length=300,
        trim_whitespace=True,
        allow_blank=True,
        required=False,
        default="",
    )

    def validate_repartidor_id(self, value):
        if isinstance(self.initial_data.get("repartidor_id"), bool):
            raise serializers.ValidationError("Debe ser un entero positivo.")
        return value

    def validate(self, attrs):
        if (
            attrs["estatus"] == SolicitudDomicilio.ESTATUS_INCIDENCIA
            and not attrs["motivo"]
        ):
            raise serializers.ValidationError(
                {"motivo": "El motivo de la incidencia es obligatorio."}
            )
        if (
            attrs["estatus"] != SolicitudDomicilio.ESTATUS_INCIDENCIA
            and attrs["motivo"]
        ):
            raise serializers.ValidationError(
                {"motivo": "El motivo solo aplica a una incidencia."}
            )
        return attrs


class PublicDomicilioLocationSerializer(serializers.Serializer):
    repartidor_id = serializers.IntegerField(min_value=1)
    operation_id = serializers.UUIDField()
    latitud = serializers.DecimalField(
        max_digits=9, decimal_places=6, min_value=-90, max_value=90
    )
    longitud = serializers.DecimalField(
        max_digits=10, decimal_places=6, min_value=-180, max_value=180
    )
    accuracy_m = serializers.DecimalField(
        max_digits=8, decimal_places=2, min_value=0, max_value=500
    )
    captured_at = serializers.DateTimeField()
    actor = ExternalActorSerializer()

    def validate_repartidor_id(self, value):
        if isinstance(self.initial_data.get("repartidor_id"), bool):
            raise serializers.ValidationError("Debe ser un entero positivo.")
        return value


class PublicLogisticaCatalogQuerySerializer(serializers.Serializer):
    solicitud_id = serializers.RegexField(
        regex=r"^[1-9][0-9]{0,18}$",
        min_length=1,
        max_length=19,
    )

    def validate_solicitud_id(self, value):
        return int(value)


def _authorize_logistica_assignment(api_client, request):
    capability = PublicApiClient.CAPABILITY_LOGISTICA_ASSIGNMENT
    if api_client.has_capability(capability):
        return None
    _log_access(api_client, request, status.HTTP_403_FORBIDDEN)
    return Response(
        {
            "detail": "La API key no tiene autorización para asignación logística.",
            "code": "LOGISTICA_ASSIGNMENT_CAPABILITY_REQUIRED",
        },
        status=status.HTTP_403_FORBIDDEN,
    )


class PublicLogisticaRepartidoresDisponiblesView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_logistica_assignment(api_client, request)
        if capability_error:
            return capability_error

        serializer = PublicLogisticaCatalogQuerySerializer(
            data=request.query_params,
        )
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(
                {"detail": "solicitud_id es obligatorio."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        solicitud_id = serializer.validated_data["solicitud_id"]
        try:
            get_owned_domicilio_or_404(
                solicitud_id=solicitud_id,
                api_client=api_client,
            )
        except Http404:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise

        payload = {
            "results": list_repartidores_disponibles_minimal(
                api_client=api_client,
            )
        }
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload, status=status.HTTP_200_OK)


class PublicLogisticaDomicilioAsignarView(APIView):
    permission_classes = [AllowAny]

    def post(self, request, solicitud_id: int):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_logistica_assignment(api_client, request)
        if capability_error:
            return capability_error

        serializer = PublicDomicilioAssignmentSerializer(data=request.data)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        actor = serializer.validated_data["actor"]
        unidad = None
        unidad_id = serializer.validated_data.get("unidad_id")
        if unidad_id is not None:
            unidad = unidades_disponibles_queryset().filter(pk=unidad_id).first()
            if unidad is None:
                _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
                return Response(
                    {"detail": "Unidad no disponible."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
        try:
            payload = assign_domicilio(
                solicitud_id=solicitud_id,
                repartidor_id=serializer.validated_data["repartidor_id"],
                unidad=unidad,
                audit_metadata={
                    "api_client": {
                        "id": api_client.id,
                        "nombre": api_client.nombre,
                    },
                    "actor_externo": {
                        "id": actor["id"],
                        "nombre": actor["nombre"],
                    },
                },
                owner_api_client=api_client,
            )
        except Http404:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise
        except DomicilioAssignmentError as exc:
            _log_access(api_client, request, exc.status_code)
            return Response(
                {"detail": exc.detail},
                status=exc.status_code,
            )

        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload, status=status.HTTP_200_OK)


def _serialize_domicilio(solicitud):
    direccion = solicitud.direccion_cliente
    pedido = solicitud.pedido_cliente
    snapshot = pedido.point_note_snapshot or {}
    unit = solicitud.unidad
    return {
        "id": solicitud.id,
        "estatus": (
            "ASIGNADO"
            if solicitud.estatus == SolicitudDomicilio.ESTATUS_LISTO
            else solicitud.estatus
        ),
        "revision": solicitud.revision,
        "cliente_nombre": solicitud.cliente_nombre,
        "cliente_telefono": solicitud.cliente_telefono,
        "direccion": direccion.direccion if direccion else solicitud.direccion,
        "referencias": direccion.referencias if direccion else "",
        "latitud": direccion.latitud if direccion else None,
        "longitud": direccion.longitud if direccion else None,
        "place_id": direccion.place_id if direccion else "",
        "descripcion": pedido.descripcion,
        "canal": pedido.canal,
        "fecha_compromiso": pedido.fecha_compromiso,
        "folio": pedido.point_note_folio or pedido.folio,
        "instrucciones_entrega": solicitud.instrucciones_entrega,
        "unidad": (
            {"id": unit.id, "codigo": unit.codigo}
            if unit is not None
            else None
        ),
        "productos": [
            {
                "codigo": line.get("point_code", ""),
                "descripcion": line.get("description", ""),
                "cantidad": line.get("quantity", "0"),
            }
            for line in snapshot.get("lines", [])
            if isinstance(line, dict)
        ],
        "total": pedido.monto_estimado,
    }


class PublicLogisticaRepartidorDomiciliosView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, repartidor_id: int):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_logistica_assignment(api_client, request)
        if capability_error:
            return capability_error
        repartidor_visible = (
            repartidores_disponibles_queryset()
            .filter(
                pk=repartidor_id,
                api_clients_logistica_autorizados=api_client,
            )
            .exists()
        )
        if not repartidor_visible:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise Http404
        solicitudes = (
            SolicitudDomicilio.objects.filter(
                pedido_cliente__public_api_client=api_client,
                repartidor_id=repartidor_id,
                estatus__in=[
                    SolicitudDomicilio.ESTATUS_LISTO,
                    SolicitudDomicilio.ESTATUS_EN_RUTA,
                ],
            )
            .select_related("pedido_cliente", "direccion_cliente", "unidad")
            .order_by("id")
        )
        payload = {"results": [_serialize_domicilio(item) for item in solicitudes]}
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload)


class PublicLogisticaDomicilioStatusView(APIView):
    permission_classes = [AllowAny]

    def post(self, request, solicitud_id: int):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_logistica_assignment(api_client, request)
        if capability_error:
            return capability_error
        serializer = PublicDomicilioStatusSerializer(data=request.data)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        values = serializer.validated_data
        try:
            payload = update_domicilio_status(
                solicitud_id=solicitud_id,
                api_client=api_client,
                repartidor_id=values["repartidor_id"],
                requested_status=values["estatus"],
                operation_id=values["operation_id"],
                actor=values["actor"],
                reason=values["motivo"],
            )
        except Http404:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise
        except DomicilioStatusError as exc:
            _log_access(api_client, request, exc.status_code)
            return Response({"detail": exc.detail}, status=exc.status_code)
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload)


class PublicLogisticaDomicilioLocationView(APIView):
    permission_classes = [AllowAny]

    def post(self, request, solicitud_id: int):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_logistica_assignment(api_client, request)
        if capability_error:
            return capability_error
        serializer = PublicDomicilioLocationSerializer(data=request.data)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        values = serializer.validated_data
        try:
            payload = record_domicilio_location(
                solicitud_id=solicitud_id,
                api_client=api_client,
                repartidor_id=values["repartidor_id"],
                operation_id=values["operation_id"],
                latitud=values["latitud"],
                longitud=values["longitud"],
                accuracy_m=values["accuracy_m"],
                captured_at=values["captured_at"],
                actor=values["actor"],
            )
        except Http404:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise
        except DomicilioLocationError as exc:
            _log_access(api_client, request, exc.status_code)
            return Response({"detail": exc.detail}, status=exc.status_code)
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload)
