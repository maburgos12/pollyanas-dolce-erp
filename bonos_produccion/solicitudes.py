from django.db.models import Q
from rest_framework import permissions, viewsets
from rest_framework.exceptions import ValidationError

from core.access import is_bonos_produccion_capture_only
from rrhh.models import Empleado, Prestamo
from rrhh.serializers import PrestamoSerializer
from rrhh.services_prestamos import crear_solicitud_prestamo


def empleados_operables_solicitudes_produccion():
    """Personal al que una operadora acotada puede capturar tramites."""
    return (
        Empleado.objects.filter(
            Q(departamento=Empleado.DEP_PRODUCCION) | Q(departamento_origen=Empleado.DEP_PRODUCCION),
            activo=True,
            jefe_directo__activo=True,
            jefe_directo__usuario_erp__is_active=True,
        )
        .select_related("jefe_directo__usuario_erp", "sucursal_ref")
        .distinct()
    )


class CanCaptureSolicitudesProduccion(permissions.BasePermission):
    def has_permission(self, request, view):
        user = request.user
        return bool(
            user
            and user.is_authenticated
            and (user.is_superuser or is_bonos_produccion_capture_only(user))
        )


class PrestamoProduccionSerializer(PrestamoSerializer):
    class Meta(PrestamoSerializer.Meta):
        extra_kwargs = {"empleado": {"required": True}}


class PrestamosProduccionViewSet(viewsets.ModelViewSet):
    serializer_class = PrestamoProduccionSerializer
    permission_classes = [permissions.IsAuthenticated, CanCaptureSolicitudesProduccion]
    http_method_names = ["get", "post", "head", "options"]

    def get_queryset(self):
        return (
            Prestamo.objects.select_related("empleado", "jefe_directo")
            .filter(Q(creado_por=self.request.user) | Q(empleado__usuario_erp=self.request.user))
            .order_by("-fecha_solicitud", "-id")
            .distinct()
        )

    def perform_create(self, serializer):
        empleado_id = serializer.validated_data["empleado"].pk
        empleado = empleados_operables_solicitudes_produccion().filter(pk=empleado_id).first()
        if not empleado:
            raise ValidationError({"empleado": "Empleado fuera del alcance activo de Produccion."})
        serializer.instance = crear_solicitud_prestamo(
            empleado=empleado,
            actor=self.request.user,
            concepto=serializer.validated_data["concepto"],
            metodo_pago=serializer.validated_data["metodo_pago"],
            importe=serializer.validated_data["importe"],
            num_quincenas=serializer.validated_data["num_quincenas"],
            fecha_deposito=serializer.validated_data.get("fecha_deposito"),
        )
