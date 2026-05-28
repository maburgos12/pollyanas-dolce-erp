from __future__ import annotations

from rest_framework import serializers

from core.access import can_view_rrhh

from .models import AsistenciaEmpleado, Empleado, HoraExtra, PermisoSalida


class AsistenciaSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)

    class Meta:
        model = AsistenciaEmpleado
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "fecha",
            "entrada",
            "salida",
            "minutos_trabajados",
            "turno",
            "fuente",
            "observacion",
        ]


class HoraExtraSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    jefe_directo_nombre = serializers.SerializerMethodField()

    class Meta:
        model = HoraExtra
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "fecha",
            "horas",
            "monto_calculado",
            "estado",
            "jefe_directo",
            "jefe_directo_nombre",
            "autorizado_por",
            "fecha_autorizacion_jefe",
            "notas",
            "creado_en",
        ]
        read_only_fields = [
            "monto_calculado",
            "estado",
            "jefe_directo",
            "jefe_directo_nombre",
            "autorizado_por",
            "fecha_autorizacion_jefe",
            "creado_en",
        ]
        extra_kwargs = {"empleado": {"required": False}}

    def get_jefe_directo_nombre(self, obj):
        if not obj.jefe_directo_id:
            return ""
        return obj.jefe_directo.get_full_name() or obj.jefe_directo.username


class PermisoSalidaSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)

    class Meta:
        model = PermisoSalida
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "tipo",
            "fecha_inicio",
            "fecha_fin",
            "motivo",
            "estado",
            "estado_jefe",
            "requiere_direccion",
            "estado_direccion",
            "goce_sueldo",
            "autorizado_jefe_por",
            "fecha_autorizacion_jefe",
            "autorizado_direccion_por",
            "fecha_autorizacion_direccion",
            "origen_solicitud",
            "folio",
            "foto_evidencia",
            "creado_en",
        ]
        read_only_fields = [
            "folio",
            "estado",
            "estado_jefe",
            "requiere_direccion",
            "estado_direccion",
            "autorizado_jefe_por",
            "fecha_autorizacion_jefe",
            "autorizado_direccion_por",
            "fecha_autorizacion_direccion",
            "origen_solicitud",
            "creado_en",
        ]
        extra_kwargs = {"empleado": {"required": False}}

    def validate_empleado(self, empleado: Empleado) -> Empleado:
        request = self.context.get("request")
        if request and can_view_rrhh(request.user):
            return empleado
        if request and not request.user.is_staff and empleado.email and request.user.email:
            if empleado.email.lower() != request.user.email.lower():
                raise serializers.ValidationError("No puedes solicitar permisos para otro empleado.")
        return empleado
