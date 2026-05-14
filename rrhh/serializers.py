from __future__ import annotations

from rest_framework import serializers

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
            "notas",
            "creado_en",
        ]
        read_only_fields = ["monto_calculado", "estado", "creado_en"]
        extra_kwargs = {"empleado": {"required": False}}


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
            "folio",
            "foto_evidencia",
            "creado_en",
        ]
        read_only_fields = ["folio", "estado", "creado_en"]
        extra_kwargs = {"empleado": {"required": False}}

    def validate_empleado(self, empleado: Empleado) -> Empleado:
        request = self.context.get("request")
        if request and not request.user.is_staff and empleado.email and request.user.email:
            if empleado.email.lower() != request.user.email.lower():
                raise serializers.ValidationError("No puedes solicitar permisos para otro empleado.")
        return empleado
