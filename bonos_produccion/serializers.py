from rest_framework import serializers

from rrhh.models import Empleado

from .models import BonoProduccionEmpleado, ConfigBonoPeriodo, RegistroDiarioProduccion


class EmpleadoMiniSerializer(serializers.ModelSerializer):
    sucursal_nombre = serializers.CharField(source="sucursal", read_only=True)

    class Meta:
        model = Empleado
        fields = ["id", "codigo", "nombre", "area", "puesto", "sucursal", "sucursal_nombre", "activo"]


class ConfigBonoPeriodoSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConfigBonoPeriodo
        fields = "__all__"
        read_only_fields = ["creado_por", "creado_en", "actualizado_en"]


class RegistroDiarioSerializer(serializers.ModelSerializer):
    class Meta:
        model = RegistroDiarioProduccion
        fields = "__all__"
        read_only_fields = ["capturado_por", "creado_en", "actualizado_en"]


class BonoProduccionSerializer(serializers.ModelSerializer):
    empleado_detalle = EmpleadoMiniSerializer(source="empleado", read_only=True)
    registros = RegistroDiarioSerializer(many=True, read_only=True)

    class Meta:
        model = BonoProduccionEmpleado
        fields = "__all__"
        read_only_fields = [
            "creado_en",
            "actualizado_en",
            "pasa_uniforme",
            "pasa_puntualidad",
            "pasa_asistencia",
            "pasa_produccion",
            "monto_uniforme",
            "monto_puntualidad",
            "monto_asistencia",
            "monto_produccion",
            "monto_premio_embetunado",
            "total_a_pagar",
        ]


class BonoProduccionResumenSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    empleado_codigo = serializers.CharField(source="empleado.codigo", read_only=True)

    class Meta:
        model = BonoProduccionEmpleado
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "empleado_codigo",
            "area",
            "dias_trabajados",
            "dias_uniforme",
            "dias_puntualidad",
            "dias_asistencia",
            "dias_produccion",
            "total_embetunados",
            "pasa_uniforme",
            "pasa_puntualidad",
            "pasa_asistencia",
            "pasa_produccion",
            "gano_premio_embetunado",
            "monto_uniforme",
            "monto_puntualidad",
            "monto_asistencia",
            "monto_produccion",
            "monto_premio_embetunado",
            "ajuste_positivo",
            "ajuste_negativo",
            "bono_extra",
            "total_a_pagar",
            "estatus",
        ]
