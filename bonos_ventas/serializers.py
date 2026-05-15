from rest_framework import serializers

from rrhh.models import Empleado

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, RegistroDiarioVentas, VentaCategoriaSucursal


class EmpleadoMiniSerializer(serializers.ModelSerializer):
    sucursal_nombre = serializers.CharField(source="sucursal", read_only=True)

    class Meta:
        model = Empleado
        fields = ["id", "codigo", "nombre", "area", "puesto", "sucursal", "sucursal_nombre", "activo"]


class ConfigBonoVentasPeriodoSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConfigBonoVentasPeriodo
        fields = "__all__"
        read_only_fields = ["creado_por", "creado_en", "actualizado_en"]


class VentaCategoriaSucursalSerializer(serializers.ModelSerializer):
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)

    class Meta:
        model = VentaCategoriaSucursal
        fields = "__all__"
        read_only_fields = ["pct_crecimiento", "activo_bono", "monto_bono_categoria", "actualizado_en"]


class RegistroDiarioVentasSerializer(serializers.ModelSerializer):
    class Meta:
        model = RegistroDiarioVentas
        fields = "__all__"
        read_only_fields = ["capturado_por", "creado_en", "actualizado_en"]


class BonoVentasEmpleadoSerializer(serializers.ModelSerializer):
    empleado_detalle = EmpleadoMiniSerializer(source="empleado", read_only=True)
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    empleado_codigo = serializers.CharField(source="empleado.codigo", read_only=True)
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)
    registros = RegistroDiarioVentasSerializer(many=True, read_only=True)

    class Meta:
        model = BonoVentasEmpleado
        fields = "__all__"
        read_only_fields = [
            "pasa_uniforme",
            "pasa_asistencia",
            "pasa_puntualidad",
            "monto_uniforme",
            "monto_asistencia",
            "monto_puntualidad",
            "sub1",
            "bono_ventas",
            "pasa_bono_ventas",
            "total_a_pagar",
            "creado_en",
            "actualizado_en",
        ]


class BonoVentasResumenSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    empleado_codigo = serializers.CharField(source="empleado.codigo", read_only=True)
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)

    class Meta:
        model = BonoVentasEmpleado
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "empleado_codigo",
            "sucursal",
            "sucursal_nombre",
            "dias_trabajados",
            "dias_asistencia",
            "dias_uniforme",
            "dias_puntualidad",
            "pasa_uniforme",
            "pasa_asistencia",
            "pasa_puntualidad",
            "sub1",
            "bono_ventas",
            "pasa_bono_ventas",
            "ajuste_positivo",
            "ajuste_negativo",
            "bono_extra",
            "total_a_pagar",
            "estatus",
        ]
