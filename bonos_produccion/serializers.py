from datetime import timedelta

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
        read_only_fields = [
            "fecha",
            "estado_rrhh",
            "motivo_rrhh",
            "falta_penalizable",
            "capturado_por",
            "creado_en",
            "actualizado_en",
        ]


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
            "cancela_bono",
            "cancela_motivo",
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


class BonoProduccionCapturaSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    empleado_codigo = serializers.CharField(source="empleado.codigo", read_only=True)
    puesto = serializers.CharField(source="empleado.puesto", read_only=True)
    sucursal_nombre = serializers.CharField(source="empleado.sucursal_display", read_only=True)
    periodo_mes = serializers.IntegerField(source="periodo.mes", read_only=True)
    periodo_anio = serializers.IntegerField(source="periodo.anio", read_only=True)
    periodo_fecha_inicio = serializers.DateField(source="periodo.fecha_inicio", read_only=True)
    periodo_fecha_fin = serializers.DateField(source="periodo.fecha_fin", read_only=True)

    class Meta:
        model = BonoProduccionEmpleado
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "empleado_codigo",
            "puesto",
            "sucursal_nombre",
            "periodo_mes",
            "periodo_anio",
            "periodo_fecha_inicio",
            "periodo_fecha_fin",
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
        ]
        read_only_fields = fields


class RegistroDiarioCapturaSerializer(serializers.ModelSerializer):
    class Meta:
        model = RegistroDiarioProduccion
        fields = [
            "id",
            "bono",
            "dia",
            "fecha",
            "estado_rrhh",
            "motivo_rrhh",
            "falta_penalizable",
            "tiene_uniforme",
            "tiene_puntualidad",
            "tiene_asistencia",
            "tiene_produccion",
            "cantidad_embetunados",
            "observacion",
        ]
        read_only_fields = [
            "id",
            "fecha",
            "estado_rrhh",
            "motivo_rrhh",
            "falta_penalizable",
        ]

    def validate_bono(self, bono):
        bonos_permitidos = self.context["bonos_permitidos"]
        if not bonos_permitidos.filter(pk=bono.pk).exists():
            raise serializers.ValidationError("Bono fuera del periodo o alcance de Produccion.")
        return bono

    def validate(self, attrs):
        unexpected = set(self.initial_data) - set(self.fields)
        if unexpected:
            raise serializers.ValidationError(
                {key: "Campo no permitido para captura operativa." for key in unexpected}
            )
        bono = attrs.get("bono") or getattr(self.instance, "bono", None)
        dia = attrs.get("dia", getattr(self.instance, "dia", None))
        if bono is not None and dia is not None:
            inicio, fin = bono.periodo.rango_fechas()
            fecha = inicio
            dias_validos = set()
            while fecha <= fin:
                dias_validos.add(fecha.day)
                fecha += timedelta(days=1)
            if dia not in dias_validos:
                raise serializers.ValidationError({"dia": "Día fuera del rango del corte."})
        return attrs
