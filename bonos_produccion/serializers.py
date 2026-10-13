from datetime import timedelta

from django.utils import timezone
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


class FechaRegistroMixin:
    def validate(self, attrs):
        attrs = super().validate(attrs)
        bono = attrs.get("bono") or getattr(self.instance, "bono", None)
        dia = attrs.get("dia", getattr(self.instance, "dia", None))
        fecha = attrs.get("fecha", getattr(self.instance, "fecha", None))
        if self.instance and (
            bono.pk != self.instance.bono_id or dia != self.instance.dia
            or fecha != self.instance.fecha
        ):
            raise serializers.ValidationError("No se puede cambiar la identidad de un registro diario.")
        inicio, fin = bono.periodo.rango_fechas()
        if fecha is None:
            coincidencias = []
            candidata = inicio
            while candidata <= fin:
                if candidata.day == dia:
                    coincidencias.append(candidata)
                candidata += timedelta(days=1)
            if len(coincidencias) != 1:
                # Los históricos ambiguos siguen editables por ID, sin inventar su fecha.
                if self.instance and "fecha" not in attrs:
                    return attrs
                raise serializers.ValidationError({"fecha": "Indica la fecha completa; el día es ambiguo o está fuera del corte."})
            fecha = coincidencias[0]
        if not inicio <= fecha <= fin or fecha.day != dia:
            raise serializers.ValidationError({"fecha": "Fecha y día deben coincidir y estar dentro del corte."})
        if fecha > timezone.localdate():
            raise serializers.ValidationError({"fecha": "No se permite capturar una fecha futura."})
        existentes = RegistroDiarioProduccion.objects.filter(bono=bono, fecha=fecha)
        if self.instance:
            existentes = existentes.exclude(pk=self.instance.pk)
        if existentes.exists():
            raise serializers.ValidationError({"fecha": "Ya existe un registro para esta fecha."})
        attrs["fecha"] = fecha
        return attrs


class RegistroDiarioSerializer(FechaRegistroMixin, serializers.ModelSerializer):
    class Meta:
        model = RegistroDiarioProduccion
        validators = []
        fields = "__all__"
        read_only_fields = [
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
            "faltas_rrhh",
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
            "faltas_rrhh",
            "dias_produccion",
            "total_embetunados",
            "pasa_uniforme",
            "pasa_puntualidad",
            "pasa_asistencia",
            "pasa_produccion",
        ]
        read_only_fields = fields


class RegistroDiarioCapturaSerializer(FechaRegistroMixin, serializers.ModelSerializer):
    class Meta:
        model = RegistroDiarioProduccion
        validators = []
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
        return super().validate(attrs)
