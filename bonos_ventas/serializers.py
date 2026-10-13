from django.utils import timezone
from rest_framework import serializers

from rrhh.models import Empleado

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, RegistroDiarioVentas, VentaCategoriaSucursal, fecha_registro_legacy


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
        validators = []
        fields = "__all__"
        read_only_fields = ["capturado_por", "creado_en", "actualizado_en"]

    def validate(self, attrs):
        bono = attrs.get("bono") or getattr(self.instance, "bono", None)
        dia = attrs.get("dia", getattr(self.instance, "dia", None))
        fecha = attrs.get("fecha", getattr(self.instance, "fecha", None))
        if self.instance and (
            bono.pk != self.instance.bono_id or dia != self.instance.dia or fecha != self.instance.fecha
        ):
            raise serializers.ValidationError("No se puede cambiar la identidad de un registro diario.")
        if self.instance and self.instance.fecha is not None and "fecha" not in attrs:
            raise serializers.ValidationError({"fecha": "La fecha completa es obligatoria; actualiza el calendario antes de editar."})
        if not self.instance and bono.registros.filter(fecha__isnull=True).exists():
            raise serializers.ValidationError("El bono tiene históricos sin fecha; revisarlos antes de agregar días.")
        if fecha is None:
            fecha = fecha_registro_legacy(bono.periodo, dia, captura_manual=True)
            if fecha is None:
                if self.instance and set(attrs) <= {"puntos_de_vista"}:
                    return attrs
                raise serializers.ValidationError({"fecha": "Indica la fecha completa; el día legado no identifica una fecha segura."})
        inicio, fin = bono.periodo.rango_fechas()
        if not inicio <= fecha <= fin or fecha.day != dia:
            raise serializers.ValidationError({"fecha": "Fecha y día deben coincidir y estar dentro del corte."})
        if fecha > timezone.localdate():
            raise serializers.ValidationError({"fecha": "No se permite capturar una fecha futura."})
        existentes = RegistroDiarioVentas.objects.filter(bono=bono, fecha=fecha)
        if self.instance:
            existentes = existentes.exclude(pk=self.instance.pk)
        if existentes.exists():
            raise serializers.ValidationError({"fecha": "Ya existe un registro para esta fecha."})
        attrs["fecha"] = fecha
        return attrs


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
            "cancela_bono",
            "cancela_motivo",
            "monto_uniforme",
            "monto_asistencia",
            "monto_puntualidad",
            "sub1",
            "pct_efectividad_entrega",
            "monto_bono_entregas",
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
    es_repartidor = serializers.SerializerMethodField()

    def get_es_repartidor(self, obj):
        return obj._es_repartidor()

    class Meta:
        model = BonoVentasEmpleado
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "empleado_codigo",
            "sucursal",
            "sucursal_nombre",
            "es_repartidor",
            "dias_trabajados",
            "dias_asistencia",
            "dias_uniforme",
            "dias_puntualidad",
            "pasa_uniforme",
            "pasa_asistencia",
            "pasa_puntualidad",
            "cancela_bono",
            "cancela_motivo",
            "sub1",
            "dias_con_bitacora",
            "pct_efectividad_entrega",
            "monto_bono_entregas",
            "bono_ventas",
            "pasa_bono_ventas",
            "ajuste_positivo",
            "ajuste_negativo",
            "bono_extra",
            "total_a_pagar",
            "estatus",
        ]
