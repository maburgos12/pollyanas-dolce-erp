from __future__ import annotations

from datetime import datetime, time

from django.core.exceptions import ValidationError as DjangoValidationError
from django.utils import timezone
from django.utils.crypto import constant_time_compare
from rest_framework import serializers

from core.access import can_view_rrhh

from .models import AsistenciaEmpleado, Empleado, HoraExtra, PermisoSalida, Prestamo, SolicitudVacaciones
from .services_prestamos import can_autorizar_prestamo_direccion, can_autorizar_prestamo_jefe
from .services_permisos import permisos_empleados_usuario
from .services_permisos_repetidos import calcular_permiso_repetido


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
            "salida_comida",
            "regreso_comida",
            "salida",
            "minutos_comida",
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


def validar_fechas_permiso(tipo, inicio, fin, modalidad=None):
    if fin and fin < inicio:
        raise serializers.ValidationError({"fecha_fin": "La fecha final no puede ser anterior al inicio."})
    if modalidad == "laborables":
        if tipo != PermisoSalida.TIPO_PERMISO_HORA:
            raise serializers.ValidationError({"modalidad_dias": "Selecciona tiempo parcial."})
        return inicio, fin
    if not modalidad:
        return inicio, fin
    if modalidad == "dia" and tipo == PermisoSalida.TIPO_PERMISO_HORA:
        if fin and timezone.localdate(fin) != timezone.localdate(inicio):
            raise serializers.ValidationError({"fecha_fin": "Para repetir el horario, selecciona días laborables."})
        return inicio, fin
    if tipo != PermisoSalida.TIPO_PERMISO_DIA:
        raise serializers.ValidationError({"modalidad_dias": "Selecciona día completo para usar esta modalidad."})
    zona = timezone.get_default_timezone()
    primer_dia = timezone.localtime(inicio, zona).date()
    inicio = datetime.combine(primer_dia, time.min, zona)
    if modalidad == "dia":
        return inicio, None
    if not fin:
        raise serializers.ValidationError({"fecha_fin": "Selecciona el último día del rango."})
    ultimo_dia = timezone.localtime(fin, zona).date()
    return inicio, datetime.combine(ultimo_dia, time.max, zona)


class FechasPermisoSerializer(serializers.Serializer):
    fecha_inicio = serializers.DateTimeField()
    fecha_fin = serializers.DateTimeField(required=False, allow_null=True)
    modalidad_dias = serializers.ChoiceField(choices=("dia", "rango", "laborables"), required=False, write_only=True)

    vista_previa = serializers.BooleanField(required=False, write_only=True)
    confirmacion = serializers.CharField(required=False, allow_blank=True, write_only=True)

    @staticmethod
    def procesar_repeticion(empleado, fechas, modalidad, confirmacion, vista_previa):
        if modalidad != "laborables":
            if vista_previa:
                raise serializers.ValidationError({"modalidad_dias": "Selecciona repetir en días laborables."})
            return []
        try:
            resumen = calcular_permiso_repetido(empleado, fechas["fecha_inicio"], fechas.get("fecha_fin"))
        except DjangoValidationError as exc:
            raise serializers.ValidationError({"fecha_fin": exc.messages}) from exc
        if vista_previa:
            return resumen
        if not constant_time_compare(confirmacion or "", resumen["confirmacion"]):
            raise serializers.ValidationError({"confirmacion": "Revisa y confirma las fechas laborables actualizadas."})
        return resumen["fechas_laborables"]


class PermisoSalidaSerializer(FechasPermisoSerializer, serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)

    class Meta:
        model = PermisoSalida
        fields = [
            "id",
            "empleado",
            "empleado_nombre",
            "tipo",
            "modalidad_dias",
            "vista_previa",
            "confirmacion",
            "fechas_laborables",
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
            "fechas_laborables",
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
        if request and not permisos_empleados_usuario(request.user).filter(pk=empleado.pk).exists():
            raise serializers.ValidationError("Empleado fuera de tu equipo o inactivo.")
        return empleado

    def validate(self, attrs):
        modalidad = attrs.get("modalidad_dias")
        attrs["fecha_inicio"], attrs["fecha_fin"] = validar_fechas_permiso(
            attrs["tipo"], attrs["fecha_inicio"], attrs.get("fecha_fin"), modalidad,
        )
        return attrs


    def repeticion(self, empleado):
        datos = self.validated_data
        return self.procesar_repeticion(
            empleado, datos, datos.get("modalidad_dias"), datos.get("confirmacion"), datos.get("vista_previa"),
        )

    def create(self, validated_data):
        for campo in ("modalidad_dias", "confirmacion", "vista_previa"):
            validated_data.pop(campo, None)
        return super().create(validated_data)


class SolicitudVacacionesSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    jefe_directo_nombre = serializers.SerializerMethodField()

    class Meta:
        model = SolicitudVacaciones
        fields = [
            "id",
            "folio",
            "empleado",
            "empleado_nombre",
            "fecha_inicio",
            "fecha_fin",
            "dias_laborables",
            "motivo",
            "estado",
            "jefe_directo",
            "jefe_directo_nombre",
            "preautorizado_por",
            "fecha_preautorizacion",
            "aprobado_rrhh_por",
            "fecha_aprobacion_rrhh",
            "creado_en",
        ]
        read_only_fields = [
            "folio",
            "dias_laborables",
            "estado",
            "jefe_directo",
            "jefe_directo_nombre",
            "preautorizado_por",
            "fecha_preautorizacion",
            "aprobado_rrhh_por",
            "fecha_aprobacion_rrhh",
            "creado_en",
        ]
        extra_kwargs = {"empleado": {"required": False}}

    def get_jefe_directo_nombre(self, obj):
        if not obj.jefe_directo_id:
            return ""
        return obj.jefe_directo.get_full_name() or obj.jefe_directo.username


class PrestamoSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    jefe_directo_nombre = serializers.SerializerMethodField()
    estado_display = serializers.CharField(source="get_estado_display", read_only=True)
    metodo_pago_display = serializers.CharField(source="get_metodo_pago_display", read_only=True)
    puede_autorizar_jefe = serializers.SerializerMethodField()
    puede_aprobar_direccion = serializers.SerializerMethodField()

    class Meta:
        model = Prestamo
        fields = [
            "id",
            "folio",
            "empleado",
            "empleado_nombre",
            "concepto",
            "metodo_pago",
            "metodo_pago_display",
            "fecha_solicitud",
            "fecha_deposito",
            "importe",
            "num_quincenas",
            "descuento_quincenal",
            "saldo_actual",
            "estado",
            "estado_display",
            "jefe_directo",
            "jefe_directo_nombre",
            "puede_autorizar_jefe",
            "puede_aprobar_direccion",
            "creado_en",
        ]
        read_only_fields = [
            "folio",
            "empleado_nombre",
            "fecha_solicitud",
            "descuento_quincenal",
            "saldo_actual",
            "estado",
            "estado_display",
            "jefe_directo",
            "jefe_directo_nombre",
            "puede_autorizar_jefe",
            "puede_aprobar_direccion",
            "creado_en",
        ]
        extra_kwargs = {"empleado": {"required": False}}

    def validate_importe(self, value):
        if value <= 0:
            raise serializers.ValidationError("El importe debe ser mayor a cero.")
        return value

    def validate_num_quincenas(self, value):
        if value <= 0:
            raise serializers.ValidationError("Indica al menos una quincena.")
        return value

    def get_jefe_directo_nombre(self, obj):
        if not obj.jefe_directo_id:
            return ""
        return obj.jefe_directo.get_full_name() or obj.jefe_directo.username

    def get_puede_autorizar_jefe(self, obj):
        request = self.context.get("request")
        return can_autorizar_prestamo_jefe(getattr(request, "user", None), obj)

    def get_puede_aprobar_direccion(self, obj):
        request = self.context.get("request")
        user = getattr(request, "user", None)
        return obj.estado == Prestamo.ESTADO_AUTORIZADO and can_autorizar_prestamo_direccion(user, obj)
