from __future__ import annotations

from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework import serializers

from logistica.models import (
    BitacoraRepartidor,
    BitacoraSalidaLlegada,
    EntregaRuta,
    InspeccionVehiculo,
    Repartidor,
    ReporteUnidad,
    RutaEntrega,
    Unidad,
)


class LogisticaRutaSerializer(serializers.ModelSerializer):
    class Meta:
        model = RutaEntrega
        fields = [
            "id",
            "folio",
            "nombre",
            "fecha_ruta",
            "chofer",
            "unidad",
            "estatus",
            "km_estimado",
            "notas",
            "total_entregas",
            "entregas_completadas",
            "entregas_incidencia",
            "monto_estimado_total",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "folio",
            "total_entregas",
            "entregas_completadas",
            "entregas_incidencia",
            "monto_estimado_total",
            "created_at",
            "updated_at",
        ]


class LogisticaEntregaSerializer(serializers.ModelSerializer):
    pedido_folio = serializers.CharField(source="pedido.folio", read_only=True)

    class Meta:
        model = EntregaRuta
        fields = [
            "id",
            "ruta",
            "secuencia",
            "pedido",
            "pedido_folio",
            "cliente_nombre",
            "direccion",
            "contacto",
            "telefono",
            "ventana_inicio",
            "ventana_fin",
            "estatus",
            "monto_estimado",
            "comentario",
            "evidencia_url",
            "entregado_at",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "ruta", "entregado_at", "created_at", "updated_at", "pedido_folio"]


class LogisticaEntregaCreateSerializer(serializers.Serializer):
    secuencia = serializers.IntegerField(min_value=1, required=False, default=1)
    pedido_id = serializers.IntegerField(required=False, allow_null=True)
    cliente_nombre = serializers.CharField(required=False, allow_blank=True, default="")
    direccion = serializers.CharField(required=False, allow_blank=True, default="")
    contacto = serializers.CharField(required=False, allow_blank=True, default="")
    telefono = serializers.CharField(required=False, allow_blank=True, default="")
    ventana_inicio = serializers.DateTimeField(required=False, allow_null=True)
    ventana_fin = serializers.DateTimeField(required=False, allow_null=True)
    estatus = serializers.ChoiceField(choices=EntregaRuta.ESTATUS_CHOICES, required=False, default=EntregaRuta.ESTATUS_PENDIENTE)
    monto_estimado = serializers.DecimalField(max_digits=12, decimal_places=2, required=False, default=0)
    comentario = serializers.CharField(required=False, allow_blank=True, default="")


class LogisticaUnidadSerializer(serializers.ModelSerializer):
    sucursal_codigo = serializers.CharField(source="sucursal.codigo", read_only=True)
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)

    class Meta:
        model = Unidad
        fields = ["id", "codigo", "descripcion", "sucursal", "sucursal_codigo", "sucursal_nombre", "placa", "activa"]
        read_only_fields = ["id", "sucursal_codigo", "sucursal_nombre"]


class LogisticaRepartidorSerializer(serializers.ModelSerializer):
    nombre = serializers.SerializerMethodField()
    username = serializers.CharField(source="user.username", read_only=True)
    unidad_asignada = LogisticaUnidadSerializer(read_only=True)
    sucursal_codigo = serializers.CharField(source="sucursal.codigo", read_only=True)
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)

    class Meta:
        model = Repartidor
        fields = [
            "id",
            "username",
            "nombre",
            "telefono",
            "sucursal",
            "sucursal_codigo",
            "sucursal_nombre",
            "unidad_asignada",
        ]
        read_only_fields = fields

    def get_nombre(self, obj):
        return obj.user.get_full_name() or obj.user.username


class LogisticaReporteSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    unidad_placa = serializers.CharField(source="unidad.placa", read_only=True)
    tipo_display = serializers.CharField(source="get_tipo_display", read_only=True)
    severidad_display = serializers.CharField(source="get_severidad_display", read_only=True)
    estatus_display = serializers.CharField(source="get_estatus_display", read_only=True)
    asignado_a_nombre = serializers.SerializerMethodField()

    class Meta:
        model = ReporteUnidad
        fields = [
            "id",
            "repartidor",
            "repartidor_nombre",
            "unidad",
            "unidad_codigo",
            "unidad_placa",
            "tipo",
            "tipo_display",
            "severidad",
            "severidad_display",
            "descripcion",
            "foto",
            "kilometraje",
            "latitud",
            "longitud",
            "estatus",
            "estatus_display",
            "fecha_reporte",
            "asignado_a",
            "asignado_a_nombre",
            "proveedor_servicio",
            "fecha_servicio_programado",
            "costo_servicio",
            "notas_compras",
            "notificacion_escalada",
            "actualizado_en",
        ]
        read_only_fields = [
            "id",
            "repartidor",
            "repartidor_nombre",
            "unidad_codigo",
            "unidad_placa",
            "tipo_display",
            "severidad_display",
            "estatus_display",
            "fecha_reporte",
            "asignado_a_nombre",
            "notificacion_escalada",
            "actualizado_en",
        ]

    def get_repartidor_nombre(self, obj):
        return obj.repartidor.user.get_full_name() or obj.repartidor.user.username

    def get_asignado_a_nombre(self, obj):
        if not obj.asignado_a_id:
            return ""
        return obj.asignado_a.get_full_name() or obj.asignado_a.username


class LogisticaReporteCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReporteUnidad
        fields = ["tipo", "severidad", "descripcion", "foto", "kilometraje", "latitud", "longitud"]

    def create(self, validated_data):
        repartidor = self.context["repartidor"]
        unidad = repartidor.unidad_asignada
        if unidad is None:
            raise serializers.ValidationError("No tienes una unidad asignada para levantar reportes.")
        return ReporteUnidad.objects.create(repartidor=repartidor, unidad=unidad, **validated_data)


class LogisticaReportePatchSerializer(serializers.ModelSerializer):
    asignado_a = serializers.PrimaryKeyRelatedField(queryset=get_user_model().objects.all(), required=False, allow_null=True)

    class Meta:
        model = ReporteUnidad
        fields = [
            "tipo",
            "severidad",
            "descripcion",
            "foto",
            "kilometraje",
            "latitud",
            "longitud",
            "estatus",
            "asignado_a",
            "proveedor_servicio",
            "fecha_servicio_programado",
            "costo_servicio",
            "notas_compras",
        ]


class LogisticaBitacoraSerializer(serializers.ModelSerializer):
    fecha = serializers.DateField(required=False, default=timezone.localdate)
    repartidor_nombre = serializers.SerializerMethodField()

    class Meta:
        model = BitacoraRepartidor
        fields = [
            "id",
            "repartidor",
            "repartidor_nombre",
            "fecha",
            "km_inicio",
            "km_fin",
            "novedades",
            "creado_en",
            "actualizado_en",
        ]
        read_only_fields = ["id", "repartidor", "repartidor_nombre", "creado_en", "actualizado_en"]

    def get_repartidor_nombre(self, obj):
        return obj.repartidor.user.get_full_name() or obj.repartidor.user.username

    def validate(self, attrs):
        km_inicio = attrs.get("km_inicio")
        km_fin = attrs.get("km_fin")
        if km_fin is not None and km_inicio is not None and km_fin < km_inicio:
            raise serializers.ValidationError("El kilometraje final no puede ser menor al inicial.")
        return attrs


class LogisticaBitacoraSalidaLlegadaSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    unidad_descripcion = serializers.CharField(source="unidad.descripcion", read_only=True)

    class Meta:
        model = BitacoraSalidaLlegada
        fields = [
            "id",
            "repartidor",
            "repartidor_nombre",
            "unidad",
            "unidad_codigo",
            "unidad_descripcion",
            "fecha",
            "folio",
            "hora_salida",
            "km_salida",
            "nivel_gas_salida",
            "foto_tablero_salida",
            "hora_llegada",
            "km_llegada",
            "nivel_gas_llegada",
            "foto_tablero_llegada",
            "litros_cargados",
            "costo_combustible",
            "cerrada",
            "ip_registro",
            "latitud_salida",
            "longitud_salida",
        ]
        read_only_fields = [
            "id",
            "repartidor",
            "repartidor_nombre",
            "unidad",
            "unidad_codigo",
            "unidad_descripcion",
            "fecha",
            "folio",
            "hora_salida",
            "hora_llegada",
            "cerrada",
            "ip_registro",
        ]

    def get_repartidor_nombre(self, obj):
        return obj.repartidor.user.get_full_name() or obj.repartidor.user.username


class LogisticaBitacoraSalidaCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = BitacoraSalidaLlegada
        fields = ["unidad", "km_salida", "nivel_gas_salida", "foto_tablero_salida", "latitud_salida", "longitud_salida"]

    def create(self, validated_data):
        repartidor = self.context["repartidor"]
        unidad = validated_data.pop("unidad", None)
        if not unidad or not unidad.activa:
            raise serializers.ValidationError("Selecciona una unidad activa para iniciar bitácora.")
        return BitacoraSalidaLlegada.objects.create(
            repartidor=repartidor,
            unidad=unidad,
            **validated_data,
        )


class LogisticaBitacoraLlegadaSerializer(serializers.ModelSerializer):
    class Meta:
        model = BitacoraSalidaLlegada
        fields = ["km_llegada", "nivel_gas_llegada", "litros_cargados", "costo_combustible", "foto_tablero_llegada"]

    def validate(self, attrs):
        km_llegada = attrs.get("km_llegada")
        if km_llegada is None and self.instance and self.instance.km_llegada is None:
            raise serializers.ValidationError("El kilometraje de llegada es obligatorio.")
        if not attrs.get("nivel_gas_llegada") and self.instance and not self.instance.nivel_gas_llegada:
            raise serializers.ValidationError("El nivel de gas de llegada es obligatorio.")
        if km_llegada is not None and self.instance and km_llegada < self.instance.km_salida:
            raise serializers.ValidationError("El kilometraje de llegada no puede ser menor al de salida.")
        if not attrs.get("foto_tablero_llegada") and self.instance and not self.instance.foto_tablero_llegada:
            raise serializers.ValidationError("La foto del tablero de llegada es obligatoria.")
        return attrs


class LogisticaInspeccionVehiculoSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    unidad_descripcion = serializers.CharField(source="unidad.descripcion", read_only=True)

    class Meta:
        model = InspeccionVehiculo
        fields = "__all__"
        read_only_fields = ["id", "repartidor", "unidad", "fecha", "ip_registro", "repartidor_nombre", "unidad_codigo", "unidad_descripcion"]

    def get_repartidor_nombre(self, obj):
        return obj.repartidor.user.get_full_name() or obj.repartidor.user.username


class LogisticaInspeccionVehiculoCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = InspeccionVehiculo
        exclude = ["repartidor", "unidad", "fecha", "ip_registro"]

    def create(self, validated_data):
        repartidor = self.context["repartidor"]
        if repartidor.unidad_asignada is None:
            raise serializers.ValidationError("No tienes una unidad asignada para inspeccionar.")
        return InspeccionVehiculo.objects.create(
            repartidor=repartidor,
            unidad=repartidor.unidad_asignada,
            **validated_data,
        )
