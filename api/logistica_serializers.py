from __future__ import annotations

import json
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db.models import Q
from django.utils import timezone
from rest_framework import serializers

from logistica.models import (
    BitacoraRepartidor,
    BitacoraSalidaLlegada,
    CargaCombustibleUnidad,
    EntregaRuta,
    EventoRuta,
    InspeccionDiaria,
    InspeccionVehiculo,
    LavadoUnidad,
    ParadaRuta,
    ParadaEntregaEvidencia,
    PuntoLogistico,
    Repartidor,
    ReporteUnidad,
    ReporteUnidadReafirmacion,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
    UbicacionRuta,
    Unidad,
)
from logistica.domain_ruta import parada_resuelta_operativamente, point_transfer_enviada
from logistica.services_entregas import geocercas_confiables_por_parada, tiene_llegada_geocerca_confiable
from logistica.services_rutas_control import validar_coordenadas
from rrhh.services_identidad import nombre_operativo_usuario


def _format_quantity(value):
    if value is None:
        return None
    quantity = Decimal(str(value)).quantize(Decimal("0.01"))
    return format(quantity.normalize(), "f")


class LogisticaRutaSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()
    acompanante_nombre = serializers.SerializerMethodField()
    unidad_operativa_codigo = serializers.CharField(source="unidad_operativa.codigo", read_only=True)
    estatus_display = serializers.CharField(source="get_estatus_display", read_only=True)

    class Meta:
        model = RutaEntrega
        fields = [
            "id",
            "folio",
            "nombre",
            "fecha_ruta",
            "chofer",
            "unidad",
            "repartidor",
            "repartidor_nombre",
            "acompanante",
            "acompanante_nombre",
            "acompanante_manual",
            "unidad_operativa",
            "unidad_operativa_codigo",
            "bitacora_salida",
            "estatus",
            "estatus_display",
            "km_estimado",
            "notas",
            "hora_inicio_real",
            "hora_cierre_real",
            "cumplimiento_porcentaje",
            "ruta_programada_polyline",
            "ruta_programada_distancia_metros",
            "ruta_programada_duracion_segundos",
            "ruta_programada_fuente",
            "ruta_programada_actualizada_en",
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
            "repartidor_nombre",
            "acompanante_nombre",
            "unidad_operativa_codigo",
            "estatus_display",
            "bitacora_salida",
            "hora_inicio_real",
            "hora_cierre_real",
            "cumplimiento_porcentaje",
            "ruta_programada_polyline",
            "ruta_programada_distancia_metros",
            "ruta_programada_duracion_segundos",
            "ruta_programada_fuente",
            "ruta_programada_actualizada_en",
            "total_entregas",
            "entregas_completadas",
            "entregas_incidencia",
            "monto_estimado_total",
            "created_at",
            "updated_at",
        ]

    def get_repartidor_nombre(self, obj):
        if not obj.repartidor_id:
            return ""
        return nombre_operativo_usuario(obj.repartidor.user)

    def get_acompanante_nombre(self, obj):
        if not obj.acompanante_id:
            return ""
        return nombre_operativo_usuario(obj.acompanante.user)

    def validate(self, attrs):
        unidad_operativa = attrs.get("unidad_operativa") or getattr(self.instance, "unidad_operativa", None)
        if unidad_operativa and not unidad_operativa.activa:
            raise serializers.ValidationError({"unidad_operativa": "La unidad operativa debe estar activa."})
        return attrs


class LogisticaRutaParadaCreateSerializer(serializers.Serializer):
    punto_id = serializers.IntegerField()
    orden = serializers.IntegerField(min_value=1, required=False)


class LogisticaRutaCreateSerializer(serializers.Serializer):
    nombre = serializers.CharField()
    fecha_ruta = serializers.DateField()
    repartidor = serializers.PrimaryKeyRelatedField(queryset=Repartidor.objects.all())
    acompanante = serializers.PrimaryKeyRelatedField(queryset=Repartidor.objects.all(), required=False, allow_null=True)
    acompanante_manual = serializers.CharField(required=False, allow_blank=True, default="")
    unidad_operativa = serializers.PrimaryKeyRelatedField(queryset=Unidad.objects.filter(activa=True))
    chofer = serializers.CharField(required=False, allow_blank=True, default="")
    unidad = serializers.CharField(required=False, allow_blank=True, default="")
    estatus = serializers.ChoiceField(choices=RutaEntrega.ESTATUS_CHOICES, required=False, default=RutaEntrega.ESTATUS_PLANEADA)
    km_estimado = serializers.DecimalField(max_digits=10, decimal_places=2, required=False, default=Decimal("0"))
    notas = serializers.CharField(required=False, allow_blank=True, default="")
    paradas = LogisticaRutaParadaCreateSerializer(many=True)

    def validate(self, attrs):
        if attrs.get("estatus", RutaEntrega.ESTATUS_PLANEADA) != RutaEntrega.ESTATUS_PLANEADA:
            raise serializers.ValidationError({"estatus": "Crea la ruta como planeada y libérala desde el flujo de planeación."})
        if not attrs.get("paradas"):
            raise serializers.ValidationError({"paradas": "Selecciona al menos una sucursal o punto para planear la ruta del día."})
        return attrs


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


class PuntoLogisticoSerializer(serializers.ModelSerializer):
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)
    tipo_display = serializers.CharField(source="get_tipo_display", read_only=True)

    class Meta:
        model = PuntoLogistico
        fields = [
            "id",
            "sucursal",
            "sucursal_nombre",
            "nombre",
            "tipo",
            "tipo_display",
            "latitud",
            "longitud",
            "radio_geocerca_metros",
            "activo",
            "notas",
        ]
        read_only_fields = ["id", "sucursal_nombre"]


class ParadaRutaListSerializer(serializers.ListSerializer):
    def to_representation(self, data):
        iterable = data.all() if hasattr(data, "all") else data
        paradas = list(iterable)
        if "_geocercas_confiables" not in self.context:
            self.context["_geocercas_confiables"] = geocercas_confiables_por_parada(paradas)
        cedis_iniciales = {
            parada.id
            for parada in paradas
            if parada.punto.tipo == PuntoLogistico.TIPO_CEDIS and parada.orden == 1
        }
        if "_recarga_cedis_parada_ids" not in self.context:
            cedis_ids = [
                parada.id
                for parada in paradas
                if parada.punto.tipo == PuntoLogistico.TIPO_CEDIS
            ]
            self.context["_recarga_cedis_parada_ids"] = (
                set(
                    EventoRuta.objects.filter(parada_id__in=cedis_ids)
                    .filter(
                        Q(tipo=EventoRuta.TIPO_RECARGA_CEDIS)
                        | Q(
                            tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
                            metadata__tipo__in=["recarga_cedis", "recarga_cedis_pwa"],
                        )
                    )
                    .values_list("parada_id", flat=True)
                )
                if cedis_ids
                else set()
            )
        self.context["_recarga_cedis_parada_ids"] = set(
            self.context["_recarga_cedis_parada_ids"]
        ) | cedis_iniciales
        return [self.child.to_representation(parada) for parada in paradas]


class ParadaRutaSerializer(serializers.ModelSerializer):
    punto = PuntoLogisticoSerializer(read_only=True)
    estado_display = serializers.CharField(source="get_estado_display", read_only=True)
    entrega_estado = serializers.SerializerMethodField()
    entrega_estado_display = serializers.SerializerMethodField()
    entrega_confirmada_por_nombre = serializers.SerializerMethodField()
    geocerca_confiable = serializers.SerializerMethodField()
    operativamente_resuelta = serializers.SerializerMethodField()
    recarga_cedis_resuelta = serializers.SerializerMethodField()
    revision_entrega_revisada_por = serializers.IntegerField(source="revision_entrega_revisada_por_id", read_only=True)
    revision_entrega_revisada_por_nombre = serializers.SerializerMethodField()

    class Meta:
        model = ParadaRuta
        list_serializer_class = ParadaRutaListSerializer
        fields = [
            "id",
            "ruta",
            "punto",
            "orden",
            "punto_nombre_snapshot",
            "latitud_geocerca",
            "longitud_geocerca",
            "radio_geocerca_metros",
            "hora_estimada",
            "hora_llegada_real",
            "hora_salida_real",
            "estado",
            "estado_display",
            "entrega_estado",
            "entrega_estado_display",
            "entrega_confirmada_en",
            "entrega_confirmada_por_nombre",
            "entrega_notas",
            "geocerca_confiable",
            "operativamente_resuelta",
            "recarga_cedis_resuelta",
            "revision_entrega_estado",
            "revision_entrega_causa",
            "revision_entrega_datos",
            "revision_entrega_revisada_en",
            "revision_entrega_revisada_por",
            "revision_entrega_revisada_por_nombre",
            "revision_entrega_resolucion",
            "distancia_llegada_metros",
            "notas",
        ]
        read_only_fields = fields

    def get_entrega_estado(self, obj):
        if obj.punto.tipo == PuntoLogistico.TIPO_CEDIS:
            return "NO_APLICA"
        return obj.entrega_estado

    def get_entrega_estado_display(self, obj):
        if obj.punto.tipo == PuntoLogistico.TIPO_CEDIS:
            return "No aplica"
        return obj.get_entrega_estado_display()

    def get_entrega_confirmada_por_nombre(self, obj):
        if not obj.entrega_confirmada_por_id:
            return ""
        return nombre_operativo_usuario(obj.entrega_confirmada_por)

    def get_geocerca_confiable(self, obj):
        cache = self.context.get("_geocercas_confiables")
        if cache is not None:
            return obj.id in cache
        return tiene_llegada_geocerca_confiable(ruta=obj.ruta, parada=obj)

    def get_operativamente_resuelta(self, obj):
        return parada_resuelta_operativamente(obj)

    def get_recarga_cedis_resuelta(self, obj):
        if obj.punto.tipo != PuntoLogistico.TIPO_CEDIS:
            return False
        if obj.orden == 1:
            return True

        cache = self.context.get("_recarga_cedis_parada_ids")
        if cache is not None:
            return obj.id in cache

        return EventoRuta.objects.filter(ruta_id=obj.ruta_id, parada_id=obj.id).filter(
            Q(tipo=EventoRuta.TIPO_RECARGA_CEDIS)
            | Q(
                tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
                metadata__tipo__in=["recarga_cedis", "recarga_cedis_pwa"],
            )
        ).exists()

    def get_revision_entrega_revisada_por_nombre(self, obj):
        if not obj.revision_entrega_revisada_por_id:
            return ""
        return nombre_operativo_usuario(obj.revision_entrega_revisada_por)


class UbicacionRutaSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)

    class Meta:
        model = UbicacionRuta
        fields = [
            "id",
            "ruta",
            "repartidor",
            "repartidor_nombre",
            "unidad",
            "unidad_codigo",
            "latitud",
            "longitud",
            "precision_metros",
            "velocidad_kmh",
            "bateria_porcentaje",
            "timestamp_dispositivo",
            "timestamp_servidor",
            "fuera_de_geocerca",
        ]
        read_only_fields = fields

    def get_repartidor_nombre(self, obj):
        return nombre_operativo_usuario(obj.repartidor.user)


class UbicacionRutaCreateSerializer(serializers.Serializer):
    TRACKING_ORIGEN_CHOICES = ["manual_pwa", "automatico_pwa"]

    latitud = serializers.DecimalField(max_digits=9, decimal_places=6)
    longitud = serializers.DecimalField(max_digits=9, decimal_places=6)
    precision_metros = serializers.DecimalField(max_digits=8, decimal_places=2, min_value=Decimal("0"), required=False, allow_null=True)
    velocidad_kmh = serializers.DecimalField(max_digits=8, decimal_places=2, min_value=Decimal("0"), required=False, allow_null=True)
    bateria_porcentaje = serializers.IntegerField(min_value=0, max_value=100, required=False, allow_null=True)
    timestamp_dispositivo = serializers.DateTimeField(required=False, allow_null=True)
    client_event_id = serializers.CharField(required=False, allow_blank=True, max_length=80, default="")
    fuera_de_ruta_confirmado = serializers.BooleanField(required=False, default=False)
    desvio_motivo = serializers.CharField(required=False, allow_blank=True, default="")
    tracking_origen = serializers.ChoiceField(choices=TRACKING_ORIGEN_CHOICES, required=False, default="manual_pwa")

    def validate(self, attrs):
        try:
            validar_coordenadas(attrs.get("latitud"), attrs.get("longitud"))
        except Exception as exc:
            if hasattr(exc, "message_dict"):
                raise serializers.ValidationError(exc.message_dict)
            raise
        if attrs.get("fuera_de_ruta_confirmado") is True and not (attrs.get("desvio_motivo") or "").strip():
            raise serializers.ValidationError({"desvio_motivo": "Indica un motivo breve para confirmar el desvío."})
        return attrs


class RutaCargaChecklistLineaSerializer(serializers.ModelSerializer):
    parada_nombre = serializers.CharField(source="parada.punto_nombre_snapshot", read_only=True)
    parada_orden = serializers.IntegerField(source="parada.orden", read_only=True)
    cantidad_solicitada = serializers.SerializerMethodField()
    cantidad_solicitada_point = serializers.SerializerMethodField()
    cantidad_enviada_esperada = serializers.SerializerMethodField()
    cantidad_enviada_point = serializers.SerializerMethodField()
    cantidad_cargada = serializers.SerializerMethodField()
    cantidad_cargada_pwa = serializers.SerializerMethodField()
    estatus_display = serializers.CharField(source="get_estatus_display", read_only=True)
    motivo_diferencia_display = serializers.CharField(source="get_motivo_diferencia_display", read_only=True)
    validado_por_nombre = serializers.SerializerMethodField()
    point_is_received = serializers.SerializerMethodField()
    point_received_quantity = serializers.SerializerMethodField()
    point_received_at = serializers.SerializerMethodField()
    point_received_by = serializers.SerializerMethodField()
    point_recepcion_estado = serializers.SerializerMethodField()
    point_enviada = serializers.SerializerMethodField()

    class Meta:
        model = RutaCargaChecklistLinea
        fields = [
            "id",
            "checklist",
            "parada",
            "parada_nombre",
            "parada_orden",
            "transfer_external_id",
            "detail_external_id",
            "source_hash",
            "item_code",
            "item_name",
            "unit",
            "cantidad_solicitada",
            "cantidad_solicitada_point",
            "cantidad_enviada_esperada",
            "cantidad_enviada_point",
            "point_enviada",
            "cantidad_cargada",
            "cantidad_cargada_pwa",
            "estatus",
            "estatus_display",
            "motivo_diferencia",
            "motivo_diferencia_display",
            "notas",
            "validado_por_nombre",
            "validado_en",
            "point_is_received",
            "point_received_quantity",
            "point_received_at",
            "point_received_by",
            "point_recepcion_estado",
        ]
        read_only_fields = fields

    def get_validado_por_nombre(self, obj):
        if not obj.validado_por_id:
            return ""
        return nombre_operativo_usuario(obj.validado_por)

    def get_cantidad_solicitada(self, obj):
        return self.get_cantidad_solicitada_point(obj)

    def get_cantidad_solicitada_point(self, obj):
        point_line = self._point_line(obj)
        if point_line:
            return _format_quantity(point_line.requested_quantity)
        return _format_quantity(obj.cantidad_solicitada)

    def get_cantidad_enviada_esperada(self, obj):
        return self.get_cantidad_enviada_point(obj)

    def get_cantidad_enviada_point(self, obj):
        point_line = self._point_line(obj)
        if point_line:
            return _format_quantity(point_line.sent_quantity)
        return _format_quantity(obj.cantidad_enviada_esperada)

    def get_point_enviada(self, obj):
        point_line = self._point_line(obj)
        return bool(point_line and point_transfer_enviada(point_line))

    def get_cantidad_cargada(self, obj):
        return self.get_cantidad_cargada_pwa(obj)

    def get_cantidad_cargada_pwa(self, obj):
        return _format_quantity(obj.cantidad_cargada)

    def _point_line(self, obj):
        return getattr(obj, "point_transfer_line", None)

    def get_point_is_received(self, obj):
        point_line = self._point_line(obj)
        return bool(point_line and point_line.is_received)

    def get_point_received_quantity(self, obj):
        point_line = self._point_line(obj)
        if not point_line or not point_line.is_received:
            return None
        return _format_quantity(point_line.received_quantity)

    def get_point_received_at(self, obj):
        point_line = self._point_line(obj)
        if not point_line or not point_line.received_at:
            return None
        return point_line.received_at

    def get_point_received_by(self, obj):
        point_line = self._point_line(obj)
        if not point_line or not point_line.is_received:
            return ""
        return point_line.received_by

    def get_point_recepcion_estado(self, obj):
        point_line = self._point_line(obj)
        if not point_line or not point_line.is_received:
            return "PENDIENTE_POINT"
        cantidad_enviada = point_line.sent_quantity if point_line else obj.cantidad_enviada_esperada
        esperado = Decimal(str(obj.cantidad_cargada if obj.cantidad_cargada is not None else cantidad_enviada or 0))
        recibido = Decimal(str(point_line.received_quantity or 0))
        if recibido == esperado:
            return "RECIBIDO_OK"
        if recibido == 0:
            return "RECIBIDO_CERO"
        return "RECIBIDO_DIFERENCIA"


class RutaCargaChecklistSerializer(serializers.ModelSerializer):
    estatus_display = serializers.CharField(source="get_estatus_display", read_only=True)
    lineas = RutaCargaChecklistLineaSerializer(many=True, read_only=True)
    total_lineas = serializers.SerializerMethodField()
    lineas_confirmadas = serializers.SerializerMethodField()
    lineas_pendientes = serializers.SerializerMethodField()

    class Meta:
        model = RutaCargaChecklist
        fields = [
            "id",
            "ruta",
            "estatus",
            "estatus_display",
            "sincronizado_en",
            "confirmado_en",
            "motivo_override",
            "notas",
            "total_lineas",
            "lineas_confirmadas",
            "lineas_pendientes",
            "lineas",
        ]
        read_only_fields = fields

    def _lineas_prefetched(self, obj):
        cache = getattr(obj, "_prefetched_objects_cache", {})
        return cache.get("lineas")

    def get_total_lineas(self, obj):
        lineas = self._lineas_prefetched(obj)
        if lineas is not None:
            return len(lineas)
        return obj.lineas.count()

    def get_lineas_confirmadas(self, obj):
        lineas = self._lineas_prefetched(obj)
        if lineas is not None:
            return sum(
                1
                for linea in lineas
                if linea.estatus
                not in (RutaCargaChecklistLinea.ESTATUS_PENDIENTE, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
            )
        return (
            obj.lineas.exclude(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
            .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
            .count()
        )

    def get_lineas_pendientes(self, obj):
        lineas = self._lineas_prefetched(obj)
        if lineas is not None:
            return sum(1 for linea in lineas if linea.estatus == RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        return obj.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).count()


class RutaCargaLineaValidarSerializer(serializers.Serializer):
    cantidad_cargada = serializers.DecimalField(max_digits=18, decimal_places=3, min_value=Decimal("0"))
    motivo_diferencia = serializers.ChoiceField(
        choices=RutaCargaChecklistLinea.MOTIVO_CHOICES,
        required=False,
        allow_blank=True,
        default="",
    )
    notas = serializers.CharField(required=False, allow_blank=True, default="")
    client_event_id = serializers.CharField(required=False, allow_blank=True, max_length=80, default="")


class RutaCargaProductoTramoValidarSerializer(serializers.Serializer):
    item_code = serializers.CharField(required=False, allow_blank=True, max_length=120, default="")
    item_name = serializers.CharField(max_length=255)
    unit = serializers.CharField(required=False, allow_blank=True, max_length=50, default="")
    cantidad_cargada = serializers.DecimalField(max_digits=18, decimal_places=3, min_value=Decimal("0"))
    client_event_id = serializers.CharField(required=False, allow_blank=True, max_length=80, default="")


class ParadaEntregaEvidenciaCreateSerializer(serializers.Serializer):
    linea_carga_id = serializers.IntegerField(required=False, allow_null=True)
    tipo = serializers.ChoiceField(
        choices=ParadaEntregaEvidencia.TIPO_CHOICES,
        required=False,
        default=ParadaEntregaEvidencia.TIPO_CONFIRMACION,
    )
    cantidad_entregada = serializers.DecimalField(
        max_digits=18,
        decimal_places=3,
        min_value=Decimal("0"),
        required=False,
        allow_null=True,
    )
    comentario = serializers.CharField(required=False, allow_blank=True, default="")
    latitud = serializers.DecimalField(max_digits=9, decimal_places=6, required=False, allow_null=True)
    longitud = serializers.DecimalField(max_digits=9, decimal_places=6, required=False, allow_null=True)
    precision_metros = serializers.DecimalField(
        max_digits=8,
        decimal_places=2,
        min_value=Decimal("0"),
        required=False,
        allow_null=True,
    )
    client_event_id = serializers.CharField(required=False, allow_blank=True, max_length=80, default="")

    def validate(self, attrs):
        latitud = attrs.get("latitud")
        longitud = attrs.get("longitud")
        if (latitud is None) != (longitud is None):
            raise serializers.ValidationError("Latitud y longitud deben enviarse juntas.")
        if latitud is not None and longitud is not None:
            validar_coordenadas(latitud, longitud)
        return attrs


class ParadaEntregaConfirmarSerializer(serializers.Serializer):
    entrega_estado = serializers.ChoiceField(choices=ParadaRuta.ENTREGA_ESTADO_CHOICES)
    notas = serializers.CharField(required=False, allow_blank=True, default="")
    client_event_id = serializers.CharField(required=False, allow_blank=True, max_length=80, default="")
    client_context = serializers.DictField(required=False, default=dict)
    evidencias = ParadaEntregaEvidenciaCreateSerializer(many=True, required=False, default=list)

    def validate(self, attrs):
        entrega_estado = attrs["entrega_estado"]
        notas = (attrs.get("notas") or "").strip()
        evidencias = attrs.get("evidencias") or []
        client_context = attrs.get("client_context") or {}
        allowed_context = {
            "causa", "latitud", "longitud", "precision_metros", "distancia_metros",
            "client_timestamp", "client_version",
        }
        extra = set(client_context) - allowed_context
        if extra:
            raise serializers.ValidationError({"client_context": f"Campos no permitidos: {', '.join(sorted(extra))}."})
        if len(json.dumps(client_context, default=str)) > 2048:
            raise serializers.ValidationError({"client_context": "El contexto excede el tamaño permitido."})
        validators = {
            "causa": serializers.ChoiceField(choices=[
                "GPS_SIN_SENAL", "FUERA_DE_RADIO", "DENTRO_GEOFENCE",
                "AJUSTE_ADMINISTRATIVO", "GEOFENCE_LEGACY_NO_CONFIABLE",
                "PRECISION_INSUFICIENTE", "UBICACION_TARDIA", "SALTO_IMPOSIBLE",
                "SUCURSAL_SIN_COORDENADAS", "GPS_DENEGADO",
            ]),
            "latitud": serializers.DecimalField(max_digits=9, decimal_places=6, min_value=Decimal("-90"), max_value=Decimal("90")),
            "longitud": serializers.DecimalField(max_digits=9, decimal_places=6, min_value=Decimal("-180"), max_value=Decimal("180")),
            "precision_metros": serializers.DecimalField(max_digits=8, decimal_places=2, min_value=Decimal("0"), max_value=Decimal("999999.99")),
            "distancia_metros": serializers.IntegerField(min_value=0, max_value=10_000_000),
            "client_timestamp": serializers.DateTimeField(),
            "client_version": serializers.CharField(max_length=120),
        }
        for key, field in validators.items():
            if key in client_context and client_context[key] is not None:
                try:
                    client_context[key] = field.run_validation(client_context[key])
                except serializers.ValidationError as exc:
                    raise serializers.ValidationError({"client_context": {key: exc.detail}})
        client_event_id = (attrs.get("client_event_id") or "").strip()
        if not client_event_id and evidencias:
            client_event_id = str(evidencias[0].get("client_event_id") or "").strip()
        attrs["client_event_id"] = client_event_id
        if entrega_estado == ParadaRuta.ENTREGA_PENDIENTE:
            raise serializers.ValidationError({"entrega_estado": "La entrega debe quedar entregada, con diferencia o no entregada."})
        if entrega_estado in {ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA}:
            comentarios = [str(evidencia.get("comentario") or "").strip() for evidencia in evidencias]
            if not notas and not any(comentarios):
                raise serializers.ValidationError({"notas": "Describe la diferencia o el motivo de no entrega."})
        return attrs


class ParadaEntregaEvidenciaSerializer(serializers.ModelSerializer):
    tipo_display = serializers.CharField(source="get_tipo_display", read_only=True)
    capturado_por_nombre = serializers.SerializerMethodField()

    class Meta:
        model = ParadaEntregaEvidencia
        fields = [
            "id",
            "ruta",
            "parada",
            "linea_carga",
            "tipo",
            "tipo_display",
            "cantidad_entregada",
            "foto",
            "comentario",
            "latitud",
            "longitud",
            "precision_metros",
            "client_event_id",
            "capturado_por_nombre",
            "capturado_en",
        ]
        read_only_fields = fields

    def get_capturado_por_nombre(self, obj):
        if not obj.capturado_por_id:
            return ""
        return nombre_operativo_usuario(obj.capturado_por)


class EventoRutaSerializer(serializers.ModelSerializer):
    tipo_display = serializers.CharField(source="get_tipo_display", read_only=True)
    severidad_display = serializers.CharField(source="get_severidad_display", read_only=True)
    punto_nombre = serializers.CharField(source="parada.punto_nombre_snapshot", read_only=True)
    creado_por_nombre = serializers.SerializerMethodField()

    class Meta:
        model = EventoRuta
        fields = [
            "id",
            "ruta",
            "parada",
            "punto_nombre",
            "ubicacion",
            "tipo",
            "tipo_display",
            "severidad",
            "severidad_display",
            "descripcion",
            "latitud",
            "longitud",
            "distancia_metros",
            "metadata",
            "creado_por",
            "creado_por_nombre",
            "creado_en",
        ]
        read_only_fields = fields

    def get_creado_por_nombre(self, obj):
        if not obj.creado_por_id:
            return ""
        return nombre_operativo_usuario(obj.creado_por)


class EventoRutaCreateSerializer(serializers.Serializer):
    tipo = serializers.ChoiceField(choices=EventoRuta.TIPO_CHOICES, default=EventoRuta.TIPO_INCIDENCIA_MANUAL)
    severidad = serializers.ChoiceField(choices=EventoRuta.SEVERIDAD_CHOICES, default=EventoRuta.SEVERIDAD_ALERTA)
    descripcion = serializers.CharField()
    parada_id = serializers.IntegerField(required=False, allow_null=True)
    latitud = serializers.DecimalField(max_digits=9, decimal_places=6, required=False, allow_null=True)
    longitud = serializers.DecimalField(max_digits=9, decimal_places=6, required=False, allow_null=True)
    metadata = serializers.JSONField(required=False, default=dict)

    def validate(self, attrs):
        latitud = attrs.get("latitud")
        longitud = attrs.get("longitud")
        if latitud is None and longitud is None:
            return attrs
        if latitud is None or longitud is None:
            raise serializers.ValidationError("Latitud y longitud deben capturarse juntas.")
        try:
            validar_coordenadas(latitud, longitud)
        except Exception as exc:
            if hasattr(exc, "message_dict"):
                raise serializers.ValidationError(exc.message_dict)
            raise
        return attrs


class LogisticaUnidadSerializer(serializers.ModelSerializer):
    sucursal_codigo = serializers.CharField(source="sucursal.codigo", read_only=True)
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)

    class Meta:
        model = Unidad
        fields = ["id", "codigo", "descripcion", "sucursal", "sucursal_codigo", "sucursal_nombre", "placa", "activa"]
        read_only_fields = ["id", "sucursal_codigo", "sucursal_nombre"]


class LogisticaRepartidorSerializer(serializers.ModelSerializer):
    nombre = serializers.SerializerMethodField()
    licencia_estado = serializers.SerializerMethodField()
    licencia_dias_para_vencer = serializers.SerializerMethodField()
    licencia_mensaje = serializers.SerializerMethodField()
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
            "numero_licencia",
            "licencia_expedicion",
            "licencia_expiracion",
            "archivo_licencia",
            "licencia_estado",
            "licencia_dias_para_vencer",
            "licencia_mensaje",
        ]
        read_only_fields = fields

    def get_nombre(self, obj):
        return nombre_operativo_usuario(obj.user)

    def _licencia_delta(self, obj):
        if not obj.licencia_expiracion:
            return None
        return (obj.licencia_expiracion - timezone.localdate()).days

    def get_licencia_estado(self, obj):
        dias = self._licencia_delta(obj)
        if dias is None:
            return "sin_datos"
        if dias < 0:
            return "vencida"
        if dias <= 30:
            return "por_vencer"
        return "vigente"

    def get_licencia_dias_para_vencer(self, obj):
        return self._licencia_delta(obj)

    def get_licencia_mensaje(self, obj):
        estado = self.get_licencia_estado(obj)
        dias = self._licencia_delta(obj)
        if estado == "sin_datos":
            return "Licencia no registrada."
        if estado == "vencida":
            return "Licencia vencida."
        if estado == "por_vencer":
            return f"Tu licencia vence en {dias} día{'s' if dias != 1 else ''}."
        return "Licencia vigente."


class LogisticaReporteSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    unidad_placa = serializers.CharField(source="unidad.placa", read_only=True)
    tipo_display = serializers.CharField(source="get_tipo_display", read_only=True)
    severidad_display = serializers.CharField(source="get_severidad_display", read_only=True)
    estatus_display = serializers.CharField(source="get_estatus_display", read_only=True)
    asignado_a_nombre = serializers.SerializerMethodField()
    reafirmaciones_count = serializers.SerializerMethodField()
    ultima_reafirmacion = serializers.SerializerMethodField()

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
            "reafirmaciones_count",
            "ultima_reafirmacion",
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
            "reafirmaciones_count",
            "ultima_reafirmacion",
        ]

    def get_repartidor_nombre(self, obj):
        return nombre_operativo_usuario(obj.repartidor.user)

    def get_asignado_a_nombre(self, obj):
        if not obj.asignado_a_id:
            return ""
        return nombre_operativo_usuario(obj.asignado_a)

    def get_reafirmaciones_count(self, obj):
        if hasattr(obj, "reafirmaciones_count"):
            return obj.reafirmaciones_count
        return obj.reafirmaciones.count()

    def get_ultima_reafirmacion(self, obj):
        if hasattr(obj, "ultima_reafirmacion"):
            return obj.ultima_reafirmacion
        ultima = obj.reafirmaciones.order_by("-creado_en").values_list("creado_en", flat=True).first()
        return ultima


class LogisticaReporteCreateSerializer(serializers.ModelSerializer):
    unidad = serializers.PrimaryKeyRelatedField(queryset=Unidad.objects.filter(activa=True), required=False, allow_null=True)

    class Meta:
        model = ReporteUnidad
        fields = ["unidad", "tipo", "severidad", "descripcion", "foto", "kilometraje", "latitud", "longitud"]

    def create(self, validated_data):
        repartidor = self.context["repartidor"]
        unidad = validated_data.pop("unidad", None) or repartidor.unidad_asignada
        if not unidad or not unidad.activa:
            raise serializers.ValidationError("Selecciona una unidad activa para levantar reportes.")
        return ReporteUnidad.objects.create(repartidor=repartidor, unidad=unidad, **validated_data)


class LogisticaReporteReafirmacionSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()

    class Meta:
        model = ReporteUnidadReafirmacion
        fields = [
            "id",
            "reporte",
            "repartidor",
            "repartidor_nombre",
            "comentario",
            "latitud",
            "longitud",
            "creado_en",
        ]
        read_only_fields = ["id", "reporte", "repartidor", "repartidor_nombre", "creado_en"]

    def get_repartidor_nombre(self, obj):
        return nombre_operativo_usuario(obj.repartidor.user)


class LogisticaReportePatchSerializer(serializers.ModelSerializer):
    asignado_a = serializers.PrimaryKeyRelatedField(queryset=get_user_model().objects.filter(is_active=True), required=False, allow_null=True)

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
        return nombre_operativo_usuario(obj.repartidor.user)

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
    cargas_combustible = serializers.SerializerMethodField()
    ruta_folio = serializers.SerializerMethodField()
    ruta_estatus = serializers.SerializerMethodField()
    alerta_operativa = serializers.SerializerMethodField()

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
            "foto_ticket_combustible",
            "cargas_combustible",
            "ruta_folio",
            "ruta_estatus",
            "alerta_operativa",
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
        return nombre_operativo_usuario(obj.repartidor.user)

    def get_cargas_combustible(self, obj):
        return LogisticaCargaCombustibleSerializer(
            obj.cargas_combustible.all(),
            many=True,
            context=self.context,
        ).data

    def _ruta_operativa(self, obj):
        return obj.rutas_operativas.order_by("-fecha_ruta", "-id").first()

    def get_ruta_folio(self, obj):
        ruta = self._ruta_operativa(obj)
        return ruta.folio if ruta else ""

    def get_ruta_estatus(self, obj):
        ruta = self._ruta_operativa(obj)
        return ruta.estatus if ruta else ""

    def get_alerta_operativa(self, obj):
        ruta = self._ruta_operativa(obj)
        if ruta and ruta.estatus == RutaEntrega.ESTATUS_PLANEADA:
            return f"Turno abierto, pero {ruta.folio} sigue planeada. Cierra este turno accidental y revisa la carga antes de salir."
        return ""


class LogisticaCargaCombustibleSerializer(serializers.ModelSerializer):
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    repartidor_nombre = serializers.SerializerMethodField()
    bitacora_folio = serializers.CharField(source="bitacora.folio", read_only=True)

    class Meta:
        model = CargaCombustibleUnidad
        fields = [
            "id",
            "bitacora",
            "bitacora_folio",
            "unidad",
            "unidad_codigo",
            "repartidor",
            "repartidor_nombre",
            "litros",
            "importe_total",
            "nivel_gas_despues",
            "foto_ticket",
            "fecha_registro",
            "latitud",
            "longitud",
            "auditoria_estado",
            "auditoria_score",
            "auditoria_motivos",
            "auditoria_detalle",
            "auditoria_analizada_en",
        ]
        read_only_fields = [
            "id",
            "bitacora",
            "bitacora_folio",
            "unidad",
            "unidad_codigo",
            "repartidor",
            "repartidor_nombre",
            "fecha_registro",
            "auditoria_estado",
            "auditoria_score",
            "auditoria_motivos",
            "auditoria_detalle",
            "auditoria_analizada_en",
        ]

    def get_repartidor_nombre(self, obj):
        return nombre_operativo_usuario(obj.repartidor.user)


class LogisticaCargaCombustibleCreateSerializer(serializers.ModelSerializer):
    nivel_gas_despues = serializers.CharField(required=False, allow_blank=True)

    class Meta:
        model = CargaCombustibleUnidad
        fields = ["litros", "importe_total", "nivel_gas_despues", "foto_ticket", "latitud", "longitud"]

    def validate_nivel_gas_despues(self, value):
        if not value:
            return ""
        normalized = str(value or "").strip().lower()
        aliases = {
            "vacio": "vacio",
            "vacío": "vacio",
            "1/4": "1/4",
            "¼": "1/4",
            "1⁄4": "1/4",
            "1/2": "1/2",
            "½": "1/2",
            "1⁄2": "1/2",
            "3/4": "3/4",
            "¾": "3/4",
            "3⁄4": "3/4",
            "lleno": "lleno",
        }
        if normalized not in aliases:
            raise serializers.ValidationError("Selecciona un nivel de gas válido.")
        return aliases[normalized]

    def validate(self, attrs):
        if attrs.get("litros") is None:
            raise serializers.ValidationError("Captura los litros cargados.")
        if attrs.get("importe_total") is None:
            raise serializers.ValidationError("Captura el importe total.")
        if not attrs.get("foto_ticket"):
            raise serializers.ValidationError("La foto del ticket es obligatoria.")
        if attrs["litros"] <= 0:
            raise serializers.ValidationError("Los litros deben ser mayores a cero.")
        if attrs["importe_total"] <= 0:
            raise serializers.ValidationError("El importe total debe ser mayor a cero.")
        if attrs["litros"] >= 5 and attrs["importe_total"] / attrs["litros"] < 10:
            raise serializers.ValidationError(
                "El importe parece precio por litro. Captura el importe total del ticket."
            )
        return attrs


class LogisticaLavadoUnidadSerializer(serializers.ModelSerializer):
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    unidad_descripcion = serializers.CharField(source="unidad.descripcion", read_only=True)
    repartidor_nombre = serializers.SerializerMethodField()
    tipo_lavado_display = serializers.CharField(source="get_tipo_lavado_display", read_only=True)
    partes_lavadas = serializers.ListField(child=serializers.CharField(), read_only=True)
    partes_lavadas_display = serializers.CharField(read_only=True)

    class Meta:
        model = LavadoUnidad
        fields = [
            "id",
            "unidad",
            "unidad_codigo",
            "unidad_descripcion",
            "fecha",
            "tipo_lavado",
            "tipo_lavado_display",
            "lavado_exterior",
            "lavado_interior",
            "lavado_caja_refrigerada",
            "partes_lavadas",
            "partes_lavadas_display",
            "costo",
            "foto_evidencia",
            "registrado_por",
            "repartidor_nombre",
            "fecha_registro",
            "latitud",
            "longitud",
            "notas",
        ]
        read_only_fields = [
            "id",
            "unidad_codigo",
            "unidad_descripcion",
            "fecha",
            "registrado_por",
            "repartidor_nombre",
            "fecha_registro",
        ]

    def get_repartidor_nombre(self, obj):
        if not obj.registrado_por:
            return ""
        return nombre_operativo_usuario(obj.registrado_por)


class LogisticaLavadoUnidadCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = LavadoUnidad
        fields = [
            "unidad",
            "lavado_exterior",
            "lavado_interior",
            "lavado_caja_refrigerada",
            "costo",
            "foto_evidencia",
            "latitud",
            "longitud",
            "notas",
        ]

    def validate(self, attrs):
        unidad = attrs.get("unidad")
        if not unidad or not unidad.activa:
            raise serializers.ValidationError("Selecciona una unidad activa.")
        if not any(
            [
                attrs.get("lavado_exterior"),
                attrs.get("lavado_interior"),
                attrs.get("lavado_caja_refrigerada"),
            ]
        ):
            raise serializers.ValidationError("Selecciona al menos una parte lavada.")
        if not attrs.get("foto_evidencia"):
            raise serializers.ValidationError("La foto del lavado es obligatoria.")
        costo = attrs.get("costo")
        if costo is not None and costo < 0:
            raise serializers.ValidationError("El importe del lavado no puede ser negativo.")
        return attrs


class LogisticaBitacoraSalidaCreateSerializer(serializers.ModelSerializer):
    nivel_gas_salida = serializers.CharField()
    MAX_KM_SALTO_SALIDA = 1000

    class Meta:
        model = BitacoraSalidaLlegada
        fields = ["unidad", "km_salida", "nivel_gas_salida", "foto_tablero_salida", "latitud_salida", "longitud_salida"]

    def validate_nivel_gas_salida(self, value):
        normalized = str(value or "").strip().lower()
        aliases = {
            "vacio": "vacio",
            "vacío": "vacio",
            "1/4": "1/4",
            "¼": "1/4",
            "1⁄4": "1/4",
            "1/2": "1/2",
            "½": "1/2",
            "1⁄2": "1/2",
            "3/4": "3/4",
            "¾": "3/4",
            "3⁄4": "3/4",
            "lleno": "lleno",
        }
        if normalized not in aliases:
            raise serializers.ValidationError("Selecciona un nivel de gas válido.")
        return aliases[normalized]

    def validate(self, attrs):
        unidad = attrs.get("unidad")
        km_salida = attrs.get("km_salida")
        if unidad and km_salida is not None:
            ultimo_turno = (
                BitacoraSalidaLlegada.objects.filter(
                    unidad=unidad,
                    cerrada=True,
                    km_llegada__isnull=False,
                )
                .order_by("-hora_llegada", "-id")
                .first()
            )
            if ultimo_turno and ultimo_turno.km_llegada is not None:
                ultimo_km = ultimo_turno.km_llegada
                if km_salida < ultimo_km:
                    raise serializers.ValidationError(
                        f"El KM salida no puede ser menor al último cierre de la unidad ({ultimo_km})."
                    )
                if km_salida - ultimo_km > self.MAX_KM_SALTO_SALIDA:
                    raise serializers.ValidationError(
                        f"El KM salida parece demasiado alto. Último cierre: {ultimo_km}. Revisa el odómetro antes de iniciar turno."
                    )
        return attrs

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
    MAX_KM_RECORRIDO_TURNO = 1000

    class Meta:
        model = BitacoraSalidaLlegada
        fields = [
            "km_llegada",
            "nivel_gas_llegada",
            "litros_cargados",
            "costo_combustible",
            "foto_tablero_llegada",
            "foto_ticket_combustible",
        ]

    def validate(self, attrs):
        km_llegada = attrs.get("km_llegada")
        if km_llegada is None and self.instance and self.instance.km_llegada is None:
            raise serializers.ValidationError("El kilometraje de llegada es obligatorio.")
        if not attrs.get("nivel_gas_llegada") and self.instance and not self.instance.nivel_gas_llegada:
            raise serializers.ValidationError("El nivel de gas de llegada es obligatorio.")
        if km_llegada is not None and self.instance and km_llegada < self.instance.km_salida:
            raise serializers.ValidationError("El kilometraje de llegada no puede ser menor al de salida.")
        if km_llegada is not None and self.instance and km_llegada - self.instance.km_salida > self.MAX_KM_RECORRIDO_TURNO:
            raise serializers.ValidationError(
                f"El KM llegada parece demasiado alto. KM salida: {self.instance.km_salida}. Revisa el odómetro antes de cerrar turno."
            )
        if not attrs.get("foto_tablero_llegada") and self.instance and not self.instance.foto_tablero_llegada:
            raise serializers.ValidationError("La foto del tablero de llegada es obligatoria.")
        carga_combustible = any(
            attrs.get(field) not in (None, "")
            for field in ["litros_cargados", "costo_combustible", "foto_ticket_combustible"]
        )
        if carga_combustible:
            if attrs.get("litros_cargados") in (None, ""):
                raise serializers.ValidationError("Captura los litros cargados.")
            if attrs.get("costo_combustible") in (None, ""):
                raise serializers.ValidationError("Captura el importe total del ticket de combustible.")
            if not attrs.get("foto_ticket_combustible") and self.instance and not self.instance.foto_ticket_combustible:
                raise serializers.ValidationError("La foto del ticket de combustible es obligatoria.")
            litros = attrs.get("litros_cargados") or self.instance.litros_cargados
            importe = attrs.get("costo_combustible") or self.instance.costo_combustible
            if litros and importe and litros >= 5 and importe / litros < 10:
                raise serializers.ValidationError(
                    "El importe parece precio por litro. Captura el importe total del ticket."
                )
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
        return nombre_operativo_usuario(obj.repartidor.user)


class LogisticaInspeccionVehiculoCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = InspeccionVehiculo
        exclude = ["repartidor", "fecha", "ip_registro"]

    def create(self, validated_data):
        repartidor = self.context["repartidor"]
        unidad = validated_data.pop("unidad", None)
        if not unidad or not unidad.activa:
            raise serializers.ValidationError("Selecciona una unidad activa para inspeccionar.")
        return InspeccionVehiculo.objects.create(
            repartidor=repartidor,
            unidad=unidad,
            **validated_data,
        )


class LogisticaInspeccionDiariaSerializer(serializers.ModelSerializer):
    repartidor_nombre = serializers.SerializerMethodField()
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    unidad_descripcion = serializers.CharField(source="unidad.descripcion", read_only=True)
    reporte_generado_id = serializers.IntegerField(source="reporte_generado.id", read_only=True)

    class Meta:
        model = InspeccionDiaria
        fields = "__all__"
        read_only_fields = [
            "id",
            "repartidor",
            "repartidor_nombre",
            "unidad_codigo",
            "unidad_descripcion",
            "fecha",
            "hora",
            "tiene_fallas",
            "reporte_generado",
            "reporte_generado_id",
            "ip_registro",
        ]

    def get_repartidor_nombre(self, obj):
        return nombre_operativo_usuario(obj.repartidor.user)

    def create(self, validated_data):
        repartidor = self.context["repartidor"]
        unidad = validated_data.get("unidad")
        if not unidad or not unidad.activa:
            raise serializers.ValidationError("Selecciona una unidad activa para inspeccionar.")
        return InspeccionDiaria.objects.create(repartidor=repartidor, **validated_data)
