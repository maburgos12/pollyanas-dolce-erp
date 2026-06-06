from __future__ import annotations

from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework import serializers

from logistica.models import (
    BitacoraRepartidor,
    BitacoraSalidaLlegada,
    CargaCombustibleUnidad,
    EntregaRuta,
    InspeccionDiaria,
    InspeccionVehiculo,
    LavadoUnidad,
    Repartidor,
    ReporteUnidad,
    ReporteUnidadReafirmacion,
    RutaEntrega,
    Unidad,
)
from rrhh.services_identidad import nombre_operativo_usuario


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
