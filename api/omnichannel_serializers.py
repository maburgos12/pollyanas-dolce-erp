from decimal import Decimal

from rest_framework import serializers

from crm.models import PedidoCliente
from logistica.models import SolicitudDomicilio


class OmnichannelCustomerInputSerializer(serializers.Serializer):
    nombre = serializers.CharField(max_length=180)
    telefono = serializers.CharField(max_length=40, required=False, allow_blank=True, default="")
    email = serializers.EmailField(required=False, allow_blank=True, default="")


class OmnichannelAddressInputSerializer(serializers.Serializer):
    direccion = serializers.CharField(max_length=300)
    referencias = serializers.CharField(max_length=300, required=False, allow_blank=True, default="")
    latitud = serializers.DecimalField(
        max_digits=9,
        decimal_places=6,
        required=False,
        allow_null=True,
        min_value=Decimal("-90"),
        max_value=Decimal("90"),
    )
    longitud = serializers.DecimalField(
        max_digits=9,
        decimal_places=6,
        required=False,
        allow_null=True,
        min_value=Decimal("-180"),
        max_value=Decimal("180"),
    )
    place_id = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")

    def validate(self, attrs):
        latitude_present = attrs.get("latitud") is not None
        longitude_present = attrs.get("longitud") is not None
        if latitude_present != longitude_present:
            raise serializers.ValidationError(
                "latitud y longitud deben enviarse juntas",
            )
        return attrs


class OmnichannelOrderDetailInputSerializer(serializers.Serializer):
    descripcion = serializers.CharField(max_length=250)
    fecha_compromiso = serializers.DateField(required=False, allow_null=True)
    monto_estimado = serializers.DecimalField(
        max_digits=12,
        decimal_places=2,
        required=False,
        default=Decimal("0"),
    )


class OmnichannelOrderInputSerializer(serializers.Serializer):
    external_source = serializers.ChoiceField(
        choices=("POLLYANAS_ECOMMERCE", "POLLYANAS_OPERACIONES"),
    )
    external_id = serializers.CharField(max_length=120)
    canal = serializers.ChoiceField(choices=PedidoCliente.CANAL_CHOICES)
    cliente = OmnichannelCustomerInputSerializer()
    direccion = OmnichannelAddressInputSerializer()
    pedido = OmnichannelOrderDetailInputSerializer()
    instrucciones_entrega = serializers.CharField(max_length=500, required=False, allow_blank=True, default="", trim_whitespace=True)

    def validate_external_source(self, value):
        value = value.strip().upper()
        if not value:
            raise serializers.ValidationError("Este campo no puede estar vacío.")
        return value

    def validate(self, attrs):
        source = attrs.get("external_source")
        channel = attrs.get("canal")
        if source == "POLLYANAS_ECOMMERCE" and channel != PedidoCliente.CANAL_WEB:
            raise serializers.ValidationError(
                {"canal": "La fuente e-commerce requiere canal WEB."},
            )
        if source == "POLLYANAS_OPERACIONES" and channel == PedidoCliente.CANAL_WEB:
            raise serializers.ValidationError(
                {"canal": "El canal WEB pertenece a la fuente e-commerce."},
            )
        return attrs

    def validate_external_id(self, value):
        value = value.strip()
        if not value:
            raise serializers.ValidationError("Este campo no puede estar vacío.")
        return value


class OmnichannelAddressOutputSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    direccion = serializers.CharField()
    referencias = serializers.CharField()
    latitud = serializers.DecimalField(max_digits=9, decimal_places=6, allow_null=True)
    longitud = serializers.DecimalField(max_digits=9, decimal_places=6, allow_null=True)
    place_id = serializers.CharField()
    es_predeterminada = serializers.BooleanField()


class OmnichannelCustomerOutputSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    codigo = serializers.CharField()
    nombre = serializers.CharField()
    telefono = serializers.CharField()
    email = serializers.EmailField()
    direcciones = OmnichannelAddressOutputSerializer(
        source="direcciones_activas",
        many=True,
    )


class PointNoteSearchSerializer(serializers.Serializer):
    folio = serializers.CharField(max_length=80, trim_whitespace=True)
    sucursal = serializers.CharField(max_length=120, trim_whitespace=True)
    fecha = serializers.DateField(required=False)


class PointLinkedOrderSerializer(serializers.Serializer):
    pk_nota = serializers.CharField(max_length=120, trim_whitespace=True)
    canal = serializers.ChoiceField(choices=PedidoCliente.CANAL_CHOICES)
    social_reference = serializers.CharField(
        max_length=180, required=False, allow_blank=True, default="",
    )
    cliente = OmnichannelCustomerInputSerializer()
    direccion = OmnichannelAddressInputSerializer()
    ventana_inicio = serializers.DateTimeField(required=False, allow_null=True)
    ventana_fin = serializers.DateTimeField(required=False, allow_null=True)
    instrucciones_entrega = serializers.CharField(
        max_length=500, required=False, allow_blank=True, default="",
    )

    def validate(self, attrs):
        start = attrs.get("ventana_inicio")
        end = attrs.get("ventana_fin")
        if (start is None) != (end is None):
            raise serializers.ValidationError(
                "La ventana de entrega requiere inicio y fin.",
            )
        if start is not None and end < start:
            raise serializers.ValidationError(
                {"ventana_fin": "No puede ser anterior al inicio."},
            )
        if (
            attrs.get("canal") == PedidoCliente.CANAL_OTRO
            and not attrs.get("social_reference", "").strip()
        ):
            raise serializers.ValidationError(
                {"social_reference": "El canal Otro requiere una descripción."},
            )
        return attrs


class OmnichannelDeliveryQuerySerializer(serializers.Serializer):
    estatus = serializers.ChoiceField(
        choices=SolicitudDomicilio.ESTATUS_CHOICES, required=False,
    )
    canal = serializers.ChoiceField(
        choices=PedidoCliente.CANAL_CHOICES, required=False,
    )
    repartidor_id = serializers.IntegerField(min_value=1, required=False)
    sucursal = serializers.CharField(
        max_length=120, required=False, trim_whitespace=True,
    )
    fecha = serializers.DateField(required=False)
    page = serializers.IntegerField(min_value=1, max_value=10000, default=1)
    page_size = serializers.IntegerField(min_value=1, max_value=100, default=25)
    ordering = serializers.ChoiceField(
        choices=("created_at", "-created_at", "ventana_inicio", "-ventana_inicio"),
        default="-created_at",
    )

    def validate_repartidor_id(self, value):
        if isinstance(self.initial_data.get("repartidor_id"), bool):
            raise serializers.ValidationError("Debe ser un entero positivo.")
        return value


class OmnichannelDeliveryStatusSerializer(serializers.Serializer):
    repartidor_id = serializers.IntegerField(min_value=1)
    estatus = serializers.ChoiceField(
        choices=(
            SolicitudDomicilio.ESTATUS_EN_RUTA,
            SolicitudDomicilio.ESTATUS_ENTREGADO,
        ),
    )
    operation_id = serializers.UUIDField()
    actor = serializers.DictField()

    def validate_repartidor_id(self, value):
        if isinstance(self.initial_data.get("repartidor_id"), bool):
            raise serializers.ValidationError("Debe ser un entero positivo.")
        return value

    def validate_actor(self, value):
        if set(value) != {"id", "nombre"}:
            raise serializers.ValidationError("actor requiere id y nombre.")
        actor_id = str(value.get("id") or "").strip()
        name = str(value.get("nombre") or "").strip()
        if not actor_id or len(actor_id) > 64 or not name or len(name) > 120:
            raise serializers.ValidationError("actor no es válido.")
        return {"id": actor_id, "nombre": name}


class OmnichannelDeliveryPreparationStatusSerializer(serializers.Serializer):
    estatus = serializers.ChoiceField(
        choices=(
            SolicitudDomicilio.ESTATUS_PREPARANDO,
            SolicitudDomicilio.ESTATUS_LISTO,
        ),
    )
    operation_id = serializers.UUIDField()
    actor = serializers.DictField()

    def validate_actor(self, value):
        if set(value) != {"id", "nombre"}:
            raise serializers.ValidationError("actor requiere id y nombre.")
        actor_id = str(value.get("id") or "").strip()
        name = str(value.get("nombre") or "").strip()
        if not actor_id or len(actor_id) > 64 or not name or len(name) > 120:
            raise serializers.ValidationError("actor no es válido.")
        return {"id": actor_id, "nombre": name}
