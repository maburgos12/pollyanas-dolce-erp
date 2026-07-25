from decimal import Decimal

from rest_framework import serializers

from crm.models import PedidoCliente


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
    external_source = serializers.CharField(max_length=40)
    external_id = serializers.CharField(max_length=120)
    canal = serializers.ChoiceField(choices=PedidoCliente.CANAL_CHOICES)
    cliente = OmnichannelCustomerInputSerializer()
    direccion = OmnichannelAddressInputSerializer()
    pedido = OmnichannelOrderDetailInputSerializer()

    def validate_external_source(self, value):
        value = value.strip().upper()
        if not value:
            raise serializers.ValidationError("Este campo no puede estar vacío.")
        return value

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
