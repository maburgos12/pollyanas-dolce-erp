from rest_framework import serializers

from crm.models import Cliente, DireccionCliente, PedidoCliente
from crm.services import SucursalResolutionError, resolve_sucursal


class CRMClienteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cliente
        fields = [
            "id",
            "codigo",
            "nombre",
            "telefono",
            "email",
            "tipo_cliente",
            "sucursal_referencia",
            "notas",
            "activo",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "codigo", "created_at", "updated_at"]


class CRMDireccionClienteSerializer(serializers.ModelSerializer):
    class Meta:
        model = DireccionCliente
        fields = [
            "id",
            "alias",
            "direccion",
            "referencias",
            "latitud",
            "longitud",
            "place_id",
            "es_predeterminada",
            "activa",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]

    def validate(self, attrs):
        latitud = attrs.get("latitud")
        longitud = attrs.get("longitud")
        if (latitud is None) != (longitud is None):
            missing = "longitud" if longitud is None else "latitud"
            raise serializers.ValidationError({missing: "Latitud y longitud deben enviarse juntas."})
        if latitud is not None and not (-90 <= latitud <= 90):
            raise serializers.ValidationError({"latitud": "La latitud debe estar entre -90 y 90."})
        if longitud is not None and not (-180 <= longitud <= 180):
            raise serializers.ValidationError({"longitud": "La longitud debe estar entre -180 y 180."})
        return attrs


class CRMPedidoSerializer(serializers.ModelSerializer):
    cliente_nombre = serializers.CharField(source="cliente.nombre", read_only=True)
    sucursal_ref = serializers.IntegerField(source="sucursal_ref_id", required=False, read_only=True)

    class Meta:
        model = PedidoCliente
        fields = [
            "id",
            "folio",
            "cliente",
            "cliente_nombre",
            "direccion_entrega",
            "external_source",
            "external_id",
            "descripcion",
            "fecha_compromiso",
            "sucursal",
            "sucursal_ref",
            "estatus",
            "prioridad",
            "canal",
            "monto_estimado",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "folio", "created_at", "updated_at", "cliente_nombre"]

    def validate(self, attrs):
        cliente = attrs.get("cliente")
        direccion = attrs.get("direccion_entrega")
        if direccion is not None and cliente is not None and direccion.cliente_id != cliente.id:
            raise serializers.ValidationError(
                {"direccion_entrega": "La dirección seleccionada no pertenece al cliente del pedido."}
            )

        external_source = (attrs.get("external_source") or "").strip()
        external_id = (attrs.get("external_id") or "").strip()
        if bool(external_source) != bool(external_id):
            missing = "external_id" if not external_id else "external_source"
            raise serializers.ValidationError(
                {missing: "external_source y external_id deben enviarse juntos."}
            )
        attrs["external_source"] = external_source
        attrs["external_id"] = external_id
        return attrs

    def validate_sucursal(self, value: str) -> str:
        raw_value = (value or "").strip()
        if not raw_value:
            raise serializers.ValidationError("sucursal es obligatoria.")
        return raw_value

    def create(self, validated_data):
        sucursal_raw = validated_data.pop("sucursal", "")
        try:
            resolution = resolve_sucursal(sucursal_raw)
        except SucursalResolutionError as exc:
            raise serializers.ValidationError({"sucursal": str(exc)}) from exc
        validated_data["sucursal_ref"] = resolution.sucursal
        return super().create(validated_data)


class CRMSeguimientoCreateSerializer(serializers.Serializer):
    comentario = serializers.CharField(required=False, allow_blank=True, max_length=2000)
    estatus_nuevo = serializers.ChoiceField(
        choices=[choice[0] for choice in PedidoCliente.ESTATUS_CHOICES],
        required=False,
        allow_blank=True,
    )

    def validate(self, attrs):
        comentario = (attrs.get("comentario") or "").strip()
        estatus_nuevo = (attrs.get("estatus_nuevo") or "").strip()
        if not comentario and not estatus_nuevo:
            raise serializers.ValidationError("Envía comentario o estatus_nuevo para registrar seguimiento.")
        attrs["comentario"] = comentario
        attrs["estatus_nuevo"] = estatus_nuevo
        return attrs
