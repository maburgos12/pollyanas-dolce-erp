from rest_framework import serializers

from crm.models import Cliente, PedidoCliente
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
