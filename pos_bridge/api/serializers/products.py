from rest_framework import serializers

from pos_bridge.models import PointProduct


class PointProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = PointProduct
        fields = (
            "id",
            "external_id",
            "sku",
            "name",
            "category",
            "active",
            "metadata",
            "created_at",
            "updated_at",
        )


class ProductRecipeSerializer(serializers.Serializer):
    product_sku = serializers.CharField()
    product_name = serializers.CharField()
    receta_id = serializers.IntegerField()
    receta_nombre = serializers.CharField()
    receta_tipo = serializers.CharField()
    bom = serializers.ListField(child=serializers.DictField())
