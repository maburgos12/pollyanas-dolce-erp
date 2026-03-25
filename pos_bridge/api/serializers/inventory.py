from rest_framework import serializers

from pos_bridge.models import PointInventorySnapshot


class PointInventorySnapshotSerializer(serializers.ModelSerializer):
    branch_name = serializers.CharField(source="branch.name", read_only=True)
    branch_external_id = serializers.CharField(source="branch.external_id", read_only=True)
    erp_branch_code = serializers.CharField(source="branch.erp_branch.codigo", default="", read_only=True)
    product_name = serializers.CharField(source="product.name", read_only=True)
    product_sku = serializers.CharField(source="product.sku", read_only=True)
    product_category = serializers.CharField(source="product.category", read_only=True)

    class Meta:
        model = PointInventorySnapshot
        fields = (
            "id",
            "branch_name",
            "branch_external_id",
            "erp_branch_code",
            "product_name",
            "product_sku",
            "product_category",
            "stock",
            "min_stock",
            "max_stock",
            "captured_at",
        )


class CurrentStockSerializer(serializers.Serializer):
    product_sku = serializers.CharField()
    product_name = serializers.CharField()
    product_category = serializers.CharField()
    branches = serializers.ListField(child=serializers.DictField())
    total_stock = serializers.DecimalField(max_digits=18, decimal_places=3)


class InventoryAvailabilitySerializer(serializers.Serializer):
    sku = serializers.CharField()
    name = serializers.CharField()
    category = serializers.CharField()
    total_stock = serializers.DecimalField(max_digits=18, decimal_places=3)
    available = serializers.BooleanField()
    last_updated = serializers.DateTimeField()
    branches = serializers.ListField(child=serializers.DictField())


class LowStockAlertSerializer(serializers.Serializer):
    branch_name = serializers.CharField()
    branch_external_id = serializers.CharField()
    erp_branch_code = serializers.CharField(allow_blank=True)
    product_sku = serializers.CharField()
    product_name = serializers.CharField()
    current_stock = serializers.DecimalField(max_digits=18, decimal_places=3)
    min_stock = serializers.DecimalField(max_digits=18, decimal_places=3)
    deficit = serializers.DecimalField(max_digits=18, decimal_places=3)
