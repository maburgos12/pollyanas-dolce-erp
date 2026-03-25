from rest_framework import serializers

from pos_bridge.models import PointDailySale


class PointDailySaleSerializer(serializers.ModelSerializer):
    branch_name = serializers.CharField(source="branch.name", read_only=True)
    branch_external_id = serializers.CharField(source="branch.external_id", read_only=True)
    product_name = serializers.CharField(source="product.name", read_only=True)
    product_sku = serializers.CharField(source="product.sku", read_only=True)
    receta_name = serializers.CharField(source="receta.nombre", default="", read_only=True)

    class Meta:
        model = PointDailySale
        fields = (
            "id",
            "sale_date",
            "branch_name",
            "branch_external_id",
            "product_name",
            "product_sku",
            "receta_name",
            "quantity",
            "tickets",
            "gross_amount",
            "discount_amount",
            "total_amount",
            "tax_amount",
            "net_amount",
            "created_at",
        )


class SalesSummarySerializer(serializers.Serializer):
    period_start = serializers.DateField()
    period_end = serializers.DateField()
    total_sales = serializers.DecimalField(max_digits=18, decimal_places=2)
    total_quantity = serializers.DecimalField(max_digits=18, decimal_places=3)
    total_tickets = serializers.IntegerField()
    total_discount = serializers.DecimalField(max_digits=18, decimal_places=2)
    total_tax = serializers.DecimalField(max_digits=18, decimal_places=2)
    total_net = serializers.DecimalField(max_digits=18, decimal_places=2)
    branches_count = serializers.IntegerField()
    products_count = serializers.IntegerField()
    days_count = serializers.IntegerField()


class SalesByGroupSerializer(serializers.Serializer):
    group_key = serializers.CharField()
    group_name = serializers.CharField()
    total_sales = serializers.DecimalField(max_digits=18, decimal_places=2)
    total_quantity = serializers.DecimalField(max_digits=18, decimal_places=3)
    total_tickets = serializers.IntegerField()
    percentage = serializers.DecimalField(max_digits=8, decimal_places=2)


class SalesTrendSerializer(serializers.Serializer):
    period = serializers.CharField()
    total_sales = serializers.DecimalField(max_digits=18, decimal_places=2)
    total_quantity = serializers.DecimalField(max_digits=18, decimal_places=3)
    total_tickets = serializers.IntegerField()
    avg_ticket = serializers.DecimalField(max_digits=18, decimal_places=2)
