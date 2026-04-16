from rest_framework import serializers

from recetas.models import ProductoMonthClosure, ProductoMonthClosureLine


class ProductMonthClosureLineSerializer(serializers.ModelSerializer):
    receta_padre_nombre = serializers.CharField(source="receta_padre.nombre", read_only=True)
    receta_padre_codigo_point = serializers.CharField(source="receta_padre.codigo_point", read_only=True)

    class Meta:
        model = ProductoMonthClosureLine
        fields = (
            "id",
            "receta_padre",
            "receta_padre_nombre",
            "receta_padre_codigo_point",
            "inventario_inicial_teorico",
            "produccion_mes",
            "venta_directa_enteros",
            "venta_derivada_equivalente",
            "venta_total_equivalente",
            "merma_directa_enteros",
            "merma_derivada_equivalente",
            "merma_total_equivalente",
            "inventario_final_teorico",
            "source_snapshot_count",
            "source_sale_rows",
            "source_production_rows",
            "source_waste_rows",
            "has_catalog_issue",
            "catalog_issue_note",
        )


class ProductMonthClosureSerializer(serializers.ModelSerializer):
    month = serializers.SerializerMethodField()
    line_count = serializers.SerializerMethodField()
    total_opening_inventory = serializers.SerializerMethodField()
    total_production = serializers.SerializerMethodField()
    total_sales = serializers.SerializerMethodField()
    total_waste = serializers.SerializerMethodField()
    total_ending_inventory = serializers.SerializerMethodField()
    validation = serializers.SerializerMethodField()

    class Meta:
        model = ProductoMonthClosure
        fields = (
            "id",
            "month",
            "month_start",
            "month_end",
            "status",
            "opening_source",
            "opening_reference_date",
            "upstream_sync_cutoff_at",
            "built_at",
            "is_locked",
            "line_count",
            "total_opening_inventory",
            "total_production",
            "total_sales",
            "total_waste",
            "total_ending_inventory",
            "validation",
            "notes",
        )

    def get_month(self, obj):
        return obj.month_start.strftime("%Y-%m")

    def get_line_count(self, obj):
        return obj.lines.count()

    def _sum_field(self, obj, field_name: str):
        total = 0
        for line in obj.lines.all():
            total += getattr(line, field_name, 0) or 0
        return total

    def _sum_field_str(self, obj, field_name: str):
        return f"{self._sum_field(obj, field_name):.6f}"

    def get_total_opening_inventory(self, obj):
        return self._sum_field_str(obj, "inventario_inicial_teorico")

    def get_total_production(self, obj):
        return self._sum_field_str(obj, "produccion_mes")

    def get_total_sales(self, obj):
        return self._sum_field_str(obj, "venta_total_equivalente")

    def get_total_waste(self, obj):
        return self._sum_field_str(obj, "merma_total_equivalente")

    def get_total_ending_inventory(self, obj):
        return self._sum_field_str(obj, "inventario_final_teorico")

    def get_validation(self, obj):
        return (obj.metadata or {}).get("validation", {})


class ProductMonthClosureDetailSerializer(ProductMonthClosureSerializer):
    lines = ProductMonthClosureLineSerializer(many=True, read_only=True)

    class Meta(ProductMonthClosureSerializer.Meta):
        fields = ProductMonthClosureSerializer.Meta.fields + ("lines",)


class ProductMonthClosureBuildSerializer(serializers.Serializer):
    month = serializers.RegexField(r"^\d{4}-\d{2}$")
    rebuild = serializers.BooleanField(required=False, default=False)
    lock_after_build = serializers.BooleanField(required=False, default=False)
    approval_note = serializers.CharField(required=False, allow_blank=True, max_length=255)
    approval_reason = serializers.CharField(required=False, allow_blank=True, max_length=120)


class ProductMonthClosureLockSerializer(serializers.Serializer):
    approval_note = serializers.CharField(required=False, allow_blank=True, max_length=255)
    approval_reason = serializers.CharField(required=False, allow_blank=True, max_length=120)
