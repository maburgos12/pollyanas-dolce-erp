from rest_framework import serializers

from pos_bridge.services.product_closure_projection import (
    is_historical_closure,
    project_product_closure_line,
    sum_complete_values,
)
from recetas.models import ProductoMonthClosure, ProductoMonthClosureLine


PUBLIC_SOURCE_AUTHORITY_FIELDS = (
    "source",
    "mode",
    "selected_source",
    "source_present",
    "job_present",
    "authoritative",
    "status",
    "job_status",
    "within_tolerance",
    "fallback_used",
    "coverage_scope",
    "authority_issues",
    "snapshot_issues",
    "blocking_issues",
    "job_id",
    "selected_sync_job_ids",
    "restricted_sync_job_ids",
    "restricted_row_sync_job_ids",
    "target_date",
    "effective_date",
    "selected_dates",
    "coverage_start",
    "coverage_end",
    "row_count",
    "snapshot_rows",
    "selected_rows",
    "applied_rows",
    "rows_bound_to_job",
    "matched_recipe_count",
    "unresolved_rows",
    "official_daily_row_count",
    "legacy_daily_row_count",
    "legacy_bridge_row_count",
    "selected_branch_count",
    "expected_branch_count",
    "applied_branch_count",
    "expected_product_count",
    "coverage_expected_branch_days",
    "coverage_logged_branch_days",
)


def _public_source_authority(metadata: object) -> dict[str, object]:
    source = dict(metadata or {}) if isinstance(metadata, dict) else {}
    return {key: source[key] for key in PUBLIC_SOURCE_AUTHORITY_FIELDS if key in source}


class ProductMonthClosureLineSerializer(serializers.ModelSerializer):
    receta_padre_nombre = serializers.CharField(source="receta_padre.nombre", read_only=True)
    receta_padre_codigo_point = serializers.CharField(source="receta_padre.codigo_point", read_only=True)
    opening_point = serializers.SerializerMethodField()
    opening_source = serializers.SerializerMethodField()
    production = serializers.SerializerMethodField()
    sales_direct = serializers.SerializerMethodField()
    sales_derived = serializers.SerializerMethodField()
    sales_total = serializers.SerializerMethodField()
    waste_total = serializers.SerializerMethodField()
    point_conversion_in = serializers.SerializerMethodField()
    point_conversion_out = serializers.SerializerMethodField()
    calculated_closing = serializers.SerializerMethodField()
    closing_point_cedis = serializers.SerializerMethodField()
    closing_point_sucursales = serializers.SerializerMethodField()
    closing_point = serializers.SerializerMethodField()
    point_difference = serializers.SerializerMethodField()
    point_status = serializers.SerializerMethodField()
    point_status_label = serializers.SerializerMethodField()
    conversion_origin = serializers.SerializerMethodField()
    conversion_origins = serializers.SerializerMethodField()
    projection_sources = serializers.SerializerMethodField()
    source_authority = serializers.SerializerMethodField()
    source_issues = serializers.SerializerMethodField()
    is_historical_inventory = serializers.SerializerMethodField()
    historical_count = serializers.SerializerMethodField()
    historical_difference = serializers.SerializerMethodField()

    class Meta:
        model = ProductoMonthClosureLine
        fields = (
            "id",
            "receta_padre",
            "receta_padre_nombre",
            "receta_padre_codigo_point",
            "opening_point",
            "opening_source",
            "production",
            "sales_direct",
            "sales_derived",
            "sales_total",
            "waste_total",
            "point_conversion_in",
            "point_conversion_out",
            "calculated_closing",
            "closing_point_cedis",
            "closing_point_sucursales",
            "closing_point",
            "point_difference",
            "point_status",
            "point_status_label",
            "conversion_origin",
            "conversion_origins",
            "projection_sources",
            "source_authority",
            "source_issues",
            "is_historical_inventory",
            "historical_count",
            "historical_difference",
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

    @staticmethod
    def _decimal(value):
        return None if value is None else f"{value:.6f}"

    def _projection(self, obj):
        cached = getattr(obj, "_canonical_api_projection", None)
        if cached is not None:
            return cached
        closure = obj.closure
        projection = project_product_closure_line(
            obj,
            historical_excel_import=is_historical_closure(closure, lines=[obj]),
        )
        obj._canonical_api_projection = projection
        return projection

    def get_opening_point(self, obj):
        return self._decimal(self._projection(obj)["opening_point"])

    def get_opening_source(self, obj):
        return (obj.metadata or {}).get("opening_source") or obj.closure.opening_source

    def get_production(self, obj):
        return self._decimal(self._projection(obj)["production"])

    def get_sales_direct(self, obj):
        return self._decimal(self._projection(obj)["sales_direct"])

    def get_sales_derived(self, obj):
        return self._decimal(self._projection(obj)["sales_derived"])

    def get_sales_total(self, obj):
        return self._decimal(self._projection(obj)["sales_total"])

    def get_waste_total(self, obj):
        return self._decimal(self._projection(obj)["waste_total"])

    def get_point_conversion_in(self, obj):
        return self._decimal(self._projection(obj)["point_conversion_in"])

    def get_point_conversion_out(self, obj):
        return self._decimal(self._projection(obj)["point_conversion_out"])

    def get_calculated_closing(self, obj):
        return self._decimal(self._projection(obj)["calculated_closing"])

    def get_closing_point_cedis(self, obj):
        return self._decimal(self._projection(obj)["closing_point_cedis"])

    def get_closing_point_sucursales(self, obj):
        return self._decimal(self._projection(obj)["closing_point_sucursales"])

    def get_closing_point(self, obj):
        return self._decimal(self._projection(obj)["closing_point"])

    def get_point_difference(self, obj):
        return self._decimal(self._projection(obj)["point_difference"])

    def get_point_status(self, obj):
        return self._projection(obj)["point_status"]

    def get_point_status_label(self, obj):
        return self._projection(obj)["status_label"]

    def get_conversion_origin(self, obj):
        return list(self._projection(obj)["conversion_origin"])

    def get_conversion_origins(self, obj):
        return list(self._projection(obj)["conversion_origins"])

    def get_projection_sources(self, obj):
        return list(self._projection(obj)["projection_sources"])

    def get_source_authority(self, obj):
        return self._projection(obj)["source_authority"]

    def get_source_issues(self, obj):
        return list(self._projection(obj)["source_issues"])

    def get_is_historical_inventory(self, obj):
        return self._projection(obj)["is_historical_inventory"]

    def get_historical_count(self, obj):
        return self._decimal(self._projection(obj)["historical_count"])

    def get_historical_difference(self, obj):
        return self._decimal(self._projection(obj)["historical_difference"])


class ProductMonthClosureSerializer(serializers.ModelSerializer):
    month = serializers.SerializerMethodField()
    line_count = serializers.SerializerMethodField()
    total_opening_inventory = serializers.SerializerMethodField()
    total_production = serializers.SerializerMethodField()
    total_sales = serializers.SerializerMethodField()
    total_waste = serializers.SerializerMethodField()
    total_ending_inventory = serializers.SerializerMethodField()
    validation = serializers.SerializerMethodField()
    source_authority = serializers.SerializerMethodField()
    source_issues = serializers.SerializerMethodField()
    total_direct_sales = serializers.SerializerMethodField()
    total_derived_sales = serializers.SerializerMethodField()
    total_conversion_in = serializers.SerializerMethodField()
    total_conversion_out = serializers.SerializerMethodField()
    total_closing_point = serializers.SerializerMethodField()
    total_point_difference = serializers.SerializerMethodField()
    total_historical_count = serializers.SerializerMethodField()
    total_historical_difference = serializers.SerializerMethodField()

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
            "total_direct_sales",
            "total_derived_sales",
            "total_conversion_in",
            "total_conversion_out",
            "total_closing_point",
            "total_point_difference",
            "total_historical_count",
            "total_historical_difference",
            "validation",
            "source_authority",
            "source_issues",
            "notes",
        )

    def get_month(self, obj):
        return obj.month_start.strftime("%Y-%m")

    def get_line_count(self, obj):
        return obj.lines.count()

    def _projection_rows(self, obj):
        cached = getattr(obj, "_canonical_api_projection_rows", None)
        if cached is not None:
            return cached
        lines = list(obj.lines.all())
        historical = is_historical_closure(obj, lines=lines)
        rows = []
        for line in lines:
            projection = project_product_closure_line(
                line,
                historical_excel_import=historical,
            )
            line._canonical_api_projection = projection
            rows.append(projection)
        obj._canonical_api_projection_rows = rows
        return rows

    def _sum_projection(self, obj, field_name: str):
        value = sum_complete_values(self._projection_rows(obj), field_name)
        return None if value is None else f"{value:.6f}"

    def get_total_opening_inventory(self, obj):
        return self._sum_projection(obj, "opening_point")

    def get_total_production(self, obj):
        return self._sum_projection(obj, "production")

    def get_total_sales(self, obj):
        return self._sum_projection(obj, "sales_total")

    def get_total_waste(self, obj):
        return self._sum_projection(obj, "waste_total")

    def get_total_ending_inventory(self, obj):
        return self._sum_projection(obj, "calculated_closing")

    def get_total_direct_sales(self, obj):
        return self._sum_projection(obj, "sales_direct")

    def get_total_derived_sales(self, obj):
        return self._sum_projection(obj, "sales_derived")

    def get_total_conversion_in(self, obj):
        return self._sum_projection(obj, "point_conversion_in")

    def get_total_conversion_out(self, obj):
        return self._sum_projection(obj, "point_conversion_out")

    def get_total_closing_point(self, obj):
        return self._sum_projection(obj, "closing_point")

    def get_total_point_difference(self, obj):
        return self._sum_projection(obj, "point_difference")

    def get_total_historical_count(self, obj):
        return self._sum_projection(obj, "historical_count")

    def get_total_historical_difference(self, obj):
        return self._sum_projection(obj, "historical_difference")

    def get_validation(self, obj):
        return (obj.metadata or {}).get("validation", {})

    def get_source_authority(self, obj):
        metadata = dict(obj.metadata or {})
        return {
            "opening": _public_source_authority(metadata.get("opening_meta")),
            "sales": _public_source_authority(metadata.get("sales_meta")),
            "production": _public_source_authority(metadata.get("production_meta")),
            "waste": _public_source_authority(metadata.get("waste_meta")),
            "conversions": _public_source_authority(metadata.get("conversion_meta")),
            "closing": _public_source_authority(metadata.get("closing_inventory_meta")),
        }

    def get_source_issues(self, obj):
        metadata = dict(obj.metadata or {})
        issues = list((metadata.get("validation") or {}).get("blocking_issues") or [])
        issues.extend((metadata.get("balance") or {}).get("issues") or [])
        for source_key in (
            "opening_meta",
            "sales_meta",
            "production_meta",
            "waste_meta",
            "conversion_meta",
            "closing_inventory_meta",
        ):
            issues.extend((metadata.get(source_key) or {}).get("authority_issues") or [])
        return list(dict.fromkeys(str(issue) for issue in issues))


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
