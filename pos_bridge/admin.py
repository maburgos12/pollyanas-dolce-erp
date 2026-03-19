from django.contrib import admin

from pos_bridge.models import (
    PointBranch,
    PointDailySale,
    PointExtractionLog,
    PointInventorySnapshot,
    PointProduct,
    PointSyncJob,
)


@admin.register(PointBranch)
class PointBranchAdmin(admin.ModelAdmin):
    list_display = ("external_id", "name", "status", "erp_branch", "updated_at")
    search_fields = ("external_id", "name", "normalized_name", "erp_branch__codigo", "erp_branch__nombre")
    list_filter = ("status",)
    readonly_fields = ("created_at", "updated_at", "last_seen_at", "normalized_name")


@admin.register(PointProduct)
class PointProductAdmin(admin.ModelAdmin):
    list_display = ("external_id", "sku", "name", "category", "active", "updated_at")
    search_fields = ("external_id", "sku", "name", "normalized_name")
    list_filter = ("active", "category")
    readonly_fields = ("created_at", "updated_at", "normalized_name")


@admin.register(PointSyncJob)
class PointSyncJobAdmin(admin.ModelAdmin):
    list_display = ("id", "job_type", "status", "started_at", "finished_at", "attempt_count")
    list_filter = ("job_type", "status")
    search_fields = ("error_message",)
    readonly_fields = ("started_at", "finished_at", "created_at", "updated_at")


@admin.register(PointExtractionLog)
class PointExtractionLogAdmin(admin.ModelAdmin):
    list_display = ("sync_job", "level", "message", "created_at")
    list_filter = ("level",)
    search_fields = ("message",)
    readonly_fields = ("created_at", "context")


@admin.register(PointInventorySnapshot)
class PointInventorySnapshotAdmin(admin.ModelAdmin):
    list_display = ("branch", "product", "stock", "captured_at", "sync_job")
    list_filter = ("captured_at",)
    search_fields = ("branch__name", "product__name", "product__sku", "product__external_id")
    readonly_fields = ("captured_at",)


@admin.register(PointDailySale)
class PointDailySaleAdmin(admin.ModelAdmin):
    list_display = ("sale_date", "branch", "product", "receta", "quantity", "total_amount", "sync_job")
    list_filter = ("sale_date", "branch", "receta")
    search_fields = (
        "branch__name",
        "product__name",
        "product__sku",
        "product__external_id",
        "receta__nombre",
        "receta__codigo_point",
    )
    readonly_fields = ("created_at", "updated_at")
