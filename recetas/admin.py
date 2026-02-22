from django.contrib import admin
from decimal import Decimal
from django.db import OperationalError, ProgrammingError
from django.shortcuts import get_object_or_404
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils.html import format_html
from .models import (
    CostoDriver,
    LineaReceta,
    PlanProduccion,
    PlanProduccionItem,
    PronosticoVenta,
    Receta,
    RecetaCodigoPointAlias,
    RecetaCostoVersion,
    VentaHistorica,
)

class LineaRecetaInline(admin.TabularInline):
    model = LineaReceta
    extra = 0
    readonly_fields = ("insumo_texto", "cantidad", "unidad_texto", "costo_linea_excel", "match_score", "match_method", "match_status")
    can_delete = False


class RecetaCodigoPointAliasInline(admin.TabularInline):
    model = RecetaCodigoPointAlias
    extra = 0
    fields = ("codigo_point", "codigo_point_normalizado", "nombre_point", "activo")
    readonly_fields = ("codigo_point_normalizado",)

@admin.register(Receta)
class RecetaAdmin(admin.ModelAdmin):
    list_display = ("nombre", "codigo_point", "sheet_name", "pendientes_matching")
    search_fields = ("nombre", "codigo_point", "sheet_name")
    inlines = [LineaRecetaInline, RecetaCodigoPointAliasInline]

@admin.register(LineaReceta)
class LineaRecetaAdmin(admin.ModelAdmin):
    list_display = ("receta", "insumo_texto", "insumo", "cantidad", "unidad_texto", "match_status", "match_score")
    search_fields = ("receta__nombre", "insumo_texto", "insumo__nombre")
    list_filter = ("match_status", "match_method")
    autocomplete_fields = ("insumo",)


class PlanProduccionItemInline(admin.TabularInline):
    model = PlanProduccionItem
    extra = 0
    autocomplete_fields = ("receta",)


@admin.register(PlanProduccion)
class PlanProduccionAdmin(admin.ModelAdmin):
    list_display = ("nombre", "fecha_produccion", "creado_por", "items_count", "comparativo_link", "creado_en")
    search_fields = ("nombre",)
    list_filter = ("fecha_produccion",)
    inlines = [PlanProduccionItemInline]
    readonly_fields = ("comparativo_link_change",)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.prefetch_related("items")

    def items_count(self, obj):
        return obj.items.count()
    items_count.short_description = "Renglones"

    def comparativo_link(self, obj):
        url = reverse("admin:recetas_planproduccion_comparativo_pronostico", args=[obj.pk])
        return format_html('<a href="{}">Ver plan vs pronóstico</a>', url)
    comparativo_link.short_description = "Comparativo"

    def comparativo_link_change(self, obj):
        if not obj or not obj.pk:
            return "Guarda el plan para habilitar el comparativo."
        url = reverse("admin:recetas_planproduccion_comparativo_pronostico", args=[obj.pk])
        return format_html('<a class="button" href="{}">Abrir comparativo plan vs pronóstico</a>', url)
    comparativo_link_change.short_description = "Plan vs pronóstico"

    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path(
                "<int:plan_id>/comparativo-pronostico/",
                self.admin_site.admin_view(self.comparativo_pronostico_view),
                name="recetas_planproduccion_comparativo_pronostico",
            ),
        ]
        return custom + urls

    def comparativo_pronostico_view(self, request, plan_id: int):
        plan = get_object_or_404(
            PlanProduccion.objects.prefetch_related("items__receta"),
            pk=plan_id,
        )
        periodo = plan.fecha_produccion.strftime("%Y-%m")

        plan_map = {}
        for item in plan.items.all():
            rid = item.receta_id
            current = plan_map.get(rid)
            qty = Decimal(str(item.cantidad or 0))
            if current:
                current["cantidad_plan"] += qty
            else:
                plan_map[rid] = {
                    "receta_id": rid,
                    "receta": item.receta.nombre,
                    "cantidad_plan": qty,
                    "cantidad_pronostico": Decimal("0"),
                }

        pronosticos_unavailable = False
        try:
            pronosticos = list(
                PronosticoVenta.objects.filter(periodo=periodo).select_related("receta")
            )
        except (OperationalError, ProgrammingError):
            pronosticos = []
            pronosticos_unavailable = True

        for p in pronosticos:
            row = plan_map.get(p.receta_id)
            if row:
                row["cantidad_pronostico"] = Decimal(str(p.cantidad or 0))
            else:
                plan_map[p.receta_id] = {
                    "receta_id": p.receta_id,
                    "receta": p.receta.nombre,
                    "cantidad_plan": Decimal("0"),
                    "cantidad_pronostico": Decimal(str(p.cantidad or 0)),
                }

        rows = sorted(plan_map.values(), key=lambda x: x["receta"].lower())
        total_plan = Decimal("0")
        total_pronostico = Decimal("0")
        total_delta_abs = Decimal("0")
        desviaciones = 0
        for row in rows:
            row["delta"] = row["cantidad_plan"] - row["cantidad_pronostico"]
            row["sin_pronostico"] = row["cantidad_plan"] > 0 and row["cantidad_pronostico"] <= 0
            total_plan += row["cantidad_plan"]
            total_pronostico += row["cantidad_pronostico"]
            total_delta_abs += abs(row["delta"])
            if row["delta"] != 0:
                desviaciones += 1

        context = dict(
            self.admin_site.each_context(request),
            opts=self.model._meta,
            title=f"Comparativo Plan vs Pronóstico · {plan.nombre}",
            plan=plan,
            periodo=periodo,
            rows=rows,
            total_plan=total_plan,
            total_pronostico=total_pronostico,
            total_delta=total_plan - total_pronostico,
            total_delta_abs=total_delta_abs,
            desviaciones=desviaciones,
            pronosticos_unavailable=pronosticos_unavailable,
        )
        return TemplateResponse(
            request,
            "admin/recetas/planproduccion/comparativo_pronostico.html",
            context,
        )


@admin.register(PlanProduccionItem)
class PlanProduccionItemAdmin(admin.ModelAdmin):
    list_display = ("plan", "receta", "cantidad", "creado_en")
    search_fields = ("plan__nombre", "receta__nombre")
    autocomplete_fields = ("plan", "receta")


@admin.register(RecetaCodigoPointAlias)
class RecetaCodigoPointAliasAdmin(admin.ModelAdmin):
    list_display = ("codigo_point", "codigo_point_normalizado", "receta", "activo", "actualizado_en")
    search_fields = ("codigo_point", "codigo_point_normalizado", "nombre_point", "receta__nombre")
    list_filter = ("activo",)


@admin.register(CostoDriver)
class CostoDriverAdmin(admin.ModelAdmin):
    list_display = (
        "nombre",
        "scope",
        "receta",
        "familia",
        "mo_pct",
        "indirecto_pct",
        "prioridad",
        "activo",
    )
    list_filter = ("scope", "activo")
    search_fields = ("nombre", "receta__nombre", "familia")
    autocomplete_fields = ("receta",)


@admin.register(RecetaCostoVersion)
class RecetaCostoVersionAdmin(admin.ModelAdmin):
    list_display = (
        "receta",
        "version_num",
        "costo_mp",
        "costo_mo",
        "costo_indirecto",
        "costo_total",
        "fuente",
        "creado_en",
    )
    list_filter = ("fuente", "driver_scope", "creado_en")
    search_fields = ("receta__nombre", "hash_snapshot", "driver_nombre")
    autocomplete_fields = ("receta",)


@admin.register(PronosticoVenta)
class PronosticoVentaAdmin(admin.ModelAdmin):
    list_display = ("periodo", "receta", "cantidad", "fuente", "actualizado_en")
    list_filter = ("periodo", "fuente")
    search_fields = ("receta__nombre", "receta__codigo_point", "periodo")
    autocomplete_fields = ("receta",)


@admin.register(VentaHistorica)
class VentaHistoricaAdmin(admin.ModelAdmin):
    list_display = ("fecha", "sucursal", "receta", "cantidad", "tickets", "fuente", "actualizado_en")
    list_filter = ("sucursal", "fuente", "fecha")
    search_fields = ("receta__nombre", "receta__codigo_point", "sucursal__codigo", "sucursal__nombre")
    autocomplete_fields = ("receta", "sucursal")
