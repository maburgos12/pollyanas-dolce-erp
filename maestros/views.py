import csv
from collections import defaultdict
from io import BytesIO
from urllib.parse import urlencode
from datetime import timedelta
from decimal import Decimal

from django.contrib import messages
from django.http import HttpResponse
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, redirect, get_object_or_404
from django.core.paginator import Paginator
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse, reverse_lazy
from django.db import transaction
from django.db.models import Count, DateField, DecimalField, OuterRef, Q, Subquery, Sum
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.utils import timezone
from openpyxl import Workbook
from core.access import ROLE_ADMIN, ROLE_COMPRAS, can_view_maestros, has_any_role
from core.audit import log_event
from recetas.models import LineaReceta, Receta, RecetaCodigoPointAlias, VentaHistorica, normalizar_codigo_point
from recetas.utils.normalizacion import normalizar_nombre

from .models import CostoInsumo, PointPendingMatch, Proveedor, Insumo, InsumoAlias, UnidadMedida
from .utils.canonical_catalog import (
    canonical_insumo,
    canonical_insumo_by_id,
    canonicalized_active_insumos,
    canonicalized_insumo_selector,
    duplicate_priority,
    latest_costo_canonico,
)


def _insumo_usage_maps(insumo_ids: list[int]) -> dict[str, object]:
    if not insumo_ids:
        return {
            "recipe_counts": {},
            "final_recipe_counts": {},
            "base_recipe_counts": {},
            "purchase_counts": {},
            "movement_counts": {},
            "adjustment_counts": {},
            "existence_ids": set(),
        }

    from compras.models import SolicitudCompra
    from inventario.models import AjusteInventario, ExistenciaInsumo, MovimientoInventario
    from recetas.models import LineaReceta

    recipe_counts = dict(
        LineaReceta.objects.filter(insumo_id__in=insumo_ids)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .values("insumo_id")
        .annotate(total=Count("id"))
        .values_list("insumo_id", "total")
    )
    final_recipe_counts = dict(
        LineaReceta.objects.filter(insumo_id__in=insumo_ids, receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .values("insumo_id")
        .annotate(total=Count("receta_id", distinct=True))
        .values_list("insumo_id", "total")
    )
    base_recipe_counts = dict(
        LineaReceta.objects.filter(insumo_id__in=insumo_ids, receta__tipo=Receta.TIPO_PREPARACION)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .values("insumo_id")
        .annotate(total=Count("receta_id", distinct=True))
        .values_list("insumo_id", "total")
    )
    purchase_counts = dict(
        SolicitudCompra.objects.filter(insumo_id__in=insumo_ids)
        .values("insumo_id")
        .annotate(total=Count("id"))
        .values_list("insumo_id", "total")
    )
    movement_counts = dict(
        MovimientoInventario.objects.filter(insumo_id__in=insumo_ids)
        .values("insumo_id")
        .annotate(total=Count("id"))
        .values_list("insumo_id", "total")
    )
    adjustment_counts = dict(
        AjusteInventario.objects.filter(insumo_id__in=insumo_ids)
        .values("insumo_id")
        .annotate(total=Count("id"))
        .values_list("insumo_id", "total")
    )
    existence_ids = set(ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids).values_list("insumo_id", flat=True))
    return {
        "recipe_counts": recipe_counts,
        "final_recipe_counts": final_recipe_counts,
        "base_recipe_counts": base_recipe_counts,
        "purchase_counts": purchase_counts,
        "movement_counts": movement_counts,
        "adjustment_counts": adjustment_counts,
        "existence_ids": existence_ids,
    }


def _insumo_type_cards():
    return [
        {
            "code": Insumo.TIPO_MATERIA_PRIMA,
            "title": "Materia prima",
            "description": "Compra directa a proveedor. Base del costeo y abastecimiento.",
        },
        {
            "code": Insumo.TIPO_INTERNO,
            "title": "Insumo interno",
            "description": "Batida o mezcla producida dentro de la empresa con rendimiento controlado.",
        },
        {
            "code": Insumo.TIPO_EMPAQUE,
            "title": "Empaque",
            "description": "Material de presentación final: caja, domo, etiqueta, base, vaso.",
        },
    ]


def _insumo_type_card(tipo_item: str):
    for item in _insumo_type_cards():
        if item["code"] == tipo_item:
            return item
    return {
        "code": tipo_item or Insumo.TIPO_MATERIA_PRIMA,
        "title": _insumo_type_label(tipo_item),
        "description": "Artículo del catálogo estándar.",
    }


def _insumo_type_label(tipo_item: str) -> str:
    mapping = {
        Insumo.TIPO_MATERIA_PRIMA: "Materia prima",
        Insumo.TIPO_INTERNO: "Insumo interno",
        Insumo.TIPO_EMPAQUE: "Empaque",
    }
    return mapping.get(tipo_item, "Artículo")


def _match_enterprise_status(insumo: Insumo, readiness_filter: str) -> bool:
    profile = _insumo_operational_profile(insumo)
    if readiness_filter == "listos":
        return profile["readiness_label"] == "Lista para operar"
    if readiness_filter == "incompletos":
        return profile["readiness_label"] == "Incompleto"
    if readiness_filter == "inactivos":
        return profile["readiness_label"] == "Inactivo"
    return True


def _insumo_category_presets():
    return {
        Insumo.TIPO_MATERIA_PRIMA: [
            "Harinas",
            "Lácteos",
            "Frutas",
            "Chocolate",
            "Endulzantes",
            "Decoración",
        ],
        Insumo.TIPO_INTERNO: [
            "Batidas",
            "Panes",
            "Betunes",
            "Rellenos",
            "Coberturas",
            "Bases",
        ],
        Insumo.TIPO_EMPAQUE: [
            "Caja pastel",
            "Domo",
            "Etiqueta",
            "Base",
            "Vaso",
            "Accesorio",
        ],
    }


def _insumo_type_requirements(tipo_item: str) -> list[str]:
    base = ["Nombre maestro", "Unidad base", "Estatus activo/inactivo"]
    if tipo_item == Insumo.TIPO_MATERIA_PRIMA:
        return base + ["Proveedor principal", "Código comercial recomendado para conciliación"]
    if tipo_item == Insumo.TIPO_INTERNO:
        return base + ["Categoría operativa", "Uso posterior en receta o producto final"]
    if tipo_item == Insumo.TIPO_EMPAQUE:
        return base + ["Categoría operativa", "Uso en producto final o presentación"]
    return base


def _insumo_type_requirements_map() -> dict[str, list[str]]:
    return {
        Insumo.TIPO_MATERIA_PRIMA: _insumo_type_requirements(Insumo.TIPO_MATERIA_PRIMA),
        Insumo.TIPO_INTERNO: _insumo_type_requirements(Insumo.TIPO_INTERNO),
        Insumo.TIPO_EMPAQUE: _insumo_type_requirements(Insumo.TIPO_EMPAQUE),
    }


def _insumo_type_titles_map() -> dict[str, str]:
    return {item["code"]: item["title"] for item in _insumo_type_cards()}


def _insumo_category_suggestions():
    presets = _insumo_category_presets()
    categories = (
        Insumo.objects.filter(activo=True)
        .exclude(categoria__exact="")
        .values_list("tipo_item", "categoria")
        .distinct()
    )
    suggestions = {key: list(values) for key, values in presets.items()}
    for tipo_item, categoria in categories:
        if tipo_item not in suggestions:
            suggestions[tipo_item] = []
        if categoria not in suggestions[tipo_item]:
            suggestions[tipo_item].append(categoria)
    for tipo_item in suggestions:
        suggestions[tipo_item] = sorted(suggestions[tipo_item])[:12]
    return suggestions


def _insumo_operational_profile(insumo: Insumo):
    tipo = insumo.tipo_item
    if tipo == Insumo.TIPO_MATERIA_PRIMA:
        usage_label = "Compra directa"
        usage_hint = "Se compra directo a proveedor y alimenta batidas o productos."
    elif tipo == Insumo.TIPO_INTERNO:
        usage_label = "Producción interna"
        usage_hint = "Se produce en planta y luego se usa como componente de otras recetas."
    else:
        usage_label = "Empaque final"
        usage_hint = "Se usa como material de presentación o entrega final."

    missing = []
    if not insumo.unidad_base_id:
        missing.append("unidad base")
    if tipo == Insumo.TIPO_MATERIA_PRIMA and not insumo.proveedor_principal_id:
        missing.append("proveedor principal")
    if tipo in {Insumo.TIPO_INTERNO, Insumo.TIPO_EMPAQUE} and not (insumo.categoria or "").strip():
        missing.append("categoría")
    if insumo.activo and not (insumo.codigo_point or "").strip():
        missing.append("código comercial")

    if not insumo.activo:
        readiness_label = "Inactivo"
        readiness_level = "danger"
        readiness_hint = "Registro fuera de operación."
    elif missing:
        readiness_label = "Incompleto"
        readiness_level = "warning"
        readiness_hint = "Faltan datos para operar con consistencia."
    else:
        readiness_label = "Lista para operar"
        readiness_level = "success"
        readiness_hint = "Cumple datos mínimos para compras, recetas e inventario."

    return {
        "usage_label": usage_label,
        "usage_hint": usage_hint,
        "missing": missing,
        "readiness_label": readiness_label,
        "readiness_level": readiness_level,
        "readiness_hint": readiness_hint,
    }


def _insumo_impact_profile(
    *,
    recipe_count: int,
    final_recipe_count: int,
    base_recipe_count: int,
    purchase_count: int,
    inventory_refs: int,
    operational_profile: dict,
):
    used_in_recipes = recipe_count > 0
    used_in_purchases = purchase_count > 0
    used_in_inventory = inventory_refs > 0
    active_scopes = sum([used_in_recipes, used_in_purchases, used_in_inventory])
    is_incomplete = operational_profile["readiness_label"] == "Incompleto"
    blocks_final = is_incomplete and final_recipe_count > 0
    blocks_purchases = is_incomplete and used_in_purchases
    blocks_inventory = is_incomplete and used_in_inventory
    blocks_costing = is_incomplete and used_in_recipes

    if blocks_final:
        level = "Crítico"
        tone = "danger"
        detail = "Bloquea producto final"
    elif blocks_costing:
        level = "Alto"
        tone = "warning"
        detail = "Bloquea costeo/MRP"
    elif blocks_purchases:
        level = "Alto"
        tone = "warning"
        detail = "Bloquea compras"
    elif blocks_inventory:
        level = "Alto"
        tone = "warning"
        detail = "Bloquea inventario"
    elif final_recipe_count > 0 or (active_scopes >= 2 and is_incomplete):
        level = "Alto"
        tone = "warning"
        detail = "Impacta venta final"
    elif active_scopes >= 2:
        level = "Medio"
        tone = "primary"
        detail = "Impacto multimódulo"
    elif used_in_recipes or used_in_purchases or used_in_inventory:
        level = "Bajo"
        tone = "secondary"
        detail = "Uso operativo acotado"
    else:
        level = "Sin impacto"
        tone = "success"
        detail = "Sin uso documentado"

    return {
        "level": level,
        "tone": tone,
        "detail": detail,
        "active_scopes": active_scopes,
        "used_in_final_products": final_recipe_count > 0,
        "used_in_purchases": used_in_purchases,
        "used_in_inventory": used_in_inventory,
        "is_multimodule": active_scopes >= 2,
        "is_critical": blocks_final,
        "blocks_purchases": blocks_purchases,
        "blocks_inventory": blocks_inventory,
        "blocks_costing": blocks_costing,
    }


def _insumo_recent_commercial_signal(insumo: Insumo, *, lookback_days: int = 45) -> dict[str, object]:
    final_recipe_ids = list(
        LineaReceta.objects.filter(
            insumo=insumo,
            receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .values_list("receta_id", flat=True)
        .distinct()
    )
    if not final_recipe_ids:
        return {
            "status": "Sin demanda final",
            "tone": "warning",
            "detail": "Este artículo todavía no está conectado a productos finales con venta directa reciente.",
            "days_count": 0,
            "branch_count": 0,
            "recipe_count": 0,
            "total_units": Decimal("0.0"),
            "avg_daily": Decimal("0.0"),
            "top_recipes": [],
            "scope_label": "Sin ventana comercial",
        }

    end_date = timezone.localdate() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(lookback_days - 1, 0))
    sales_qs = VentaHistorica.objects.select_related("receta", "sucursal").filter(
        receta_id__in=final_recipe_ids,
        fecha__gte=start_date,
        fecha__lte=end_date,
    )
    days_count = sales_qs.values("fecha").distinct().count()
    branch_count = sales_qs.values("sucursal_id").distinct().count()
    recipe_count = sales_qs.values("receta_id").distinct().count()
    total_units = sales_qs.aggregate(total=Sum("cantidad")).get("total") or Decimal("0")
    avg_daily = (
        (Decimal(str(total_units)) / Decimal(str(days_count))).quantize(Decimal("0.1"))
        if days_count > 0
        else Decimal("0.0")
    )
    top_recipes = list(
        sales_qs.values("receta__nombre")
        .annotate(total=Sum("cantidad"))
        .order_by("-total", "receta__nombre")[:4]
    )

    if days_count >= 12 and total_units > 0:
        status = "Demanda activa"
        tone = "success"
        detail = "El artículo ya impacta productos con venta reciente suficiente para priorizarlo en el maestro."
    elif days_count >= 5 and total_units > 0:
        status = "Demanda utilizable"
        tone = "warning"
        detail = "El artículo ya tiene huella comercial, pero todavía con cobertura parcial."
    elif total_units > 0:
        status = "Demanda limitada"
        tone = "warning"
        detail = "Ya existe demanda asociada, aunque todavía con poca profundidad histórica."
    else:
        status = "Sin demanda reciente"
        tone = "warning"
        detail = "El artículo está ligado a producto final, pero no registra venta reciente en la ventana observada."

    return {
        "status": status,
        "tone": tone,
        "detail": detail,
        "days_count": days_count,
        "branch_count": branch_count,
        "recipe_count": recipe_count,
        "total_units": Decimal(str(total_units)).quantize(Decimal("0.1")),
        "avg_daily": avg_daily,
        "top_recipes": top_recipes,
        "scope_label": f"{start_date.isoformat()} a {end_date.isoformat()}",
    }


def _maestro_demand_priority_rows(
    active_qs,
    active_usage_maps: dict[str, object],
    *,
    lookback_days: int = 45,
    limit: int = 6,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    def _priority_meta(historico_units: Decimal, days_count: int, final_recipe_count: int) -> dict[str, object]:
        if historico_units >= Decimal("80") or (days_count >= 12 and final_recipe_count >= 2):
            return {
                "label": "Demanda crítica bloqueada",
                "tone": "danger",
                "detail": "El artículo ya sostiene venta final relevante y su brecha maestra debe cerrarse de inmediato.",
                "is_critical": True,
            }
        if historico_units >= Decimal("30") or days_count >= 5:
            return {
                "label": "Alta demanda en revisión",
                "tone": "warning",
                "detail": "El artículo ya tiene tracción comercial suficiente para priorizar su cierre maestro.",
                "is_critical": False,
            }
        return {
            "label": "Seguimiento comercial",
            "tone": "primary",
            "detail": "El artículo ya tiene uso comercial, pero todavía sin presión crítica de demanda.",
            "is_critical": False,
        }

    candidates: list[dict[str, object]] = []
    for insumo in active_qs.select_related("unidad_base", "proveedor_principal"):
        operational_profile = _insumo_operational_profile(insumo)
        final_recipe_count = int(active_usage_maps["final_recipe_counts"].get(insumo.id, 0))
        if operational_profile["readiness_label"] != "Incompleto" or final_recipe_count <= 0:
            continue
        candidates.append(
            {
                "insumo": insumo,
                "operational_profile": operational_profile,
                "final_recipe_count": final_recipe_count,
                "recipe_count": int(active_usage_maps["recipe_counts"].get(insumo.id, 0)),
                "purchase_count": int(active_usage_maps["purchase_counts"].get(insumo.id, 0)),
                "inventory_refs": int(active_usage_maps["movement_counts"].get(insumo.id, 0))
                + int(active_usage_maps["adjustment_counts"].get(insumo.id, 0))
                + (1 if insumo.id in active_usage_maps["existence_ids"] else 0),
            }
        )
    if not candidates:
        return [], {
            "status": "Sin bloqueos críticos por demanda",
            "tone": "success",
            "detail": "No hay artículos incompletos de alta demanda ligados a venta final.",
            "count": 0,
            "scope_label": "Sin ventana comercial",
        }

    candidate_ids = [row["insumo"].id for row in candidates]
    receta_map: dict[int, list[int]] = defaultdict(list)
    for receta_id, insumo_id in (
        LineaReceta.objects.filter(
            insumo_id__in=candidate_ids,
            receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .values_list("receta_id", "insumo_id")
    ):
        receta_map[int(insumo_id)].append(int(receta_id))

    receta_ids = sorted({rid for values in receta_map.values() for rid in values})
    end_date = timezone.localdate() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(lookback_days - 1, 0))
    historico_totals: dict[int, Decimal] = {}
    historico_days: dict[int, int] = {}
    receta_name_map: dict[int, str] = {}
    if receta_ids:
        historico_totals = {
            int(row["receta_id"]): Decimal(str(row["total"] or 0))
            for row in (
                VentaHistorica.objects.filter(
                    receta_id__in=receta_ids,
                    fecha__gte=start_date,
                    fecha__lte=end_date,
                )
                .values("receta_id")
                .annotate(total=Sum("cantidad"))
            )
        }
        historico_days = {
            int(row["receta_id"]): int(row["days"] or 0)
            for row in (
                VentaHistorica.objects.filter(
                    receta_id__in=receta_ids,
                    fecha__gte=start_date,
                    fecha__lte=end_date,
                )
                .values("receta_id")
                .annotate(days=Count("fecha", distinct=True))
            )
        }
        receta_name_map = {
            int(row["id"]): row["nombre"]
            for row in Receta.objects.filter(id__in=receta_ids).values("id", "nombre")
        }

    rows: list[dict[str, object]] = []
    for item in candidates:
        insumo = item["insumo"]
        linked_recipe_ids = receta_map.get(insumo.id, [])
        historico_units = sum((historico_totals.get(rid, Decimal("0")) for rid in linked_recipe_ids), Decimal("0"))
        days_count = sum(historico_days.get(rid, 0) for rid in linked_recipe_ids)
        impact = _insumo_impact_profile(
            recipe_count=item["recipe_count"],
            final_recipe_count=item["final_recipe_count"],
            base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(insumo.id, 0)),
            purchase_count=item["purchase_count"],
            inventory_refs=item["inventory_refs"],
            operational_profile=item["operational_profile"],
        )
        if impact["is_critical"]:
            blocker_label = "Producto final"
        elif impact["blocks_costing"]:
            blocker_label = "Costeo / BOM"
        elif impact["blocks_purchases"]:
            blocker_label = "Compras"
        elif impact["blocks_inventory"]:
            blocker_label = "Inventario"
        else:
            blocker_label = "Maestro ERP"
        priority_meta = _priority_meta(historico_units, days_count, item["final_recipe_count"])
        rows.append(
            {
                "insumo_id": insumo.id,
                "insumo_nombre": insumo.nombre,
                "priority_label": priority_meta["label"],
                "priority_tone": priority_meta["tone"],
                "priority_detail": priority_meta["detail"],
                "is_demand_critical": priority_meta["is_critical"],
                "historico_units": historico_units.quantize(Decimal("0.1")),
                "days_count": days_count,
                "blocker_label": blocker_label,
                "final_recipe_count": item["final_recipe_count"],
                "missing": item["operational_profile"]["missing"][:2],
                "recipe_names": [receta_name_map[rid] for rid in linked_recipe_ids[:3] if rid in receta_name_map],
                "priority_score": (
                    historico_units * Decimal("10")
                    + Decimal(str(item["final_recipe_count"] * 8))
                    + (Decimal("40") if impact["is_critical"] else Decimal("0"))
                ),
                "detail_url": reverse("maestros:insumo_update", args=[insumo.id]),
            }
        )

    rows.sort(
        key=lambda row: (
            Decimal(str(row["priority_score"] or 0)),
            Decimal(str(row["historico_units"] or 0)),
            int(row["final_recipe_count"] or 0),
        ),
        reverse=True,
    )
    limited_rows = rows[:limit]
    critical_count = sum(1 for row in rows if row["is_demand_critical"])
    return limited_rows, {
        "status": (
            "Demanda crítica bloqueada"
            if critical_count
            else "Demanda priorizada"
            if limited_rows
            else "Sin bloqueos críticos por demanda"
        ),
        "tone": "danger" if critical_count else "warning" if limited_rows else "success",
        "detail": (
            f"{critical_count} artículo(s) ya sostienen venta final relevante y deben cerrarse primero."
            if critical_count
            else "Estos artículos incompletos ya afectan productos con venta reciente y deben cerrarse primero."
            if limited_rows
            else "No hay artículos incompletos de alta demanda ligados a venta final."
        ),
        "count": len(rows),
        "critical_count": critical_count,
        "scope_label": f"{start_date.isoformat()} a {end_date.isoformat()}",
    }


def _maestro_operational_health_cards(
    *,
    total_ready: int,
    total_incomplete: int,
    total_duplicate_groups: int,
    final_blockers: int,
    purchase_blockers: int,
    inventory_blockers: int,
) -> list[dict[str, object]]:
    return [
        {
            "label": "Artículos listos para operar",
            "value": total_ready,
            "tone": "success" if total_ready else "warning",
            "detail": "Registros activos completos para operar en compras, recetas e inventario.",
        },
        {
            "label": "Artículos incompletos",
            "value": total_incomplete,
            "tone": "danger" if total_incomplete else "success",
            "detail": "Registros con faltantes que todavía bloquean flujo operativo.",
        },
        {
            "label": "Grupos duplicados",
            "value": total_duplicate_groups,
            "tone": "warning" if total_duplicate_groups else "success",
            "detail": "Conjuntos de referencias que requieren consolidación canónica.",
        },
        {
            "label": "Bloquean producto final",
            "value": final_blockers,
            "tone": "danger" if final_blockers else "success",
            "detail": "Artículos incompletos ya consumidos por productos finales.",
        },
        {
            "label": "Bloquean compras",
            "value": purchase_blockers,
            "tone": "warning" if purchase_blockers else "success",
            "detail": "Artículos incompletos todavía usados por solicitudes u órdenes.",
        },
        {
            "label": "Bloquean inventario",
            "value": inventory_blockers,
            "tone": "warning" if inventory_blockers else "success",
            "detail": "Artículos incompletos con existencias, movimientos o ajustes.",
        },
    ]


def _erp_model_stage_rows(
    *,
    total_active: int,
    total_ready: int,
    recipe_used_count: int,
    purchase_used_count: int,
    inventory_used_count: int,
) -> list[dict[str, object]]:
    def _completion(closed: int, total: int) -> int:
        if total <= 0:
            return 0
        return int(round((closed / total) * 100))

    maestro_closed = total_ready
    bom_closed = min(recipe_used_count, maestro_closed)
    compras_closed = min(purchase_used_count, maestro_closed)
    inventario_closed = min(inventory_used_count, maestro_closed)

    return [
        {
            "step": "01",
            "title": "Datos maestros",
            "closed": maestro_closed,
            "pending": max(total_active - maestro_closed, 0),
            "completion": _completion(maestro_closed, total_active),
            "detail": "Unidad base, categoría, proveedor y clasificación operativa del artículo.",
            "url": reverse("maestros:insumo_list"),
            "cta": "Abrir maestro",
        },
        {
            "step": "02",
            "title": "Uso en BOM",
            "closed": bom_closed,
            "pending": max(total_active - bom_closed, 0),
            "completion": _completion(bom_closed, total_active),
            "detail": "Artículos ya integrados en recetas base, derivados y producto final.",
            "url": reverse("recetas:recetas_list"),
            "cta": "Abrir recetas",
        },
        {
            "step": "03",
            "title": "Compras documentales",
            "closed": compras_closed,
            "pending": max(total_active - compras_closed, 0),
            "completion": _completion(compras_closed, total_active),
            "detail": "Artículos ya trazados en solicitudes, órdenes y recepciones.",
            "url": reverse("compras:solicitudes"),
            "cta": "Abrir compras",
        },
        {
            "step": "04",
            "title": "Inventario y reabasto",
            "closed": inventario_closed,
            "pending": max(total_active - inventario_closed, 0),
            "completion": _completion(inventario_closed, total_active),
            "detail": "Artículos con existencia, movimiento operativo o punto de reorden vivo.",
            "url": reverse("inventario:existencias"),
            "cta": "Abrir inventario",
        },
    ]


def _maestros_critical_path_rows(chain: list[dict[str, object]]) -> list[dict[str, object]]:
    severity_order = {"danger": 0, "warning": 1, "success": 2, "primary": 3}
    ranked = sorted(
        chain,
        key=lambda item: (
            severity_order.get(str(item.get("tone") or "warning"), 9),
            -int(item.get("count") or 0),
            int(item.get("completion") or 0),
        ),
    )
    rows: list[dict[str, object]] = []
    for index, item in enumerate(ranked[:4], start=1):
        rows.append(
            {
                "rank": f"R{index}",
                "title": item.get("title", "Tramo del maestro"),
                "owner": item.get("owner", "Maestros / Operación"),
                "status": item.get("status", "Sin estado"),
                "tone": item.get("tone", "warning"),
                "count": int(item.get("count") or 0),
                "completion": int(item.get("completion") or 0),
                "depends_on": item.get("depends_on", "Inicio del flujo"),
                "dependency_status": item.get("dependency_status", "Sin dependencia registrada"),
                "detail": item.get("detail", ""),
                "next_step": item.get("next_step", "Revisar tramo"),
                "url": item.get("url", reverse("maestros:insumo_list")),
                "cta": item.get("cta", "Abrir"),
            }
        )
    return rows


def _maestros_executive_radar_rows(
    document_stage_rows: list[dict[str, object]],
    enterprise_chain: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, stage in enumerate(document_stage_rows, start=1):
        chain = enterprise_chain[min(index - 1, len(enterprise_chain) - 1)] if enterprise_chain else {}
        open_count = int(stage.get("open", 0) or 0)
        completion = int(stage.get("completion", 0) or 0)
        if completion >= 90:
            tone = "success"
            status = "Controlado"
            dominant_blocker = "Sin brecha dominante"
        elif completion >= 50:
            tone = "warning"
            status = "En seguimiento"
            dominant_blocker = stage.get("detail") or "Brecha operativa en seguimiento"
        else:
            tone = "danger"
            status = "Con bloqueo"
            dominant_blocker = stage.get("detail") or "Bloqueo operativo abierto"
        rows.append(
            {
                "phase": stage.get("label", f"Fase {index}"),
                "owner": (
                    "Maestros / DG"
                    if index == 1
                    else "Producción / Costeo"
                    if index == 2
                    else "Compras"
                    if index == 3
                    else "Inventario / Almacén"
                ),
                "status": status,
                "tone": tone,
                "blockers": open_count,
                "progress_pct": completion,
                "dominant_blocker": dominant_blocker,
                "depends_on": chain.get("title", "Origen del maestro"),
                "dependency_status": chain.get("status", "Sin dependencia registrada"),
                "next_step": stage.get("next_step") or chain.get("cta") or "Abrir fase",
                "url": stage.get("url") or chain.get("url") or reverse("maestros:insumo_list"),
                "cta": chain.get("cta") or "Abrir",
            }
        )
    return rows


def _insumo_critical_path_rows(module_blocker_cards: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    previous_blocked = False
    for index, card in enumerate(module_blocker_cards, start=1):
        is_blocked = bool(card.get("is_blocked"))
        dependency_status = (
            f"Condicionado por {module_blocker_cards[index - 2]['title']}"
            if index > 1 and previous_blocked
            else "Listo para avanzar"
            if not is_blocked
            else "Con bloqueo propio"
        )
        rows.append(
            {
                "rank": f"R{index}",
                "title": card["title"],
                "owner": card["owner"],
                "status": "Bloqueado" if is_blocked else "Liberado",
                "tone": card["tone"],
                "count": int(card.get("count") or 0),
                "completion": 0 if is_blocked else 100,
                "depends_on": module_blocker_cards[index - 2]["title"] if index > 1 else "Inicio del artículo",
                "dependency_status": dependency_status,
                "detail": card["detail"],
                "next_step": card["action_label"],
                "url": card["action_url"],
                "cta": card["action_label"],
            }
        )
        previous_blocked = is_blocked
    return rows


def _insumo_release_rows(module_blocker_cards: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for card in module_blocker_cards:
        is_blocked = bool(card.get("is_blocked"))
        blocked_count = int(card.get("count") or 0) if is_blocked else 0
        closed_count = 0 if is_blocked else 1
        rows.append(
            {
                "module": card["title"],
                "owner": card["owner"],
                "open": blocked_count,
                "closed": closed_count,
                "completion": 0 if is_blocked else 100,
                "status": "Bloqueado" if is_blocked else "Liberado",
                "tone": card["tone"],
                "detail": card["detail"],
                "next_step": card["action_label"],
                "url": card["action_url"],
                "cta": card["action_label"],
            }
        )
    return rows


def _insumo_downstream_handoff_rows(
    module_blocker_cards: list[dict[str, object]],
    *,
    completion_map: dict[str, int] | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    completion_map = completion_map or {}
    meta_map = {
        "Producto final": {
            "depends_on": "Maestro + BOM final",
            "exit_criteria": "El artículo ya puede participar en producto final sin bloqueo maestro.",
        },
        "Costeo / BOM": {
            "depends_on": "Maestro + recetas base/finales",
            "exit_criteria": "El artículo ya costea y explota BOM/MRP con identidad estable.",
        },
        "Compras": {
            "depends_on": "Proveedor + abastecimiento documental",
            "exit_criteria": "El artículo ya puede fluir por solicitud, orden y recepción sin fricción documental.",
        },
        "Inventario": {
            "depends_on": "Maestro + recepciones + ledger",
            "exit_criteria": "El artículo ya puede sostener stock, movimientos y ajustes con trazabilidad consistente.",
        },
    }
    for card in module_blocker_cards:
        blockers = int(card.get("count") or 0) if bool(card.get("is_blocked")) else 0
        completion = int(completion_map.get(card["title"], 100 if blockers == 0 else 0))
        meta = meta_map.get(
            card["title"],
            {
                "depends_on": "Maestro estable",
                "exit_criteria": "El artículo ya puede operar downstream sin bloqueos maestros.",
            },
        )
        rows.append(
            {
                "label": card["title"],
                "owner": card["owner"],
                "status": "Bloqueado" if blockers else "Listo para bajar",
                "blockers": blockers,
                "completion": completion,
                "depends_on": meta["depends_on"],
                "exit_criteria": meta["exit_criteria"],
                "detail": card["detail"],
                "next_step": card.get("action_label") or card.get("cta") or "Abrir",
                "url": card.get("action_url") or card.get("url") or reverse("maestros:insumo_list"),
                "cta": card.get("action_label") or card.get("cta") or "Abrir",
                "tone": card["tone"],
            }
        )
    return rows


def _insumo_trunk_handoff_rows(
    module_blocker_cards: list[dict[str, object]],
    *,
    completion_map: dict[str, int] | None = None,
) -> list[dict[str, object]]:
    completion_map = completion_map or {}
    by_title = {card["title"]: card for card in module_blocker_cards}
    costeo = by_title.get("Costeo / BOM", {})
    finales = by_title.get("Producto final", {})
    compras = by_title.get("Compras", {})
    inventario = by_title.get("Inventario", {})

    recetas_blockers = int(costeo.get("count") or 0)
    if bool(finales.get("is_blocked")):
        recetas_blockers += int(finales.get("count") or 0)
    recetas_completion = min(
        100,
        max(
            int(completion_map.get("Costeo / BOM", 100 if recetas_blockers == 0 else 25)),
            int(completion_map.get("Producto final", 100 if not bool(finales.get("is_blocked")) else 25)),
        ),
    ) if recetas_blockers == 0 else min(
        int(completion_map.get("Costeo / BOM", 25)),
        int(completion_map.get("Producto final", 25)),
    )

    return [
        {
            "label": "Recetas / BOM",
            "owner": "Producción / Costeo",
            "status": "Bloqueado" if recetas_blockers else "Listo para operar",
            "blockers": recetas_blockers,
            "completion": recetas_completion,
            "depends_on": "Maestro listo + estructura BOM consistente",
            "exit_criteria": "El artículo ya puede usarse en bases, derivados y producto final sin bloqueo maestro.",
            "detail": "Integra costeo, cadena base-derivados y liberación del producto final.",
            "next_step": costeo.get("action_label") or finales.get("action_label") or "Abrir recetas",
            "url": costeo.get("action_url") or finales.get("action_url") or reverse("recetas:recetas_list"),
            "cta": costeo.get("action_label") or finales.get("action_label") or "Abrir recetas",
            "tone": "danger" if recetas_blockers else "success",
        },
        {
            "label": "Compras documentales",
            "owner": "Compras",
            "status": "Bloqueado" if bool(compras.get("is_blocked")) else "Listo para operar",
            "blockers": int(compras.get("count") or 0) if bool(compras.get("is_blocked")) else 0,
            "completion": int(completion_map.get("Compras", 100 if not bool(compras.get("is_blocked")) else 25)),
            "depends_on": "Proveedor principal + trazabilidad documental",
            "exit_criteria": "El artículo ya puede bajar a solicitud, orden y recepción sin fricción documental.",
            "detail": "Controla abastecimiento formal, costos y cierre del ciclo de compra.",
            "next_step": compras.get("action_label") or "Abrir compras",
            "url": compras.get("action_url") or reverse("compras:solicitudes"),
            "cta": compras.get("action_label") or "Abrir compras",
            "tone": compras.get("tone", "success"),
        },
        {
            "label": "Inventario / Reabasto",
            "owner": "Inventario / Almacén",
            "status": "Bloqueado" if bool(inventario.get("is_blocked")) else "Listo para operar",
            "blockers": int(inventario.get("count") or 0) if bool(inventario.get("is_blocked")) else 0,
            "completion": int(completion_map.get("Inventario", 100 if not bool(inventario.get("is_blocked")) else 25)),
            "depends_on": "Recepciones aplicadas + ledger consistente",
            "exit_criteria": "El artículo ya puede sostener stock, movimientos, ajustes y reabasto sin brecha maestra.",
            "detail": "Asegura existencia viva, rotación y abastecimiento operativo.",
            "next_step": inventario.get("action_label") or "Abrir inventario",
            "url": inventario.get("action_url") or reverse("inventario:existencias"),
            "cta": inventario.get("action_label") or "Abrir inventario",
            "tone": inventario.get("tone", "success"),
        },
    ]


def _match_impact_scope(impact_profile: dict, impact_scope: str) -> bool:
    if impact_scope == "critical":
        return impact_profile["is_critical"]
    if impact_scope == "high":
        return impact_profile["level"] == "Alto"
    if impact_scope == "multimodule":
        return impact_profile["is_multimodule"]
    if impact_scope == "finales":
        return impact_profile["used_in_final_products"]
    if impact_scope == "compras":
        return impact_profile["used_in_purchases"]
    if impact_scope == "inventario":
        return impact_profile["used_in_inventory"]
    if impact_scope == "bloquea_compras":
        return impact_profile["blocks_purchases"]
    if impact_scope == "bloquea_inventario":
        return impact_profile["blocks_inventory"]
    if impact_scope == "bloquea_costeo":
        return impact_profile["blocks_costing"]
    return True


def _validate_insumo_enterprise_form(form):
    cleaned = getattr(form, "cleaned_data", {})
    tipo_item = cleaned.get("tipo_item")
    activo = bool(cleaned.get("activo"))
    unidad_base = cleaned.get("unidad_base")
    proveedor_principal = cleaned.get("proveedor_principal")
    categoria = (cleaned.get("categoria") or "").strip()
    codigo_point = (cleaned.get("codigo_point") or "").strip()
    instance = getattr(form, "instance", None)

    if activo and not unidad_base:
        form.add_error("unidad_base", "Todo artículo activo debe tener unidad base para operar en ERP.")

    if activo and tipo_item == Insumo.TIPO_MATERIA_PRIMA and not proveedor_principal:
        form.add_error("proveedor_principal", "La materia prima activa debe tener proveedor principal.")

    if activo and tipo_item in {Insumo.TIPO_INTERNO, Insumo.TIPO_EMPAQUE} and not categoria:
        form.add_error("categoria", "Este artículo activo debe tener categoría para orden operativo.")

    if codigo_point:
        normalized_point = normalizar_codigo_point(codigo_point)
        existing_qs = Insumo.objects.filter(codigo_point__gt="", activo=True)
        if instance and instance.pk:
            existing_qs = existing_qs.exclude(pk=instance.pk)
        for existing in existing_qs.only("id", "codigo_point", "nombre"):
            if normalizar_codigo_point(existing.codigo_point) == normalized_point:
                form.add_error(
                    "codigo_point",
                    f"El código comercial ya está ligado a otro artículo activo: {existing.nombre}.",
                )
                break


def _normalize_insumo_form_instance(form):
    instance = form.instance
    instance.codigo = (instance.codigo or "").strip()
    instance.codigo_point = (instance.codigo_point or "").strip().upper()
    instance.nombre = (instance.nombre or "").strip()
    instance.nombre_point = (instance.nombre_point or "").strip()
    instance.categoria = (instance.categoria or "").strip()


def _match_missing_field(insumo: Insumo, missing_field: str) -> bool:
    profile = _insumo_operational_profile(insumo)
    if missing_field == "unidad":
        return "unidad base" in profile["missing"]
    if missing_field == "proveedor":
        return "proveedor principal" in profile["missing"]
    if missing_field == "categoria":
        return "categoría" in profile["missing"]
    if missing_field == "codigo_point":
        return "código comercial" in profile["missing"] or "código externo" in profile["missing"]
    return True


def _missing_impact_navigation(active_qs, active_usage_maps, active_profiles):
    impact_definitions = [
        {
            "key": "critical",
            "title": "Bloquea producto final",
            "description": "Artículos incompletos que ya frenan venta final.",
            "query_suffix": "usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos&impact_scope=critical",
        },
        {
            "key": "finales",
            "title": "Impacta producto final",
            "description": "Artículos incompletos usados por productos finales.",
            "query_suffix": "usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos&impact_scope=finales",
        },
        {
            "key": "compras",
            "title": "Impacta compras",
            "description": "Artículos incompletos ya presentes en solicitudes u órdenes.",
            "query_suffix": "usage_scope=purchases&enterprise_status=incompletos&impact_scope=compras",
        },
        {
            "key": "inventario",
            "title": "Impacta inventario",
            "description": "Artículos incompletos con stock, movimiento o ajuste.",
            "query_suffix": "usage_scope=inventory&enterprise_status=incompletos&impact_scope=inventario",
        },
        {
            "key": "multimodule",
            "title": "Impacta varios módulos",
            "description": "Artículos incompletos con alcance multimódulo.",
            "query_suffix": "enterprise_status=incompletos&impact_scope=multimodule",
        },
    ]
    missing_definitions = [
        ("unidad", "Unidad base"),
        ("proveedor", "Proveedor principal"),
        ("categoria", "Categoría"),
        ("codigo_point", "Código comercial"),
    ]

    cards = []
    items = list(active_qs.select_related("unidad_base", "proveedor_principal"))
    for missing_key, missing_label in missing_definitions:
        filtered_items = [
            item
            for item in items
            if active_profiles[item.id]["readiness_label"] == "Incompleto"
            and _match_missing_field(item, missing_key)
        ]
        for impact in impact_definitions:
            matched = []
            for item in filtered_items:
                impact_profile = _insumo_impact_profile(
                    recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                    final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                    base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                    purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                    inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                    + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                    + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                    operational_profile=active_profiles[item.id],
                )
                if _match_impact_scope(impact_profile, impact["key"]):
                    matched.append(item)
            if not matched:
                continue
            dominant_type = max(
                (item.tipo_item or Insumo.TIPO_MATERIA_PRIMA for item in matched),
                key=lambda tipo: sum(1 for item in matched if (item.tipo_item or Insumo.TIPO_MATERIA_PRIMA) == tipo),
            )
            cards.append(
                {
                    "key": f"{missing_key}:{impact['key']}",
                    "missing_key": missing_key,
                    "missing_label": missing_label,
                    "impact_key": impact["key"],
                    "impact_title": impact["title"],
                    "description": impact["description"],
                    "count": len(matched),
                    "dominant_type_label": _insumo_type_label(dominant_type),
                    "query": f"?missing_field={missing_key}&{impact['query_suffix']}",
                }
            )

    cards.sort(key=lambda item: (-item["count"], item["missing_label"], item["impact_title"]))
    return cards[:10]


def _build_duplicate_groups(active_qs):
    duplicate_norms = list(
        active_qs.values("nombre_normalizado")
        .annotate(total=Count("id"))
        .filter(total__gt=1)
        .values_list("nombre_normalizado", flat=True)
    )
    duplicate_norms_set = set(duplicate_norms)
    canonical_rows = canonicalized_active_insumos(limit=5000)
    duplicate_candidates = [
        item
        for row in canonical_rows
        for item in row["items"]
        if row["normalized_name"] in duplicate_norms_set
    ]
    duplicate_candidates.sort(key=lambda item: ((item.nombre_normalizado or ""), item.nombre))
    duplicate_groups_map = {}
    for item in duplicate_candidates:
        duplicate_groups_map.setdefault(item.nombre_normalizado, []).append(item)

    duplicate_groups = []
    canonical_ids = set()
    duplicate_ids = set()
    for _, items in duplicate_groups_map.items():
        ordered = sorted(items, key=lambda x: (duplicate_priority(x), x.id), reverse=True)
        canonical = ordered[0]
        latest_costo = latest_costo_canonico(canonical)
        canonical.latest_costo_unitario = latest_costo
        for item in ordered:
            item.latest_costo_unitario = latest_costo
        canonical_ids.add(canonical.id)
        duplicate_ids.update(item.id for item in ordered)
        duplicate_groups.append(
            {
                "normalized_name": canonical.nombre_normalizado,
                "display_name": canonical.nombre,
                "canonical": canonical,
                "duplicates": ordered,
                "count": len(ordered),
            }
        )
    duplicate_groups.sort(key=lambda g: (-g["count"], g["display_name"].lower()))
    variant_ids = duplicate_ids - canonical_ids
    return duplicate_groups, duplicate_ids, canonical_ids, variant_ids


def _canonicalize_insumo_target(insumo: Insumo | None) -> Insumo | None:
    return canonical_insumo(insumo)


def _point_pending_canonical_targets(rows):
    rows = list(rows)
    suggestion_norms = {
        normalizar_nombre(row.fuzzy_sugerencia or "")
        for row in rows
        if (row.fuzzy_sugerencia or "").strip()
    }
    suggestion_norms.discard("")
    if not suggestion_norms:
        return {}

    candidates = [
        row["canonical"]
        for row in canonicalized_active_insumos(limit=5000)
        if row["normalized_name"] in suggestion_norms
    ]
    by_norm = {}
    for insumo in candidates:
        key = insumo.nombre_normalizado or normalizar_nombre(insumo.nombre or "")
        insumo.latest_costo_unitario = latest_costo_canonico(insumo)
        by_norm[key] = insumo
    return by_norm

# ============ PROVEEDORES ============

class ProveedorListView(LoginRequiredMixin, ListView):
    model = Proveedor
    template_name = 'maestros/proveedor_list.html'
    context_object_name = 'proveedores'
    paginate_by = 20
    
    def get_queryset(self):
        queryset = Proveedor.objects.all()
        search = self.request.GET.get('q')
        selected_insumo_id = (self.request.GET.get("insumo_id") or "").strip()
        estado = self.request.GET.get('estado')
        if search:
            queryset = queryset.filter(nombre__icontains=search)
        if estado == "activos":
            queryset = queryset.filter(activo=True)
        elif estado == "inactivos":
            queryset = queryset.filter(activo=False)
        return queryset.order_by('nombre')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        qs = self.get_queryset()
        total_proveedores = qs.count()
        total_activos = qs.filter(activo=True).count()
        total_inactivos = max(total_proveedores - total_activos, 0)
        avg_lead_time = (
            round(sum((p.lead_time_dias or 0) for p in qs) / total_proveedores, 1)
            if total_proveedores
            else 0
        )
        if total_inactivos > 0:
            erp_command_center = {
                "owner": "Compras / DG",
                "status": "En revisión",
                "tone": "warning",
                "blockers": total_inactivos,
                "next_step": "Revisar proveedores inactivos o incompletos antes de usarlos en compras y abastecimiento.",
                "url": reverse("maestros:proveedor_list") + "?estado=inactivos",
                "cta": "Abrir proveedores inactivos",
            }
        else:
            erp_command_center = {
                "owner": "Compras / DG",
                "status": "Estable",
                "tone": "success",
                "blockers": 0,
                "next_step": "Mantener proveedores vigentes, lead time actualizado y cobertura de compra estable.",
                "url": reverse("maestros:proveedor_create"),
                "cta": "Registrar proveedor",
            }
        workflow_rows = [
            {
                "step": "01",
                "title": "Alta del proveedor",
                "owner": "Compras",
                "completion": 100 if total_proveedores else 0,
                "tone": "success" if total_proveedores else "warning",
                "detail": f"{total_proveedores} proveedor(es) registrados en el maestro.",
                "next_step": "Mantener razón social y estatus operativo actualizados." if total_proveedores else "Dar de alta el primer proveedor operativo.",
                "action_label": "Nuevo proveedor",
                "action_href": reverse("maestros:proveedor_create"),
            },
            {
                "step": "02",
                "title": "Vigencia operativa",
                "owner": "Compras / DG",
                "completion": int(round((total_activos / total_proveedores) * 100)) if total_proveedores else 0,
                "tone": "success" if total_inactivos == 0 and total_proveedores else "warning",
                "detail": f"{total_activos} activo(s) y {total_inactivos} inactivo(s).",
                "next_step": "Reactivar o depurar proveedores fuera de operación." if total_inactivos else "Mantener cartera vigente.",
                "action_label": "Ver inactivos" if total_inactivos else "Ver activos",
                "action_href": reverse("maestros:proveedor_list") + (("?estado=inactivos") if total_inactivos else ("?estado=activos")),
            },
            {
                "step": "03",
                "title": "Abastecimiento",
                "owner": "Compras / Operaciones",
                "completion": 100 if avg_lead_time >= 0 else 0,
                "tone": "success" if total_activos else "warning",
                "detail": f"Lead time promedio {avg_lead_time} día(s).",
                "next_step": "Usar proveedores vigentes en solicitudes y órdenes documentales.",
                "action_label": "Abrir compras",
                "action_href": reverse("compras:solicitudes"),
            },
        ]
        erp_governance_rows = [
            {
                "front": "Carpeta de proveedores",
                "owner": "Compras / DG",
                "blockers": total_inactivos,
                "completion": workflow_rows[1]["completion"],
                "detail": "Control de vigencia y disponibilidad del maestro de proveedores.",
                "next_step": workflow_rows[1]["next_step"],
                "url": workflow_rows[1]["action_href"],
                "cta": workflow_rows[1]["action_label"],
            },
            {
                "front": "Compras documentales",
                "owner": "Compras",
                "blockers": 0 if total_activos else 1,
                "completion": 100 if total_activos else 0,
                "detail": "Uso del proveedor dentro del flujo documental de solicitudes, órdenes y recepciones.",
                "next_step": "Asegurar que los artículos estratégicos tengan proveedor principal vigente.",
                "url": reverse("compras:solicitudes"),
                "cta": "Abrir compras",
            },
        ]
        context['search_query'] = self.request.GET.get('q', '')
        context['estado'] = self.request.GET.get('estado', '')
        context['total_proveedores'] = total_proveedores
        context['total_activos'] = total_activos
        context['total_inactivos'] = total_inactivos
        context['avg_lead_time'] = avg_lead_time
        context['erp_command_center'] = erp_command_center
        context['workflow_rows'] = workflow_rows
        context['erp_governance_rows'] = erp_governance_rows
        context["critical_path_rows"] = _maestros_critical_path_rows(workflow_rows)
        context["executive_radar_rows"] = _maestros_executive_radar_rows(workflow_rows, workflow_rows)
        return context


def _proveedor_form_context(*, provider: Proveedor | None, values=None) -> dict[str, object]:
    values = values or {}
    is_edit = bool(provider and provider.pk)
    nombre = (values.get("nombre") if values else None) or (provider.nombre if provider else "")
    lead_time_raw = (values.get("lead_time_dias") if values else None)
    lead_time = lead_time_raw if lead_time_raw not in (None, "") else (provider.lead_time_dias if provider else 0)
    activo_raw = values.get("activo") if values else None
    activo = bool(activo_raw) if values else bool(provider.activo if provider else True)
    readiness = "Listo" if activo and str(nombre).strip() else "En revisión"
    tone = "success" if readiness == "Listo" else "warning"
    blockers = 0 if readiness == "Listo" else 1
    workflow_rows = [
        {
            "step": "01",
            "title": "Alta maestra",
            "owner": "Compras",
            "completion": 100 if str(nombre).strip() else 40,
            "tone": "success" if str(nombre).strip() else "warning",
            "detail": "Nombre y registro base del proveedor dentro del maestro ERP.",
            "next_step": "Completar identificación del proveedor." if not str(nombre).strip() else "Validar vigencia operativa.",
        },
        {
            "step": "02",
            "title": "Vigencia operativa",
            "owner": "Compras / DG",
            "completion": 100 if activo else 50,
            "tone": "success" if activo else "warning",
            "detail": f"Lead time actual: {lead_time} día(s).",
            "next_step": "Mantener proveedor activo para compras documentales." if activo else "Revisar si debe volver a operación o quedar histórico.",
        },
        {
            "step": "03",
            "title": "Uso documental",
            "owner": "Compras / Operaciones",
            "completion": 100 if activo and str(nombre).strip() else 50,
            "tone": "success" if activo and str(nombre).strip() else "warning",
            "detail": "Proveedor listo para solicitudes, órdenes y recepciones.",
            "next_step": "Vincularlo con artículos estratégicos del maestro." if activo else "Corregir estatus antes de usarlo en abastecimiento.",
        },
    ]
    return {
        "erp_command_center": {
            "owner": "Compras / DG",
            "status": readiness,
            "tone": tone,
            "blockers": blockers,
            "next_step": (
                "Validar vigencia, lead time y uso documental del proveedor."
                if readiness == "Listo"
                else "Completar datos mínimos y definir si el proveedor sigue activo."
            ),
            "url": reverse("maestros:proveedor_list"),
            "cta": "Volver al catálogo",
        },
        "workflow_rows": workflow_rows,
        "critical_path_rows": _maestros_critical_path_rows(workflow_rows),
        "executive_radar_rows": _maestros_executive_radar_rows(workflow_rows, workflow_rows),
        "provider_checklist": [
            {"label": "Nombre maestro", "ok": bool(str(nombre).strip())},
            {"label": "Lead time definido", "ok": int(lead_time or 0) >= 0},
            {"label": "Estatus operativo", "ok": bool(activo)},
        ],
        "provider_form_mode": "edición" if is_edit else "alta",
    }


def _proveedor_delete_context(provider: Proveedor) -> dict[str, object]:
    from compras.models import OrdenCompra, SolicitudCompra

    insumos_count = Insumo.objects.filter(proveedor_principal=provider).count()
    solicitudes_count = SolicitudCompra.objects.filter(proveedor_sugerido=provider).count()
    ordenes_count = OrdenCompra.objects.filter(proveedor=provider).count()
    blockers = insumos_count + solicitudes_count + ordenes_count
    status = "Crítico" if blockers else "Controlado"
    tone = "danger" if blockers else "success"
    workflow_rows = [
        {
            "step": "01",
            "title": "Revisión de impacto",
            "owner": "Compras / DG",
            "completion": 100 if not blockers else 50,
            "tone": tone if blockers else "success",
            "detail": f"{insumos_count} artículo(s), {solicitudes_count} solicitud(es) y {ordenes_count} orden(es) ligados.",
            "next_step": "Reasignar referencias activas antes de retirar el proveedor." if blockers else "Confirmar baja documental del proveedor.",
        },
        {
            "step": "02",
            "title": "Retiro documental",
            "owner": "Compras",
            "completion": 100 if not provider.activo else 60,
            "tone": "success" if not provider.activo else "warning",
            "detail": "La baja debe preservar trazabilidad del histórico documental.",
            "next_step": "Desactivar primero si el proveedor aún sigue vigente." if provider.activo else "Validar confirmación final de baja.",
        },
    ]
    return {
        "erp_command_center": {
            "owner": "Compras / DG",
            "status": status,
            "tone": tone,
            "blockers": blockers,
            "next_step": (
                "Revisar impacto en artículos y documentos antes de eliminar."
                if blockers
                else "Confirmar retiro definitivo del proveedor del maestro."
            ),
            "url": reverse("maestros:proveedor_list"),
            "cta": "Volver al catálogo",
        },
        "workflow_rows": workflow_rows,
        "critical_path_rows": _maestros_critical_path_rows(workflow_rows),
        "executive_radar_rows": _maestros_executive_radar_rows(workflow_rows, workflow_rows),
        "provider_delete_checklist": [
            {"label": "Sin artículos ligados", "ok": insumos_count == 0},
            {"label": "Sin solicitudes ligadas", "ok": solicitudes_count == 0},
            {"label": "Sin órdenes ligadas", "ok": ordenes_count == 0},
        ],
        "provider_usage_totals": {
            "insumos": insumos_count,
            "solicitudes": solicitudes_count,
            "ordenes": ordenes_count,
        },
    }

class ProveedorCreateView(LoginRequiredMixin, CreateView):
    model = Proveedor
    template_name = 'maestros/proveedor_form.html'
    fields = ['nombre', 'lead_time_dias', 'activo']
    success_url = reverse_lazy('maestros:proveedor_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_proveedor_form_context(provider=None, values=self.request.POST or None))
        return context

class ProveedorUpdateView(LoginRequiredMixin, UpdateView):
    model = Proveedor
    template_name = 'maestros/proveedor_form.html'
    fields = ['nombre', 'lead_time_dias', 'activo']
    success_url = reverse_lazy('maestros:proveedor_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_proveedor_form_context(provider=self.object, values=self.request.POST or None))
        return context

class ProveedorDeleteView(LoginRequiredMixin, DeleteView):
    model = Proveedor
    template_name = 'maestros/proveedor_confirm_delete.html'
    success_url = reverse_lazy('maestros:proveedor_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_proveedor_delete_context(self.object))
        return context

# ============ INSUMOS ============

class InsumoListView(LoginRequiredMixin, ListView):
    model = Insumo
    template_name = 'maestros/insumo_list.html'
    context_object_name = 'insumos'
    paginate_by = 20
    
    def get_queryset(self):
        queryset = (
            Insumo.objects.select_related("unidad_base", "proveedor_principal")
            .annotate(
                latest_costo_unitario=Subquery(
                    CostoInsumo.objects.filter(insumo=OuterRef("pk")).order_by("-fecha", "-id").values("costo_unitario")[:1],
                    output_field=DecimalField(max_digits=18, decimal_places=6),
                ),
                latest_costo_fecha=Subquery(
                    CostoInsumo.objects.filter(insumo=OuterRef("pk")).order_by("-fecha", "-id").values("fecha")[:1],
                    output_field=DateField(),
                ),
            )
        )
        search = self.request.GET.get('q')
        estado = self.request.GET.get('estado')
        point_status = self.request.GET.get('point_status')
        costo_status = self.request.GET.get("costo_status")
        tipo_item = self.request.GET.get("tipo_item")
        categoria = (self.request.GET.get("categoria") or "").strip()
        enterprise_status = self.request.GET.get("enterprise_status")
        missing_field = self.request.GET.get("missing_field")
        duplicate_status = self.request.GET.get("duplicate_status")
        canonical_status = self.request.GET.get("canonical_status")
        usage_scope = self.request.GET.get("usage_scope")
        recipe_scope = self.request.GET.get("recipe_scope")
        impact_scope = self.request.GET.get("impact_scope")
        linked_recipe_id = (self.request.GET.get("linked_recipe_id") or "").strip()
        selected_insumo_id = (self.request.GET.get("insumo_id") or "").strip()
        if search:
            queryset = queryset.filter(
                Q(nombre__icontains=search)
                | Q(codigo__icontains=search)
                | Q(codigo_point__icontains=search)
                | Q(nombre_point__icontains=search)
            )
        if estado == "activos":
            queryset = queryset.filter(activo=True)
        elif estado == "inactivos":
            queryset = queryset.filter(activo=False)
        if selected_insumo_id.isdigit():
            queryset = queryset.filter(id=int(selected_insumo_id))
        if point_status == "pendientes":
            queryset = queryset.filter(activo=True).filter(Q(codigo_point="") | Q(codigo_point__isnull=True))
        elif point_status == "completos":
            queryset = queryset.filter(activo=True).exclude(Q(codigo_point="") | Q(codigo_point__isnull=True))
        if costo_status in {"sin_costo", "con_costo"}:
            canonical_rows = canonicalized_active_insumos(limit=5000)
            normalized_with_cost = {
                row["normalized_name"]
                for row in canonical_rows
                if row["canonical"].latest_costo_unitario is not None
                and row["canonical"].latest_costo_unitario > 0
            }
            if costo_status == "sin_costo":
                queryset = queryset.exclude(nombre_normalizado__in=normalized_with_cost)
            elif costo_status == "con_costo":
                queryset = queryset.filter(nombre_normalizado__in=normalized_with_cost)
        if tipo_item in {Insumo.TIPO_MATERIA_PRIMA, Insumo.TIPO_INTERNO, Insumo.TIPO_EMPAQUE}:
            queryset = queryset.filter(tipo_item=tipo_item)
        if categoria:
            queryset = queryset.filter(categoria=categoria)
        if enterprise_status in {"listos", "incompletos", "inactivos"}:
            matching_ids = [
                item.id
                for item in queryset
                if _match_enterprise_status(item, enterprise_status)
            ]
            queryset = queryset.filter(id__in=matching_ids)
        if missing_field in {"unidad", "proveedor", "categoria", "codigo_point"}:
            matching_ids = [
                item.id
                for item in queryset
                if _match_missing_field(item, missing_field)
            ]
            queryset = queryset.filter(id__in=matching_ids)
        if duplicate_status == "duplicados":
            duplicated_names = (
                Insumo.objects.filter(activo=True)
                .values("nombre_normalizado")
                .annotate(total=Count("id"))
                .filter(total__gt=1)
                .values_list("nombre_normalizado", flat=True)
            )
            queryset = queryset.filter(nombre_normalizado__in=duplicated_names)
        if canonical_status in {"canonicos", "variantes"}:
            _, _, canonical_ids, variant_ids = _build_duplicate_groups(Insumo.objects.filter(activo=True))
            if canonical_status == "canonicos":
                queryset = queryset.exclude(id__in=variant_ids)
            elif canonical_status == "variantes":
                queryset = queryset.filter(id__in=variant_ids)
        if usage_scope in {"recipes", "purchases", "inventory", "unused"}:
            usage_maps = _insumo_usage_maps(list(queryset.values_list("id", flat=True)))
            recipe_ids = set(usage_maps["recipe_counts"].keys())
            final_recipe_ids = set(usage_maps["final_recipe_counts"].keys())
            base_recipe_ids = set(usage_maps["base_recipe_counts"].keys())
            purchase_ids = set(usage_maps["purchase_counts"].keys())
            inventory_ids = (
                set(usage_maps["movement_counts"].keys())
                | set(usage_maps["adjustment_counts"].keys())
                | set(usage_maps["existence_ids"])
            )
            if usage_scope == "recipes":
                target_ids = recipe_ids
                if recipe_scope == "finales":
                    target_ids = final_recipe_ids
                elif recipe_scope == "bases":
                    target_ids = base_recipe_ids
                queryset = queryset.filter(id__in=target_ids)
            elif usage_scope == "purchases":
                queryset = queryset.filter(id__in=purchase_ids)
            elif usage_scope == "inventory":
                queryset = queryset.filter(id__in=inventory_ids)
            elif usage_scope == "unused":
                queryset = queryset.exclude(id__in=(recipe_ids | purchase_ids | inventory_ids))
        if impact_scope in {"critical", "high", "multimodule", "finales", "compras", "inventario", "bloquea_compras", "bloquea_inventario", "bloquea_costeo"}:
            usage_maps = _insumo_usage_maps(list(queryset.values_list("id", flat=True)))
            matching_ids = []
            for item in queryset.select_related("unidad_base", "proveedor_principal"):
                impact_profile = _insumo_impact_profile(
                    recipe_count=int(usage_maps["recipe_counts"].get(item.id, 0)),
                    final_recipe_count=int(usage_maps["final_recipe_counts"].get(item.id, 0)),
                    base_recipe_count=int(usage_maps["base_recipe_counts"].get(item.id, 0)),
                    purchase_count=int(usage_maps["purchase_counts"].get(item.id, 0)),
                    inventory_refs=int(usage_maps["movement_counts"].get(item.id, 0))
                    + int(usage_maps["adjustment_counts"].get(item.id, 0))
                    + (1 if item.id in usage_maps["existence_ids"] else 0),
                    operational_profile=_insumo_operational_profile(item),
                )
                if _match_impact_scope(impact_profile, impact_scope):
                    matching_ids.append(item.id)
            queryset = queryset.filter(id__in=matching_ids)
        if linked_recipe_id.isdigit():
            linked_insumo_ids = (
                LineaReceta.objects.filter(receta_id=int(linked_recipe_id), insumo__isnull=False)
                .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
                .values_list("insumo_id", flat=True)
                .distinct()
            )
            queryset = queryset.filter(id__in=linked_insumo_ids)
        return queryset.order_by('nombre')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        qs = self.get_queryset()
        active_qs = Insumo.objects.filter(activo=True)
        pending_point_qs = active_qs.filter(Q(codigo_point="") | Q(codigo_point__isnull=True))
        total_active = active_qs.count()
        total_pending_point = pending_point_qs.count()
        total_complete_point = max(total_active - total_pending_point, 0)
        point_ratio = round((total_complete_point * 100.0 / total_active), 2) if total_active else 100.0

        context['search_query'] = self.request.GET.get('q', '')
        context['estado'] = self.request.GET.get('estado', '')
        context['point_status'] = self.request.GET.get('point_status', '')
        context["costo_status"] = self.request.GET.get("costo_status", "")
        context["tipo_item"] = self.request.GET.get("tipo_item", "")
        context["categoria"] = (self.request.GET.get("categoria") or "").strip()
        context["enterprise_status"] = self.request.GET.get("enterprise_status", "")
        context["missing_field"] = self.request.GET.get("missing_field", "")
        context["duplicate_status"] = self.request.GET.get("duplicate_status", "")
        context["canonical_status"] = self.request.GET.get("canonical_status", "")
        context["usage_scope"] = self.request.GET.get("usage_scope", "")
        context["recipe_scope"] = self.request.GET.get("recipe_scope", "")
        context["impact_scope"] = self.request.GET.get("impact_scope", "")
        context["linked_recipe_id"] = (self.request.GET.get("linked_recipe_id") or "").strip()
        context["linked_recipe"] = None
        if context["linked_recipe_id"].isdigit():
            context["linked_recipe"] = Receta.objects.filter(pk=int(context["linked_recipe_id"])).only("id", "nombre").first()
        context["selected_insumo_id"] = (self.request.GET.get("insumo_id") or "").strip()
        context["selected_insumo"] = None
        if context["selected_insumo_id"].isdigit():
            context["selected_insumo"] = Insumo.objects.filter(pk=int(context["selected_insumo_id"])).only("id", "nombre").first()
        context['total_insumos'] = qs.count()
        context['total_activos'] = qs.filter(activo=True).count()
        context['total_point_pendientes'] = total_pending_point
        context['total_point_completos'] = total_complete_point
        context['point_ratio'] = point_ratio
        context["total_materia_prima"] = active_qs.filter(tipo_item=Insumo.TIPO_MATERIA_PRIMA).count()
        context["total_insumos_internos"] = active_qs.filter(tipo_item=Insumo.TIPO_INTERNO).count()
        context["total_empaques"] = active_qs.filter(tipo_item=Insumo.TIPO_EMPAQUE).count()
        recetas_qs = Receta.objects.all()
        context["total_productos_finales"] = recetas_qs.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).count()
        context["total_batidas_base"] = recetas_qs.filter(tipo=Receta.TIPO_PREPARACION, usa_presentaciones=False).count()
        context["total_subinsumos_derivados"] = recetas_qs.filter(tipo=Receta.TIPO_PREPARACION, usa_presentaciones=True).count()
        context["item_type_cards"] = _insumo_type_cards()
        context["categorias_catalogo"] = list(
            active_qs.exclude(categoria__exact="")
            .values_list("categoria", flat=True)
            .distinct()
            .order_by("categoria")
        )
        context["categorias_top"] = list(
            active_qs.exclude(categoria__exact="")
            .values("categoria")
            .annotate(total=Count("id"))
            .order_by("-total", "categoria")[:8]
        )
        duplicate_groups, duplicate_ids, canonical_ids, variant_ids = _build_duplicate_groups(active_qs)
        canonical_by_variant_id = {}
        for group in duplicate_groups:
            canonical = group["canonical"]
            for item in group["duplicates"]:
                if item.id != canonical.id:
                    canonical_by_variant_id[item.id] = canonical
        page_items = list(context["insumos"])
        page_duplicate_ids = duplicate_ids.intersection({item.id for item in page_items})
        usage_maps = _insumo_usage_maps([item.id for item in page_items])
        active_usage_maps = _insumo_usage_maps(list(active_qs.values_list("id", flat=True)))
        recipe_used_ids = set(active_usage_maps["recipe_counts"].keys())
        final_recipe_used_ids = set(active_usage_maps["final_recipe_counts"].keys())
        base_recipe_used_ids = set(active_usage_maps["base_recipe_counts"].keys())
        purchase_used_ids = set(active_usage_maps["purchase_counts"].keys())
        inventory_used_ids = (
            set(active_usage_maps["movement_counts"].keys())
            | set(active_usage_maps["adjustment_counts"].keys())
            | set(active_usage_maps["existence_ids"])
        )
        page_usage_summary = {
            "recipes": 0,
            "purchases": 0,
            "inventory": 0,
            "without_usage": 0,
        }
        page_impact_summary = {
            "critical": 0,
            "high": 0,
            "multimodule": 0,
            "final_products": 0,
        }
        for item in page_items:
            canonical_display = canonical_insumo(item) if item.activo else None
            if canonical_display:
                item.latest_costo_unitario = latest_costo_canonico(canonical_display)
                item.canonical_display = canonical_display
            item.has_duplicate_group = item.id in page_duplicate_ids
            item.is_canonical_record = item.id in canonical_ids or item.id not in duplicate_ids
            item.is_duplicate_variant = item.id in variant_ids
            item.canonical_target = canonical_by_variant_id.get(item.id)
            item.operational_profile = _insumo_operational_profile(item)
            recipe_count = int(usage_maps["recipe_counts"].get(item.id, 0))
            final_recipe_count = int(usage_maps["final_recipe_counts"].get(item.id, 0))
            base_recipe_count = int(usage_maps["base_recipe_counts"].get(item.id, 0))
            purchase_count = int(usage_maps["purchase_counts"].get(item.id, 0))
            movement_count = int(usage_maps["movement_counts"].get(item.id, 0))
            adjustment_count = int(usage_maps["adjustment_counts"].get(item.id, 0))
            has_existence = item.id in usage_maps["existence_ids"]
            inventory_refs = movement_count + adjustment_count + (1 if has_existence else 0)
            used_in_recipes = recipe_count > 0
            used_in_purchases = purchase_count > 0
            used_in_inventory = inventory_refs > 0
            if used_in_recipes:
                page_usage_summary["recipes"] += 1
            if used_in_purchases:
                page_usage_summary["purchases"] += 1
            if used_in_inventory:
                page_usage_summary["inventory"] += 1
            if not (used_in_recipes or used_in_purchases or used_in_inventory):
                page_usage_summary["without_usage"] += 1
            final_recipe_examples = []
            if final_recipe_count:
                final_recipe_examples = list(
                    LineaReceta.objects.filter(insumo_id=item.id, receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
                    .select_related("receta")
                    .order_by("receta__nombre")
                    .values_list("receta__nombre", flat=True)
                    .distinct()[:3]
                )
            item.usage_profile = {
                "recipe_count": recipe_count,
                "final_recipe_count": final_recipe_count,
                "base_recipe_count": base_recipe_count,
                "purchase_count": purchase_count,
                "movement_count": movement_count,
                "adjustment_count": adjustment_count,
                "has_existence": has_existence,
                "inventory_refs": inventory_refs,
                "used_in_recipes": used_in_recipes,
                "used_in_purchases": used_in_purchases,
                "used_in_inventory": used_in_inventory,
                "is_operational_blocker": item.operational_profile["readiness_label"] == "Incompleto" and (used_in_recipes or used_in_purchases or used_in_inventory),
                "blocks_costing": item.operational_profile["readiness_label"] == "Incompleto" and used_in_recipes,
                "blocks_purchases": item.operational_profile["readiness_label"] == "Incompleto" and used_in_purchases,
                "blocks_inventory": item.operational_profile["readiness_label"] == "Incompleto" and used_in_inventory,
                "blocks_final_products": item.operational_profile["readiness_label"] == "Incompleto" and final_recipe_count > 0,
                "blocking_final_products_count": final_recipe_count,
                "final_recipe_examples": final_recipe_examples,
                "recipes_url": f"{reverse('recetas:recetas_list')}?q={urlencode({'q': item.nombre})[2:]}",
                "final_products_url": f"{reverse('recetas:recetas_list')}?vista=productos&q={urlencode({'q': item.nombre})[2:]}",
                "detail_url": reverse("maestros:insumo_update", args=[item.id]),
            }
            item.impact_profile = _insumo_impact_profile(
                recipe_count=recipe_count,
                final_recipe_count=final_recipe_count,
                base_recipe_count=base_recipe_count,
                purchase_count=purchase_count,
                inventory_refs=inventory_refs,
                operational_profile=item.operational_profile,
            )
            if item.impact_profile["is_critical"]:
                page_impact_summary["critical"] += 1
            if item.impact_profile["level"] == "Alto":
                page_impact_summary["high"] += 1
            if item.impact_profile["is_multimodule"]:
                page_impact_summary["multimodule"] += 1
            if item.impact_profile["used_in_final_products"]:
                page_impact_summary["final_products"] += 1
        context["insumos"] = page_items
        final_product_blockers_preview = sorted(
            [
                {
                    "id": item.id,
                    "nombre": item.nombre,
                    "final_recipe_count": item.usage_profile["blocking_final_products_count"],
                    "missing": item.operational_profile["missing"],
                    "detail_url": item.usage_profile["detail_url"],
                    "final_products_url": item.usage_profile["final_products_url"],
                    "examples": item.usage_profile["final_recipe_examples"],
                }
                for item in page_items
                if item.usage_profile["blocks_final_products"]
            ],
            key=lambda row: (-row["final_recipe_count"], row["nombre"].lower()),
        )[:6]
        context["final_product_blockers_preview"] = final_product_blockers_preview
        context["duplicate_groups"] = duplicate_groups[:8]
        context["total_duplicate_groups"] = len(duplicate_groups)
        context["total_duplicate_items"] = len(duplicate_ids)
        context["total_canonical_items"] = max(active_qs.count() - len(variant_ids), 0)
        context["total_variant_items"] = len(variant_ids)
        context["usage_navigation"] = [
            {
                "key": "recipes",
                "title": "En recetas",
                "count": len(recipe_used_ids),
                "description": "Artículos ya ligados al BOM y costeo.",
                "query": "?usage_scope=recipes",
            },
            {
                "key": "recipes_finales",
                "title": "En producto final",
                "count": len(final_recipe_used_ids),
                "description": "Artículos ya consumidos por productos finales de venta.",
                "query": "?usage_scope=recipes&recipe_scope=finales",
            },
            {
                "key": "purchases",
                "title": "En compras",
                "count": len(purchase_used_ids),
                "description": "Artículos ya solicitados desde Compras.",
                "query": "?usage_scope=purchases",
            },
            {
                "key": "inventory",
                "title": "En inventario",
                "count": len(inventory_used_ids),
                "description": "Artículos con existencia, movimientos o ajustes.",
                "query": "?usage_scope=inventory",
            },
            {
                "key": "unused",
                "title": "Sin uso documentado",
                "count": max(active_qs.count() - len(recipe_used_ids | purchase_used_ids | inventory_used_ids), 0),
                "description": "Registros activos aún sin operación registrada.",
                "query": "?usage_scope=unused",
            },
        ]
        context["page_usage_summary"] = page_usage_summary
        context["page_impact_summary"] = page_impact_summary
        active_profiles = {
            item.id: _insumo_operational_profile(item)
            for item in active_qs.select_related("unidad_base", "proveedor_principal")
        }
        incomplete_active_ids = {
            insumo_id for insumo_id, profile in active_profiles.items() if profile["readiness_label"] == "Incompleto"
        }
        context["operational_blockers_navigation"] = [
            {
                "title": "Bloquea recetas",
                "count": len(recipe_used_ids & incomplete_active_ids),
                "description": "Artículos incompletos ya ligados al BOM o costeo.",
                "query": "?usage_scope=recipes&enterprise_status=incompletos",
            },
            {
                "title": "Bloquea producto final",
                "count": len(final_recipe_used_ids & incomplete_active_ids),
                "description": "Artículos incompletos ya consumidos en productos finales.",
                "query": "?usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos",
            },
            {
                "title": "Bloquea compras",
                "count": len(purchase_used_ids & incomplete_active_ids),
                "description": "Artículos incompletos que ya se están solicitando.",
                "query": "?usage_scope=purchases&enterprise_status=incompletos",
            },
            {
                "title": "Bloquea inventario",
                "count": len(inventory_used_ids & incomplete_active_ids),
                "description": "Artículos incompletos con stock, movimiento o ajuste.",
                "query": "?usage_scope=inventory&enterprise_status=incompletos",
            },
        ]
        final_product_blockers_by_missing = []
        for missing_key, missing_label in [
            ("unidad", "Sin unidad base"),
            ("proveedor", "Sin proveedor principal"),
            ("categoria", "Sin categoría"),
            ("codigo_point", "Sin código comercial"),
        ]:
            blocker_count = sum(
                1
                for item in active_qs.select_related("unidad_base", "proveedor_principal")
                if int(active_usage_maps["final_recipe_counts"].get(item.id, 0)) > 0
                and _insumo_operational_profile(item)["readiness_label"] == "Incompleto"
                and _match_missing_field(item, missing_key)
            )
            final_product_blockers_by_missing.append(
                {
                    "key": missing_key,
                    "label": missing_label,
                    "count": blocker_count,
                    "query": f"?usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos&missing_field={missing_key}",
                }
            )
        context["final_product_blockers_by_missing"] = final_product_blockers_by_missing
        context["total_enterprise_ready"] = sum(
            1 for item in active_qs.select_related("unidad_base", "proveedor_principal")
            if _insumo_operational_profile(item)["readiness_label"] == "Lista para operar"
        )
        context["total_enterprise_incomplete"] = sum(
            1 for item in active_qs.select_related("unidad_base", "proveedor_principal")
            if _insumo_operational_profile(item)["readiness_label"] == "Incompleto"
        )
        context["enterprise_progress_pct"] = round(
            (context["total_enterprise_ready"] * 100.0 / total_active),
            1,
        ) if total_active else 100.0
        context["enterprise_chain"] = [
            {
                "step": "01",
                "title": "Maestro ERP",
                "count": context["total_enterprise_ready"],
                "status": "Listos" if context["total_enterprise_incomplete"] == 0 else "En cierre",
                "detail": "Artículos activos completos para operar en recetas, compras e inventario.",
                "cta": "Ver incompletos" if context["total_enterprise_incomplete"] else "Abrir maestro",
                "url": f"{reverse('maestros:insumo_list')}?enterprise_status=incompletos" if context["total_enterprise_incomplete"] else reverse("maestros:insumo_list"),
                "tone": "warning" if context["total_enterprise_incomplete"] else "success",
            },
            {
                "step": "02",
                "title": "BOM y costeo",
                "count": len(recipe_used_ids),
                "status": "Integrado" if recipe_used_ids else "Sin uso",
                "detail": "Artículos que ya impactan recetas, costos y producto final.",
                "cta": "Abrir recetas",
                "url": reverse("recetas:recetas_list"),
                "tone": "success" if recipe_used_ids else "neutral",
            },
            {
                "step": "03",
                "title": "Compras",
                "count": len(purchase_used_ids),
                "status": "Documentado" if purchase_used_ids else "Sin documentos",
                "detail": "Artículos que ya se mueven por solicitudes, órdenes o recepciones.",
                "cta": "Abrir compras",
                "url": reverse("compras:solicitudes"),
                "tone": "success" if purchase_used_ids else "neutral",
            },
            {
                "step": "04",
                "title": "Inventario",
                "count": len(inventory_used_ids),
                "status": "Trazado" if inventory_used_ids else "Sin traza",
                "detail": "Artículos con existencia, movimientos o ajustes dentro del ERP.",
                "cta": "Abrir inventario",
                "url": reverse("inventario:existencias"),
                "tone": "success" if inventory_used_ids else "neutral",
            },
        ]
        context["erp_model_rows"] = _erp_model_stage_rows(
            total_active=context["total_activos"],
            total_ready=context["total_enterprise_ready"],
            recipe_used_count=len(recipe_used_ids),
            purchase_used_count=len(purchase_used_ids),
            inventory_used_count=len(inventory_used_ids),
        )
        context["erp_model_completion"] = (
            sum(int(row["completion"]) for row in context["erp_model_rows"]) // len(context["erp_model_rows"])
            if context["erp_model_rows"]
            else 0
        )
        context["critical_path_rows"] = _maestros_critical_path_rows(context["enterprise_chain"])
        context["module_closure_cards"] = [
            {
                "step": row["step"],
                "title": row["title"],
                "completion": row["completion"],
                "closed": row["closed"],
                "pending": row["pending"],
                "tone": (
                    "success"
                    if row["completion"] >= 90
                    else "warning"
                    if row["completion"] >= 50
                    else "danger"
                ),
                "detail": row["detail"],
                "url": row["url"],
                "cta": row["cta"],
            }
            for row in context["erp_model_rows"]
        ]
        context["document_stage_rows"] = [
            {
                "label": "Maestro completo",
                "open": context["total_enterprise_incomplete"],
                "closed": context["total_enterprise_ready"],
                "completion": round(
                    (context["total_enterprise_ready"] / max(context["total_activos"], 1)) * 100
                )
                if context["total_activos"]
                else 0,
                "detail": "Artículos incompletos frente a artículos ya listos para operar.",
                "next_step": "Cerrar faltantes de maestro",
                "url": reverse("maestros:insumo_list"),
            },
            {
                "label": "Integrados a recetas",
                "open": len(recipe_used_ids),
                "closed": max(context["total_activos"] - len(recipe_used_ids), 0),
                "completion": round(
                    (len(recipe_used_ids) / max(context["total_activos"], 1)) * 100
                )
                if context["total_activos"]
                else 0,
                "detail": "Artículos ya ligados al BOM y artículos aún fuera de recetas.",
                "next_step": "Ligar artículos al BOM",
                "url": reverse("recetas:recetas_list"),
            },
            {
                "label": "Integrados a compras",
                "open": len(purchase_used_ids),
                "closed": max(context["total_activos"] - len(purchase_used_ids), 0),
                "completion": round(
                    (len(purchase_used_ids) / max(context["total_activos"], 1)) * 100
                )
                if context["total_activos"]
                else 0,
                "detail": "Artículos ya presentes en documentos de compra.",
                "next_step": "Documentar abastecimiento",
                "url": reverse("compras:solicitudes"),
            },
            {
                "label": "Integrados a inventario",
                "open": len(inventory_used_ids),
                "closed": max(context["total_activos"] - len(inventory_used_ids), 0),
                "completion": round(
                    (len(inventory_used_ids) / max(context["total_activos"], 1)) * 100
                )
                if context["total_activos"]
                else 0,
                "detail": "Artículos ya trazados en existencias, movimientos o ajustes.",
                "next_step": "Cerrar trazabilidad de stock",
                "url": reverse("inventario:existencias"),
            },
        ]
        context["executive_radar_rows"] = _maestros_executive_radar_rows(
            context["document_stage_rows"],
            context["enterprise_chain"],
        )
        context["workflow_stage_rows"] = [
            {
                "step": index + 1,
                "label": row["label"],
                "completion": row["completion"],
                "detail": row["detail"],
                "owner": (
                    "Maestros / DG"
                    if index == 0
                    else "Producción / Costeo"
                    if index == 1
                    else "Compras"
                    if index == 2
                    else "Inventario / Almacén"
                ),
                "next_step": row["next_step"],
                "url": row["url"],
                "tone": "success" if row["completion"] >= 85 else "warning" if row["completion"] >= 50 else "danger",
            }
            for index, row in enumerate(context["document_stage_rows"])
        ]
        context["operational_health_cards"] = _maestro_operational_health_cards(
            total_ready=context["total_enterprise_ready"],
            total_incomplete=context["total_enterprise_incomplete"],
            total_duplicate_groups=context["total_duplicate_groups"],
            final_blockers=len(final_recipe_used_ids & incomplete_active_ids),
            purchase_blockers=len(purchase_used_ids & incomplete_active_ids),
            inventory_blockers=len(inventory_used_ids & incomplete_active_ids),
        )
        context["module_blocker_cards"] = [
            {
                "title": "Producto final",
                "owner": "Producción / Costeo",
                "count": len(final_recipe_used_ids & incomplete_active_ids),
                "tone": "danger" if len(final_recipe_used_ids & incomplete_active_ids) else "success",
                "detail": "Artículos incompletos ya consumidos en venta final.",
                "url": reverse("maestros:insumo_list")
                + "?usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos",
                "cta": "Abrir bloqueo final",
            },
            {
                "title": "Costeo / BOM",
                "owner": "Producción / Costeo",
                "count": len(recipe_used_ids & incomplete_active_ids),
                "tone": "warning" if len(recipe_used_ids & incomplete_active_ids) else "success",
                "detail": "Artículos que todavía frenan costeo, BOM o MRP.",
                "url": reverse("maestros:insumo_list") + "?usage_scope=recipes&enterprise_status=incompletos",
                "cta": "Abrir bloqueo BOM",
            },
            {
                "title": "Compras",
                "owner": "Compras",
                "count": len(purchase_used_ids & incomplete_active_ids),
                "tone": "warning" if len(purchase_used_ids & incomplete_active_ids) else "success",
                "detail": "Artículos incompletos aún presentes en solicitudes u órdenes.",
                "url": reverse("maestros:insumo_list") + "?usage_scope=purchases&enterprise_status=incompletos",
                "cta": "Abrir bloqueo compras",
            },
            {
                "title": "Inventario",
                "owner": "Inventario / Almacén",
                "count": len(inventory_used_ids & incomplete_active_ids),
                "tone": "warning" if len(inventory_used_ids & incomplete_active_ids) else "success",
                "detail": "Artículos incompletos con existencia, movimientos o ajustes vivos.",
                "url": reverse("maestros:insumo_list") + "?usage_scope=inventory&enterprise_status=incompletos",
                "cta": "Abrir bloqueo inventario",
            },
        ]
        closure_by_title = {row["title"]: row for row in context["module_closure_cards"]}
        context["downstream_handoff_rows"] = _insumo_downstream_handoff_rows(
            context["module_blocker_cards"],
            completion_map={
                title: int(row.get("completion") or 0)
                for title, row in closure_by_title.items()
            },
        )
        context["trunk_handoff_rows"] = _insumo_trunk_handoff_rows(
            context["module_blocker_cards"],
            completion_map={
                title: int(row.get("completion") or 0)
                for title, row in closure_by_title.items()
            },
        )
        context["erp_governance_rows"] = [
            {
                "front": "Maestro ERP",
                "owner": "Maestros / DG",
                "blockers": context["total_enterprise_incomplete"],
                "completion": closure_by_title.get("Maestro ERP", {}).get("completion", 0),
                "detail": "Datos maestros, clase operativa, unidad base y proveedor listos para operar.",
                "next_step": "Cerrar faltantes de maestro",
                "url": reverse("maestros:insumo_list") + "?enterprise_status=incompletos",
                "cta": "Abrir maestro",
            },
            {
                "front": "Producto final",
                "owner": "Producción / Costeo",
                "blockers": len(final_recipe_used_ids & incomplete_active_ids),
                "completion": closure_by_title.get("BOM y costeo", {}).get("completion", 0),
                "detail": "Artículos incompletos ya consumidos por productos finales o empaques de venta.",
                "next_step": "Resolver bloqueo final",
                "url": reverse("maestros:insumo_list")
                + "?usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos",
                "cta": "Abrir finales",
            },
            {
                "front": "Costeo / BOM",
                "owner": "Producción / Costeo",
                "blockers": len(recipe_used_ids & incomplete_active_ids),
                "completion": closure_by_title.get("BOM y costeo", {}).get("completion", 0),
                "detail": "Artículos que siguen bloqueando BOM, derivados, costeo o MRP.",
                "next_step": "Cerrar bloqueo de BOM",
                "url": reverse("maestros:insumo_list") + "?usage_scope=recipes&enterprise_status=incompletos",
                "cta": "Abrir BOM",
            },
            {
                "front": "Compras",
                "owner": "Compras",
                "blockers": len(purchase_used_ids & incomplete_active_ids),
                "completion": closure_by_title.get("Compras", {}).get("completion", 0),
                "detail": "Artículos incompletos todavía presentes en solicitudes, órdenes o recepciones.",
                "next_step": "Cerrar bloqueo de compras",
                "url": reverse("maestros:insumo_list") + "?usage_scope=purchases&enterprise_status=incompletos",
                "cta": "Abrir compras",
            },
            {
                "front": "Inventario",
                "owner": "Inventario / Almacén",
                "blockers": len(inventory_used_ids & incomplete_active_ids),
                "completion": closure_by_title.get("Inventario", {}).get("completion", 0),
                "detail": "Artículos incompletos con existencia, movimientos, ajustes o reabasto activo.",
                "next_step": "Cerrar bloqueo de inventario",
                "url": reverse("maestros:insumo_list") + "?usage_scope=inventory&enterprise_status=incompletos",
                "cta": "Abrir inventario",
            },
        ]
        context["impact_navigation"] = [
            {
                "key": "critical",
                "title": "Críticos",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["is_critical"]
                ),
                "description": "Incompletos que ya bloquean producto final.",
                "query": "?impact_scope=critical&usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos",
            },
            {
                "key": "finales",
                "title": "En producto final",
                "count": len(final_recipe_used_ids),
                "description": "Artículos ya consumidos en venta final.",
                "query": "?impact_scope=finales&usage_scope=recipes&recipe_scope=finales",
            },
            {
                "key": "bloquea_costeo",
                "title": "Bloquea costeo/MRP",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["blocks_costing"]
                ),
                "description": "Artículos incompletos que ya frenan costeo o MRP.",
                "query": "?impact_scope=bloquea_costeo&usage_scope=recipes&enterprise_status=incompletos",
            },
            {
                "key": "multimodule",
                "title": "Multimódulo",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["is_multimodule"]
                ),
                "description": "Artículos activos en más de un módulo del ERP.",
                "query": "?impact_scope=multimodule",
            },
            {
                "key": "compras",
                "title": "Con compras activas",
                "count": len(purchase_used_ids),
                "description": "Ya aparecen en solicitudes u órdenes.",
                "query": "?impact_scope=compras&usage_scope=purchases",
            },
            {
                "key": "bloquea_compras",
                "title": "Bloquea compras",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["blocks_purchases"]
                ),
                "description": "Artículos incompletos que ya frenan compras operativas.",
                "query": "?impact_scope=bloquea_compras&usage_scope=purchases&enterprise_status=incompletos",
            },
            {
                "key": "inventario",
                "title": "Con inventario vivo",
                "count": len(inventory_used_ids),
                "description": "Ya tienen stock, movimientos o ajustes.",
                "query": "?impact_scope=inventario&usage_scope=inventory",
            },
            {
                "key": "bloquea_inventario",
                "title": "Bloquea inventario",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["blocks_inventory"]
                ),
                "description": "Artículos incompletos que ya frenan inventario operativo.",
                "query": "?impact_scope=bloquea_inventario&usage_scope=inventory&enterprise_status=incompletos",
            },
        ]
        context["enterprise_blocker_navigation"] = [
            {
                "key": "critical",
                "title": "Bloquea producto final",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["is_critical"]
                ),
                "description": "Frena venta final por dato maestro incompleto.",
                "query": "?impact_scope=critical&usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos",
            },
            {
                "key": "bloquea_costeo",
                "title": "Bloquea costeo/MRP",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["blocks_costing"]
                ),
                "description": "Afecta costeo, BOM o MRP por datos faltantes.",
                "query": "?impact_scope=bloquea_costeo&usage_scope=recipes&enterprise_status=incompletos",
            },
            {
                "key": "bloquea_compras",
                "title": "Bloquea compras",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["blocks_purchases"]
                ),
                "description": "Detiene compras operativas por maestro incompleto.",
                "query": "?impact_scope=bloquea_compras&usage_scope=purchases&enterprise_status=incompletos",
            },
            {
                "key": "bloquea_inventario",
                "title": "Bloquea inventario",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _insumo_impact_profile(
                        recipe_count=int(active_usage_maps["recipe_counts"].get(item.id, 0)),
                        final_recipe_count=int(active_usage_maps["final_recipe_counts"].get(item.id, 0)),
                        base_recipe_count=int(active_usage_maps["base_recipe_counts"].get(item.id, 0)),
                        purchase_count=int(active_usage_maps["purchase_counts"].get(item.id, 0)),
                        inventory_refs=int(active_usage_maps["movement_counts"].get(item.id, 0))
                        + int(active_usage_maps["adjustment_counts"].get(item.id, 0))
                        + (1 if item.id in active_usage_maps["existence_ids"] else 0),
                        operational_profile=active_profiles[item.id],
                    )["blocks_inventory"]
                ),
                "description": "Frena control de stock, movimientos o ajustes.",
                "query": "?impact_scope=bloquea_inventario&usage_scope=inventory&enterprise_status=incompletos",
            },
        ]
        context["missing_impact_navigation"] = _missing_impact_navigation(
            active_qs,
            active_usage_maps,
            active_profiles,
        )
        demand_priority_rows, demand_priority_summary = _maestro_demand_priority_rows(
            active_qs,
            active_usage_maps,
        )
        context["demand_priority_rows"] = demand_priority_rows
        context["demand_priority_summary"] = demand_priority_summary
        context["critical_demand_priority_rows"] = [
            row for row in demand_priority_rows if row.get("is_demand_critical")
        ][:4]
        if context["critical_demand_priority_rows"]:
            top_priority = context["critical_demand_priority_rows"][0]
            context["daily_critical_close_focus"] = {
                "title": "Cierre prioritario del día",
                "detail": (
                    f"{top_priority['insumo_nombre']} debe cerrarse primero para liberar la venta y el troncal operativo."
                ),
                "historico_units": top_priority["historico_units"],
                "blocker_label": top_priority["blocker_label"],
                "missing": top_priority["missing"],
                "recipe_names": top_priority["recipe_names"],
                "url": top_priority["detail_url"],
                "cta": "Cerrar artículo ahora",
                "tone": "danger",
            }
        else:
            context["daily_critical_close_focus"] = None
        type_navigation = []
        for item_type in _insumo_type_cards():
            type_qs = active_qs.filter(tipo_item=item_type["code"]).select_related("unidad_base", "proveedor_principal")
            ready_count = 0
            incomplete_count = 0
            for insumo in type_qs:
                readiness = _insumo_operational_profile(insumo)["readiness_label"]
                if readiness == "Lista para operar":
                    ready_count += 1
                elif readiness == "Incompleto":
                    incomplete_count += 1
            type_navigation.append(
                {
                    **item_type,
                    "total": type_qs.count(),
                    "ready": ready_count,
                    "incomplete": incomplete_count,
                }
            )
        context["type_navigation"] = type_navigation
        governance_navigation = [
            {
                "key": "unidad",
                "title": "Sin unidad base",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _match_missing_field(item, "unidad")
                ),
                "description": "No puede costear ni moverse correctamente en ERP.",
            },
            {
                "key": "proveedor",
                "title": "Sin proveedor principal",
                "count": sum(
                    1
                    for item in active_qs.filter(tipo_item=Insumo.TIPO_MATERIA_PRIMA).select_related("unidad_base", "proveedor_principal")
                    if _match_missing_field(item, "proveedor")
                ),
                "description": "Materia prima activa sin abastecimiento principal definido.",
            },
            {
                "key": "categoria",
                "title": "Sin categoría",
                "count": sum(
                    1
                    for item in active_qs.filter(tipo_item__in=[Insumo.TIPO_INTERNO, Insumo.TIPO_EMPAQUE]).select_related("unidad_base", "proveedor_principal")
                    if _match_missing_field(item, "categoria")
                ),
                "description": "Artículo interno o empaque sin orden operativo.",
            },
            {
                "key": "codigo_point",
                "title": "Sin código comercial",
                "count": sum(
                    1
                    for item in active_qs.select_related("unidad_base", "proveedor_principal")
                    if _match_missing_field(item, "codigo_point")
                ),
                "description": "Queda fuera de conciliación y gobierno del catálogo comercial.",
            },
        ]
        context["governance_navigation"] = governance_navigation
        category_navigation = []
        grouped_categories = (
            active_qs.exclude(categoria__exact="")
            .values("tipo_item", "categoria")
            .annotate(total=Count("id"))
            .order_by("tipo_item", "-total", "categoria")
        )
        for row in grouped_categories:
            category_qs = active_qs.filter(tipo_item=row["tipo_item"], categoria=row["categoria"]).select_related(
                "unidad_base",
                "proveedor_principal",
            )
            ready_count = 0
            incomplete_count = 0
            for insumo in category_qs:
                readiness = _insumo_operational_profile(insumo)["readiness_label"]
                if readiness == "Lista para operar":
                    ready_count += 1
                elif readiness == "Incompleto":
                    incomplete_count += 1
            category_navigation.append(
                {
                    "tipo_item": row["tipo_item"],
                    "tipo_label": _insumo_type_label(row["tipo_item"]),
                    "categoria": row["categoria"],
                    "total": row["total"],
                    "ready": ready_count,
                    "incomplete": incomplete_count,
                }
            )
        context["category_navigation"] = category_navigation[:18]
        if demand_priority_summary.get("critical_count", 0) > 0:
            context["erp_priority_focus"] = {
                "title": "Demanda crítica bloqueada",
                "detail": demand_priority_summary["detail"],
                "value": demand_priority_summary["critical_count"],
                "url": f"{reverse('maestros:insumo_list')}?enterprise_status=incompletos&impact_scope=finales",
                "cta": "Cerrar prioridad crítica",
                "tone": "danger",
            }
        elif any(item["key"] == "critical" and item["count"] > 0 for item in context["enterprise_blocker_navigation"]):
            critical = next(item for item in context["enterprise_blocker_navigation"] if item["key"] == "critical")
            context["erp_priority_focus"] = {
                "title": "Bloqueo crítico en producto final",
                "detail": critical["description"],
                "value": critical["count"],
                "url": f"{reverse('maestros:insumo_list')}{critical['query']}",
                "cta": "Resolver bloqueos críticos",
                "tone": "danger",
            }
        elif context["total_enterprise_incomplete"] > 0:
            context["erp_priority_focus"] = {
                "title": "Completar maestro ERP",
                "detail": "Hay artículos incompletos que siguen frenando costeo, compras o inventario.",
                "value": context["total_enterprise_incomplete"],
                "url": f"{reverse('maestros:insumo_list')}?enterprise_status=incompletos",
                "cta": "Ver incompletos",
                "tone": "warning",
            }
        elif context["total_duplicate_groups"] > 0:
            context["erp_priority_focus"] = {
                "title": "Consolidar duplicados",
                "detail": "El maestro ya está operativo, pero aún hay variantes duplicadas que conviene cerrar para mantener una sola referencia.",
                "value": context["total_duplicate_groups"],
                "url": f"{reverse('maestros:insumo_list')}?canonical_status=variantes",
                "cta": "Revisar duplicados",
                "tone": "warning",
            }
        else:
            context["erp_priority_focus"] = {
                "title": "Maestro estable",
                "detail": "El maestro está listo para seguir cerrando BOM, compras e inventario sin bloqueos mayores.",
                "value": context["total_enterprise_ready"],
                "url": reverse("maestros:insumo_list"),
                "cta": "Abrir maestro",
                "tone": "success",
            }
        return context

class InsumoCreateView(LoginRequiredMixin, CreateView):
    model = Insumo
    template_name = 'maestros/insumo_form.html'
    fields = ['codigo', 'codigo_point', 'nombre', 'nombre_point', 'tipo_item', 'categoria', 'unidad_base', 'proveedor_principal', 'activo']
    success_url = reverse_lazy('maestros:insumo_list')

    def get_initial(self):
        initial = super().get_initial()
        tipo_item = (self.request.GET.get("tipo_item") or "").strip().upper()
        suggested_name = (self.request.GET.get("nombre") or "").strip()
        if tipo_item in {Insumo.TIPO_MATERIA_PRIMA, Insumo.TIPO_INTERNO, Insumo.TIPO_EMPAQUE}:
            initial["tipo_item"] = tipo_item
        if suggested_name:
            initial["nombre"] = suggested_name[:250]
        return initial

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['unidades'] = UnidadMedida.objects.all()
        context['proveedores'] = Proveedor.objects.filter(activo=True)
        context["item_type_cards"] = _insumo_type_cards()
        context["return_to"] = (self.request.GET.get("next") or "").strip()
        selected_type = context["form"].data.get("tipo_item") or context["form"].initial.get(
            "tipo_item", Insumo.TIPO_MATERIA_PRIMA
        )
        insumo_preview = self.object or Insumo(
            tipo_item=selected_type,
            activo=True,
        )
        context["operational_profile"] = _insumo_operational_profile(insumo_preview)
        context["category_suggestions"] = _insumo_category_suggestions()
        context["selected_type_card"] = _insumo_type_card(selected_type)
        context["selected_type_requirements"] = _insumo_type_requirements(selected_type)
        context["selected_type_requirements_map"] = _insumo_type_requirements_map()
        context["type_titles_map"] = _insumo_type_titles_map()
        context["selected_type_category_examples"] = context["category_suggestions"].get(selected_type, [])
        context["form_mode_title"] = f"Nueva {context['selected_type_card']['title'].lower()}"
        context["form_mode_hint"] = context["selected_type_card"]["description"]
        context["submit_label"] = f"Guardar {context['selected_type_card']['title'].lower()}"
        return context

    def form_valid(self, form):
        _normalize_insumo_form_instance(form)
        _validate_insumo_enterprise_form(form)
        if form.errors:
            return self.form_invalid(form)
        response = super().form_valid(form)
        if self.object.activo and not (self.object.codigo_point or "").strip():
            messages.warning(
                self.request,
                "Artículo activo sin código comercial: queda pendiente para conciliación con el catálogo comercial.",
            )
        next_url = (self.request.GET.get("next") or "").strip()
        if next_url:
            return redirect(next_url)
        return response

class InsumoUpdateView(LoginRequiredMixin, UpdateView):
    model = Insumo
    template_name = 'maestros/insumo_form.html'
    fields = ['codigo', 'codigo_point', 'nombre', 'nombre_point', 'tipo_item', 'categoria', 'unidad_base', 'proveedor_principal', 'activo']
    success_url = reverse_lazy('maestros:insumo_list')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        from recetas.models import LineaReceta

        context['unidades'] = UnidadMedida.objects.all()
        context['proveedores'] = Proveedor.objects.filter(activo=True)
        context["item_type_cards"] = _insumo_type_cards()
        context["operational_profile"] = _insumo_operational_profile(self.object)
        context["category_suggestions"] = _insumo_category_suggestions()
        context["selected_type_card"] = _insumo_type_card(self.object.tipo_item)
        context["selected_type_requirements"] = _insumo_type_requirements(self.object.tipo_item)
        context["selected_type_requirements_map"] = _insumo_type_requirements_map()
        context["type_titles_map"] = _insumo_type_titles_map()
        context["selected_type_category_examples"] = context["category_suggestions"].get(self.object.tipo_item, [])
        usage_maps = _insumo_usage_maps([self.object.id])
        recipe_count = int(usage_maps["recipe_counts"].get(self.object.id, 0))
        final_recipe_count = int(usage_maps["final_recipe_counts"].get(self.object.id, 0))
        base_recipe_count = int(usage_maps["base_recipe_counts"].get(self.object.id, 0))
        purchase_count = int(usage_maps["purchase_counts"].get(self.object.id, 0))
        movement_count = int(usage_maps["movement_counts"].get(self.object.id, 0))
        adjustment_count = int(usage_maps["adjustment_counts"].get(self.object.id, 0))
        has_existence = self.object.id in usage_maps["existence_ids"]
        impact_profile = _insumo_impact_profile(
            recipe_count=recipe_count,
            final_recipe_count=final_recipe_count,
            base_recipe_count=base_recipe_count,
            purchase_count=purchase_count,
            inventory_refs=movement_count + adjustment_count + (1 if has_existence else 0),
            operational_profile=context["operational_profile"],
        )
        final_recipe_examples = list(
            LineaReceta.objects.filter(insumo=self.object, receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
            .select_related("receta")
            .order_by("receta__nombre")
            .values_list("receta__nombre", flat=True)
            .distinct()[:5]
        )
        base_recipe_examples = list(
            LineaReceta.objects.filter(insumo=self.object, receta__tipo=Receta.TIPO_PREPARACION)
            .select_related("receta")
            .order_by("receta__nombre")
            .values_list("receta__nombre", flat=True)
            .distinct()[:5]
        )
        context["usage_profile"] = {
            "recipe_count": recipe_count,
            "final_recipe_count": final_recipe_count,
            "base_recipe_count": base_recipe_count,
            "purchase_count": purchase_count,
            "movement_count": movement_count,
            "adjustment_count": adjustment_count,
            "has_existence": has_existence,
            "inventory_refs": movement_count + adjustment_count + (1 if has_existence else 0),
            "recipes_url": f"{reverse('recetas:recetas_list')}?q={urlencode({'q': self.object.nombre})[2:]}",
            "final_products_url": f"{reverse('recetas:recetas_list')}?vista=productos&q={urlencode({'q': self.object.nombre})[2:]}",
            "purchases_url": f"{reverse('compras:solicitudes')}?q={urlencode({'q': self.object.nombre})[2:]}",
            "inventory_url": reverse('inventario:existencias'),
            "impact_profile": impact_profile,
            "final_recipe_examples": final_recipe_examples,
            "base_recipe_examples": base_recipe_examples,
            "is_operational_blocker": (
                impact_profile["is_critical"]
                or impact_profile["blocks_costing"]
                or impact_profile["blocks_purchases"]
                or impact_profile["blocks_inventory"]
            ),
            "blocks_final_products": impact_profile["is_critical"],
            "blocks_costing": impact_profile["blocks_costing"],
            "blocks_purchases": impact_profile["blocks_purchases"],
            "blocks_inventory": impact_profile["blocks_inventory"],
            "blocking_final_products_count": final_recipe_count,
            "detail_url": reverse("maestros:insumo_update", args=[self.object.id]),
        }
        context["commercial_signal"] = _insumo_recent_commercial_signal(self.object)
        context["module_blocker_cards"] = [
            {
                "key": "final_products",
                "title": "Producto final",
                "owner": "Producción / Costeo",
                "count": final_recipe_count,
                "is_blocked": impact_profile["is_critical"],
                "tone": "danger" if impact_profile["is_critical"] else "success",
                "detail": (
                    f"Bloquea {final_recipe_count} producto(s) final(es)."
                    if impact_profile["is_critical"]
                    else ("Ya participa en producto final sin bloqueo crítico." if final_recipe_count else "Sin impacto directo en producto final.")
                ),
                "action_label": "Ver finales",
                "action_url": f"{reverse('recetas:recetas_list')}?vista=productos&q={urlencode({'q': self.object.nombre})[2:]}",
            },
            {
                "key": "costing",
                "title": "Costeo / MRP",
                "owner": "Producción / Costeo",
                "count": recipe_count,
                "is_blocked": impact_profile["blocks_costing"],
                "tone": "danger" if impact_profile["blocks_costing"] else "success",
                "detail": (
                    f"Afecta {recipe_count} línea(s) de receta y costeo."
                    if impact_profile["blocks_costing"]
                    else ("Usado en recetas sin bloqueo actual." if recipe_count else "Sin uso actual en recetas.")
                ),
                "action_label": "Ver recetas",
                "action_url": f"{reverse('recetas:recetas_list')}?q={urlencode({'q': self.object.nombre})[2:]}",
            },
            {
                "key": "purchases",
                "title": "Compras",
                "owner": "Compras",
                "count": purchase_count,
                "is_blocked": impact_profile["blocks_purchases"],
                "tone": "danger" if impact_profile["blocks_purchases"] else "success",
                "detail": (
                    f"Bloquea {purchase_count} documento(s) de compra."
                    if impact_profile["blocks_purchases"]
                    else ("Ya aparece en compras sin bloqueo actual." if purchase_count else "Sin referencia actual en compras.")
                ),
                "action_label": "Ver compras",
                "action_url": f"{reverse('compras:solicitudes')}?q={urlencode({'q': self.object.nombre})[2:]}",
            },
            {
                "key": "inventory",
                "title": "Inventario",
                "owner": "Inventario / Almacén",
                "count": movement_count + adjustment_count + (1 if has_existence else 0),
                "is_blocked": impact_profile["blocks_inventory"],
                "tone": "danger" if impact_profile["blocks_inventory"] else "success",
                "detail": (
                    "Bloquea control de stock, movimientos o ajustes."
                    if impact_profile["blocks_inventory"]
                    else (
                        "Ya tiene huella operativa en inventario."
                        if (movement_count or adjustment_count or has_existence)
                        else "Sin huella actual en inventario."
                    )
                ),
                "action_label": "Ver inventario",
                "action_url": reverse("inventario:existencias"),
            },
        ]
        context["erp_governance_rows"] = [
            {
                "front": card["title"],
                "owner": card["owner"],
                "blockers": card["count"] if card["is_blocked"] else 0,
                "completion": 25 if card["is_blocked"] else 100,
                "detail": card["detail"],
                "next_step": card["action_label"],
                "url": card["action_url"],
                "cta": card["action_label"],
            }
            for card in context["module_blocker_cards"]
        ]
        context["downstream_handoff_rows"] = _insumo_downstream_handoff_rows(
            context["module_blocker_cards"],
            completion_map={card["title"]: (25 if card["is_blocked"] else 100) for card in context["module_blocker_cards"]},
        )
        context["trunk_handoff_rows"] = _insumo_trunk_handoff_rows(
            context["module_blocker_cards"],
            completion_map={card["title"]: (25 if card["is_blocked"] else 100) for card in context["module_blocker_cards"]},
        )
        context["erp_critical_path_rows"] = _insumo_critical_path_rows(context["module_blocker_cards"])
        context["erp_release_rows"] = _insumo_release_rows(context["module_blocker_cards"])
        previous_stage_blocked = False
        context["erp_article_chain_rows"] = []
        for idx, card in enumerate(context["module_blocker_cards"], start=1):
            dependency_status = (
                f"Condicionado por {context['module_blocker_cards'][idx - 2]['title']}"
                if idx > 1 and previous_stage_blocked
                else "Listo para avanzar"
                if not card["is_blocked"]
                else "Con bloqueo propio"
            )
            context["erp_article_chain_rows"].append(
                {
                    "step": f"{idx:02d}",
                    "title": card["title"],
                    "owner": card["owner"],
                    "count": card["count"],
                    "tone": card["tone"],
                    "status": "Bloqueado" if card["is_blocked"] else "Liberado",
                    "dependency_status": dependency_status,
                    "next_step": card["action_label"],
                    "detail": card["detail"],
                    "url": card["action_url"],
                }
            )
            previous_stage_blocked = card["is_blocked"]
        radar_stage_rows = [
            {
                "label": row["front"],
                "open": row["blockers"],
                "completion": row["completion"],
                "detail": row["detail"],
                "next_step": row["next_step"],
                "url": row["url"],
            }
            for row in context["erp_governance_rows"]
        ]
        context["executive_radar_rows"] = _maestros_executive_radar_rows(
            radar_stage_rows,
            context["erp_article_chain_rows"],
        )
        if impact_profile["is_critical"]:
            next_action = {
                "title": "Corregir producto final bloqueado",
                "detail": "Este artículo ya está frenando producto final. Completa datos maestros y valida sus recetas finales.",
                "action_label": "Ver productos finales",
                "action_url": f"{reverse('recetas:recetas_list')}?vista=productos&q={urlencode({'q': self.object.nombre})[2:]}",
            }
        elif impact_profile["blocks_costing"]:
            next_action = {
                "title": "Liberar costeo y MRP",
                "detail": "Completa los datos faltantes y revisa las recetas donde este artículo participa para estabilizar el costeo.",
                "action_label": "Ver recetas",
                "action_url": f"{reverse('recetas:recetas_list')}?q={urlencode({'q': self.object.nombre})[2:]}",
            }
        elif impact_profile["blocks_purchases"]:
            next_action = {
                "title": "Liberar compras",
                "detail": "Completa proveedor y datos maestros para que el artículo pueda entrar correctamente al flujo documental de compras.",
                "action_label": "Ver compras",
                "action_url": f"{reverse('compras:solicitudes')}?q={urlencode({'q': self.object.nombre})[2:]}",
            }
        elif impact_profile["blocks_inventory"]:
            next_action = {
                "title": "Liberar inventario",
                "detail": "Completa la estructura del artículo para evitar distorsiones en stock, movimientos y ajustes.",
                "action_label": "Ver inventario",
                "action_url": reverse("inventario:existencias"),
            }
        else:
            next_action = {
                "title": "Artículo estable",
                "detail": "El artículo no presenta bloqueo operativo inmediato. Puedes continuar con mantenimiento preventivo del catálogo.",
                "action_label": "Volver al catálogo",
                "action_url": reverse("maestros:insumo_list"),
            }
        context["next_action_recommendation"] = next_action
        return context

    def form_valid(self, form):
        _normalize_insumo_form_instance(form)
        _validate_insumo_enterprise_form(form)
        if form.errors:
            return self.form_invalid(form)
        response = super().form_valid(form)
        if self.object.activo and not (self.object.codigo_point or "").strip():
            messages.warning(
                self.request,
                "Artículo activo sin código comercial: queda pendiente para conciliación con el catálogo comercial.",
            )
        return response

class InsumoDeleteView(LoginRequiredMixin, DeleteView):
    model = Insumo
    template_name = 'maestros/insumo_confirm_delete.html'
    success_url = reverse_lazy('maestros:insumo_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        usage_maps = _insumo_usage_maps([self.object.id])
        recipe_count = int(usage_maps["recipe_counts"].get(self.object.id, 0))
        purchase_count = int(usage_maps["purchase_counts"].get(self.object.id, 0))
        inventory_count = (
            int(usage_maps["movement_counts"].get(self.object.id, 0))
            + int(usage_maps["adjustment_counts"].get(self.object.id, 0))
            + (1 if self.object.id in usage_maps["existence_ids"] else 0)
        )
        blockers = recipe_count + purchase_count + inventory_count
        context["erp_command_center"] = {
            "owner": "Maestros / DG",
            "status": "Crítico" if blockers else "Controlado",
            "tone": "danger" if blockers else "success",
            "blockers": blockers,
            "next_step": (
                "Revisar recetas, compras e inventario antes de eliminar este artículo."
                if blockers
                else "Confirmar retiro definitivo del artículo del maestro."
            ),
            "url": reverse("maestros:insumo_list"),
            "cta": "Volver al catálogo",
        }
        context["workflow_rows"] = [
            {
                "step": "01",
                "title": "Impacto en BOM",
                "owner": "Producción / Costeo",
                "completion": 100 if recipe_count == 0 else 40,
                "tone": "success" if recipe_count == 0 else "danger",
                "detail": f"{recipe_count} receta(s) referencian este artículo.",
                "next_step": "Reasignar o retirar el artículo de las recetas afectadas." if recipe_count else "Sin bloqueo en BOM.",
            },
            {
                "step": "02",
                "title": "Impacto documental",
                "owner": "Compras / Inventario",
                "completion": 100 if (purchase_count + inventory_count) == 0 else 40,
                "tone": "success" if (purchase_count + inventory_count) == 0 else "danger",
                "detail": f"{purchase_count} documento(s) de compra y {inventory_count} huella(s) de inventario.",
                "next_step": "Cerrar o reasignar referencias antes de la baja." if (purchase_count + inventory_count) else "Sin bloqueo documental.",
            },
        ]
        context["delete_checklist"] = [
            {"label": "Sin recetas ligadas", "ok": recipe_count == 0},
            {"label": "Sin compras ligadas", "ok": purchase_count == 0},
            {"label": "Sin huella de inventario", "ok": inventory_count == 0},
        ]
        return context


@login_required
@require_POST
def insumo_resolve_duplicate(request):
    if not request.user.has_perm("maestros.change_insumo"):
        raise PermissionDenied("No tienes permisos para consolidar insumos.")

    source = Insumo.objects.filter(pk=request.POST.get("source_insumo_id")).first()
    target = Insumo.objects.filter(pk=request.POST.get("target_insumo_id")).first()
    next_url = (request.POST.get("next") or "").strip() or reverse_lazy("maestros:insumo_list")

    if not source or not target:
        messages.error(request, "Origen o destino inválido para consolidar.")
        return redirect(next_url)

    if source.id == target.id:
        messages.error(request, "Origen y destino no pueden ser el mismo insumo.")
        return redirect(next_url)

    from inventario.views import (
        _merge_insumo_into_target,
        _remove_pending_name_from_recent_runs,
        _remove_pending_name_from_session,
        _resolve_cross_source_with_alias,
        _upsert_alias,
    )

    with transaction.atomic():
        merge_stats = _merge_insumo_into_target(source, target)
        ok_alias, alias_norm, _ = _upsert_alias(source.nombre, target)
        if ok_alias:
            _remove_pending_name_from_session(request, alias_norm)
            _remove_pending_name_from_recent_runs(alias_norm)
            _resolve_cross_source_with_alias(source.nombre, target)

    messages.success(
        request,
        (
            f"Consolidado en artículo estándar: '{source.nombre}' → '{target.nombre}'. "
            f"Referencias {merge_stats['aliases_updated']}, Costos {merge_stats['costos_updated']}, "
            f"Líneas receta {merge_stats['lineas_updated']}, Movimientos {merge_stats['movimientos_updated']}, "
            f"Ajustes {merge_stats['ajustes_updated']}, Solicitudes {merge_stats['solicitudes_updated']}."
        ),
    )
    log_event(
        request.user,
        "MASTER_DUPLICATE_RESOLVE_INSUMO",
        "maestros.Insumo",
        target.id,
        payload={
            "source_id": source.id,
            "target_id": target.id,
            "source_nombre": source.nombre,
            "target_nombre": target.nombre,
            **merge_stats,
            "source": "maestros.insumo_list",
        },
    )
    return redirect(next_url)


@login_required
@require_POST
def insumo_resolve_duplicate_group(request):
    if not request.user.has_perm("maestros.change_insumo"):
        raise PermissionDenied("No tienes permisos para consolidar grupos de insumos.")

    duplicate_key = normalizar_nombre((request.POST.get("duplicate_key") or "").strip())
    target_id_raw = (request.POST.get("target_insumo_id") or "").strip()
    next_url = (request.POST.get("next") or "").strip() or reverse_lazy("maestros:insumo_list")

    if not duplicate_key:
        messages.error(request, "Grupo duplicado inválido.")
        return redirect(next_url)

    members = list(
        Insumo.objects.filter(activo=True, nombre_normalizado=duplicate_key)
        .select_related("unidad_base", "proveedor_principal")
        .annotate(
            latest_costo_unitario=Subquery(
                CostoInsumo.objects.filter(insumo=OuterRef("pk")).order_by("-fecha", "-id").values("costo_unitario")[:1],
                output_field=DecimalField(max_digits=18, decimal_places=6),
            )
        )
        .order_by("nombre", "id")
    )
    if len(members) < 2:
        messages.warning(request, "Ese grupo ya no tiene duplicados activos para consolidar.")
        return redirect(next_url)

    target = None
    if target_id_raw.isdigit():
        target = next((item for item in members if item.id == int(target_id_raw)), None)
    if not target:
        ordered = sorted(members, key=lambda item: (duplicate_priority(item), item.id), reverse=True)
        target = ordered[0]

    from inventario.views import (
        _merge_insumo_into_target,
        _remove_pending_name_from_recent_runs,
        _remove_pending_name_from_session,
        _resolve_cross_source_with_alias,
        _upsert_alias,
    )

    sources = [item for item in members if item.id != target.id]
    totals = {
        "sources_resolved": 0,
        "aliases_updated": 0,
        "costos_updated": 0,
        "lineas_updated": 0,
        "movimientos_updated": 0,
        "ajustes_updated": 0,
        "solicitudes_updated": 0,
        "existencia_merged": 0,
    }

    with transaction.atomic():
        for source in sources:
            merge_stats = _merge_insumo_into_target(source, target)
            totals["sources_resolved"] += 1
            for key, value in merge_stats.items():
                totals[key] += int(value or 0)
            ok_alias, alias_norm, _ = _upsert_alias(source.nombre, target)
            if ok_alias:
                _remove_pending_name_from_session(request, alias_norm)
                _remove_pending_name_from_recent_runs(alias_norm)
                _resolve_cross_source_with_alias(source.nombre, target)

    messages.success(
        request,
        (
            f"Grupo consolidado en artículo estándar: '{target.nombre}'. "
            f"Fuentes {totals['sources_resolved']}, Referencias {totals['aliases_updated']}, "
            f"Costos {totals['costos_updated']}, Líneas receta {totals['lineas_updated']}."
        ),
    )
    log_event(
        request.user,
        "MASTER_DUPLICATE_RESOLVE_GROUP",
        "maestros.Insumo",
        target.id,
        payload={
            "duplicate_key": duplicate_key,
            "target_id": target.id,
            "target_nombre": target.nombre,
            "source": "maestros.insumo_list",
            **totals,
        },
    )
    return redirect(next_url)


@login_required
def insumo_point_mapping_csv(request):
    qs = (
        Insumo.objects.select_related('unidad_base')
        .annotate(alias_count=Count("aliases"))
        .order_by("nombre")
    )
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="insumos_point_mapping.csv"'
    writer = csv.writer(response)
    writer.writerow([
        "insumo_id",
        "codigo_interno",
        "codigo_point",
        "nombre_interno",
        "nombre_point",
        "nombre_normalizado",
        "unidad_base",
        "alias_count",
        "activo",
    ])
    for i in qs:
        writer.writerow([
            i.id,
            i.codigo or "",
            i.codigo_point or "",
            i.nombre or "",
            i.nombre_point or "",
            i.nombre_normalizado or "",
            i.unidad_base.codigo if i.unidad_base else "",
            i.alias_count,
            "1" if i.activo else "0",
        ])
    return response


def _to_float(raw, default=0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _export_point_pending_csv(tipo: str, q: str, score_min: float, qs):
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="point_pendientes_{tipo.lower()}.csv"'
    writer = csv.writer(response)
    writer.writerow(["tipo", "filtro_q", "score_min", "count"])
    writer.writerow([tipo, q or "", f"{score_min:.1f}", qs.count()])
    writer.writerow([])
    writer.writerow(
        [
            "id",
            "tipo",
            "codigo_point",
            "nombre_point",
            "sugerencia",
            "score",
            "metodo",
            "creado_en",
        ]
    )
    for row in qs.iterator(chunk_size=500):
        writer.writerow(
            [
                row.id,
                row.tipo,
                row.point_codigo or "",
                row.point_nombre or "",
                row.fuzzy_sugerencia or "",
                f"{float(row.fuzzy_score or 0):.1f}",
                row.method or "",
                row.creado_en.strftime("%Y-%m-%d %H:%M") if row.creado_en else "",
            ]
        )
    return response


def _export_point_pending_xlsx(tipo: str, q: str, score_min: float, qs):
    wb = Workbook()
    ws = wb.active
    ws.title = "point_pendientes"

    ws.append(["tipo", "filtro_q", "score_min", "count"])
    ws.append([tipo, q or "", float(score_min), int(qs.count())])
    ws.append([])
    ws.append(
        [
            "id",
            "tipo",
            "codigo_point",
            "nombre_point",
            "sugerencia",
            "score",
            "metodo",
            "creado_en",
        ]
    )

    for row in qs.iterator(chunk_size=500):
        ws.append(
            [
                row.id,
                row.tipo,
                row.point_codigo or "",
                row.point_nombre or "",
                row.fuzzy_sugerencia or "",
                float(row.fuzzy_score or 0),
                row.method or "",
                row.creado_en.strftime("%Y-%m-%d %H:%M") if row.creado_en else "",
            ]
        )

    stream = BytesIO()
    wb.save(stream)
    response = HttpResponse(
        stream.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="point_pendientes_{tipo.lower()}.xlsx"'
    return response


@login_required
def point_pending_review(request):
    if not can_view_maestros(request.user):
        raise PermissionDenied("No tienes permisos para ver Maestros.")

    can_manage = has_any_role(request.user, ROLE_ADMIN, ROLE_COMPRAS)
    allowed_types = {
        PointPendingMatch.TIPO_PROVEEDOR,
        PointPendingMatch.TIPO_INSUMO,
        PointPendingMatch.TIPO_PRODUCTO,
    }

    def _pending_review_redirect(tipo_value, params_source):
        query = {
            "tipo": tipo_value,
            "q": (params_source.get("q") or "").strip(),
            "score_min": max(0.0, min(100.0, _to_float(params_source.get("score_min"), 0))),
        }
        classification_value = (params_source.get("clasificacion_operativa") or "").strip().upper()
        if classification_value in {
            PointPendingMatch.CLASIFICACION_OPERATIVA_ACTIVO,
            PointPendingMatch.CLASIFICACION_OPERATIVA_TEMPORADA,
            PointPendingMatch.CLASIFICACION_OPERATIVA_HISTORICO,
            PointPendingMatch.CLASIFICACION_OPERATIVA_OCULTO,
        }:
            query["clasificacion_operativa"] = classification_value
        if str(params_source.get("show_hidden") or "").strip() in {"1", "true", "on"}:
            query["show_hidden"] = "1"
        page_value = (params_source.get("page") or "").strip()
        if page_value.isdigit() and int(page_value) > 1:
            query["page"] = page_value
        return redirect(f"{reverse_lazy('maestros:point_pending_review')}?{urlencode(query)}")

    def _base_pending_qs(*, tipo_value: str, q_value: str = "", show_hidden: bool = False, classification_value: str = ""):
        qs = PointPendingMatch.objects.filter(tipo=tipo_value)
        valid_classifications = {
            PointPendingMatch.CLASIFICACION_OPERATIVA_ACTIVO,
            PointPendingMatch.CLASIFICACION_OPERATIVA_TEMPORADA,
            PointPendingMatch.CLASIFICACION_OPERATIVA_HISTORICO,
            PointPendingMatch.CLASIFICACION_OPERATIVA_OCULTO,
        }
        if classification_value in valid_classifications:
            return qs.filter(clasificacion_operativa=classification_value)
        if show_hidden or q_value:
            return qs
        return qs.visible_en_operacion()

    if request.method == "POST":
        if not can_manage:
            raise PermissionDenied("No tienes permisos para resolver pendientes del sistema comercial.")

        action = (request.POST.get("action") or "").strip().lower()
        tipo = (request.POST.get("tipo") or PointPendingMatch.TIPO_INSUMO).strip().upper()
        if tipo not in allowed_types:
            tipo = PointPendingMatch.TIPO_INSUMO
        q_filter = (request.POST.get("q") or "").strip()
        score_filter = max(0.0, min(100.0, _to_float(request.POST.get("score_min"), 0)))
        show_hidden = str(request.POST.get("show_hidden") or "").strip() in {"1", "true", "on"}
        classification_value = (request.POST.get("clasificacion_operativa") or "").strip().upper()

        pending_ids = [pid for pid in request.POST.getlist("pending_ids") if pid.isdigit()]
        selected = PointPendingMatch.objects.filter(id__in=pending_ids, tipo=tipo)

        if not pending_ids and action == "resolve_sugerencias_insumos":
            selected = _base_pending_qs(
                tipo_value=tipo,
                q_value=q_filter,
                show_hidden=show_hidden,
                classification_value=classification_value,
            )
            if q_filter:
                selected = selected.filter(
                    Q(point_nombre__icontains=q_filter)
                    | Q(point_codigo__icontains=q_filter)
                    | Q(fuzzy_sugerencia__icontains=q_filter)
                )
            if score_filter > 0:
                selected = selected.filter(fuzzy_score__gte=score_filter)
            selected = selected.order_by("-fuzzy_score", "point_nombre")

        if not pending_ids and not selected.exists():
            messages.error(request, "Selecciona al menos un pendiente.")
            return _pending_review_redirect(tipo, request.POST)

        def _resolve_pending_insumo_row(pending, insumo_target, create_aliases_enabled):
            insumo_target = _canonicalize_insumo_target(insumo_target)
            if not insumo_target:
                return False, False, 0
            point_code = (pending.point_codigo or "").strip()
            if point_code and insumo_target.codigo_point and insumo_target.codigo_point != point_code:
                return False, True, 0

            changed = []
            if point_code and insumo_target.codigo_point != point_code:
                insumo_target.codigo_point = point_code
                changed.append("codigo_point")
            if insumo_target.nombre_point != pending.point_nombre:
                insumo_target.nombre_point = pending.point_nombre
                changed.append("nombre_point")
            if changed:
                insumo_target.save(update_fields=changed)

            alias_created = 0
            if create_aliases_enabled:
                alias_norm = normalizar_nombre(pending.point_nombre)
                if alias_norm and alias_norm != insumo_target.nombre_normalizado:
                    alias, was_created = InsumoAlias.objects.get_or_create(
                        nombre_normalizado=alias_norm,
                        defaults={"nombre": pending.point_nombre[:250], "insumo": insumo_target},
                    )
                    if not was_created and alias.insumo_id != insumo_target.id:
                        alias.insumo = insumo_target
                        alias.save(update_fields=["insumo"])
                    if was_created:
                        alias_created = 1

            pending.delete()
            return True, False, alias_created

        if action == "resolve_insumos":
            insumo_id = (request.POST.get("insumo_id") or "").strip()
            create_aliases = request.POST.get("create_aliases") == "on"
            target = canonical_insumo_by_id(insumo_id) if insumo_id else None
            if not target:
                messages.error(request, "Selecciona un insumo destino.")
                return _pending_review_redirect(tipo, request.POST)

            resolved = 0
            conflicts = 0
            aliases_created = 0
            for p in selected:
                row_resolved, row_conflict, row_alias_created = _resolve_pending_insumo_row(
                    p,
                    target,
                    create_aliases,
                )
                if row_conflict:
                    conflicts += 1
                    continue
                if row_resolved:
                    resolved += 1
                    aliases_created += row_alias_created

            messages.success(
                request,
                f"Pendientes resueltos (insumos): {resolved}. Referencias creadas: {aliases_created}.",
            )
            if conflicts:
                messages.warning(
                    request,
                    f"Pendientes con conflicto de código externo (no aplicados): {conflicts}.",
                )
        elif action == "resolve_sugerencias_insumos":
            if tipo != PointPendingMatch.TIPO_INSUMO:
                messages.error(request, "La auto-resolución por sugerencia aplica solo para pendientes de insumos.")
                return _pending_review_redirect(tipo, request.POST)
            min_score = max(0.0, min(100.0, _to_float(request.POST.get("auto_score_min"), 90.0)))
            create_aliases = request.POST.get("create_aliases") == "on"

            resolved = 0
            conflicts = 0
            skipped_low_score = 0
            skipped_no_suggestion = 0
            skipped_no_target = 0
            aliases_created = 0

            for p in selected:
                if float(p.fuzzy_score or 0.0) < min_score:
                    skipped_low_score += 1
                    continue

                sugerencia_norm = normalizar_nombre(p.fuzzy_sugerencia or "")
                if not sugerencia_norm:
                    skipped_no_suggestion += 1
                    continue

                target = Insumo.objects.filter(
                    activo=True,
                    nombre_normalizado=sugerencia_norm,
                ).only("id", "codigo_point", "nombre_point", "nombre_normalizado").first()
                if not target:
                    skipped_no_target += 1
                    continue

                row_resolved, row_conflict, row_alias_created = _resolve_pending_insumo_row(
                    p,
                    target,
                    create_aliases,
                )
                if row_conflict:
                    conflicts += 1
                    continue
                if row_resolved:
                    resolved += 1
                    aliases_created += row_alias_created

            messages.success(
                request,
                (
                    f"Auto-resueltos por sugerencia: {resolved}. "
                    f"Referencias creadas: {aliases_created}. "
                    f"Score mínimo: {min_score:.1f}."
                ),
            )
            if conflicts or skipped_low_score or skipped_no_suggestion or skipped_no_target:
                messages.warning(
                    request,
                    (
                        "No procesados: "
                        f"conflicto código externo {conflicts}, "
                        f"score bajo {skipped_low_score}, "
                        f"sin sugerencia {skipped_no_suggestion}, "
                        f"sugerencia sin insumo activo {skipped_no_target}."
                    ),
                )

        elif action == "resolve_productos":
            receta_id = (request.POST.get("receta_id") or "").strip()
            create_aliases = request.POST.get("create_aliases") == "on"
            target = Receta.objects.filter(pk=receta_id).first() if receta_id else None
            if not target:
                messages.error(request, "Selecciona una receta destino.")
                return _pending_review_redirect(tipo, request.POST)

            resolved = 0
            conflicts = 0
            aliases_created = 0
            for p in selected:
                point_code = (p.point_codigo or "").strip()
                if point_code:
                    point_norm = normalizar_codigo_point(point_code)
                    primary_norm = normalizar_codigo_point(target.codigo_point)
                    if not target.codigo_point:
                        target.codigo_point = point_code[:80]
                        target.save(update_fields=["codigo_point"])
                    elif primary_norm != point_norm:
                        if not point_norm:
                            conflicts += 1
                            continue
                        if not create_aliases:
                            conflicts += 1
                            continue
                        alias, was_created = RecetaCodigoPointAlias.objects.get_or_create(
                            codigo_point_normalizado=point_norm,
                            defaults={
                                "receta": target,
                                "codigo_point": point_code[:80],
                                "nombre_point": (p.point_nombre or "")[:250],
                                "activo": True,
                            },
                        )
                        if not was_created and alias.receta_id != target.id:
                            conflicts += 1
                            continue
                        changed = []
                        if alias.codigo_point != point_code[:80]:
                            alias.codigo_point = point_code[:80]
                            changed.append("codigo_point")
                        if (p.point_nombre or "").strip() and alias.nombre_point != (p.point_nombre or "")[:250]:
                            alias.nombre_point = (p.point_nombre or "")[:250]
                            changed.append("nombre_point")
                        if not alias.activo:
                            alias.activo = True
                            changed.append("activo")
                        if changed:
                            alias.save(update_fields=changed)
                        if was_created:
                            aliases_created += 1

                p.delete()
                resolved += 1

            messages.success(
                request,
                f"Pendientes resueltos (productos): {resolved}. Referencias creadas: {aliases_created}.",
            )
            if conflicts:
                messages.warning(request, f"Conflictos de código externo en productos: {conflicts}.")

        elif action == "resolve_proveedores":
            proveedor_id = (request.POST.get("proveedor_id") or "").strip()
            target = Proveedor.objects.filter(pk=proveedor_id).first() if proveedor_id else None
            resolved = 0
            created = 0
            for p in selected:
                if not target:
                    _, was_created = Proveedor.objects.get_or_create(nombre=p.point_nombre[:200], defaults={"activo": True})
                    if was_created:
                        created += 1
                p.delete()
                resolved += 1
            messages.success(
                request,
                f"Pendientes resueltos (proveedores): {resolved}. Proveedores nuevos creados: {created}.",
            )

        elif action == "discard_selected":
            deleted, _ = selected.delete()
            messages.success(request, f"Pendientes descartados: {deleted}.")
        elif action == "mark_historical_selected":
            updated = selected.update(
                clasificacion_operativa=PointPendingMatch.CLASIFICACION_OPERATIVA_HISTORICO,
                visible_en_operacion=False,
            )
            messages.success(request, f"Pendientes marcados como solo histórico: {updated}.")
        elif action == "mark_seasonal_selected":
            updated = selected.update(
                clasificacion_operativa=PointPendingMatch.CLASIFICACION_OPERATIVA_TEMPORADA,
                visible_en_operacion=False,
            )
            messages.success(request, f"Pendientes marcados como temporada: {updated}.")
        elif action == "restore_operational_selected":
            updated = selected.update(
                clasificacion_operativa=PointPendingMatch.CLASIFICACION_OPERATIVA_ACTIVO,
                visible_en_operacion=True,
            )
            messages.success(request, f"Pendientes restaurados como operativos: {updated}.")
        else:
            messages.error(request, "Acción no válida.")

        return _pending_review_redirect(tipo, request.POST)

    tipo = (request.GET.get("tipo") or PointPendingMatch.TIPO_INSUMO).strip().upper()
    if tipo not in allowed_types:
        tipo = PointPendingMatch.TIPO_INSUMO
    q = (request.GET.get("q") or "").strip()
    score_min = max(0.0, min(100.0, _to_float(request.GET.get("score_min"), 0)))
    show_hidden = str(request.GET.get("show_hidden") or "").strip() in {"1", "true", "on"}
    clasificacion_operativa = (request.GET.get("clasificacion_operativa") or "").strip().upper()

    qs = _base_pending_qs(
        tipo_value=tipo,
        q_value=q,
        show_hidden=show_hidden,
        classification_value=clasificacion_operativa,
    ).order_by("-fuzzy_score", "point_nombre")
    if q:
        qs = qs.filter(
            Q(point_nombre__icontains=q)
            | Q(point_codigo__icontains=q)
            | Q(fuzzy_sugerencia__icontains=q)
        )
    if score_min > 0:
        qs = qs.filter(fuzzy_score__gte=score_min)

    export_format = (request.GET.get("export") or "").strip().lower()
    if export_format == "csv":
        return _export_point_pending_csv(tipo, q, score_min, qs)
    if export_format == "xlsx":
        return _export_point_pending_xlsx(tipo, q, score_min, qs)

    paginator = Paginator(qs, 200)
    page = paginator.get_page(request.GET.get("page"))
    canonical_targets = {}
    if tipo == PointPendingMatch.TIPO_INSUMO:
        canonical_targets = _point_pending_canonical_targets(page.object_list)
        for pending in page.object_list:
            pending.canonical_target = None
            pending.canonical_target_variants = 0
            pending.suggestion_is_canonical = False
            suggestion_norm = normalizar_nombre(pending.fuzzy_sugerencia or "")
            if not suggestion_norm:
                continue
            target = canonical_targets.get(suggestion_norm)
            if not target:
                continue
            pending.canonical_target = target
            pending.canonical_target_variants = getattr(target, "canonical_variant_count", 1)
            pending.suggestion_is_canonical = normalizar_nombre(pending.fuzzy_sugerencia or "") == (
                target.nombre_normalizado or normalizar_nombre(target.nombre or "")
            )
    counts = {
        PointPendingMatch.TIPO_INSUMO: PointPendingMatch.qs_operativos().filter(tipo=PointPendingMatch.TIPO_INSUMO).count(),
        PointPendingMatch.TIPO_PRODUCTO: PointPendingMatch.qs_operativos().filter(tipo=PointPendingMatch.TIPO_PRODUCTO).count(),
        PointPendingMatch.TIPO_PROVEEDOR: PointPendingMatch.qs_operativos().filter(tipo=PointPendingMatch.TIPO_PROVEEDOR).count(),
    }
    total_pending = counts[PointPendingMatch.TIPO_INSUMO] + counts[PointPendingMatch.TIPO_PRODUCTO] + counts[PointPendingMatch.TIPO_PROVEEDOR]
    closed_stage_count = sum(
        1
        for is_closed in (
            counts[PointPendingMatch.TIPO_INSUMO] == 0,
            counts[PointPendingMatch.TIPO_PRODUCTO] == 0,
            counts[PointPendingMatch.TIPO_PROVEEDOR] == 0,
            total_pending == 0,
        )
        if is_closed
    )
    progress_pct = round((closed_stage_count / 4) * 100, 1)
    workflow_rows = [
        {
            "step": "01",
            "title": "Entrada externa",
            "detail": "Registros comerciales abiertos que ingresan al ERP para revisión controlada.",
            "open": total_pending,
            "closed": 0,
            "tone": "warning" if total_pending else "success",
            "owner": "Comercial / Administración",
            "completion": 100 if total_pending == 0 else 25,
            "next_step": "Revisar bandeja de entrada comercial" if total_pending else "Circuito estabilizado",
            "action_label": "Revisar bandeja",
            "action_href": f"{reverse_lazy('maestros:point_pending_review')}?tipo={tipo}",
        },
        {
            "step": "02",
            "title": "Resolución por tipo",
            "detail": "Asociación controlada contra insumos, productos o proveedores oficiales.",
            "open": counts[tipo],
            "closed": max(total_pending - counts[tipo], 0),
            "tone": "warning" if counts[tipo] else "success",
            "owner": (
                "Maestros / Producción"
                if tipo == PointPendingMatch.TIPO_PRODUCTO
                else "Maestros / Compras"
                if tipo == PointPendingMatch.TIPO_PROVEEDOR
                else "Maestros / Inventario"
            ),
            "completion": 100 if counts[tipo] == 0 else 50,
            "next_step": "Resolver registros del tipo seleccionado" if counts[tipo] else "Tipo actual estabilizado",
            "action_label": "Resolver por tipo",
            "action_href": f"{reverse_lazy('maestros:point_pending_review')}?tipo={tipo}",
        },
        {
            "step": "03",
            "title": "Cierre en catálogo ERP",
            "detail": "Alta o consolidación al catálogo ERP con referencia estándar y trazabilidad.",
            "open": counts[PointPendingMatch.TIPO_INSUMO],
            "closed": counts[PointPendingMatch.TIPO_PRODUCTO] + counts[PointPendingMatch.TIPO_PROVEEDOR],
            "tone": "warning" if counts[PointPendingMatch.TIPO_INSUMO] else "success",
            "owner": "Maestros / Dirección operativa",
            "completion": 100 if counts[PointPendingMatch.TIPO_INSUMO] == 0 else 75,
            "next_step": "Cerrar artículos en maestro" if counts[PointPendingMatch.TIPO_INSUMO] else "Catálogo maestro estabilizado",
            "action_label": "Abrir maestro",
            "action_href": reverse_lazy("maestros:insumo_list"),
        },
        {
            "step": "04",
            "title": "Impacto operativo",
            "detail": "Recetas, compras e inventario ya consumen el artículo estándar liberado.",
            "open": counts[PointPendingMatch.TIPO_PRODUCTO],
            "closed": counts[PointPendingMatch.TIPO_INSUMO] + counts[PointPendingMatch.TIPO_PROVEEDOR],
            "tone": "warning" if counts[PointPendingMatch.TIPO_PRODUCTO] else "success",
            "owner": "ERP / Operaciones",
            "completion": 100 if counts[PointPendingMatch.TIPO_PRODUCTO] == 0 else 85,
            "next_step": "Liberar impacto en módulos" if counts[PointPendingMatch.TIPO_PRODUCTO] else "Circuito comercial cerrado",
            "action_label": "Ver impacto ERP",
            "action_href": reverse_lazy("dashboard"),
        },
    ]
    release_ready = total_pending == 0
    release_summary = {
        "label": "Catálogo listo para operar" if release_ready else "Catálogo por validar",
        "tone": "success" if release_ready else "warning",
        "detail": (
            "No quedan registros comerciales abiertos; el catálogo ERP puede operar sin bloqueos."
            if release_ready
            else f"Quedan {total_pending} registro(s) comerciales abiertos antes del cierre operativo del catálogo."
        ),
    }
    type_release_cards = [
        {
            "title": "Artículo estándar",
            "count": counts[PointPendingMatch.TIPO_INSUMO],
            "tone": "warning" if counts[PointPendingMatch.TIPO_INSUMO] else "success",
            "detail": (
                f"{counts[PointPendingMatch.TIPO_INSUMO]} artículo(s) comerciales siguen abiertos antes de liberar recetas, compras e inventario."
                if counts[PointPendingMatch.TIPO_INSUMO]
                else "Los artículos comerciales ya quedaron cerrados contra el catálogo ERP."
            ),
            "action_label": "Abrir artículos",
            "action_href": f"{reverse_lazy('maestros:point_pending_review')}?tipo=INSUMO",
        },
        {
            "title": "Producto comercial",
            "count": counts[PointPendingMatch.TIPO_PRODUCTO],
            "tone": "warning" if counts[PointPendingMatch.TIPO_PRODUCTO] else "success",
            "detail": (
                f"{counts[PointPendingMatch.TIPO_PRODUCTO]} producto(s) externos todavía requieren resolución contra receta o producto final."
                if counts[PointPendingMatch.TIPO_PRODUCTO]
                else "Los productos comerciales abiertos ya quedaron cerrados dentro del ERP."
            ),
            "action_label": "Abrir productos",
            "action_href": f"{reverse_lazy('maestros:point_pending_review')}?tipo=PRODUCTO",
        },
        {
            "title": "Proveedor comercial",
            "count": counts[PointPendingMatch.TIPO_PROVEEDOR],
            "tone": "warning" if counts[PointPendingMatch.TIPO_PROVEEDOR] else "success",
            "detail": (
                f"{counts[PointPendingMatch.TIPO_PROVEEDOR]} proveedor(es) comerciales siguen abiertos antes de cerrar compras."
                if counts[PointPendingMatch.TIPO_PROVEEDOR]
                else "Los proveedores comerciales abiertos ya quedaron cerrados contra el catálogo ERP."
            ),
            "action_label": "Abrir proveedores",
            "action_href": f"{reverse_lazy('maestros:point_pending_review')}?tipo=PROVEEDOR",
        },
    ]
    module_release_cards = [
        {
            "title": "Recetas / BOM",
            "tone": "warning" if counts[PointPendingMatch.TIPO_INSUMO] or counts[PointPendingMatch.TIPO_PRODUCTO] else "success",
            "detail": (
                f"Bloqueado por {counts[PointPendingMatch.TIPO_INSUMO] + counts[PointPendingMatch.TIPO_PRODUCTO]} registro(s) comerciales ligados a artículos o productos."
                if counts[PointPendingMatch.TIPO_INSUMO] or counts[PointPendingMatch.TIPO_PRODUCTO]
                else "Liberado para operar con catálogo comercial ya integrado."
            ),
            "action_label": "Abrir recetas",
            "action_href": reverse_lazy("recetas:recetas_list"),
        },
        {
            "title": "Compras",
            "tone": "warning" if counts[PointPendingMatch.TIPO_INSUMO] or counts[PointPendingMatch.TIPO_PROVEEDOR] else "success",
            "detail": (
                f"Bloqueado por {counts[PointPendingMatch.TIPO_INSUMO] + counts[PointPendingMatch.TIPO_PROVEEDOR]} registro(s) comerciales en artículos o proveedores."
                if counts[PointPendingMatch.TIPO_INSUMO] or counts[PointPendingMatch.TIPO_PROVEEDOR]
                else "Liberado para emitir documentos de compra con maestro comercial ya controlado."
            ),
            "action_label": "Abrir compras",
            "action_href": reverse_lazy("compras:solicitudes"),
        },
        {
            "title": "Inventario y reportes",
            "tone": "warning" if counts[PointPendingMatch.TIPO_INSUMO] else "success",
            "detail": (
                f"Bloqueado por {counts[PointPendingMatch.TIPO_INSUMO]} artículo(s) comerciales aún fuera del catálogo ERP."
                if counts[PointPendingMatch.TIPO_INSUMO]
                else "Liberado para costeo, stock y análisis con catálogo comercial estable."
            ),
            "action_label": "Abrir inventario",
            "action_href": reverse_lazy("inventario:aliases_catalog"),
        },
    ]
    next_step_summary = (
        {
            "title": "Siguiente paso ERP",
            "label": "Circuito comercial validado",
            "detail": "No hay registros comerciales abiertos. El catálogo comercial ya puede operar como fuente estable del ERP.",
            "action_label": "Abrir catálogo ERP",
            "action_href": reverse_lazy("maestros:insumo_list"),
            "tone": "success",
        }
        if release_ready
        else {
            "title": "Siguiente paso ERP",
            "label": (
                "Resolver artículos comerciales"
                if tipo == PointPendingMatch.TIPO_INSUMO
                else "Resolver productos comerciales"
                if tipo == PointPendingMatch.TIPO_PRODUCTO
                else "Resolver proveedores comerciales"
            ),
            "detail": (
                f"Empieza por la bandeja de {page.paginator.count} registro(s) abierta en este tipo antes de cerrar completamente el circuito comercial."
            ),
            "action_label": "Resolver bandeja activa",
            "action_href": f"{reverse_lazy('maestros:point_pending_review')}?tipo={tipo}",
            "tone": "warning",
        }
    )
    erp_progress_cards = [
        {
            "title": "Artículos comerciales",
            "count": counts[PointPendingMatch.TIPO_INSUMO],
            "tone": "warning" if counts[PointPendingMatch.TIPO_INSUMO] else "success",
            "detail": (
                f"{counts[PointPendingMatch.TIPO_INSUMO]} artículo(s) comerciales siguen pendientes de integración."
                if counts[PointPendingMatch.TIPO_INSUMO]
                else "Todos los artículos comerciales ya están listos para operar."
            ),
        },
        {
            "title": "Productos comerciales",
            "count": counts[PointPendingMatch.TIPO_PRODUCTO],
            "tone": "warning" if counts[PointPendingMatch.TIPO_PRODUCTO] else "success",
            "detail": (
                f"{counts[PointPendingMatch.TIPO_PRODUCTO]} producto(s) comerciales siguen pendientes de integración."
                if counts[PointPendingMatch.TIPO_PRODUCTO]
                else "Todos los productos comerciales ya están listos para operar."
            ),
        },
        {
            "title": "Proveedores comerciales",
            "count": counts[PointPendingMatch.TIPO_PROVEEDOR],
            "tone": "warning" if counts[PointPendingMatch.TIPO_PROVEEDOR] else "success",
            "detail": (
                f"{counts[PointPendingMatch.TIPO_PROVEEDOR]} proveedor(es) comerciales siguen pendientes de integración."
                if counts[PointPendingMatch.TIPO_PROVEEDOR]
                else "Todos los proveedores comerciales ya están listos para operar."
            ),
        },
    ]
    erp_governance_rows = [
        {
            "front": "Entrada comercial",
            "owner": "Comercial / Administración",
            "blockers": total_pending,
            "completion": 100 if total_pending == 0 else 25,
            "detail": (
                "La fuente comercial ya no tiene registros abiertos."
                if total_pending == 0
                else f"{total_pending} registro(s) todavía siguen abiertos en integración comercial."
            ),
            "next_step": (
                "Mantener monitoreo preventivo."
                if total_pending == 0
                else "Revisar la bandeja comercial activa."
            ),
            "url": f"{reverse_lazy('maestros:point_pending_review')}?tipo={tipo}",
            "cta": "Revisar bandeja",
        },
        {
            "front": "Artículo maestro",
            "owner": "Maestros / Inventario",
            "blockers": counts[PointPendingMatch.TIPO_INSUMO],
            "completion": 100 if counts[PointPendingMatch.TIPO_INSUMO] == 0 else 75,
            "detail": (
                "Los artículos comerciales ya quedaron consolidados al maestro ERP."
                if counts[PointPendingMatch.TIPO_INSUMO] == 0
                else f"{counts[PointPendingMatch.TIPO_INSUMO]} artículo(s) siguen abiertos antes del cierre del maestro."
            ),
            "next_step": (
                "Mantener catálogo estabilizado."
                if counts[PointPendingMatch.TIPO_INSUMO] == 0
                else "Cerrar artículos comerciales en el maestro."
            ),
            "url": reverse_lazy("maestros:insumo_list"),
            "cta": "Abrir maestro",
        },
        {
            "front": "Impacto operativo",
            "owner": "ERP / Operaciones",
            "blockers": counts[PointPendingMatch.TIPO_PRODUCTO] + counts[PointPendingMatch.TIPO_PROVEEDOR],
            "completion": 100 if counts[PointPendingMatch.TIPO_PRODUCTO] + counts[PointPendingMatch.TIPO_PROVEEDOR] == 0 else 85,
            "detail": (
                "Recetas, compras e inventario ya operan sin bloqueos comerciales."
                if counts[PointPendingMatch.TIPO_PRODUCTO] + counts[PointPendingMatch.TIPO_PROVEEDOR] == 0
                else f"{counts[PointPendingMatch.TIPO_PRODUCTO] + counts[PointPendingMatch.TIPO_PROVEEDOR]} bloqueo(s) siguen abiertos en módulos operativos."
            ),
            "next_step": (
                "Monitorear estabilidad del circuito."
                if counts[PointPendingMatch.TIPO_PRODUCTO] + counts[PointPendingMatch.TIPO_PROVEEDOR] == 0
                else "Liberar impacto operativo por módulo."
            ),
            "url": reverse_lazy("dashboard"),
            "cta": "Ver impacto ERP",
        },
    ]

    return render(
        request,
        "maestros/point_pending_review.html",
        {
            "tipo": tipo,
            "q": q,
            "page": page,
            "counts": counts,
            "can_manage": can_manage,
            "score_min": score_min,
            "show_hidden": show_hidden,
            "clasificacion_operativa": clasificacion_operativa,
            "insumos": canonicalized_insumo_selector(),
            "recetas": Receta.objects.order_by("nombre")[:1500],
            "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre")[:800],
            "workflow_rows": workflow_rows,
            "release_summary": release_summary,
            "type_release_cards": type_release_cards,
            "module_release_cards": module_release_cards,
            "next_step_summary": next_step_summary,
            "erp_progress_cards": erp_progress_cards,
            "erp_progress_pct": progress_pct,
            "erp_closed_stage_count": closed_stage_count,
            "erp_governance_rows": erp_governance_rows,
            "critical_path_rows": _maestros_critical_path_rows(workflow_rows),
            "executive_radar_rows": _maestros_executive_radar_rows(workflow_rows, workflow_rows),
        },
    )
