from decimal import Decimal
from functools import lru_cache

from django.db.models import Count, DecimalField, OuterRef, Subquery

from core.cache_versions import bump_cache_scopes, get_cache_scope_version, get_or_set_versioned_cache
from maestros.models import CostoInsumo, Insumo
from recetas.utils.normalizacion import normalizar_nombre


def enterprise_readiness_profile(insumo: Insumo) -> dict[str, object]:
    tipo = insumo.tipo_item
    missing: list[str] = []
    if not insumo.unidad_base_id:
        missing.append("unidad base")
    if tipo == Insumo.TIPO_MATERIA_PRIMA and not insumo.proveedor_principal_id:
        missing.append("proveedor principal")
    if tipo in {Insumo.TIPO_INTERNO, Insumo.TIPO_EMPAQUE} and not (insumo.categoria or "").strip():
        missing.append("categoría")
    if insumo.activo and not (insumo.codigo_point or "").strip():
        missing.append("código Point")

    if not insumo.activo:
        readiness_label = "Inactivo"
        readiness_level = "danger"
    elif missing:
        readiness_label = "Incompleto"
        readiness_level = "warning"
    else:
        readiness_label = "Lista para operar"
        readiness_level = "success"

    return {
        "missing": missing,
        "readiness_label": readiness_label,
        "readiness_level": readiness_level,
        "is_ready": readiness_label == "Lista para operar",
    }


def usage_maps_for_insumo_ids(insumo_ids: list[int]) -> dict[str, object]:
    if not insumo_ids:
        return {
            "recipe_counts": {},
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
        "purchase_counts": purchase_counts,
        "movement_counts": movement_counts,
        "adjustment_counts": adjustment_counts,
        "existence_ids": existence_ids,
    }


def duplicate_priority(insumo: Insumo) -> int:
    score = 0
    if (insumo.codigo_point or "").strip():
        score += 100
    latest_costo = getattr(insumo, "latest_costo_unitario", None)
    if latest_costo is not None and Decimal(str(latest_costo or 0)) > 0:
        score += 60
    if insumo.proveedor_principal_id:
        score += 30
    if insumo.unidad_base_id:
        score += 20
    if (insumo.codigo or "").strip():
        score += 10
    return score


def _build_canonicalized_active_insumos(limit_safe: int) -> list[dict]:
    active_insumos = list(
        Insumo.objects.filter(activo=True)
        .select_related("unidad_base", "proveedor_principal")
        .annotate(
            latest_costo_unitario=Subquery(
                CostoInsumo.objects.filter(insumo=OuterRef("pk")).order_by("-fecha", "-id").values("costo_unitario")[:1],
                output_field=DecimalField(max_digits=18, decimal_places=6),
            )
        )
        .order_by("nombre")
    )
    grouped: dict[str, list[Insumo]] = {}
    insumo_norm_by_id: dict[int, str] = {}
    for insumo in active_insumos:
        key = insumo.nombre_normalizado or normalizar_nombre(insumo.nombre or "")
        grouped.setdefault(key, []).append(insumo)
        insumo_norm_by_id[insumo.id] = key

    latest_cost_by_norm: dict[str, Decimal] = {}
    if insumo_norm_by_id:
        latest_cost_rows = (
            CostoInsumo.objects.filter(insumo_id__in=list(insumo_norm_by_id.keys()))
            .order_by("-fecha", "-id")
            .values_list("insumo_id", "costo_unitario")
        )
        for insumo_id, costo_unitario in latest_cost_rows:
            normalized_name = insumo_norm_by_id.get(insumo_id)
            if not normalized_name or normalized_name in latest_cost_by_norm:
                continue
            latest_cost_by_norm[normalized_name] = Decimal(str(costo_unitario))

    canonical_rows: list[dict] = []
    for normalized_name, items in grouped.items():
        ordered = sorted(items, key=lambda item: (duplicate_priority(item), item.id), reverse=True)
        canonical = ordered[0]
        canonical.canonical_variant_count = len(ordered)
        canonical.member_ids = [item.id for item in ordered]
        canonical.latest_costo_unitario = latest_cost_by_norm.get(
            normalized_name,
            Decimal(str(getattr(canonical, "latest_costo_unitario", 0) or 0))
            if getattr(canonical, "latest_costo_unitario", None) is not None
            else None,
        )
        canonical.enterprise_profile = enterprise_readiness_profile(canonical)
        canonical.is_enterprise_ready = bool(canonical.enterprise_profile["is_ready"])
        canonical_rows.append(
            {
                "canonical": canonical,
                "normalized_name": normalized_name,
                "items": ordered,
                "member_ids": canonical.member_ids,
                "variant_count": len(ordered),
            }
        )

    canonical_rows.sort(key=lambda row: row["canonical"].nombre.lower())
    return canonical_rows[:limit_safe]


def canonicalized_active_insumos(limit: int = 1500) -> list[dict]:
    limit_safe = max(100, min(int(limit or 1500), 5000))
    return get_or_set_versioned_cache(
        key_parts=("erp", "catalog", "canonicalized-insumos", f"limit{limit_safe}"),
        scopes=("insumos",),
        builder=lambda: _build_canonicalized_active_insumos(limit_safe),
    )


def canonical_catalog_maps(limit: int = 1500) -> tuple[list[dict], dict[int, dict], dict[int, Insumo], dict[int, dict]]:
    canonical_rows = canonicalized_active_insumos(limit=limit)
    member_to_row: dict[int, dict] = {}
    canonical_by_member_id: dict[int, Insumo] = {}
    canonical_by_id: dict[int, dict] = {}
    for row in canonical_rows:
        canonical = row["canonical"]
        canonical_by_id[canonical.id] = row
        for member_id in row["member_ids"]:
            member_to_row[member_id] = row
            canonical_by_member_id[member_id] = canonical
    return canonical_rows, member_to_row, canonical_by_member_id, canonical_by_id


@lru_cache(maxsize=8)
def _canonical_membership_maps(version: int) -> tuple[dict[int, int], dict[int, tuple[int, ...]]]:
    member_to_canonical_id: dict[int, int] = {}
    canonical_to_member_ids: dict[int, tuple[int, ...]] = {}
    for row in canonicalized_active_insumos(limit=5000):
        canonical_id = row["canonical"].id
        member_ids = tuple(int(member_id) for member_id in row["member_ids"])
        canonical_to_member_ids[canonical_id] = member_ids
        for member_id in member_ids:
            member_to_canonical_id[member_id] = canonical_id
    return member_to_canonical_id, canonical_to_member_ids


@lru_cache(maxsize=8192)
def _active_canonical_insumo(canonical_id: int, version: int) -> Insumo | None:
    return (
        Insumo.objects.filter(pk=canonical_id, activo=True)
        .select_related("unidad_base", "proveedor_principal")
        .first()
    )


def clear_canonical_catalog_runtime_caches() -> None:
    bump_cache_scopes("insumos", "inventario", "dashboard")


def canonicalized_insumo_selector(limit: int = 1500) -> list[Insumo]:
    selected = []
    for row in canonicalized_active_insumos(limit=limit):
        insumo = row["canonical"]
        insumo.canonical_variant_count = row["variant_count"]
        insumo.member_ids = row["member_ids"]
        selected.append(insumo)
    return selected


def canonical_member_ids(insumo: Insumo | None = None, *, insumo_id: int | str | None = None) -> list[int]:
    if insumo is None:
        if insumo_id is None:
            return []
        try:
            parsed_id = int(insumo_id or 0)
        except (TypeError, ValueError):
            return []
        if parsed_id <= 0:
            return []
        insumo = Insumo.objects.filter(pk=parsed_id, activo=True).first()
        if not insumo:
            return []
    version = get_cache_scope_version("insumos")
    member_to_canonical_id, canonical_to_member_ids = _canonical_membership_maps(version)
    canonical_id = member_to_canonical_id.get(insumo.id)
    if canonical_id is None:
        return [insumo.id]
    return list(canonical_to_member_ids.get(canonical_id, (insumo.id,)))


def canonical_insumo(insumo: Insumo | None) -> Insumo | None:
    if not insumo:
        return None
    version = get_cache_scope_version("insumos")
    member_to_canonical_id, canonical_to_member_ids = _canonical_membership_maps(version)
    canonical_id = member_to_canonical_id.get(insumo.id)
    if canonical_id is None:
        return insumo
    canonical = _active_canonical_insumo(canonical_id, version) or insumo
    canonical.member_ids = list(canonical_to_member_ids.get(canonical_id, (canonical.id,)))
    canonical.canonical_variant_count = len(canonical.member_ids)
    return canonical


def latest_costo_canonico(insumo: Insumo | None = None, *, insumo_id: int | str | None = None) -> Decimal | None:
    member_ids = canonical_member_ids(insumo, insumo_id=insumo_id)
    if not member_ids:
        return None
    latest = (
        CostoInsumo.objects.filter(insumo_id__in=member_ids)
        .order_by("-fecha", "-id")
        .values_list("costo_unitario", flat=True)
        .first()
    )
    return Decimal(str(latest)) if latest is not None else None


def canonical_insumo_by_id(insumo_id: int | str | None) -> Insumo | None:
    try:
        parsed_id = int(insumo_id or 0)
    except (TypeError, ValueError):
        return None
    if parsed_id <= 0:
        return None
    version = get_cache_scope_version("insumos")
    member_to_canonical_id, canonical_to_member_ids = _canonical_membership_maps(version)
    canonical_id = member_to_canonical_id.get(parsed_id)
    if canonical_id is None:
        insumo = Insumo.objects.filter(pk=parsed_id, activo=True).first()
        return insumo
    canonical = _active_canonical_insumo(canonical_id, version)
    if not canonical:
        return None
    canonical.member_ids = list(canonical_to_member_ids.get(canonical_id, (canonical.id,)))
    canonical.canonical_variant_count = len(canonical.member_ids)
    return canonical
