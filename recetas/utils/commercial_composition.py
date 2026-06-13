from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal

from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from maestros.models import Insumo, UnidadMedida
from recetas.models import LineaReceta, Receta, RecetaAgrupacionAddon
from recetas.utils.derived_product_presentations import get_total_cost_map
from recetas.utils.normalizacion import normalizar_nombre


ZERO = Decimal("0")
_CURATED_COMMERCIAL_MAPPINGS_READY = False

RULE_HISTORICO_LEGADO = "HISTORICO_LEGADO"
RULE_COMPLEMENTO_OBLIGATORIO = "COMPLEMENTO_OBLIGATORIO"
RULE_PRODUCTO_BASE_DIRECTO = "PRODUCTO_BASE_DIRECTO"
RULE_SIN_RELACION = "SIN_RELACION"
RULE_BLOQUEADO_POR_AMBIGUEDAD = "BLOQUEADO_POR_AMBIGUEDAD"

SIZE_TOKENS = {
    "grande",
    "mediano",
    "rebanada",
    "r",
    "chico",
    "mini",
    "individual",
}

AMBIGUOUS_COMMERCIAL_TOKENS = {
    "sabor",
    "topping",
    "relleno",
    "cobertura",
}

AMBIGUOUS_COMMERCIAL_FAMILY_TOKENS = {
    "pay",
    "pastel",
}

AMBIGUOUS_PREFIXES = (
    "sabor ",
    "topping ",
    "complemento ",
    "decorado ",
    "extra ",
    "rebanada ",
)

BROKEN_ALIAS_BY_NAME: dict[str, str] = {
    "Pastel Crunch - Rebanada": "0063",
}


@dataclass(frozen=True, slots=True)
class CuratedAddonApproval:
    addon_codigo_point: str
    base_codigo_point: str
    reason: str


@dataclass(frozen=True, slots=True)
class CommercialHistorySpec:
    current_name: str
    legacy_name: str
    legacy_code: str = ""


@dataclass(frozen=True, slots=True)
class CuratedLineSpec:
    insumo_name: str
    quantity: Decimal
    unit_code: str
    codigo_point: str = ""


@dataclass(frozen=True, slots=True)
class CommercialSkuInterpretation:
    sku_actual: str
    producto_actual: str
    clasificacion: str
    sku_base: str = ""
    producto_base: str = ""
    sku_historico: str = ""
    producto_historico: str = ""
    complemento: str = ""
    regla_costeo: str = ""
    regla_forecast: str = ""
    regla_insumos: str = ""
    confianza: str = "ALTA"
    estado: str = "ACTIVO"
    nota_negocio: str = ""
    origen: str = ""


CURATED_HISTORY_SPECS: tuple[CommercialHistorySpec, ...] = (
    CommercialHistorySpec("Sabor Fresa Grande Pay", "Pay de Queso Grande"),
    CommercialHistorySpec("Sabor Fresa Mediano Pay", "Pay de Queso Mediano"),
    CommercialHistorySpec("Sabor Fresa Rebanada Pay", "Pay de Queso Rebanada"),
    CommercialHistorySpec("Sabor Guayaba Grande", "Pay de Guayaba Grande"),
    CommercialHistorySpec("Sabor Guayaba Mediano", "Pay de Guayaba Mediano"),
    CommercialHistorySpec("Sabor Guayaba Rebanada", "Pay de Guayaba R", "0011"),
    CommercialHistorySpec("Sabor Galleta Cajeta Grande", "Pay de Queso con Galleta y Cajeta Grande"),
    CommercialHistorySpec("Sabor Galleta Cajeta Mediano", "Pay de Queso con Galleta y Cajeta Mediano"),
    CommercialHistorySpec("Sabor Galleta con Cajeta Rebanada", "Pay de Queso con Galleta y Cajeta R", "0019"),
)

PAY_HISTORY_EQUIVALENCE: dict[str, dict[str, str]] = {
    spec.current_name: {
        "legacy_name": spec.legacy_name,
        **({"legacy_code": spec.legacy_code} if spec.legacy_code else {}),
    }
    for spec in CURATED_HISTORY_SPECS
}

SAFE_APPROVAL_SPECS: tuple[CuratedAddonApproval, ...] = (
    CuratedAddonApproval("SFRESAG", "0001", "Pay de queso grande con sabor fresa."),
    CuratedAddonApproval("SFRESAM", "0002", "Pay de queso mediano con sabor fresa."),
    CuratedAddonApproval("03SPOREB", "0003", "Pay de queso rebanada con sabor oreo."),
    CuratedAddonApproval("SMANZANAREB", "0003", "Pay de queso rebanada con sabor manzana."),
    CuratedAddonApproval("SOREOG", "0001", "Pay de queso grande con sabor oreo."),
    CuratedAddonApproval("SOREOM", "0002", "Pay de queso mediano con sabor oreo."),
    CuratedAddonApproval("SBROWNIEG", "0001", "Pay de queso grande con sabor brownie."),
    CuratedAddonApproval("SBROWNIEM", "0002", "Pay de queso mediano con sabor brownie."),
    CuratedAddonApproval("SFRESAPC", "0101", "Pastel de fresas con crema chico con topping fresa."),
    CuratedAddonApproval("SFRESAPG", "0099", "Pastel de fresas con crema grande con topping fresa."),
    CuratedAddonApproval("SFRESAPM", "0100", "Pastel de fresas con crema mediano con topping fresa."),
    CuratedAddonApproval("SFRESAPMINI", "PFCMINI", "Pastel fresas con crema mini con topping fresa."),
    CuratedAddonApproval("1412", "0056", "Pastel de Snickers chico con topping Snickers."),
    CuratedAddonApproval("21254", "0054", "Pastel de Snickers grande con topping Snickers."),
    CuratedAddonApproval("22145", "0055", "Pastel de Snickers mediano con topping Snickers."),
    CuratedAddonApproval("214541", "0061", "Pastel de Crunch chico con topping Crunch."),
    CuratedAddonApproval("21455", "0059", "Pastel de Crunch grande con topping Crunch."),
    CuratedAddonApproval("21125", "0060", "Pastel de Crunch mediano con topping Crunch."),
    CuratedAddonApproval("1254", "0066", "Pastel de zanahoria chico con topping zanahoria."),
    CuratedAddonApproval("1125", "0064", "Pastel de zanahoria grande con topping zanahoria."),
    CuratedAddonApproval("21445", "0065", "Pastel de zanahoria mediano con topping zanahoria."),
    CuratedAddonApproval("21245", "0105", "Pastel de 3 leches mediano con topping 3 leches."),
)

KNOWN_BLOCKED_CODES: dict[str, str] = {}

EXPLICIT_DUPLICATE_ALLOWED_CODES: dict[str, str] = {
    "SMANZANAREB": "DG confirmó que corresponde a Pay de queso rebanada con sabor manzana de temporada.",
    "1254": "DG confirmó que 1254 debe tratarse como TOPPING ZANAHORIA C ligado a Pastel de Zanahoria Chico.",
}

CURATED_ADDON_SPECS: dict[str, dict[str, object]] = {
    "03SPFREB": {
        "base_code": "0003",
        "reason": "Rebanada natural de pay de queso + topping fresa prorrateado desde pay grande.",
        "lines": [
            ("CUCHARA CH", Decimal("1"), "pza"),
            ("ETIQUETA CH", Decimal("1"), "pza"),
            ("Rebanada Triangular RP25", Decimal("1"), "pza"),
            ("Fresa", Decimal("50"), "g"),
            CuratedLineSpec("Galleta Pay", Decimal("3.750000"), "g", "01GP13"),
            CuratedLineSpec("Mermelada Fresa Liquida", Decimal("35"), "ml", "01MF06"),
        ],
    },
    "SGALLETACAJETAG": {
        "base_code": "0001",
        "reason": "Pay de queso grande natural + topping cajeta/galleta con curva de tamaño homologada a fresa pay.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            CuratedLineSpec("Galleta Pay", Decimal("30"), "g", "01GP13"),
            ("Dulce de Leche", Decimal("280"), "g"),
        ],
    },
    "SGALLETACAJETAM": {
        "base_code": "0002",
        "reason": "Pay de queso mediano natural + topping cajeta/galleta con curva de tamaño homologada a fresa pay.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            CuratedLineSpec("Galleta Pay", Decimal("25"), "g", "01GP13"),
            ("Dulce de Leche", Decimal("150"), "g"),
        ],
    },
    "03SPGCCREB": {
        "base_code": "0003",
        "reason": "Rebanada natural de pay de queso + topping cajeta/galleta prorrateado desde pay grande.",
        "lines": [
            ("CUCHARA CH", Decimal("1"), "pza"),
            ("ETIQUETA CH", Decimal("1"), "pza"),
            ("Rebanada Triangular RP25", Decimal("1"), "pza"),
            CuratedLineSpec("Galleta Pay", Decimal("3.750000"), "g", "01GP13"),
            ("Dulce de Leche", Decimal("35"), "g"),
        ],
    },
    "SGUAYABAG": {
        "base_code": "0001",
        "reason": "Pay de queso grande natural + topping de guayaba con curva de tamaño homologada a sabores pay.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Guayaba", Decimal("280"), "g"),
        ],
    },
    "SGUAYABAM": {
        "base_code": "0002",
        "reason": "Pay de queso mediano natural + topping de guayaba con curva de tamaño homologada a sabores pay.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Guayaba", Decimal("150"), "g"),
        ],
    },
    "03SPGREB": {
        "base_code": "0003",
        "reason": "Rebanada natural de pay de queso + topping de guayaba prorrateado desde pay grande.",
        "lines": [
            ("CUCHARA CH", Decimal("1"), "pza"),
            ("ETIQUETA CH", Decimal("1"), "pza"),
            ("Rebanada Triangular RP25", Decimal("1"), "pza"),
            ("Guayaba", Decimal("35"), "g"),
        ],
    },
    "SFRESAPC": {
        "base_code": "0101",
        "name": "TOPPING FRESA C",
        "reason": "Topping Fresa Chico a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Fresa Fresca", Decimal("130"), "g"),
        ],
    },
    "SFRESAPG": {
        "base_code": "0099",
        "name": "TOPPING FRESA G",
        "reason": "Topping Fresa Grande a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("CAJA G", Decimal("1"), "pza"),
            ("Fresa Fresca", Decimal("400"), "g"),
        ],
    },
    "SFRESAPM": {
        "base_code": "0100",
        "name": "TOPPING FRESA M",
        "reason": "Topping Fresa Mediano a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Fresa Fresca", Decimal("200"), "g"),
        ],
    },
    "SFRESAPMINI": {
        "base_code": "PFCMINI",
        "name": "TOPPING FRESA MINI",
        "reason": "Topping Fresa Mini a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Fresa Fresca", Decimal("18"), "g"),
        ],
    },
    "1412": {
        "base_code": "0056",
        "name": "TOPPING SNICKER CH",
        "reason": "Topping Snicker Chico a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Snicker's", Decimal("90"), "g"),
        ],
    },
    "21254": {
        "base_code": "0054",
        "name": "TOPPING SNICKER G",
        "reason": "Topping Snicker Grande a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("CAJA G", Decimal("1"), "pza"),
            ("Snicker's", Decimal("190"), "g"),
        ],
    },
    "22145": {
        "base_code": "0055",
        "name": "TOPPING SNICKER M",
        "reason": "Topping Snicker Mediano a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Snicker's", Decimal("130"), "g"),
        ],
    },
    "214541": {
        "base_code": "0061",
        "name": "TOPPING CRUNCH C",
        "reason": "Topping Crunch Chico a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("Crunch Rocks", Decimal("10"), "g"),
            ("Cobertura Crunch", Decimal("80"), "g"),
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
        ],
    },
    "21455": {
        "base_code": "0059",
        "name": "TOPPING CRUNCH G",
        "reason": "Topping Crunch Grande a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("CAJA G", Decimal("1"), "pza"),
            ("Crunch Rocks", Decimal("25"), "g"),
            ("Cobertura Crunch", Decimal("180"), "g"),
        ],
    },
    "21125": {
        "base_code": "0060",
        "name": "TOPPING CRUNCH M",
        "reason": "Topping Crunch Mediano a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("Crunch Rocks", Decimal("15"), "g"),
            ("Cobertura Crunch", Decimal("140"), "g"),
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
        ],
    },
    "1254": {
        "base_code": "0066",
        "name": "TOPPING ZANAHORIA C",
        "reason": "Topping Zanahoria Chico a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("Crumble Zanahoria", Decimal("20"), "g"),
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
        ],
    },
    "1125": {
        "base_code": "0064",
        "name": "TOPPING ZANAHORIA G",
        "reason": "Topping Zanahoria Grande a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("CAJA G", Decimal("1"), "pza"),
            ("Crumble Zanahoria", Decimal("50"), "g"),
        ],
    },
    "21445": {
        "base_code": "0065",
        "name": "TOPPING ZANAHORIA M",
        "reason": "Topping Zanahoria Mediano a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("Crumble Zanahoria", Decimal("35"), "g"),
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
        ],
    },
    "21245": {
        "base_code": "0105",
        "name": "TOPPING 3 LECHES",
        "reason": "Topping 3 Leches a $0 en Point; complemento operativo de punto de venta.",
        "lines": [
            ("ETIQUETA G", Decimal("1"), "pza"),
            ("Etiqueta Rectangular Aviso", Decimal("1"), "pza"),
            ("Nuez Granillo", Decimal("30"), "g"),
        ],
    },
}


@dataclass(frozen=True, slots=True)
class CommercialRecipeResolution:
    sold_receta: Receta
    effective_receta: Receta
    component_recetas: tuple[Receta, ...]
    resolution_kind: str
    notes: tuple[str, ...] = ()
    grouped_rule: RecetaAgrupacionAddon | None = None


@dataclass(frozen=True, slots=True)
class CommercialSkuExecutionPlan:
    sold_receta: Receta
    effective_receta: Receta
    planning_receta: Receta
    inventory_receta: Receta
    pricing_receta_ids: tuple[int, ...]
    component_recetas: tuple[Receta, ...]
    resolution_kind: str
    classification: str
    is_blocked: bool = False
    blocked_reason: str = ""
    notes: tuple[str, ...] = ()
    grouped_rule: RecetaAgrupacionAddon | None = None


@dataclass(frozen=True, slots=True)
class CommercialRecipeLookupContext:
    recipes_by_id: dict[int, Receta]
    recipes_by_exact_name: dict[str, Receta]
    recipes_by_code: dict[str, Receta]
    active_line_counts: dict[int, int]
    grouped_rules_by_addon_id: dict[int, RecetaAgrupacionAddon]
    grouped_rules_by_addon_code: dict[str, RecetaAgrupacionAddon]
    addon_embeds_base: set[tuple[int, int]]


def build_commercial_recipe_lookup_context(
    recipe_ids: list[int] | set[int] | tuple[int, ...],
) -> CommercialRecipeLookupContext:
    requested_ids = {int(recipe_id) for recipe_id in recipe_ids if int(recipe_id or 0) > 0}
    recipes = list(Receta.objects.filter(id__in=requested_ids).order_by("id"))
    recipes_by_id = {recipe.id: recipe for recipe in recipes}

    requested_names = {(recipe.nombre or "").strip() for recipe in recipes if (recipe.nombre or "").strip()}
    requested_codes = {
        (recipe.codigo_point or "").strip().upper()
        for recipe in recipes
        if (recipe.codigo_point or "").strip()
    }
    alias_codes = {
        alias_code.strip().upper()
        for recipe in recipes
        if (alias_code := BROKEN_ALIAS_BY_NAME.get((recipe.nombre or "").strip()))
    }
    legacy_names = {
        spec.legacy_name.strip()
        for spec in CURATED_HISTORY_SPECS
        if spec.current_name in requested_names and spec.legacy_name.strip()
    }
    legacy_codes = {
        spec.legacy_code.strip().upper()
        for spec in CURATED_HISTORY_SPECS
        if spec.current_name in requested_names and spec.legacy_code.strip()
    }

    extra_name_filters = legacy_names
    extra_code_filters = requested_codes | alias_codes | legacy_codes
    if extra_name_filters or extra_code_filters:
        extra_q = Q()
        if extra_name_filters:
            extra_q |= Q(nombre__in=sorted(extra_name_filters))
        if extra_code_filters:
            extra_q |= Q(codigo_point__in=sorted(extra_code_filters))
        for recipe in Receta.objects.filter(extra_q).order_by("id"):
            recipes_by_id.setdefault(recipe.id, recipe)

    active_line_counts = {
        row["receta_id"]: int(row["active"] or 0)
        for row in (
            LineaReceta.objects.filter(receta_id__in=recipes_by_id.keys())
            .values("receta_id")
            .annotate(active=Count("id", filter=~Q(match_status=LineaReceta.STATUS_REJECTED)))
        )
    }

    grouped_rules = list(
        RecetaAgrupacionAddon.objects.filter(
            activo=True,
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )
        .filter(Q(addon_receta_id__in=requested_ids) | Q(addon_codigo_point__in=sorted(requested_codes)))
        .select_related("base_receta", "addon_receta")
        .order_by("id")
    )
    grouped_rules_by_addon_id = {
        int(rule.addon_receta_id): rule for rule in grouped_rules if rule.addon_receta_id
    }
    grouped_rules_by_addon_code = {
        (rule.addon_codigo_point or "").strip().upper(): rule
        for rule in grouped_rules
        if (rule.addon_codigo_point or "").strip()
    }
    for rule in grouped_rules:
        recipes_by_id.setdefault(rule.base_receta_id, rule.base_receta)
        if rule.addon_receta_id and rule.addon_receta is not None:
            recipes_by_id.setdefault(rule.addon_receta_id, rule.addon_receta)

    embed_pairs: set[tuple[int, int]] = set()
    addon_ids = {int(rule.addon_receta_id) for rule in grouped_rules if rule.addon_receta_id}
    if addon_ids:
        addon_lines: dict[int, list[LineaReceta]] = {}
        for linea in (
            LineaReceta.objects.filter(receta_id__in=addon_ids)
            .exclude(match_status=LineaReceta.STATUS_REJECTED)
            .select_related("insumo")
        ):
            addon_lines.setdefault(int(linea.receta_id), []).append(linea)
        for rule in grouped_rules:
            if not rule.addon_receta_id:
                continue
            base_tokens = _significant_tokens(rule.base_receta.nombre)
            if not base_tokens:
                continue
            for linea in addon_lines.get(int(rule.addon_receta_id), []):
                source_name = linea.insumo.nombre if linea.insumo_id else linea.insumo_texto
                line_tokens = _significant_tokens(source_name or "")
                if base_tokens.issubset(line_tokens):
                    embed_pairs.add((int(rule.addon_receta_id), int(rule.base_receta_id)))
                    break

    recipes_by_exact_name = {
        (recipe.nombre or "").strip().casefold(): recipe
        for recipe in recipes_by_id.values()
        if (recipe.nombre or "").strip()
    }
    recipes_by_code = {
        (recipe.codigo_point or "").strip().upper(): recipe
        for recipe in recipes_by_id.values()
        if (recipe.codigo_point or "").strip()
    }
    return CommercialRecipeLookupContext(
        recipes_by_id=recipes_by_id,
        recipes_by_exact_name=recipes_by_exact_name,
        recipes_by_code=recipes_by_code,
        active_line_counts=active_line_counts,
        grouped_rules_by_addon_id=grouped_rules_by_addon_id,
        grouped_rules_by_addon_code=grouped_rules_by_addon_code,
        addon_embeds_base=embed_pairs,
    )


def _find_recipe_by_exact_name(name: str, *, context: CommercialRecipeLookupContext | None = None) -> Receta | None:
    cleaned = (name or "").strip()
    if not cleaned:
        return None
    if context is not None:
        cached = context.recipes_by_exact_name.get(cleaned.casefold())
        if cached is not None:
            return cached
    return Receta.objects.filter(nombre__iexact=cleaned).order_by("id").first()


def _find_recipe_by_code(code: str, *, context: CommercialRecipeLookupContext | None = None) -> Receta | None:
    cleaned = (code or "").strip().upper()
    if not cleaned:
        return None
    if context is not None:
        cached = context.recipes_by_code.get(cleaned)
        if cached is not None:
            return cached
    return Receta.objects.filter(codigo_point__iexact=cleaned).order_by("id").first()


def _find_insumo_by_name(name: str) -> Insumo | None:
    cleaned = (name or "").strip()
    if not cleaned:
        return None
    return Insumo.objects.filter(nombre__iexact=cleaned).order_by("id").first()


def _find_insumo_by_point_code(code: str) -> Insumo | None:
    cleaned = (code or "").strip()
    if not cleaned:
        return None
    return Insumo.objects.filter(codigo_point__iexact=cleaned).order_by("id").first()


def _find_unit(unit_code: str, *, insumo: Insumo | None) -> UnidadMedida | None:
    cleaned = (unit_code or "").strip()
    if not cleaned:
        return insumo.unidad_base if insumo else None
    return UnidadMedida.objects.filter(codigo__iexact=cleaned).order_by("id").first()


def _active_recipe_line_count(receta: Receta, *, context: CommercialRecipeLookupContext | None = None) -> int:
    if context is not None:
        return int(context.active_line_counts.get(int(receta.id), 0))
    return receta.lineas.exclude(match_status=LineaReceta.STATUS_REJECTED).count()


def _significant_tokens(name: str) -> set[str]:
    return {
        token
        for token in normalizar_nombre(name).split()
        if token and token not in SIZE_TOKENS and token not in {"de", "del", "la", "el", "con", "y"}
    }


def _looks_like_ambiguous_commercial_candidate(
    receta: Receta,
    *,
    context: CommercialRecipeLookupContext | None = None,
) -> tuple[bool, str]:
    normalized_name = normalizar_nombre(receta.nombre)
    if not normalized_name:
        return False, ""

    tokens = set(normalized_name.split())
    has_active_lines = _active_recipe_line_count(receta, context=context) > 0
    blank_code = not (receta.codigo_point or "").strip()

    if tokens & AMBIGUOUS_COMMERCIAL_TOKENS:
        return True, "nombre_con_sabor_o_topping"
    if normalized_name.startswith(AMBIGUOUS_PREFIXES):
        return True, "prefijo_comercial_sensible"
    if blank_code and tokens & SIZE_TOKENS:
        return True, "sku_sin_codigo_point_con_presentacion"
    if not has_active_lines and tokens & AMBIGUOUS_COMMERCIAL_FAMILY_TOKENS and tokens & SIZE_TOKENS:
        return True, "familia_con_presentacion_sin_lineas"
    return False, ""


def _addon_embeds_base(
    *,
    addon_receta: Receta,
    base_receta: Receta,
    context: CommercialRecipeLookupContext | None = None,
) -> bool:
    if context is not None and (int(addon_receta.id), int(base_receta.id)) in context.addon_embeds_base:
        return True
    base_tokens = _significant_tokens(base_receta.nombre)
    if not base_tokens:
        return False
    for linea in addon_receta.lineas.exclude(match_status=LineaReceta.STATUS_REJECTED).select_related("insumo"):
        source_name = linea.insumo.nombre if linea.insumo_id else linea.insumo_texto
        line_tokens = _significant_tokens(source_name or "")
        if base_tokens.issubset(line_tokens):
            return True
    return False


def _coerce_curated_line_spec(raw: object) -> CuratedLineSpec:
    if isinstance(raw, CuratedLineSpec):
        return raw
    name, quantity, unit_code = raw  # type: ignore[misc]
    return CuratedLineSpec(str(name), Decimal(str(quantity)), str(unit_code))


def _sync_curated_recipe_lines(receta: Receta, line_specs: list[object]) -> list[str]:
    warnings: list[str] = []
    if _active_recipe_line_count(receta) > 0:
        return warnings

    for posicion, raw_spec in enumerate(line_specs, start=1):
        spec = _coerce_curated_line_spec(raw_spec)
        insumo = _find_insumo_by_point_code(spec.codigo_point) if spec.codigo_point else None
        if insumo is None:
            insumo = _find_insumo_by_name(spec.insumo_name)
        if insumo is None:
            label = f"{spec.codigo_point} | {spec.insumo_name}" if spec.codigo_point else spec.insumo_name
            warnings.append(f"No se encontró el insumo '{label}' para {receta.nombre}.")
            continue
        unit = _find_unit(spec.unit_code, insumo=insumo)
        if unit is None:
            warnings.append(f"No se encontró la unidad '{spec.unit_code}' para {receta.nombre}.")
            continue
        LineaReceta.objects.update_or_create(
            receta=receta,
            posicion=posicion,
            defaults={
                "tipo_linea": LineaReceta.TIPO_NORMAL,
                "etapa": "COMPOSICION_COMERCIAL",
                "insumo": insumo,
                "insumo_texto": insumo.nombre,
                "cantidad": Decimal(str(spec.quantity)),
                "unidad_texto": unit.codigo,
                "unidad": unit,
                "costo_linea_excel": None,
                "costo_unitario_snapshot": None,
                "match_score": 1.0,
                "match_method": LineaReceta.MATCH_EXACT,
                "match_status": LineaReceta.STATUS_AUTO,
                "aprobado_en": timezone.now(),
            },
        )
    return warnings


def _get_curated_point_product(addon_code: str, addon_name: str):
    from pos_bridge.models import PointProduct

    products = PointProduct.objects.filter(sku__iexact=addon_code).order_by("id")
    if addon_name:
        exact = products.filter(name__iexact=addon_name).first()
        if exact is not None:
            return exact
    return products.first()


def _ensure_curated_addon_recipe(addon_code: str, spec: dict[str, object]) -> Receta | None:
    addon_receta = _find_recipe_by_code(addon_code)
    if addon_receta is not None:
        return addon_receta

    addon_name = str(spec.get("name") or "").strip()
    if not addon_name:
        return None

    point_product = _get_curated_point_product(addon_code, addon_name)
    recipe_defaults = {
        "nombre": addon_name,
        "codigo_point": addon_code,
        "tipo": Receta.TIPO_PRODUCTO_FINAL,
        "modo_costeo": Receta.MODO_COSTEO_SERVICIO,
        "familia": "",
        "categoria": "",
        "sheet_name": "COMPLEMENTOS_POINT_CERO",
        "pasa_modulo_produccion": False,
    }
    if point_product is not None:
        recipe_defaults["familia"] = point_product.category or ""
        recipe_defaults["categoria"] = point_product.category or ""

    hash_seed = f"curated-addon:{addon_code}:{addon_name}"
    addon_receta = Receta.objects.create(
        **recipe_defaults,
        hash_contenido=hashlib.sha256(hash_seed.encode("utf-8")).hexdigest(),
    )
    return addon_receta


@transaction.atomic
def ensure_curated_commercial_mappings() -> list[str]:
    warnings: list[str] = []
    for addon_code, spec in CURATED_ADDON_SPECS.items():
        addon_receta = _ensure_curated_addon_recipe(addon_code, spec)
        base_receta = _find_recipe_by_code(str(spec["base_code"]))
        if addon_receta is None:
            warnings.append(f"No se encontró la receta add-on {addon_code}.")
            continue
        if base_receta is None:
            warnings.append(f"No se encontró la receta base {spec['base_code']} para {addon_receta.nombre}.")
            continue
        warnings.extend(_sync_curated_recipe_lines(addon_receta, spec["lines"]))  # type: ignore[arg-type]
        RecetaAgrupacionAddon.objects.update_or_create(
            base_receta=base_receta,
            addon_codigo_point=addon_code,
            defaults={
                "addon_receta": addon_receta,
                "addon_nombre_point": addon_receta.nombre,
                "addon_familia": addon_receta.familia,
                "addon_categoria": addon_receta.categoria,
                "source": RecetaAgrupacionAddon.SOURCE_POINT_ZERO_REVENUE,
                "status": RecetaAgrupacionAddon.STATUS_APPROVED,
                "cooccurrence_days": 0,
                "cooccurrence_branches": 0,
                "cooccurrence_qty": ZERO,
                "confidence_score": Decimal("100.0000"),
                "notas": f"Curado comercial DG. {spec['reason']}",
                "activo": True,
            },
        )
    for approval in SAFE_APPROVAL_SPECS:
        addon_receta = _find_recipe_by_code(approval.addon_codigo_point)
        base_receta = _find_recipe_by_code(approval.base_codigo_point)
        if addon_receta is None or base_receta is None:
            continue
        RecetaAgrupacionAddon.objects.update_or_create(
            base_receta=base_receta,
            addon_codigo_point=approval.addon_codigo_point,
            defaults={
                "addon_receta": addon_receta,
                "addon_nombre_point": addon_receta.nombre,
                "addon_familia": addon_receta.familia,
                "addon_categoria": addon_receta.categoria,
                "source": RecetaAgrupacionAddon.SOURCE_POINT_ZERO_REVENUE,
                "status": RecetaAgrupacionAddon.STATUS_APPROVED,
                "cooccurrence_days": 0,
                "cooccurrence_branches": 0,
                "cooccurrence_qty": ZERO,
                "confidence_score": Decimal("100.0000"),
                "notas": f"Curado comercial DG. {approval.reason}",
                "activo": True,
            },
        )
    return warnings


def _ensure_curated_commercial_mappings_once() -> list[str]:
    global _CURATED_COMMERCIAL_MAPPINGS_READY
    if _CURATED_COMMERCIAL_MAPPINGS_READY:
        return []
    warnings = ensure_curated_commercial_mappings()
    _CURATED_COMMERCIAL_MAPPINGS_READY = True
    return warnings


def get_legacy_history_spec(receta: Receta) -> CommercialHistorySpec | None:
    recipe_name = (receta.nombre or "").strip()
    for spec in CURATED_HISTORY_SPECS:
        if spec.current_name == recipe_name:
            return spec
    return None


def get_curated_addon_approval(*, addon_code: str | None = None, addon_receta: Receta | None = None) -> CuratedAddonApproval | None:
    code = (addon_code or (addon_receta.codigo_point if addon_receta else "") or "").strip().upper()
    if not code:
        return None
    for spec in SAFE_APPROVAL_SPECS:
        if spec.addon_codigo_point == code:
            return spec
    return None


def classify_commercial_recipe(
    receta: Receta,
    *,
    context: CommercialRecipeLookupContext | None = None,
) -> CommercialSkuInterpretation:
    _ensure_curated_commercial_mappings_once()
    legacy_spec = get_legacy_history_spec(receta)
    if legacy_spec:
        legacy_recipe = (
            _find_recipe_by_code(legacy_spec.legacy_code, context=context)
            if legacy_spec.legacy_code
            else _find_recipe_by_exact_name(legacy_spec.legacy_name, context=context)
        )
        return CommercialSkuInterpretation(
            sku_actual=(receta.codigo_point or "").strip(),
            producto_actual=receta.nombre,
            clasificacion=RULE_HISTORICO_LEGADO,
            sku_historico=legacy_spec.legacy_code or (legacy_recipe.codigo_point if legacy_recipe else ""),
            producto_historico=legacy_spec.legacy_name or (legacy_recipe.nombre if legacy_recipe else ""),
            regla_costeo="No altera costeo actual; solo aporta historico para forecast.",
            regla_forecast="Usa historico legado explicito hasta el primer dato observado del SKU actual.",
            regla_insumos="No aplica a insumos actuales; es referencia historica.",
            nota_negocio="Producto legado que alimenta el historico del SKU actual.",
            origen="recetas/utils/commercial_composition.py::CURATED_HISTORY_SPECS",
        )

    resolution = resolve_commercial_recipe(receta, context=context)
    if resolution.grouped_rule:
        base = resolution.grouped_rule.base_receta
        addon = resolution.grouped_rule.addon_receta or receta
        return CommercialSkuInterpretation(
            sku_actual=(receta.codigo_point or "").strip(),
            producto_actual=receta.nombre,
            clasificacion=RULE_COMPLEMENTO_OBLIGATORIO,
            sku_base=(base.codigo_point or "").strip(),
            producto_base=base.nombre,
            complemento=(addon.codigo_point or "").strip(),
            regla_costeo="Costo = base + complemento sin dobletear la base.",
            regla_forecast="No usar la base como historico salvo regla legado explicita.",
            regla_insumos="Explota base + complemento como 1 producto comercial.",
            nota_negocio=resolution.grouped_rule.notas or f"{receta.nombre} se interpreta como producto compuesto.",
            origen="recetas/utils/commercial_composition.py::resolve_commercial_recipe",
        )

    if resolution.resolution_kind == "ALIASED_RECIPE":
        return CommercialSkuInterpretation(
            sku_actual=(receta.codigo_point or "").strip(),
            producto_actual=receta.nombre,
            clasificacion=RULE_PRODUCTO_BASE_DIRECTO,
            sku_base=(resolution.effective_receta.codigo_point or "").strip(),
            producto_base=resolution.effective_receta.nombre,
            regla_costeo="Producto comercial corregido por alias operativo interno.",
            regla_forecast="Usa el SKU efectivo para precio/costo y el SKU comercial para venta.",
            regla_insumos="Explota como receta efectiva sin regla legado.",
            nota_negocio="Alias operativo resuelto a receta efectiva.",
            origen="recetas/utils/commercial_composition.py::BROKEN_ALIAS_BY_NAME",
        )

    ambiguous, ambiguity_reason = _looks_like_ambiguous_commercial_candidate(receta, context=context)
    if ambiguous:
        return CommercialSkuInterpretation(
            sku_actual=(receta.codigo_point or "").strip(),
            producto_actual=receta.nombre,
            clasificacion=RULE_BLOQUEADO_POR_AMBIGUEDAD,
            regla_costeo="No se costea automáticamente porque el SKU comercial parece compuesto y no tiene regla curada.",
            regla_forecast="Bloqueado hasta que se declare la relación comercial o se apruebe una regla curada.",
            regla_insumos="No se usa como directo porque puede duplicar base o complemento.",
            confianza="BAJA",
            estado="BLOQUEADO",
            nota_negocio=f"Candidato comercial ambiguo bloqueado por heurística conservadora: {ambiguity_reason}.",
            origen="recetas/utils/commercial_composition.py::heuristica_ambiguedad_conservadora",
        )

    return CommercialSkuInterpretation(
        sku_actual=(receta.codigo_point or "").strip(),
        producto_actual=receta.nombre,
        clasificacion=RULE_PRODUCTO_BASE_DIRECTO if resolution.resolution_kind == "DIRECT_RECIPE" else RULE_SIN_RELACION,
        regla_costeo="Costeo directo por receta propia.",
        regla_forecast="Forecast usa solo historia del propio SKU.",
        regla_insumos="Explota solo su receta propia.",
        nota_negocio="Sin relacion curada adicional." if resolution.resolution_kind == "DIRECT_RECIPE" else "Sin regla curada definida.",
        origen="recetas/utils/commercial_composition.py::resolve_commercial_recipe",
    )


def iter_commercial_validation_rows() -> list[CommercialSkuInterpretation]:
    rows: list[CommercialSkuInterpretation] = []
    recipe_ids: set[int] = set()
    for spec in CURATED_HISTORY_SPECS:
        current = _find_recipe_by_exact_name(spec.current_name)
        if current:
            recipe_ids.add(current.id)
    for approval in SAFE_APPROVAL_SPECS:
        addon = _find_recipe_by_code(approval.addon_codigo_point)
        if addon:
            recipe_ids.add(addon.id)
        base = _find_recipe_by_code(approval.base_codigo_point)
        if base:
            recipe_ids.add(base.id)

    candidate_qs = Receta.objects.filter(
        tipo=Receta.TIPO_PRODUCTO_FINAL,
        codigo_point__isnull=False,
    ).filter(
        Q(nombre__istartswith="Sabor ")
        | Q(nombre__istartswith="TOPPING ")
        | Q(nombre__icontains="Rebanada")
        | Q(nombre__icontains="Mini")
        | Q(nombre__icontains="Chico")
        | Q(nombre__icontains="Mediano")
        | Q(nombre__icontains="Grande")
    )
    for recipe_id in candidate_qs.values_list("id", flat=True):
        recipe_ids.add(int(recipe_id))

    for recipe in Receta.objects.filter(id__in=sorted(recipe_ids)).order_by("nombre", "codigo_point"):
        rows.append(classify_commercial_recipe(recipe))
    return rows


def resolve_effective_receta(
    receta: Receta,
    *,
    context: CommercialRecipeLookupContext | None = None,
) -> Receta:
    alias_code = BROKEN_ALIAS_BY_NAME.get((receta.nombre or "").strip())
    if alias_code:
        aliased = _find_recipe_by_code(alias_code, context=context)
        if aliased is not None:
            return aliased
    return receta


def resolve_grouped_rule_for_recipe(
    receta: Receta,
    *,
    context: CommercialRecipeLookupContext | None = None,
) -> RecetaAgrupacionAddon | None:
    code = (receta.codigo_point or "").strip().upper()
    if context is not None:
        rule = context.grouped_rules_by_addon_id.get(int(receta.id))
        if rule is not None:
            return rule
        if code:
            return context.grouped_rules_by_addon_code.get(code)
    qs = (
        RecetaAgrupacionAddon.objects.filter(
            activo=True,
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )
        .select_related("base_receta", "addon_receta")
        .order_by("id")
    )
    rule = qs.filter(addon_receta=receta).first()
    if rule is not None:
        return rule
    if code:
        return qs.filter(addon_codigo_point__iexact=code).first()
    return None


def resolve_commercial_recipe(
    receta: Receta,
    *,
    context: CommercialRecipeLookupContext | None = None,
) -> CommercialRecipeResolution:
    effective = resolve_effective_receta(receta, context=context)
    if effective.id != receta.id:
        return CommercialRecipeResolution(
            sold_receta=receta,
            effective_receta=effective,
            component_recetas=(effective,),
            resolution_kind="ALIASED_RECIPE",
            notes=(f"{receta.nombre} se costea y explota como {effective.nombre}.",),
        )

    rule = resolve_grouped_rule_for_recipe(effective, context=context)
    if rule is None or not rule.addon_receta_id:
        return CommercialRecipeResolution(
            sold_receta=receta,
            effective_receta=effective,
            component_recetas=(effective,),
            resolution_kind="DIRECT_RECIPE",
        )

    if _addon_embeds_base(addon_receta=rule.addon_receta, base_receta=rule.base_receta, context=context):
        return CommercialRecipeResolution(
            sold_receta=receta,
            effective_receta=effective,
            component_recetas=(rule.addon_receta,),
            resolution_kind="SELF_CONTAINED_ADDON",
            notes=(f"{rule.addon_receta.nombre} ya embebe la base {rule.base_receta.nombre}; se evita doble conteo.",),
            grouped_rule=rule,
        )

    return CommercialRecipeResolution(
        sold_receta=receta,
        effective_receta=effective,
        component_recetas=(rule.base_receta, rule.addon_receta),
        resolution_kind="BASE_PLUS_ADDON",
        notes=(f"{effective.nombre} se resuelve como {rule.base_receta.nombre} + {rule.addon_receta.nombre}.",),
        grouped_rule=rule,
    )


def expand_commercial_component_recipes(receta: Receta) -> CommercialRecipeResolution:
    return resolve_commercial_recipe(receta)


def resolve_commercial_sku_interpretation(
    receta: Receta,
    *,
    context: CommercialRecipeLookupContext | None = None,
) -> CommercialSkuExecutionPlan:
    resolution = resolve_commercial_recipe(receta, context=context)
    classification = classify_commercial_recipe(receta, context=context)
    pricing_ids: list[int] = []
    for candidate in (resolution.sold_receta.id, resolution.effective_receta.id):
        if candidate not in pricing_ids:
            pricing_ids.append(candidate)
    notes = list(resolution.notes)
    if classification.clasificacion == RULE_BLOQUEADO_POR_AMBIGUEDAD:
        notes.append(
            classification.nota_negocio
            or "SKU bloqueado por ambigüedad; requiere curación explícita antes de usarse."
        )
    return CommercialSkuExecutionPlan(
        sold_receta=resolution.sold_receta,
        effective_receta=resolution.effective_receta,
        planning_receta=resolution.effective_receta,
        inventory_receta=resolution.effective_receta,
        pricing_receta_ids=tuple(pricing_ids),
        component_recetas=resolution.component_recetas,
        resolution_kind=resolution.resolution_kind,
        classification=classification.clasificacion,
        is_blocked=classification.clasificacion == RULE_BLOQUEADO_POR_AMBIGUEDAD,
        blocked_reason=classification.nota_negocio,
        notes=tuple(notes),
        grouped_rule=resolution.grouped_rule,
    )


def get_commercial_total_cost_map(
    recipe_ids: list[int] | set[int] | tuple[int, ...],
    *,
    context: CommercialRecipeLookupContext | None = None,
) -> dict[int, Decimal]:
    requested_ids = {int(recipe_id) for recipe_id in recipe_ids if int(recipe_id or 0) > 0}
    if not requested_ids:
        return {}

    _ensure_curated_commercial_mappings_once()
    effective_context = context or build_commercial_recipe_lookup_context(requested_ids)
    recipe_map = {
        recipe_id: recipe
        for recipe_id, recipe in effective_context.recipes_by_id.items()
        if recipe_id in requested_ids
    }
    dependent_ids: set[int] = set()
    resolutions: dict[int, CommercialRecipeResolution] = {}
    for recipe in recipe_map.values():
        resolution = resolve_commercial_recipe(recipe, context=effective_context)
        resolutions[recipe.id] = resolution
        dependent_ids.update(component.id for component in resolution.component_recetas)

    resolved_costs = get_total_cost_map(dependent_ids)
    totals: dict[int, Decimal] = {}
    for recipe_id, resolution in resolutions.items():
        classification = classify_commercial_recipe(recipe_map[recipe_id], context=effective_context)
        if classification.clasificacion == RULE_BLOQUEADO_POR_AMBIGUEDAD:
            totals[recipe_id] = ZERO
            continue
        total = ZERO
        for component in resolution.component_recetas:
            total += resolved_costs.get(component.id, ZERO)
        totals[recipe_id] = total
    return totals
