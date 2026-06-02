from __future__ import annotations

import json
from collections import Counter, defaultdict
from decimal import Decimal

from django.core.management.base import BaseCommand

from maestros.models import CostoInsumo, Insumo
from recetas.models import LineaReceta
from recetas.utils.normalizacion import normalizar_nombre
from reportes.models import InsumoCostoHistoricoMensual, RecetaCostoHistoricoMensual


POINT_EXISTENCIA_SOURCE = "POINT_EXISTENCIA_ALMACEN"

UNIT_ALIAS = {
    "g": "g",
    "gr": "g",
    "gramo": "g",
    "gramos": "g",
    "kg": "kg",
    "kilo": "kg",
    "kilogramo": "kg",
    "kilogramos": "kg",
    "ml": "ml",
    "mililitro": "ml",
    "mililitros": "ml",
    "l": "lt",
    "lt": "lt",
    "lts": "lt",
    "litro": "lt",
    "litros": "lt",
    "pza": "pza",
    "pz": "pza",
    "pieza": "pza",
    "piezas": "pza",
    "unidad": "unidad",
    "unidades": "unidad",
    "u": "unidad",
}
UNIT_TYPE = {
    "g": "MASS",
    "kg": "MASS",
    "ml": "VOLUME",
    "lt": "VOLUME",
    "pza": "UNIT",
    "unidad": "UNIT",
}
STOPWORDS = {"de", "del", "la", "el", "y", "para", "con"}


def _decimal(value) -> Decimal | None:
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _str_decimal(value) -> str | None:
    if value is None:
        return None
    return str(value)


def _raw_unit(cost: CostoInsumo) -> str:
    raw = cost.raw or {}
    return str(raw.get("unit") or raw.get("unidad") or "").strip()


def _raw_quantity(cost: CostoInsumo) -> Decimal | None:
    raw = cost.raw or {}
    return _decimal(raw.get("quantity", raw.get("cantidad", None)))


def _point_name(cost: CostoInsumo) -> str:
    raw = cost.raw or {}
    return str(raw.get("point_name") or raw.get("article_name") or "").strip()


def _unit_code(value: str) -> str | None:
    return UNIT_ALIAS.get(str(value or "").strip().lower().rstrip("."))


def _unit_type(value: str) -> str | None:
    return UNIT_TYPE.get(_unit_code(value) or "")


def _name_matches(insumo: Insumo, point_name: str) -> bool:
    point_norm = normalizar_nombre(point_name or "")
    erp_names = [
        normalizar_nombre(insumo.nombre or ""),
        normalizar_nombre(insumo.nombre_point or ""),
        normalizar_nombre(insumo.nombre_normalizado or ""),
    ]
    erp_names = [name for name in erp_names if name]
    if not point_norm or not erp_names:
        return True
    if any(point_norm == name or point_norm in name or name in point_norm for name in erp_names):
        return True

    point_tokens = {token for token in point_norm.split() if len(token) > 2 and token not in STOPWORDS}
    erp_tokens = {
        token
        for token in " ".join(erp_names).split()
        if len(token) > 2 and token not in STOPWORDS
    }
    return bool(point_tokens and erp_tokens and len(point_tokens & erp_tokens) >= min(2, len(point_tokens)))


def classify_point_cost(cost: CostoInsumo) -> list[str]:
    classes: list[str] = []
    quantity = _raw_quantity(cost)
    unit = _raw_unit(cost)
    insumo = cost.insumo
    unit_cost = _decimal(cost.costo_unitario) or Decimal("0")

    if quantity is not None and quantity <= 0 and unit_cost > 0:
        classes.append("QTY_NO_POSITIVA_CON_COSTO")
    if not _name_matches(insumo, _point_name(cost)):
        classes.append("NOMBRE_POINT_NO_COINCIDE")

    point_unit_type = _unit_type(unit)
    erp_unit_type = getattr(insumo.unidad_base, "tipo", None) if insumo.unidad_base_id else None
    if unit and point_unit_type and erp_unit_type and point_unit_type != erp_unit_type:
        classes.append("UNIDAD_INCOMPATIBLE")
    if unit and not _unit_code(unit):
        classes.append("UNIDAD_DESCONOCIDA")
    return classes


def _cost_payload(cost: CostoInsumo, classes: list[str]) -> dict[str, object]:
    raw = cost.raw or {}
    insumo = cost.insumo
    return {
        "cost_id": cost.id,
        "insumo_id": insumo.id,
        "insumo": insumo.nombre,
        "fecha": cost.fecha.isoformat() if cost.fecha else "",
        "costo": _str_decimal(cost.costo_unitario),
        "erp_unit": insumo.unidad_base.codigo if insumo.unidad_base_id else "",
        "raw_unit": _raw_unit(cost),
        "raw_qty": _str_decimal(_raw_quantity(cost)),
        "point_code": raw.get("point_code") or "",
        "point_name": _point_name(cost),
        "classes": classes,
    }


class Command(BaseCommand):
    help = "Audita costos Point inseguros y su impacto mensual/recetas en modo solo lectura."
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument(
            "--format",
            choices=["json"],
            default="json",
            help="Formato de salida. Actualmente solo json.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=25,
            help="Numero maximo de muestras por seccion.",
        )

    def handle(self, *args, **options):
        limit = int(options["limit"])
        bad_by_id: dict[int, list[str]] = {}
        bad_insumos: dict[int, set[str]] = defaultdict(set)
        class_counts: Counter[str] = Counter()
        class_insumos: dict[str, set[int]] = defaultdict(set)
        point_name_sets: dict[int, set[str]] = defaultdict(set)
        point_code_names: dict[str, set[str]] = defaultdict(set)

        point_costs = (
            CostoInsumo.objects.filter(raw__source=POINT_EXISTENCIA_SOURCE)
            .select_related("insumo", "insumo__unidad_base")
            .order_by("fecha", "id")
        )
        point_rows = 0
        for cost in point_costs.iterator():
            point_rows += 1
            raw = cost.raw or {}
            point_name = _point_name(cost)
            point_code = str(raw.get("point_code") or "").strip()
            if point_name:
                point_name_sets[cost.insumo_id].add(point_name)
            if point_code and point_name:
                point_code_names[point_code].add(point_name)

            classes = classify_point_cost(cost)
            if not classes:
                continue
            bad_by_id[cost.id] = classes
            for class_name in classes:
                class_counts[class_name] += 1
                class_insumos[class_name].add(cost.insumo_id)
                bad_insumos[cost.insumo_id].add(class_name)

        latest_unsafe = []
        for insumo in Insumo.objects.filter(activo=True).select_related("unidad_base").order_by("id"):
            latest = CostoInsumo.objects.filter(insumo=insumo).order_by("-fecha", "-id").first()
            if latest and latest.id in bad_by_id:
                latest_unsafe.append(_cost_payload(latest, bad_by_id[latest.id]))

        monthly_bad = []
        monthly_keys: set[tuple[object, int]] = set()
        monthly_by_period: Counter[str] = Counter()
        for row in InsumoCostoHistoricoMensual.objects.select_related("insumo").order_by("periodo", "insumo_id"):
            source_rows = (row.metadata or {}).get("source_rows") or []
            overlap = [source_id for source_id in source_rows if source_id in bad_by_id]
            if not overlap:
                continue
            monthly_keys.add((row.periodo, row.insumo_id))
            monthly_by_period[row.periodo.isoformat()] += 1
            monthly_bad.append(
                {
                    "periodo": row.periodo.isoformat(),
                    "insumo_id": row.insumo_id,
                    "insumo": row.insumo.nombre,
                    "costo": _str_decimal(row.costo_unitario),
                    "metodo": row.metodo,
                    "bad_source_rows": overlap,
                    "classes": sorted({class_name for source_id in overlap for class_name in bad_by_id[source_id]}),
                }
            )

        recipe_month_impacts = []
        for period, insumo_id in monthly_keys:
            recipes = (
                LineaReceta.objects.filter(insumo_id=insumo_id)
                .values_list("receta_id", "receta__nombre", "receta__tipo")
                .distinct()
            )
            for recipe_id, recipe_name, recipe_type in recipes:
                historical = RecetaCostoHistoricoMensual.objects.filter(
                    periodo=period,
                    receta_id=recipe_id,
                ).first()
                if not historical:
                    continue
                recipe_month_impacts.append(
                    {
                        "periodo": period.isoformat(),
                        "receta_id": recipe_id,
                        "receta": recipe_name,
                        "tipo": recipe_type,
                        "insumo_id": insumo_id,
                        "costo_total": _str_decimal(historical.costo_total),
                        "unitario": _str_decimal(historical.costo_por_unidad_rendimiento),
                    }
                )

        recipe_exposure_counter: Counter[tuple[int, str, str]] = Counter()
        if bad_insumos:
            for row in (
                LineaReceta.objects.filter(insumo_id__in=bad_insumos.keys())
                .values("receta_id", "receta__nombre", "receta__tipo")
                .distinct()
            ):
                key = (row["receta_id"], row["receta__nombre"], row["receta__tipo"])
                recipe_exposure_counter[key] += 1

        multi_name_insumos = []
        for insumo_id, names in point_name_sets.items():
            if len(names) <= 1:
                continue
            insumo = Insumo.objects.select_related("unidad_base").filter(id=insumo_id).first()
            if not insumo:
                continue
            multi_name_insumos.append(
                {
                    "insumo_id": insumo.id,
                    "insumo": insumo.nombre,
                    "codigo_point": insumo.codigo_point,
                    "unidad": insumo.unidad_base.codigo if insumo.unidad_base_id else "",
                    "name_count": len(names),
                    "names": sorted(names)[:limit],
                }
            )
        multi_name_insumos.sort(key=lambda item: item["name_count"], reverse=True)

        code_collisions = [
            {"point_code": code, "name_count": len(names), "names": sorted(names)[:limit]}
            for code, names in point_code_names.items()
            if len(names) > 1
        ]
        code_collisions.sort(key=lambda item: item["name_count"], reverse=True)

        recipe_impact_counter = Counter(
            (item["receta_id"], item["receta"], item["tipo"]) for item in recipe_month_impacts
        )
        payload = {
            "mode": "dry_run_read_only",
            "point_source": POINT_EXISTENCIA_SOURCE,
            "summary": {
                "point_existencia_rows": point_rows,
                "bad_cost_rows_total": len(bad_by_id),
                "bad_rows_by_class": dict(class_counts),
                "bad_insumos_by_class": {key: len(value) for key, value in class_insumos.items()},
                "latest_unsafe_count": len(latest_unsafe),
                "monthly_bad_count": len(monthly_bad),
                "monthly_bad_periods": dict(sorted(monthly_by_period.items())),
                "recipe_month_impacts_count": len(recipe_month_impacts),
                "recipe_exposure_unique_count": len(recipe_exposure_counter),
                "multi_name_insumos_count": len(multi_name_insumos),
                "point_code_collision_count": len(code_collisions),
            },
            "latest_unsafe": latest_unsafe[:limit],
            "monthly_bad": monthly_bad[:limit],
            "recipe_month_impacts_top": [
                {
                    "receta_id": key[0],
                    "receta": key[1],
                    "tipo": key[2],
                    "impact_rows": value,
                }
                for key, value in recipe_impact_counter.most_common(limit)
            ],
            "recipe_exposure_top": [
                {
                    "receta_id": key[0],
                    "receta": key[1],
                    "tipo": key[2],
                    "bad_insumo_refs": value,
                }
                for key, value in recipe_exposure_counter.most_common(limit)
            ],
            "multi_name_insumos_top": multi_name_insumos[:limit],
            "point_code_collisions_top": code_collisions[:limit],
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
