from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.db import transaction

from maestros.models import CostoInsumo, Insumo, InsumoAlias
from recetas.models import LineaReceta, Receta, RecetaPresentacion
from recetas.utils.costeo_snapshot import (
    convert_unit_cost,
    resolve_preparation_recipe_for_insumo,
)
from recetas.utils.derived_product_presentations import get_active_derived_relation
from reportes.models import (
    InsumoCostoHistoricoMensual,
    RecetaCostoHistoricoMensual,
    ReglaCostoHistoricoInsumo,
)


Q6 = Decimal("0.000001")
DERIVED_SOURCES = {"RECETA_PREPARACION", "RECETA_PRESENTACION"}
PRESENTACION_PATTERN = re.compile(r"^DERIVADO:RECETA:(\d+):PRESENTACION:(\d+)$")


def _q6(value: Decimal | int | float | str | None) -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(Q6, rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.000000")


def _month_bounds(period_start: date) -> tuple[date, date]:
    next_month = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return period_start, next_month - timedelta(days=1)


def _raw_source(cost: CostoInsumo) -> str:
    return str((cost.raw or {}).get("source") or "").strip().upper()


def _weight_from_raw(cost: CostoInsumo) -> Decimal:
    raw = cost.raw or {}
    for key in ("cantidad", "quantity"):
        try:
            weight = Decimal(str(raw.get(key) or 0))
        except Exception:
            weight = Decimal("0")
        if weight > 0:
            return weight
    return Decimal("1")


def _source_method(rows: list[CostoInsumo]) -> str:
    if any(_raw_source(row) == "POINT_EXISTENCIA_ALMACEN" for row in rows):
        return InsumoCostoHistoricoMensual.METODO_POINT_EXISTENCIA
    return InsumoCostoHistoricoMensual.METODO_PROMEDIO_MENSUAL


@dataclass
class HistoricalCostSnapshotSummary:
    period_start: date
    insumo_rows: int
    receta_rows: int
    missing_recipe_rows: int


@dataclass
class _RecipeCostResult:
    total_cost: Decimal
    unit_cost: Decimal | None
    costed_lines: int
    total_lines: int
    metadata: dict = field(default_factory=dict)


class MonthlyHistoricalCostingService:
    def __init__(self) -> None:
        self._insumo_cache: dict[tuple[date, int], InsumoCostoHistoricoMensual | None] = {}
        self._recipe_cache: dict[tuple[date, int], _RecipeCostResult] = {}
        self._active_stack: set[tuple[date, int]] = set()
        self._insumo_rule_stack: set[tuple[date, int]] = set()

    def _purchase_like_costs(self, *, insumo: Insumo, period_end: date):
        rows = list(
            CostoInsumo.objects.filter(insumo=insumo, fecha__lte=period_end).order_by("fecha", "id")
        )
        return [row for row in rows if _raw_source(row) not in DERIVED_SOURCES]

    def _resolve_resale_insumo(self, *, receta: Receta) -> Insumo | None:
        codigo_point = (receta.codigo_point or "").strip()
        if codigo_point:
            by_code = (
                Insumo.objects.filter(codigo_point__iexact=codigo_point, activo=True)
                .order_by("id")
                .first()
            )
            if by_code is not None:
                return by_code
        normalized_name = (receta.nombre_normalizado or "").strip()
        if normalized_name:
            by_name = (
                Insumo.objects.filter(nombre_normalizado=normalized_name, activo=True)
                .order_by("id")
                .first()
            )
            if by_name is not None:
                return by_name
            alias = (
                InsumoAlias.objects.filter(nombre_normalizado=normalized_name)
                .select_related("insumo")
                .order_by("id")
                .first()
            )
            if alias is not None and alias.insumo_id and alias.insumo.activo:
                return alias.insumo
        return None

    def _build_resale_recipe_monthly_cost(self, *, period_start: date, receta: Receta) -> _RecipeCostResult:
        insumo = self._resolve_resale_insumo(receta=receta)
        if insumo is None:
            return _RecipeCostResult(
                total_cost=Decimal("0"),
                unit_cost=None,
                costed_lines=0,
                total_lines=1,
                metadata={
                    "missing_lines": [
                        {
                            "linea_id": None,
                            "insumo_texto": receta.nombre,
                            "source_label": "REVENTA_SIN_INSUMO_MATCH",
                        }
                    ],
                    "source_labels": ["REVENTA_SIN_INSUMO_MATCH"],
                    "bom_basis": "DIRECT_PURCHASE_MATCH",
                },
            )
        monthly_cost = self._build_insumo_monthly_cost(period_start=period_start, insumo=insumo)
        if monthly_cost is None or monthly_cost.costo_unitario <= 0:
            return _RecipeCostResult(
                total_cost=Decimal("0"),
                unit_cost=None,
                costed_lines=0,
                total_lines=1,
                metadata={
                    "missing_lines": [
                        {
                            "linea_id": None,
                            "insumo_texto": insumo.nombre,
                            "source_label": "REVENTA_SIN_COSTO_HISTORICO",
                        }
                    ],
                    "source_labels": ["REVENTA_SIN_COSTO_HISTORICO"],
                    "bom_basis": "DIRECT_PURCHASE_MATCH",
                    "matched_insumo_id": insumo.id,
                    "matched_insumo_nombre": insumo.nombre,
                },
            )
        unit_cost = _q6(monthly_cost.costo_unitario)
        return _RecipeCostResult(
            total_cost=unit_cost,
            unit_cost=unit_cost,
            costed_lines=1,
            total_lines=1,
            metadata={
                "missing_lines": [],
                "source_labels": [f"REVENTA_{monthly_cost.metodo}"],
                "bom_basis": "DIRECT_PURCHASE_MATCH",
                "matched_insumo_id": insumo.id,
                "matched_insumo_nombre": insumo.nombre,
                "matched_insumo_codigo_point": insumo.codigo_point,
            },
        )

    def _future_purchase_like_cost(self, *, insumo: Insumo, period_end: date) -> CostoInsumo | None:
        return (
            CostoInsumo.objects.filter(insumo=insumo, fecha__gt=period_end)
            .order_by("fecha", "id")
            .exclude(costo_unitario__lte=0)
            .first()
        )

    def _fallback_rules(self, *, insumo: Insumo):
        return list(
            ReglaCostoHistoricoInsumo.objects.filter(insumo_origen=insumo, activo=True)
            .select_related("insumo_referencia")
            .order_by("prioridad", "id")
        )

    def _apply_fallback_rule(
        self,
        *,
        period_start: date,
        insumo: Insumo,
        rule: ReglaCostoHistoricoInsumo,
        period_end: date,
    ) -> InsumoCostoHistoricoMensual | None:
        cache_key = (period_start, insumo.id)
        if cache_key in self._insumo_rule_stack:
            return None
        self._insumo_rule_stack.add(cache_key)
        try:
            if rule.metodo == ReglaCostoHistoricoInsumo.METODO_EQUIVALENCIA:
                target = rule.insumo_referencia
                if target is None or target.id == insumo.id:
                    return None
                target_row = self._build_insumo_monthly_cost(period_start=period_start, insumo=target)
                if target_row is None or target_row.costo_unitario <= 0:
                    return None
                row, _ = InsumoCostoHistoricoMensual.objects.update_or_create(
                    periodo=period_start,
                    insumo=insumo,
                    defaults={
                        "costo_unitario": target_row.costo_unitario,
                        "metodo": InsumoCostoHistoricoMensual.METODO_EQUIVALENCIA,
                        "source_date": target_row.source_date,
                        "sample_count": target_row.sample_count,
                        "weighted_quantity": target_row.weighted_quantity,
                        "metadata": {
                            "period_end": period_end.isoformat(),
                            "bom_basis": "CURRENT_RECIPE_STRUCTURE",
                            "fallback_rule_id": rule.id,
                            "fallback_method": rule.metodo,
                            "fallback_target_insumo_id": target.id,
                            "fallback_target_name": target.nombre,
                            "fallback_target_method": target_row.metodo,
                        },
                    },
                )
                self._insumo_cache[cache_key] = row
                return row

            if rule.metodo == ReglaCostoHistoricoInsumo.METODO_SIGUIENTE:
                target = rule.insumo_referencia or insumo
                future_row = self._future_purchase_like_cost(insumo=target, period_end=period_end)
                if future_row is None:
                    return None
                row, _ = InsumoCostoHistoricoMensual.objects.update_or_create(
                    periodo=period_start,
                    insumo=insumo,
                    defaults={
                        "costo_unitario": _q6(future_row.costo_unitario),
                        "metodo": InsumoCostoHistoricoMensual.METODO_SIGUIENTE,
                        "source_date": future_row.fecha,
                        "sample_count": 1,
                        "weighted_quantity": _q6(_weight_from_raw(future_row)),
                        "metadata": {
                            "period_end": period_end.isoformat(),
                            "bom_basis": "CURRENT_RECIPE_STRUCTURE",
                            "fallback_rule_id": rule.id,
                            "fallback_method": rule.metodo,
                            "fallback_target_insumo_id": target.id,
                            "fallback_target_name": target.nombre,
                            "source_rows": [future_row.id],
                        },
                    },
                )
                self._insumo_cache[cache_key] = row
                return row
            return None
        finally:
            self._insumo_rule_stack.discard(cache_key)

    def _build_insumo_monthly_cost(self, *, period_start: date, insumo: Insumo) -> InsumoCostoHistoricoMensual | None:
        cache_key = (period_start, insumo.id)
        if cache_key in self._insumo_cache:
            return self._insumo_cache[cache_key]

        period_start, period_end = _month_bounds(period_start)
        purchase_like = list(self._purchase_like_costs(insumo=insumo, period_end=period_end))
        same_month = [row for row in purchase_like if period_start <= row.fecha <= period_end and row.costo_unitario > 0]
        method = None
        source_date = None
        sample_count = 0
        weighted_qty = Decimal("0")
        cost_value = Decimal("0")
        metadata: dict[str, object] = {
            "period_end": period_end.isoformat(),
            "bom_basis": "CURRENT_RECIPE_STRUCTURE",
        }

        if same_month:
            weighted_total = Decimal("0")
            weighted_qty = Decimal("0")
            for row in same_month:
                weight = _weight_from_raw(row)
                weighted_qty += weight
                weighted_total += Decimal(str(row.costo_unitario or 0)) * weight
            sample_count = len(same_month)
            cost_value = _q6(weighted_total / weighted_qty) if weighted_qty > 0 else _q6(same_month[-1].costo_unitario)
            method = _source_method(same_month)
            source_date = same_month[-1].fecha
            metadata["source_rows"] = [row.id for row in same_month]
        else:
            latest = next((row for row in reversed(purchase_like) if row.costo_unitario > 0), None)
            if latest is None:
                for rule in self._fallback_rules(insumo=insumo):
                    fallback_row = self._apply_fallback_rule(
                        period_start=period_start,
                        insumo=insumo,
                        rule=rule,
                        period_end=period_end,
                    )
                    if fallback_row is not None:
                        return fallback_row
                self._insumo_cache[cache_key] = None
                return None
            cost_value = _q6(latest.costo_unitario)
            method = InsumoCostoHistoricoMensual.METODO_ARRASTRE
            source_date = latest.fecha
            sample_count = 1
            weighted_qty = _weight_from_raw(latest)
            metadata["source_rows"] = [latest.id]

        row, _ = InsumoCostoHistoricoMensual.objects.update_or_create(
            periodo=period_start,
            insumo=insumo,
            defaults={
                "costo_unitario": cost_value,
                "metodo": method,
                "source_date": source_date,
                "sample_count": sample_count,
                "weighted_quantity": _q6(weighted_qty),
                "metadata": metadata,
            },
        )
        self._insumo_cache[cache_key] = row
        return row

    def _presentation_unit_cost(self, *, period_start: date, presentacion: RecetaPresentacion) -> Decimal | None:
        recipe_result = self._build_recipe_monthly_cost(period_start=period_start, receta=presentacion.receta)
        if recipe_result.unit_cost is None:
            return None
        if not presentacion.peso_por_unidad_kg or presentacion.peso_por_unidad_kg <= 0:
            return None
        return _q6(recipe_result.unit_cost * Decimal(str(presentacion.peso_por_unidad_kg)))

    def _resolve_line_cost(self, *, period_start: date, linea: LineaReceta) -> tuple[Decimal | None, str]:
        if linea.tipo_linea == LineaReceta.TIPO_SUBSECCION:
            return None, "SUBSECCION_IGNORADA"
        if linea.cantidad is None or Decimal(str(linea.cantidad or 0)) <= 0:
            return Decimal("0"), "CANTIDAD_CERO"
        if linea.insumo_id is None:
            if linea.costo_linea_excel is not None and Decimal(str(linea.costo_linea_excel or 0)) > 0:
                return _q6(linea.costo_linea_excel), "COSTO_FIJO_LEGACY"
            return None, "SIN_INSUMO"

        insumo = linea.insumo
        if insumo is None:
            return None, "INSUMO_ELIMINADO"

        source_unit = insumo.unidad_base
        unit_cost: Decimal | None = None
        source_label = "INSUMO_DIRECTO"

        if linea.costo_unitario_snapshot is not None and Decimal(str(linea.costo_unitario_snapshot or 0)) > 0:
            unit_cost = _q6(linea.costo_unitario_snapshot)
            source_unit = linea.unidad or insumo.unidad_base
            source_label = "LINEA_SNAPSHOT"

        code = (insumo.codigo or "").strip()
        if unit_cost is None:
            match = PRESENTACION_PATTERN.match(code)
            if match:
                presentacion = RecetaPresentacion.objects.filter(id=int(match.group(2))).select_related("receta").first()
                if presentacion is not None:
                    unit_cost = self._presentation_unit_cost(period_start=period_start, presentacion=presentacion)
                    source_label = "RECETA_PRESENTACION_MENSUAL"
                    source_unit = insumo.unidad_base
        if unit_cost is None:
            prep_recipe = resolve_preparation_recipe_for_insumo(insumo)
            if prep_recipe is not None:
                prep_result = self._build_recipe_monthly_cost(period_start=period_start, receta=prep_recipe)
                unit_cost = prep_result.unit_cost
                source_unit = prep_recipe.rendimiento_unidad or insumo.unidad_base
                source_label = "RECETA_PREPARACION_MENSUAL"
        if unit_cost is None:
            insumo_month = self._build_insumo_monthly_cost(period_start=period_start, insumo=insumo)
            if insumo_month is not None:
                unit_cost = _q6(insumo_month.costo_unitario)
                source_unit = insumo.unidad_base
                source_label = f"INSUMO_{insumo_month.metodo}"

        if unit_cost is None or unit_cost <= 0:
            return None, f"{source_label}_SIN_COSTO"

        target_unit = linea.unidad or insumo.unidad_base or source_unit
        if target_unit is None or source_unit is None:
            return None, f"{source_label}_SIN_UNIDAD"
        converted = convert_unit_cost(unit_cost, source_unit=source_unit, target_unit=target_unit)
        if converted is None or converted <= 0:
            if source_unit.id == target_unit.id:
                converted = unit_cost
            else:
                return None, f"{source_label}_UNIDAD_INCOMPATIBLE"

        line_cost = _q6(converted * Decimal(str(linea.cantidad or 0)))
        if line_cost < 0:
            return None, f"{source_label}_SIN_CANTIDAD"
        return line_cost, source_label

    def _build_recipe_monthly_cost(self, *, period_start: date, receta: Receta) -> _RecipeCostResult:
        cache_key = (period_start, receta.id)
        if cache_key in self._recipe_cache:
            return self._recipe_cache[cache_key]
        if receta.modo_costeo == Receta.MODO_COSTEO_REVENTA:
            result = self._build_resale_recipe_monthly_cost(period_start=period_start, receta=receta)
            self._recipe_cache[cache_key] = result
            return result
        if cache_key in self._active_stack:
            result = _RecipeCostResult(
                total_cost=Decimal("0"),
                unit_cost=None,
                costed_lines=0,
                total_lines=0,
                metadata={"cycle_detected": True},
            )
            self._recipe_cache[cache_key] = result
            return result

        self._active_stack.add(cache_key)
        try:
            lineas = list(
                receta.lineas.select_related("insumo", "unidad")
                .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
                .order_by("posicion", "id")
            )
            derived_relation = get_active_derived_relation(receta)
            total_cost = Decimal("0")
            costed_lines = 0
            missing_lines: list[dict[str, object]] = []
            source_labels: list[str] = []
            for linea in lineas:
                line_cost, source_label = self._resolve_line_cost(period_start=period_start, linea=linea)
                source_labels.append(source_label)
                if line_cost is None:
                    missing_lines.append(
                        {
                            "linea_id": linea.id,
                            "insumo_texto": linea.insumo_texto,
                            "source_label": source_label,
                        }
                    )
                    continue
                total_cost += line_cost
                costed_lines += 1

            derived_parent_unit_cost: Decimal | None = None
            if derived_relation is not None:
                units_per_parent = Decimal(str(derived_relation.unidades_por_padre or 0))
                if units_per_parent > 0:
                    parent_result = self._build_recipe_monthly_cost(
                        period_start=period_start,
                        receta=derived_relation.receta_padre,
                    )
                    if parent_result.total_cost > 0:
                        derived_parent_unit_cost = _q6(parent_result.total_cost / units_per_parent)
                        total_cost += derived_parent_unit_cost
                        costed_lines += 1
                        source_labels.append("DERIVED_PARENT_UNIT")
                    else:
                        missing_lines.append(
                            {
                                "linea_id": None,
                                "insumo_texto": derived_relation.receta_padre.nombre,
                                "source_label": "DERIVED_PARENT_SIN_COSTO",
                            }
                        )
                        source_labels.append("DERIVED_PARENT_SIN_COSTO")
                else:
                    missing_lines.append(
                        {
                            "linea_id": None,
                            "insumo_texto": derived_relation.receta_padre.nombre,
                            "source_label": "DERIVED_PARENT_SIN_UNIDADES",
                        }
                    )
                    source_labels.append("DERIVED_PARENT_SIN_UNIDADES")

            unit_cost = None
            if receta.rendimiento_cantidad and Decimal(str(receta.rendimiento_cantidad or 0)) > 0:
                unit_cost = _q6(total_cost / Decimal(str(receta.rendimiento_cantidad)))
            coverage_pct = Decimal("0")
            total_lines = len(lineas) + (1 if derived_relation is not None else 0)
            if total_lines > 0:
                coverage_pct = _q6((Decimal(costed_lines) / Decimal(total_lines)) * Decimal("100"))

            result = _RecipeCostResult(
                total_cost=_q6(total_cost),
                unit_cost=unit_cost,
                costed_lines=costed_lines,
                total_lines=total_lines,
                metadata={
                    "missing_lines": missing_lines,
                    "source_labels": source_labels,
                    "bom_basis": "CURRENT_RECIPE_STRUCTURE",
                    "derived_relation_id": derived_relation.id if derived_relation is not None else None,
                    "derived_parent_recipe_id": derived_relation.receta_padre_id if derived_relation is not None else None,
                    "derived_parent_unit_cost": str(derived_parent_unit_cost or Decimal("0")),
                },
            )
            self._recipe_cache[cache_key] = result
            return result
        finally:
            self._active_stack.discard(cache_key)

    @transaction.atomic
    def build_period(self, *, period_start: date) -> HistoricalCostSnapshotSummary:
        period_start, period_end = _month_bounds(period_start)
        recetas_objetivo = list(
            Receta.objects.filter(
                tipo=Receta.TIPO_PRODUCTO_FINAL,
                modo_costeo__in=[Receta.MODO_COSTEO_FABRICADO, Receta.MODO_COSTEO_REVENTA],
                point_daily_sales__sale_date__range=(period_start, period_end),
                point_daily_sales__total_amount__gt=0,
            )
            .distinct()
            .order_by("id")
        )

        missing_recipe_rows = 0
        touched_insumos: set[int] = set()

        for receta in recetas_objetivo:
            result = self._build_recipe_monthly_cost(period_start=period_start, receta=receta)
            if result.costed_lines < result.total_lines:
                missing_recipe_rows += 1

        recipe_ids = {receta_id for snapshot_period, receta_id in self._recipe_cache if snapshot_period == period_start}
        RecetaCostoHistoricoMensual.objects.filter(periodo=period_start).exclude(receta_id__in=recipe_ids).delete()
        for receta_id in recipe_ids:
            receta = Receta.objects.filter(id=receta_id).first()
            if receta is None:
                continue
            result = self._recipe_cache[(period_start, receta_id)]
            RecetaCostoHistoricoMensual.objects.update_or_create(
                periodo=period_start,
                receta=receta,
                defaults={
                    "costo_total": result.total_cost,
                    "costo_por_unidad_rendimiento": result.unit_cost,
                    "lineas_costeadas": result.costed_lines,
                    "lineas_totales": result.total_lines,
                    "coverage_pct": _q6((Decimal(result.costed_lines) / Decimal(result.total_lines) * Decimal("100")) if result.total_lines else 0),
                    "metadata": {
                        **result.metadata,
                        "period_end": period_end.isoformat(),
                    },
                },
            )

        for (snapshot_period, insumo_id), row in self._insumo_cache.items():
            if snapshot_period != period_start or row is None:
                continue
            touched_insumos.add(insumo_id)

        return HistoricalCostSnapshotSummary(
            period_start=period_start,
            insumo_rows=len(touched_insumos),
            receta_rows=len(recipe_ids),
            missing_recipe_rows=missing_recipe_rows,
        )
