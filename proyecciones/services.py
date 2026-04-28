from __future__ import annotations

from collections import defaultdict
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from control.models import DevolucionSucursalMatriz, MermaMensualSucursal
from core.models import Sucursal, sucursales_operativas
from pos_bridge.models import PointInventorySnapshot
from recetas.models import Receta, VentaHistorica
from reportes.models import FactVentaDiaria
from ventas.services.sales_canonical_source import POINT_BRIDGE_SALES_SOURCE

from .models import ProyeccionProduccion


ZERO = Decimal("0")
ONE = Decimal("1")
UNIT = Decimal("0.001")
RATE = Decimal("0.0001")
DEFAULT_WASTE_FACTOR = Decimal("0.03")
DEFAULT_RETURN_FACTOR = Decimal("0.05")
MIN_HISTORY_DAYS = 7
NETWORK_FACTOR_HISTORY_THRESHOLD = 21
MAX_WASTE_FACTOR = Decimal("0.10")
MAX_RETURN_FACTOR = Decimal("0.15")


@dataclass
class ProjectionSummary:
    target_dates: list[date]
    dry_run: bool
    rows: list[dict[str, object]]
    created: int = 0
    updated: int = 0
    skipped: int = 0
    warnings: list[str] | None = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []

    @property
    def total_units(self) -> Decimal:
        return sum((_to_decimal(row.get("unidades_proyectadas_ajustadas")) for row in self.rows), ZERO).quantize(UNIT)

    def as_dict(self) -> dict[str, object]:
        factor_sources = Counter(str(row.get("factor_fuente") or "") for row in self.rows)
        return {
            "periodos": [item.isoformat() for item in self.target_dates],
            "dry_run": self.dry_run,
            "rows": len(self.rows),
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "total_units": str(self.total_units),
            "factor_fuentes": dict(sorted(factor_sources.items())),
            "factor_capped": sum(1 for row in self.rows if row.get("factor_capped")),
            "max_factor_merma": str(max((_to_decimal(row.get("factor_merma")) for row in self.rows), default=ZERO).quantize(RATE)),
            "max_factor_devolucion": str(max((_to_decimal(row.get("factor_devolucion")) for row in self.rows), default=ZERO).quantize(RATE)),
            "confianza": dict(sorted(Counter(str(row.get("confianza") or "") for row in self.rows).items())),
            "warnings": self.warnings[:40],
            "top_10": [_json_safe(row) for row in self.rows[:10]],
        }


class ProyeccionProduccionService:
    metodo = "PROMEDIO_MOVIL_7D"

    def proyectar_dia(
        self,
        fecha_objetivo: date,
        *,
        sucursal: Sucursal | None = None,
        dry_run: bool = True,
    ) -> ProjectionSummary:
        if fecha_objetivo.weekday() == 6:
            return ProjectionSummary(
                target_dates=[fecha_objetivo],
                dry_run=dry_run,
                rows=[],
                warnings=["Domingo no tiene producción programada."],
            )

        sucursales = [sucursal] if sucursal else list(sucursales_operativas(fecha_objetivo))
        recipes = list(
            Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL)
            .exclude(modo_costeo=Receta.MODO_COSTEO_SERVICIO)
            .exclude(excluir_cierre=True)
            .order_by("nombre")
        )
        sales_window_start = fecha_objetivo - timedelta(days=28)
        sales_rows = (
            VentaHistorica.objects.filter(
                fecha__gte=sales_window_start,
                fecha__lt=fecha_objetivo,
                fuente=POINT_BRIDGE_SALES_SOURCE,
                sucursal__in=sucursales,
                receta__in=recipes,
            )
            .values("fecha", "sucursal_id", "receta_id")
            .annotate(cantidad=Sum("cantidad"))
        )
        sales_by_key: dict[tuple[int, int], dict[date, Decimal]] = defaultdict(dict)
        for row in sales_rows:
            sales_by_key[(int(row["sucursal_id"]), int(row["receta_id"]))][row["fecha"]] = _to_decimal(row["cantidad"])

        candidate_keys = set(sales_by_key)
        stock_map = self._latest_stock_by_branch_recipe(candidate_keys)
        waste_factor_map = self._waste_factor_by_key(fecha_objetivo, candidate_keys)
        returned_factor_map = self._returned_factor_by_key(fecha_objetivo, candidate_keys, sales_by_key)
        network_waste_factor_map = self._network_waste_factor_by_recipe(fecha_objetivo, {recipe_id for _branch_id, recipe_id in candidate_keys})
        network_return_factor_map = self._network_returned_factor_by_recipe(fecha_objetivo, {recipe_id for _branch_id, recipe_id in candidate_keys})
        recipe_map = {recipe.id: recipe for recipe in recipes}
        branch_map = {branch.id: branch for branch in sucursales}

        rows: list[dict[str, object]] = []
        skipped = 0
        warnings: list[str] = []
        for key in sorted(candidate_keys, key=lambda item: (branch_map[item[0]].codigo, recipe_map[item[1]].nombre)):
            branch_id, recipe_id = key
            history = sales_by_key.get(key, {})
            history_days = len([qty for qty in history.values() if qty > ZERO])
            if history_days < MIN_HISTORY_DAYS:
                skipped += 1
                warnings.append(
                    f"HISTORIAL_INSUFICIENTE: {branch_map[branch_id].codigo} · {recipe_map[recipe_id].nombre} ({history_days} días)"
                )
                continue

            projected_sales = self._weighted_sales_average(history, fecha_objetivo)
            waste_factor, returned_factor, factor_source = self._resolve_factors(
                key=key,
                history_days=history_days,
                waste_factor_map=waste_factor_map,
                returned_factor_map=returned_factor_map,
                network_waste_factor_map=network_waste_factor_map,
                network_return_factor_map=network_return_factor_map,
            )
            waste_factor, waste_capped = _cap_factor(waste_factor, MAX_WASTE_FACTOR)
            returned_factor, returned_capped = _cap_factor(returned_factor, MAX_RETURN_FACTOR)
            factor_capped = waste_capped or returned_capped
            stock = stock_map.get(key, ZERO)
            raw_units = projected_sales * (ONE + waste_factor + returned_factor)
            adjusted_units = max(raw_units - stock, ZERO)
            confidence = self._confidence(history_days)
            rows.append(
                {
                    "periodo": fecha_objetivo,
                    "sucursal_id": branch_id,
                    "sucursal_codigo": branch_map[branch_id].codigo,
                    "receta_id": recipe_id,
                    "receta_nombre": recipe_map[recipe_id].nombre,
                    "venta_proyectada": projected_sales.quantize(UNIT),
                    "unidades_proyectadas": raw_units.quantize(UNIT),
                    "unidades_proyectadas_ajustadas": adjusted_units.quantize(UNIT),
                    "factor_merma": waste_factor.quantize(RATE),
                    "factor_devolucion": returned_factor.quantize(RATE),
                    "stock_actual": stock.quantize(UNIT),
                    "confianza": confidence,
                    "dias_historial": history_days,
                    "metodo": self.metodo,
                    "factor_fuente": factor_source,
                    "factor_capped": factor_capped,
                }
            )

        rows.sort(key=lambda row: row["unidades_proyectadas_ajustadas"], reverse=True)
        summary = ProjectionSummary(target_dates=[fecha_objetivo], dry_run=dry_run, rows=rows, skipped=skipped, warnings=warnings)
        if dry_run:
            return summary
        self._persist(rows, summary)
        return summary

    def proyectar_semana(
        self,
        fecha_inicio_semana: date,
        *,
        sucursal: Sucursal | None = None,
        dry_run: bool = True,
    ) -> ProjectionSummary:
        week_start = fecha_inicio_semana - timedelta(days=fecha_inicio_semana.weekday())
        summaries = [
            self.proyectar_dia(week_start + timedelta(days=offset), sucursal=sucursal, dry_run=True)
            for offset in range(6)
        ]
        rows: list[dict[str, object]] = []
        warnings: list[str] = []
        skipped = 0
        for summary in summaries:
            rows.extend(summary.rows)
            warnings.extend(summary.warnings or [])
            skipped += summary.skipped
        rows.sort(key=lambda row: row["unidades_proyectadas_ajustadas"], reverse=True)
        result = ProjectionSummary(
            target_dates=[week_start + timedelta(days=offset) for offset in range(6)],
            dry_run=dry_run,
            rows=rows,
            skipped=skipped,
            warnings=warnings,
        )
        if dry_run:
            return result
        self._persist(rows, result)
        return result

    def _persist(self, rows: list[dict[str, object]], summary: ProjectionSummary) -> None:
        now = timezone.now()
        with transaction.atomic():
            for row in rows:
                _, created = ProyeccionProduccion.objects.update_or_create(
                    periodo=row["periodo"],
                    sucursal_id=row["sucursal_id"],
                    receta_id=row["receta_id"],
                    defaults={
                        "venta_proyectada": row["venta_proyectada"],
                        "unidades_proyectadas": row["unidades_proyectadas"],
                        "unidades_proyectadas_ajustadas": row["unidades_proyectadas_ajustadas"],
                        "factor_merma": row["factor_merma"],
                        "factor_devolucion": row["factor_devolucion"],
                        "stock_actual": row["stock_actual"],
                        "metodo": row["metodo"],
                        "confianza": row["confianza"],
                        "dias_historial": row["dias_historial"],
                        "metadata": {
                            "source": "ProyeccionProduccionService",
                            "generated_at": now.isoformat(),
                            "factor_fuente": row["factor_fuente"],
                            "factor_capped": row["factor_capped"],
                        },
                        "generado_en": now,
                    },
                )
                summary.created += int(created)
                summary.updated += int(not created)

    def _weighted_sales_average(self, history: dict[date, Decimal], target_date: date) -> Decimal:
        last_business_days: list[Decimal] = []
        cursor = target_date - timedelta(days=1)
        while len(last_business_days) < 7 and cursor >= target_date - timedelta(days=28):
            if cursor.weekday() != 6:
                last_business_days.append(history.get(cursor, ZERO))
            cursor -= timedelta(days=1)
        same_weekday = [qty for day, qty in history.items() if day.weekday() == target_date.weekday()]
        other_days = [qty for day, qty in history.items() if day.weekday() != 6 and day.weekday() != target_date.weekday()]
        if same_weekday and other_days:
            return _avg(same_weekday) * Decimal("0.40") + _avg(other_days) * Decimal("0.60")
        if last_business_days:
            return _avg(last_business_days)
        return _avg(list(history.values()))

    def _waste_factor_by_key(self, target_date: date, keys: set[tuple[int, int]]) -> dict[tuple[int, int], Decimal]:
        if not keys:
            return {}
        month_start = (target_date - timedelta(days=28)).replace(day=1)
        rows = (
            MermaMensualSucursal.objects.filter(
                periodo__gte=month_start,
                periodo__lte=target_date.replace(day=1),
                sucursal_id__in={branch_id for branch_id, _ in keys},
                receta_id__in={recipe_id for _, recipe_id in keys},
            )
            .values("sucursal_id", "receta_id")
            .annotate(merma=Sum("unidades_merma"), vendido=Sum("unidades_vendidas"))
        )
        factors = {}
        for row in rows:
            sold = _to_decimal(row.get("vendido"))
            if sold <= ZERO:
                continue
            factors[(int(row["sucursal_id"]), int(row["receta_id"]))] = _to_decimal(row.get("merma")) / sold
        return factors

    def _returned_factor_by_key(
        self,
        target_date: date,
        keys: set[tuple[int, int]],
        sales_by_key: dict[tuple[int, int], dict[date, Decimal]],
    ) -> dict[tuple[int, int], Decimal]:
        if not keys:
            return {}
        month_start = (target_date - timedelta(days=28)).replace(day=1)
        rows = (
            DevolucionSucursalMatriz.objects.filter(
                periodo__gte=month_start,
                periodo__lte=target_date.replace(day=1),
                sucursal_origen_id__in={branch_id for branch_id, _ in keys},
                receta_id__in={recipe_id for _, recipe_id in keys},
            )
            .values("sucursal_origen_id", "receta_id")
            .annotate(unidades=Sum("unidades"))
        )
        factors = {}
        for row in rows:
            key = (int(row["sucursal_origen_id"]), int(row["receta_id"]))
            sold = sum(sales_by_key.get(key, {}).values(), ZERO)
            if sold > ZERO:
                factors[key] = _to_decimal(row.get("unidades")) / sold
        return factors

    def _network_waste_factor_by_recipe(self, target_date: date, recipe_ids: set[int]) -> dict[int, Decimal]:
        if not recipe_ids:
            return {}
        month_start = (target_date - timedelta(days=28)).replace(day=1)
        rows = (
            MermaMensualSucursal.objects.filter(
                periodo__gte=month_start,
                periodo__lte=target_date.replace(day=1),
                receta_id__in=recipe_ids,
                unidades_vendidas__gt=0,
            )
            .values("receta_id")
            .annotate(merma=Sum("unidades_merma"), vendido=Sum("unidades_vendidas"))
        )
        factors = {}
        for row in rows:
            sold = _to_decimal(row.get("vendido"))
            if sold > ZERO:
                factors[int(row["receta_id"])] = _to_decimal(row.get("merma")) / sold
        return factors

    def _network_returned_factor_by_recipe(self, target_date: date, recipe_ids: set[int]) -> dict[int, Decimal]:
        if not recipe_ids:
            return {}
        month_start = (target_date - timedelta(days=28)).replace(day=1)
        returns = {
            int(row["receta_id"]): _to_decimal(row.get("unidades"))
            for row in DevolucionSucursalMatriz.objects.filter(
                periodo__gte=month_start,
                periodo__lte=target_date.replace(day=1),
                receta_id__in=recipe_ids,
            )
            .values("receta_id")
            .annotate(unidades=Sum("unidades"))
        }
        sales = {
            int(row["receta_id"]): _to_decimal(row.get("cantidad"))
            for row in VentaHistorica.objects.filter(
                fecha__gte=target_date - timedelta(days=28),
                fecha__lt=target_date,
                fuente=POINT_BRIDGE_SALES_SOURCE,
                receta_id__in=recipe_ids,
            )
            .values("receta_id")
            .annotate(cantidad=Sum("cantidad"))
        }
        factors = {}
        for recipe_id, units in returns.items():
            sold = sales.get(recipe_id, ZERO)
            if sold > ZERO:
                factors[recipe_id] = units / sold
        return factors

    def _resolve_factors(
        self,
        *,
        key: tuple[int, int],
        history_days: int,
        waste_factor_map: dict[tuple[int, int], Decimal],
        returned_factor_map: dict[tuple[int, int], Decimal],
        network_waste_factor_map: dict[int, Decimal],
        network_return_factor_map: dict[int, Decimal],
    ) -> tuple[Decimal, Decimal, str]:
        _branch_id, recipe_id = key
        if history_days >= NETWORK_FACTOR_HISTORY_THRESHOLD:
            return waste_factor_map.get(key, ZERO), returned_factor_map.get(key, ZERO), "HISTORICO_PROPIO"
        network_waste = network_waste_factor_map.get(recipe_id)
        network_return = network_return_factor_map.get(recipe_id)
        if network_waste is not None or network_return is not None:
            return network_waste or ZERO, network_return or ZERO, "PROMEDIO_RED"
        return DEFAULT_WASTE_FACTOR, DEFAULT_RETURN_FACTOR, "DEFAULT"

    def _latest_stock_by_branch_recipe(self, keys: set[tuple[int, int]]) -> dict[tuple[int, int], Decimal]:
        if not keys:
            return {}
        branch_ids = sorted({branch_id for branch_id, _recipe_id in keys})
        recipe_ids = sorted({recipe_id for _branch_id, recipe_id in keys})
        product_recipe_map = self._point_products_for_recipes(recipe_ids=recipe_ids)
        if not product_recipe_map:
            return {}
        product_ids = sorted(product_recipe_map)
        latest_captured_at = (
            PointInventorySnapshot.objects.filter(branch__erp_branch_id__in=branch_ids, product_id__in=product_ids)
            .order_by("-captured_at")
            .values_list("captured_at", flat=True)
            .first()
        )
        if latest_captured_at is None:
            return {}
        snapshots = list(
            PointInventorySnapshot.objects.select_related("branch")
            .filter(
                branch__erp_branch_id__in=branch_ids,
                product_id__in=product_ids,
                captured_at=latest_captured_at,
            )
            .only("branch__erp_branch_id", "product_id", "stock")
            .order_by("branch_id", "product_id")
        )
        stock_map: dict[tuple[int, int], Decimal] = defaultdict(lambda: ZERO)
        for snapshot in snapshots:
            branch_id = getattr(snapshot.branch, "erp_branch_id", None)
            recipe_id = product_recipe_map.get(snapshot.product_id)
            if not branch_id or not recipe_id:
                continue
            key = (int(branch_id), int(recipe_id))
            if key in keys:
                stock_map[key] += _to_decimal(snapshot.stock)
        return dict(stock_map)

    def _point_products_for_recipes(self, *, recipe_ids: list[int]) -> dict[int, int]:
        rows = (
            FactVentaDiaria.objects.filter(receta_id__in=recipe_ids, point_product_id__isnull=False)
            .order_by("point_product_id", "-fecha")
            .distinct("point_product_id")
            .values("point_product_id", "receta_id")
        )
        product_recipe_map: dict[int, int] = {}
        for row in rows:
            product_recipe_map[int(row["point_product_id"])] = int(row["receta_id"])
        return product_recipe_map

    def _confidence(self, history_days: int) -> str:
        if history_days >= 14:
            return ProyeccionProduccion.CONFIANZA_ALTA
        if history_days >= 7:
            return ProyeccionProduccion.CONFIANZA_MEDIA
        return ProyeccionProduccion.CONFIANZA_BAJA


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value is not None else default))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _avg(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    return sum(values, ZERO) / Decimal(len(values))


def _cap_factor(value: Decimal, cap: Decimal) -> tuple[Decimal, bool]:
    if value > cap:
        return cap, True
    return value, False


def _json_safe(row: dict[str, object]) -> dict[str, object]:
    safe = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            safe[key] = str(value)
        elif hasattr(value, "isoformat"):
            safe[key] = value.isoformat()
        else:
            safe[key] = value
    return safe
