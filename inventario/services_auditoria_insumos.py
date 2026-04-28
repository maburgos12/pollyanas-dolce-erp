from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
from calendar import monthrange

from django.db.models import Count, Sum
from django.utils import timezone

from maestros.models import CostoInsumo, Insumo
from pos_bridge.models import PointProductionLine
from recetas.models import LineaReceta, Receta

from .models import ConsumoInsumoMensual, ExistenciaInsumo, MovimientoInventario


DECIMAL_ZERO = Decimal("0")
ANOMALO_COST_CAP = Decimal("50000")
CONVERSION = {
    ("g", "kg"): Decimal("0.001"),
    ("kg", "g"): Decimal("1000"),
    ("mg", "kg"): Decimal("0.000001"),
    ("oz", "kg"): Decimal("0.028349"),
    ("lb", "kg"): Decimal("0.453592"),
    ("ml", "lt"): Decimal("0.001"),
    ("ml", "l"): Decimal("0.001"),
    ("lt", "ml"): Decimal("1000"),
    ("l", "ml"): Decimal("1000"),
    ("kg", "kg"): Decimal("1"),
    ("g", "g"): Decimal("1"),
    ("lt", "lt"): Decimal("1"),
    ("l", "lt"): Decimal("1"),
    ("lt", "l"): Decimal("1"),
    ("ml", "ml"): Decimal("1"),
    ("pza", "pza"): Decimal("1"),
    ("pz", "pza"): Decimal("1"),
    ("pza", "pz"): Decimal("1"),
    ("pz", "pz"): Decimal("1"),
}


@dataclass
class CostoUnitarioAudit:
    costo: Decimal
    unidad: str
    fuente: str
    unidad_original: str = ""
    razon: str = ""
    anomalous: bool = False


@dataclass
class ConsumoAuditSummary:
    periodo: date
    rows: list[ConsumoInsumoMensual]
    dry_run: bool

    @property
    def total(self) -> int:
        return len(self.rows)

    @property
    def by_alerta(self) -> dict[str, int]:
        result = {
            ConsumoInsumoMensual.ALERTA_OK: 0,
            ConsumoInsumoMensual.ALERTA_MERMA: 0,
            ConsumoInsumoMensual.ALERTA_FALTANTE: 0,
            ConsumoInsumoMensual.ALERTA_SIN_DATOS: 0,
        }
        for row in self.rows:
            result[row.alerta] = result.get(row.alerta, 0) + 1
        return result

    @property
    def top_diferencias(self) -> list[ConsumoInsumoMensual]:
        return sorted(self.rows, key=lambda row: abs(row.diferencia_costo or DECIMAL_ZERO), reverse=True)[:5]

    @property
    def costo_fuentes(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for row in self.rows:
            source = str((row.metadata or {}).get("costo_fuente") or "SIN_COSTO")
            result[source] = result.get(source, 0) + 1
        return result

    @property
    def razones(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for row in self.rows:
            reason = str((row.metadata or {}).get("razon") or "")
            if reason:
                result[reason] = result.get(reason, 0) + 1
        return result


def parse_period(value: str) -> date:
    try:
        year_raw, month_raw = value.split("-", 1)
        return date(int(year_raw), int(month_raw), 1)
    except Exception as exc:
        raise ValueError("El periodo debe tener formato YYYY-MM.") from exc


def period_bounds(periodo: date) -> tuple[date, date]:
    return periodo, date(periodo.year, periodo.month, monthrange(periodo.year, periodo.month)[1])


def _q4(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _money(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _pct(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class ConsumoInsumoAuditService:
    """Calcula consumo teórico vs consumo real disponible sin mutar fuentes operativas."""

    def calcular_periodo(self, periodo: date, *, dry_run: bool = False) -> ConsumoAuditSummary:
        period_start, period_end = period_bounds(periodo)
        start_dt = timezone.make_aware(datetime.combine(period_start, time.min))
        end_dt = timezone.make_aware(datetime.combine(period_end, time.max))
        produccion_por_receta = self._produccion_por_receta(period_start, period_end)
        teorico_por_insumo = self._consumo_teorico_por_insumo(produccion_por_receta)

        movimiento_insumo_ids = set(
            MovimientoInventario.objects.filter(fecha__range=(start_dt, end_dt)).values_list(
                "insumo_id", flat=True
            )
        )
        existencia_insumo_ids = set(ExistenciaInsumo.objects.values_list("insumo_id", flat=True))
        candidate_ids = set(teorico_por_insumo) | movimiento_insumo_ids | existencia_insumo_ids

        costos = self._costos_unitarios(candidate_ids, period_end)
        unidades = self._unidad_por_insumo(candidate_ids)
        real_by_insumo = self._consumo_real_bulk(period_start, period_end, candidate_ids)
        rows: list[ConsumoInsumoMensual] = []

        for insumo in Insumo.objects.filter(id__in=candidate_ids).select_related("unidad_base").order_by("nombre"):
            teorico_data = teorico_por_insumo.get(insumo.id, self._empty_teorico_data())
            teorico = _q4(Decimal(str(teorico_data["cantidad"] or 0)))
            costo_info = costos.get(insumo.id) or CostoUnitarioAudit(DECIMAL_ZERO, unidades.get(insumo.id, ""), "SIN_COSTO")
            snapshot_unit_cost = (
                Decimal(str(teorico_data["costo_snapshot"] or 0)) / teorico
                if teorico > 0 and Decimal(str(teorico_data["costo_snapshot"] or 0)) > 0
                else DECIMAL_ZERO
            )
            if costo_info.fuente == "SIN_COSTO" and snapshot_unit_cost > 0:
                costo_info = CostoUnitarioAudit(snapshot_unit_cost, unidades.get(insumo.id, ""), "BOM_SNAPSHOT")
            costo_unitario = costo_info.costo
            real_data = real_by_insumo.get(insumo.id) or self._empty_real_data()
            consumo_real = _q4(real_data["consumo_real"])
            diferencia = _q4(consumo_real - teorico)
            diferencia_pct = _pct((diferencia / teorico) * Decimal("100")) if teorico > 0 else Decimal("0.00")
            alerta = self.clasificar_alerta(teorico, consumo_real, diferencia_pct, real_data)
            if costo_info.anomalous:
                alerta = ConsumoInsumoMensual.ALERTA_SIN_DATOS
            if costo_info.razon == "UNIDAD_INCOMPATIBLE":
                alerta = ConsumoInsumoMensual.ALERTA_SIN_DATOS
            if teorico_data["unidad_incompatible_count"]:
                alerta = ConsumoInsumoMensual.ALERTA_SIN_DATOS
            metadata = {
                "metodo_consumo": "DIFERENCIAL",
                "produccion_recetas": real_data.get("produccion_recetas", 0),
                "movimientos_periodo": real_data["movimientos_periodo"],
                "movimientos_por_tipo": real_data["movimientos_por_tipo"],
                "stock_reconstruido": real_data["stock_reconstruido"],
                "real_data_limited": real_data["real_data_limited"],
                "costo_unitario_usado": str(costo_unitario),
                "costo_fuente": costo_info.fuente,
                "costo_unidad": costo_info.unidad,
                "costo_unidad_original": costo_info.unidad_original,
                "lineas_bom": teorico_data["lineas"],
                "unidad_incompatible_count": teorico_data["unidad_incompatible_count"],
                "costo_snapshot_total": str(teorico_data["costo_snapshot"]),
            }
            if costo_info.razon:
                metadata["razon"] = costo_info.razon
            elif teorico_data["unidad_incompatible_count"]:
                metadata["razon"] = "UNIDAD_INCOMPATIBLE"
            row = ConsumoInsumoMensual(
                periodo=periodo,
                insumo=insumo,
                unidad=unidades.get(insumo.id) or (insumo.unidad_base.codigo if insumo.unidad_base_id else ""),
                consumo_teorico=teorico,
                costo_teorico=_money(teorico * costo_unitario),
                entradas_periodo=_q4(real_data["entradas_periodo"]),
                stock_inicial=_q4(real_data["stock_inicial"]),
                stock_final=_q4(real_data["stock_final"]),
                consumo_real=consumo_real,
                costo_real=_money(consumo_real * costo_unitario),
                diferencia_unidades=diferencia,
                diferencia_pct=diferencia_pct,
                diferencia_costo=_money(diferencia * costo_unitario),
                alerta=alerta,
                metadata=metadata,
            )
            rows.append(row)

        if not dry_run:
            for row in rows:
                ConsumoInsumoMensual.objects.update_or_create(
                    periodo=row.periodo,
                    insumo=row.insumo,
                    defaults={
                        "unidad": row.unidad,
                        "consumo_teorico": row.consumo_teorico,
                        "costo_teorico": row.costo_teorico,
                        "entradas_periodo": row.entradas_periodo,
                        "stock_inicial": row.stock_inicial,
                        "stock_final": row.stock_final,
                        "consumo_real": row.consumo_real,
                        "costo_real": row.costo_real,
                        "diferencia_unidades": row.diferencia_unidades,
                        "diferencia_pct": row.diferencia_pct,
                        "diferencia_costo": row.diferencia_costo,
                        "alerta": row.alerta,
                        "metadata": row.metadata,
                    },
                )

        return ConsumoAuditSummary(periodo=periodo, rows=rows, dry_run=dry_run)

    def _produccion_por_receta(self, period_start: date, period_end: date) -> dict[int, Decimal]:
        rows = (
            PointProductionLine.objects.filter(
                production_date__range=(period_start, period_end),
                receta__isnull=False,
                receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
            )
            .exclude(receta__modo_costeo=Receta.MODO_COSTEO_SERVICIO)
            .values("receta_id")
            .annotate(total=Sum("produced_quantity"))
        )
        return {int(row["receta_id"]): Decimal(str(row["total"] or 0)) for row in rows}

    def _consumo_teorico_por_insumo(self, produccion_por_receta: dict[int, Decimal]) -> dict[int, dict[str, object]]:
        if not produccion_por_receta:
            return {}
        totals: dict[int, dict[str, object]] = {}
        lineas = (
            LineaReceta.objects.filter(
                receta_id__in=produccion_por_receta.keys(),
                insumo__isnull=False,
                cantidad__isnull=False,
            )
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .select_related("insumo", "insumo__unidad_base", "unidad")
        )
        for linea in lineas:
            cantidad = Decimal(str(linea.cantidad or 0))
            if cantidad <= 0:
                continue
            producido = produccion_por_receta.get(linea.receta_id, DECIMAL_ZERO)
            if producido <= 0:
                continue
            target_unit = self._unit_code(linea.insumo.unidad_base) or self._unit_code(linea.unidad)
            source_unit = self._unit_code(linea.unidad) or self._normalize_unit(linea.unidad_texto) or target_unit
            entry = totals.setdefault(linea.insumo_id, self._empty_teorico_data())
            entry["lineas"] = int(entry["lineas"]) + 1
            converted_qty = self._convert_quantity(cantidad * producido, source_unit, target_unit)
            if converted_qty is None:
                entry["unidad_incompatible_count"] = int(entry["unidad_incompatible_count"]) + 1
                continue
            entry["cantidad"] = Decimal(str(entry["cantidad"])) + converted_qty
            snapshot_cost = Decimal(str(linea.costo_unitario_snapshot or 0))
            if snapshot_cost > 0:
                entry["costo_snapshot"] = Decimal(str(entry["costo_snapshot"])) + ((cantidad * producido) * snapshot_cost)
        return totals

    def _empty_teorico_data(self) -> dict[str, object]:
        return {
            "cantidad": DECIMAL_ZERO,
            "costo_snapshot": DECIMAL_ZERO,
            "lineas": 0,
            "unidad_incompatible_count": 0,
        }

    def _unidad_por_insumo(self, insumo_ids: set[int]) -> dict[int, str]:
        units: dict[int, str] = {}
        for insumo in Insumo.objects.filter(id__in=insumo_ids).select_related("unidad_base"):
            units.setdefault(insumo.id, insumo.unidad_base.codigo if insumo.unidad_base_id else "")
        return units

    def _unit_code(self, unit) -> str:
        return self._normalize_unit(getattr(unit, "codigo", "") if unit else "")

    def _normalize_unit(self, value: str) -> str:
        raw = (value or "").strip().lower()
        aliases = {
            "gr": "g",
            "gramo": "g",
            "gramos": "g",
            "kilogramo": "kg",
            "kilogramos": "kg",
            "litro": "lt",
            "litros": "lt",
            "lts": "lt",
            "l": "lt",
            "mililitro": "ml",
            "mililitros": "ml",
            "pieza": "pza",
            "piezas": "pza",
            "unidad": "pza",
            "unidades": "pza",
        }
        return aliases.get(raw, raw)

    def _convert_quantity(self, quantity: Decimal, source_unit: str, target_unit: str) -> Decimal | None:
        source = self._normalize_unit(source_unit)
        target = self._normalize_unit(target_unit)
        if not source or not target:
            return None
        factor = CONVERSION.get((source, target))
        if factor is None:
            return None
        return Decimal(str(quantity or 0)) * factor

    def _consumo_real_bulk(self, period_start: date, period_end: date, insumo_ids: set[int]) -> dict[int, dict[str, object]]:
        if not insumo_ids:
            return {}
        start_dt = timezone.make_aware(datetime.combine(period_start, time.min))
        end_dt = timezone.make_aware(datetime.combine(period_end, time.max))
        now = timezone.now()
        stock_by_insumo = {
            row["insumo_id"]: Decimal(str(row["stock_actual"] or 0))
            for row in ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids).values("insumo_id", "stock_actual")
        }
        period_rows = list(
            MovimientoInventario.objects.filter(insumo_id__in=insumo_ids, fecha__range=(start_dt, end_dt))
            .values("insumo_id", "tipo")
            .annotate(total=Sum("cantidad"), n=Count("id"))
        )
        after_start_rows = list(
            MovimientoInventario.objects.filter(insumo_id__in=insumo_ids, fecha__gt=start_dt, fecha__lte=now)
            .values("insumo_id", "tipo")
            .annotate(total=Sum("cantidad"))
        )
        after_end_rows = list(
            MovimientoInventario.objects.filter(insumo_id__in=insumo_ids, fecha__gt=end_dt, fecha__lte=now)
            .values("insumo_id", "tipo")
            .annotate(total=Sum("cantidad"))
        )

        period_by_insumo: dict[int, dict[str, object]] = {}
        for row in period_rows:
            insumo_id = int(row["insumo_id"])
            item = period_by_insumo.setdefault(
                insumo_id,
                {"entradas": DECIMAL_ZERO, "movimientos": 0, "por_tipo": {}},
            )
            tipo = row["tipo"]
            qty = Decimal(str(row["total"] or 0))
            item["movimientos"] = int(item["movimientos"]) + int(row["n"] or 0)
            item["por_tipo"][tipo] = int(row["n"] or 0)
            if tipo == MovimientoInventario.TIPO_ENTRADA:
                item["entradas"] = Decimal(str(item["entradas"])) + qty

        after_start = self._signed_rows_by_insumo(after_start_rows)
        after_end = self._signed_rows_by_insumo(after_end_rows)

        result: dict[int, dict[str, object]] = {}
        for insumo_id in insumo_ids:
            stock_actual = stock_by_insumo.get(insumo_id, DECIMAL_ZERO)
            period_data = period_by_insumo.get(insumo_id, {"entradas": DECIMAL_ZERO, "movimientos": 0, "por_tipo": {}})
            stock_inicial = stock_actual - after_start.get(insumo_id, DECIMAL_ZERO)
            stock_final = stock_actual - after_end.get(insumo_id, DECIMAL_ZERO)
            entradas = Decimal(str(period_data["entradas"] or 0))
            movimientos_por_tipo = dict(period_data["por_tipo"])
            consumo_real = stock_inicial + entradas - stock_final
            real_data_limited = not any(
                movimientos_por_tipo.get(tipo, 0)
                for tipo in [
                    MovimientoInventario.TIPO_SALIDA,
                    MovimientoInventario.TIPO_CONSUMO,
                    MovimientoInventario.TIPO_AJUSTE,
                ]
            )
            result[insumo_id] = {
                "stock_inicial": stock_inicial,
                "stock_final": stock_final,
                "entradas_periodo": entradas,
                "consumo_real": consumo_real,
                "movimientos_periodo": int(period_data["movimientos"] or 0),
                "movimientos_por_tipo": movimientos_por_tipo,
                "stock_reconstruido": True,
                "real_data_limited": real_data_limited,
            }
        return result

    def _empty_real_data(self) -> dict[str, object]:
        return {
            "stock_inicial": DECIMAL_ZERO,
            "stock_final": DECIMAL_ZERO,
            "entradas_periodo": DECIMAL_ZERO,
            "consumo_real": DECIMAL_ZERO,
            "movimientos_periodo": 0,
            "movimientos_por_tipo": {},
            "stock_reconstruido": True,
            "real_data_limited": True,
        }

    def _signed_rows_by_insumo(self, rows: list[dict[str, object]]) -> dict[int, Decimal]:
        result: dict[int, Decimal] = {}
        for row in rows:
            insumo_id = int(row["insumo_id"])
            qty = Decimal(str(row["total"] or 0))
            if row["tipo"] == MovimientoInventario.TIPO_ENTRADA:
                delta = qty
            elif row["tipo"] in {MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO}:
                delta = -qty
            elif row["tipo"] == MovimientoInventario.TIPO_AJUSTE:
                delta = qty
            else:
                delta = DECIMAL_ZERO
            result[insumo_id] = result.get(insumo_id, DECIMAL_ZERO) + delta
        return result

    def _costos_unitarios(self, insumo_ids: set[int], period_end: date) -> dict[int, CostoUnitarioAudit]:
        result: dict[int, CostoUnitarioAudit] = {
            insumo_id: CostoUnitarioAudit(DECIMAL_ZERO, "", "SIN_COSTO") for insumo_id in insumo_ids
        }
        rows = (
            CostoInsumo.objects.filter(insumo_id__in=insumo_ids, fecha__lte=period_end)
            .exclude(raw__source="RECETA_PREPARACION")
            .select_related("insumo__unidad_base")
            .order_by("insumo_id", "-fecha", "-id")
        )
        for row in rows:
            insumo_id = int(row.insumo_id)
            if result[insumo_id].fuente != "SIN_COSTO":
                continue
            result[insumo_id] = self._audit_cost_from_row(row)

        missing_ids = {insumo_id for insumo_id, costo in result.items() if costo.fuente == "SIN_COSTO"}
        if missing_ids:
            fallback_rows = (
                CostoInsumo.objects.filter(insumo_id__in=missing_ids)
                .exclude(raw__source="RECETA_PREPARACION")
                .select_related("insumo__unidad_base")
                .order_by("insumo_id", "-fecha", "-id")
            )
            for row in fallback_rows:
                insumo_id = int(row.insumo_id)
                if result[insumo_id].fuente != "SIN_COSTO":
                    continue
                result[insumo_id] = self._audit_cost_from_row(row)
        return result

    def _audit_cost_from_row(self, row: CostoInsumo) -> CostoUnitarioAudit:
        raw = row.raw or {}
        raw_source = str(raw.get("source") or "COSTO_INSUMO")
        target_unit = self._unit_code(row.insumo.unidad_base)
        original_unit = self._normalize_unit(str(raw.get("unit") or "")) or target_unit
        raw_cost = Decimal(str(row.costo_unitario or 0))
        normalized_cost = raw_cost
        reason = ""
        if original_unit and target_unit and original_unit != target_unit:
            factor = self._convert_quantity(Decimal("1"), target_unit, original_unit)
            if factor is None:
                normalized_cost = DECIMAL_ZERO
                reason = "UNIDAD_INCOMPATIBLE"
            else:
                normalized_cost = raw_cost * factor
        anomalous = self._is_anomalous_cost(raw_cost, original_unit) or self._is_anomalous_cost(
            normalized_cost,
            target_unit,
        )
        if anomalous:
            reason = "COSTO_ANOMALO"
        return CostoUnitarioAudit(
            normalized_cost,
            target_unit,
            raw_source,
            unidad_original=original_unit,
            razon=reason,
            anomalous=anomalous,
        )

    def _is_anomalous_cost(self, cost: Decimal, unit_code: str) -> bool:
        unit = self._normalize_unit(unit_code)
        return unit in {"kg", "lt", "l"} and cost > ANOMALO_COST_CAP

    def calcular_consumo_real(self, period_start: date, period_end: date, insumo: Insumo) -> dict[str, object]:
        stock_actual = Decimal(
            str(
                ExistenciaInsumo.objects.filter(insumo=insumo)
                .values_list("stock_actual", flat=True)
                .first()
                or 0
            )
        )
        start_dt = timezone.make_aware(datetime.combine(period_start, time.min))
        end_dt = timezone.make_aware(datetime.combine(period_end, time.max))
        now = timezone.now()

        after_start = self._signed_movements_after(insumo, start_dt, now)
        after_end = self._signed_movements_after(insumo, end_dt, now)
        stock_inicial = stock_actual - after_start
        stock_final = stock_actual - after_end

        period_qs = MovimientoInventario.objects.filter(insumo=insumo, fecha__range=(start_dt, end_dt))
        entradas = period_qs.filter(tipo=MovimientoInventario.TIPO_ENTRADA).aggregate(total=Sum("cantidad"))["total"] or DECIMAL_ZERO
        movimientos_por_tipo = {
            row["tipo"]: int(row["n"])
            for row in period_qs.values("tipo").annotate(n=Count("id")).order_by("tipo")
        }
        consumo_real = stock_inicial + Decimal(str(entradas or 0)) - stock_final
        real_data_limited = not any(
            movimientos_por_tipo.get(tipo, 0)
            for tipo in [
                MovimientoInventario.TIPO_SALIDA,
                MovimientoInventario.TIPO_CONSUMO,
                MovimientoInventario.TIPO_AJUSTE,
            ]
        )
        return {
            "stock_inicial": stock_inicial,
            "stock_final": stock_final,
            "entradas_periodo": Decimal(str(entradas or 0)),
            "consumo_real": consumo_real,
            "movimientos_periodo": period_qs.count(),
            "movimientos_por_tipo": movimientos_por_tipo,
            "stock_reconstruido": True,
            "real_data_limited": real_data_limited,
        }

    def _signed_movements_after(self, insumo: Insumo, since_dt: datetime, until_dt: datetime) -> Decimal:
        total = DECIMAL_ZERO
        rows = (
            MovimientoInventario.objects.filter(insumo=insumo, fecha__gt=since_dt, fecha__lte=until_dt)
            .values("tipo")
            .annotate(total=Sum("cantidad"))
        )
        for row in rows:
            qty = Decimal(str(row["total"] or 0))
            if row["tipo"] == MovimientoInventario.TIPO_ENTRADA:
                total += qty
            elif row["tipo"] in {MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO}:
                total -= qty
            elif row["tipo"] == MovimientoInventario.TIPO_AJUSTE:
                total += qty
        return total

    def clasificar_alerta(
        self,
        consumo_teorico: Decimal,
        consumo_real: Decimal,
        diferencia_pct: Decimal,
        real_data: dict[str, object],
    ) -> str:
        if consumo_teorico <= 0 and consumo_real <= 0:
            return ConsumoInsumoMensual.ALERTA_SIN_DATOS
        if real_data.get("real_data_limited") and consumo_real <= 0:
            return ConsumoInsumoMensual.ALERTA_SIN_DATOS
        if consumo_teorico <= 0:
            return ConsumoInsumoMensual.ALERTA_MERMA if consumo_real > 0 else ConsumoInsumoMensual.ALERTA_SIN_DATOS
        if diferencia_pct > Decimal("5.00"):
            return ConsumoInsumoMensual.ALERTA_MERMA
        if diferencia_pct < Decimal("-5.00"):
            return ConsumoInsumoMensual.ALERTA_FALTANTE
        return ConsumoInsumoMensual.ALERTA_OK
