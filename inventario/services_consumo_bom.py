from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from maestros.models import Insumo
from pos_bridge.models import PointProductionLine
from recetas.models import LineaReceta, Receta, VentaHistorica

from .models import ExistenciaInsumo, MovimientoInventario
from .services_auditoria_insumos import DECIMAL_ZERO, ConsumoInsumoAuditService, parse_period, period_bounds


def _q3(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def _money(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass
class ConsumoBomItem:
    insumo: Insumo
    cantidad: Decimal
    costo_unitario: Decimal
    costo_total: Decimal
    fecha: date
    source_hash: str
    referencia: str
    fuente: str
    origen: str
    producciones: set[str] = field(default_factory=set)
    recetas: set[str] = field(default_factory=set)


@dataclass
class ConsumoBomSummary:
    fecha_inicio: date
    fecha_fin: date
    dry_run: bool
    producciones_procesadas: int = 0
    lineas_produccion_procesadas: int = 0
    movimientos_generados: int = 0
    movimientos_creados: int = 0
    movimientos_actualizados: int = 0
    movimientos_sin_cambio: int = 0
    insumos_actualizados: int = 0
    omitidos_sin_receta: int = 0
    omitidos_bom_incompleto: int = 0
    omitidos_unidad_incompatible: int = 0
    omitidos_sin_insumo_reventa: int = 0
    items: list[ConsumoBomItem] = field(default_factory=list)

    @property
    def top_consumos(self) -> list[ConsumoBomItem]:
        return sorted(self.items, key=lambda item: item.costo_total, reverse=True)[:5]


class ConsumoInsumoAutoService:
    """Genera movimientos CONSUMO calculados desde BOM y producción real Point."""

    def __init__(self):
        self.audit = ConsumoInsumoAuditService()

    def generar_consumos_produccion(
        self,
        fecha_inicio: date,
        fecha_fin: date,
        *,
        dry_run: bool = False,
    ) -> ConsumoBomSummary:
        summary = ConsumoBomSummary(fecha_inicio=fecha_inicio, fecha_fin=fecha_fin, dry_run=dry_run)
        production_items = self._build_production_consumption(fecha_inicio, fecha_fin, summary)
        resale_items = self._build_resale_consumption(fecha_inicio, fecha_fin, summary)
        items = list(production_items.values()) + list(resale_items.values())
        summary.items = items
        summary.movimientos_generados = len(items)
        summary.insumos_actualizados = len({item.insumo.id for item in items})

        if dry_run:
            return summary

        with transaction.atomic():
            for item in items:
                created, updated = self._upsert_consumption(item)
                if created:
                    summary.movimientos_creados += 1
                elif updated:
                    summary.movimientos_actualizados += 1
                else:
                    summary.movimientos_sin_cambio += 1
        return summary

    def generar_consumos_periodo(self, period: str, *, dry_run: bool = False) -> ConsumoBomSummary:
        start, end = period_bounds(parse_period(period))
        return self.generar_consumos_produccion(start, end, dry_run=dry_run)

    def _build_production_consumption(
        self,
        fecha_inicio: date,
        fecha_fin: date,
        summary: ConsumoBomSummary,
    ) -> dict[str, ConsumoBomItem]:
        lines = list(
            PointProductionLine.objects.filter(
                production_date__range=(fecha_inicio, fecha_fin),
                receta__isnull=False,
                produced_quantity__gt=0,
            )
            .exclude(receta__modo_costeo=Receta.MODO_COSTEO_SERVICIO)
            .select_related("receta")
            .order_by("production_external_id", "detail_external_id")
        )
        summary.lineas_produccion_procesadas = len(lines)
        summary.producciones_procesadas = len({line.production_external_id for line in lines})
        receta_ids = {line.receta_id for line in lines if line.receta_id}
        bom_by_receta = self._bom_by_receta(receta_ids)
        costs = self._costos_for_bom(bom_by_receta, fecha_fin)

        items: dict[str, ConsumoBomItem] = {}
        for line in lines:
            if not line.receta_id:
                summary.omitidos_sin_receta += 1
                continue
            bom = bom_by_receta.get(line.receta_id) or []
            if not bom:
                summary.omitidos_bom_incompleto += 1
                continue
            produced = Decimal(str(line.produced_quantity or 0))
            for bom_line in bom:
                qty = self._converted_bom_qty(bom_line, produced)
                if qty is None:
                    summary.omitidos_unidad_incompatible += 1
                    continue
                if qty <= 0:
                    continue
                key = self._source_hash("BOM", line.production_external_id, bom_line.insumo_id)
                item = items.get(key)
                cost_info = costs.get(bom_line.insumo_id)
                unit_cost = cost_info.costo if cost_info else DECIMAL_ZERO
                if item is None:
                    item = ConsumoBomItem(
                        insumo=bom_line.insumo,
                        cantidad=DECIMAL_ZERO,
                        costo_unitario=unit_cost,
                        costo_total=DECIMAL_ZERO,
                        fecha=line.production_date,
                        source_hash=key,
                        referencia=f"PROD-{line.production_external_id}",
                        fuente=(cost_info.fuente if cost_info else "SIN_COSTO"),
                        origen="BOM_PRODUCCION",
                    )
                    items[key] = item
                item.cantidad += qty
                item.costo_total = _money(item.cantidad * item.costo_unitario)
                item.producciones.add(str(line.production_external_id))
                item.recetas.add(line.receta.nombre)
        for item in items.values():
            item.cantidad = _q3(item.cantidad)
            item.costo_total = _money(item.cantidad * item.costo_unitario)
        return items

    def _build_resale_consumption(
        self,
        fecha_inicio: date,
        fecha_fin: date,
        summary: ConsumoBomSummary,
    ) -> dict[str, ConsumoBomItem]:
        sales = (
            VentaHistorica.objects.filter(
                fecha__range=(fecha_inicio, fecha_fin),
                receta__modo_costeo=Receta.MODO_COSTEO_REVENTA,
            )
            .values("fecha", "receta_id", "receta__nombre", "receta__codigo_point")
            .annotate(total=Sum("cantidad"))
        )
        items: dict[str, ConsumoBomItem] = {}
        for row in sales:
            insumo = self._resolve_resale_insumo(row["receta__codigo_point"], row["receta__nombre"])
            if insumo is None:
                summary.omitidos_sin_insumo_reventa += 1
                continue
            qty = _q3(Decimal(str(row["total"] or 0)))
            if qty <= 0:
                continue
            source_hash = self._source_hash("VENTA", row["fecha"].isoformat(), row["receta_id"], insumo.id)
            cost_info = self.audit._costos_unitarios({insumo.id}, row["fecha"]).get(insumo.id)
            unit_cost = cost_info.costo if cost_info else DECIMAL_ZERO
            items[source_hash] = ConsumoBomItem(
                insumo=insumo,
                cantidad=qty,
                costo_unitario=unit_cost,
                costo_total=_money(qty * unit_cost),
                fecha=row["fecha"],
                source_hash=source_hash,
                referencia=f"VENTA-{row['fecha'].isoformat()}",
                fuente=(cost_info.fuente if cost_info else "SIN_COSTO"),
                origen="CONSUMO_VENTA",
                recetas={row["receta__nombre"]},
            )
        return items

    def _bom_by_receta(self, receta_ids: set[int]) -> dict[int, list[LineaReceta]]:
        result: dict[int, list[LineaReceta]] = {}
        if not receta_ids:
            return result
        for line in (
            LineaReceta.objects.filter(
                receta_id__in=receta_ids,
                insumo__isnull=False,
                cantidad__isnull=False,
            )
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .select_related("insumo", "insumo__unidad_base", "unidad")
        ):
            result.setdefault(line.receta_id, []).append(line)
        return result

    def _costos_for_bom(self, bom_by_receta: dict[int, list[LineaReceta]], fecha_fin: date):
        insumo_ids = {line.insumo_id for lines in bom_by_receta.values() for line in lines}
        return self.audit._costos_unitarios(insumo_ids, fecha_fin)

    def _converted_bom_qty(self, line: LineaReceta, produced: Decimal) -> Decimal | None:
        source_unit = self.audit._unit_code(line.unidad) or self.audit._normalize_unit(line.unidad_texto)
        target_unit = self.audit._unit_code(line.insumo.unidad_base) or source_unit
        return self.audit._convert_quantity(Decimal(str(line.cantidad or 0)) * produced, source_unit, target_unit)

    def _resolve_resale_insumo(self, codigo_point: str, nombre: str) -> Insumo | None:
        code = (codigo_point or "").strip()
        if code:
            match = Insumo.objects.filter(codigo_point__iexact=code).first()
            if match:
                return match
        name = (nombre or "").strip()
        if name:
            match = Insumo.objects.filter(nombre__iexact=name).first()
            if match:
                return match
            match = Insumo.objects.filter(nombre_point__iexact=name).first()
            if match:
                return match
        return None

    def _upsert_consumption(self, item: ConsumoBomItem) -> tuple[bool, bool]:
        defaults = {
            "fecha": timezone.make_aware(datetime.combine(item.fecha, time.min)),
            "tipo": MovimientoInventario.TIPO_CONSUMO,
            "insumo": item.insumo,
            "cantidad": item.cantidad,
            "referencia": item.referencia[:120],
        }
        existing = MovimientoInventario.objects.filter(source_hash=item.source_hash).first()
        if existing is None:
            MovimientoInventario.objects.create(source_hash=item.source_hash, **defaults)
            self._apply_stock_delta(item.insumo, -item.cantidad, created_automatically=True)
            return True, False

        old_qty = Decimal(str(existing.cantidad or 0))
        new_qty = Decimal(str(item.cantidad or 0))
        changed = (
            existing.tipo != MovimientoInventario.TIPO_CONSUMO
            or existing.insumo_id != item.insumo.id
            or old_qty != new_qty
            or existing.referencia != defaults["referencia"]
        )
        if not changed:
            return False, False
        if existing.insumo_id == item.insumo_id:
            self._apply_stock_delta(item.insumo, -(new_qty - old_qty))
        else:
            self._apply_stock_delta(existing.insumo, old_qty)
            self._apply_stock_delta(item.insumo, -new_qty, created_automatically=True)
        for field, value in defaults.items():
            setattr(existing, field, value)
        existing.save(update_fields=["fecha", "tipo", "insumo", "cantidad", "referencia"])
        return False, True

    def _apply_stock_delta(self, insumo: Insumo, delta: Decimal, *, created_automatically: bool = False) -> None:
        existencia, created = ExistenciaInsumo.objects.get_or_create(insumo=insumo)
        existencia.stock_actual = Decimal(str(existencia.stock_actual or 0)) + Decimal(str(delta or 0))
        existencia.actualizado_en = timezone.now()
        if created or created_automatically:
            trace = dict(existencia.trazabilidad_stock or {})
            trace["creado_automaticamente"] = True
            trace["fuente"] = "CONSUMO_BOM"
            existencia.trazabilidad_stock = trace
            existencia.save(update_fields=["stock_actual", "actualizado_en", "trazabilidad_stock"])
            return
        existencia.save(update_fields=["stock_actual", "actualizado_en"])

    def _source_hash(self, *parts: object) -> str:
        raw = "|".join(str(part) for part in parts)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def previous_day_bounds() -> tuple[date, date]:
    target = timezone.localdate() - timedelta(days=1)
    return target, target
