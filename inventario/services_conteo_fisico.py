from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP

from django.db import transaction
from django.utils import timezone

from maestros.models import CostoInsumo
from recetas.models import InventarioCedisProducto, MovimientoProductoCedis
from recetas.utils.derived_product_presentations import get_total_cost_map

from .models import (
    ConteoFisicoMensual,
    ExistenciaInsumo,
    LineaConteoFisico,
    MovimientoInventario,
)
from .services_auditoria_insumos import ConsumoInsumoAuditService, parse_period, period_bounds


DECIMAL_ZERO = Decimal("0")


class ConteoFisicoError(ValueError):
    pass


@dataclass
class ConteoFisicoInitSummary:
    periodo: date
    dry_run: bool
    insumos: int
    productos: int


def _q3(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def _money(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class ConteoFisicoService:
    def inicializar_conteo(
        self,
        periodo: date,
        responsable,
        *,
        fecha_conteo: date | None = None,
        dry_run: bool = False,
    ) -> ConteoFisicoInitSummary:
        if ConteoFisicoMensual.objects.filter(periodo=periodo).exists():
            raise ConteoFisicoError(f"Ya existe un conteo físico para {periodo:%Y-%m}.")
        insumo_rows = list(ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base").order_by("insumo__nombre"))
        producto_rows = list(
            InventarioCedisProducto.objects.select_related("receta").filter(receta__excluir_cierre=False).order_by("receta__nombre")
        )
        if dry_run:
            return ConteoFisicoInitSummary(
                periodo=periodo,
                dry_run=True,
                insumos=len(insumo_rows),
                productos=len(producto_rows),
            )

        with transaction.atomic():
            conteo = ConteoFisicoMensual.objects.create(
                periodo=periodo,
                fecha_conteo=fecha_conteo or timezone.localdate(),
                responsable=responsable,
                estatus=ConteoFisicoMensual.ESTATUS_BORRADOR,
                metadata={"fuente": "INICIALIZACION_AUTOMATICA"},
            )
            self._crear_lineas_insumos(conteo, insumo_rows, periodo)
            self._crear_lineas_productos(conteo, producto_rows)
        return ConteoFisicoInitSummary(
            periodo=periodo,
            dry_run=False,
            insumos=len(insumo_rows),
            productos=len(producto_rows),
        )

    def enviar_a_revision(self, conteo_id: int) -> ConteoFisicoMensual:
        conteo = ConteoFisicoMensual.objects.get(pk=conteo_id)
        if conteo.estatus != ConteoFisicoMensual.ESTATUS_BORRADOR:
            raise ConteoFisicoError("Solo los conteos en BORRADOR pueden enviarse a revisión.")
        faltantes = list(
            conteo.lineas.filter(stock_contado__isnull=True).order_by("nombre").values_list("nombre", flat=True)[:20]
        )
        if faltantes:
            raise ConteoFisicoError("Faltan líneas por capturar: " + ", ".join(faltantes))
        with transaction.atomic():
            for linea in conteo.lineas.select_for_update():
                self._recalcular_linea(linea)
                linea.save(update_fields=["diferencia", "costo_diferencia"])
            conteo.estatus = ConteoFisicoMensual.ESTATUS_REVISION
            conteo.save(update_fields=["estatus"])
        return conteo

    def cerrar_conteo(self, conteo_id: int, usuario) -> ConteoFisicoMensual:
        with transaction.atomic():
            conteo = ConteoFisicoMensual.objects.select_for_update().get(pk=conteo_id)
            if conteo.estatus != ConteoFisicoMensual.ESTATUS_REVISION:
                raise ConteoFisicoError("Solo los conteos en REVISION pueden cerrarse.")
            for linea in conteo.lineas.select_for_update().select_related("insumo", "producto"):
                if linea.stock_contado is None:
                    raise ConteoFisicoError(f"La línea {linea.nombre} no tiene stock contado.")
                self._recalcular_linea(linea)
                if linea.diferencia != 0:
                    if linea.insumo_id:
                        movimiento = self._crear_ajuste_insumo(linea, conteo)
                        linea.movimiento_inventario = movimiento
                        existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=linea.insumo)
                        existencia.stock_actual = linea.stock_contado
                        existencia.actualizado_en = timezone.now()
                        existencia.save(update_fields=["stock_actual", "actualizado_en"])
                    elif linea.producto_id:
                        movimiento_producto = self._crear_ajuste_producto(linea, conteo)
                        linea.movimiento_producto_cedis = movimiento_producto
                        inventario, _ = InventarioCedisProducto.objects.get_or_create(receta=linea.producto)
                        inventario.stock_actual = linea.stock_contado
                        inventario.save(update_fields=["stock_actual", "actualizado_en"])
                linea.ajuste_aplicado = True
                linea.save(
                    update_fields=[
                        "diferencia",
                        "costo_diferencia",
                        "ajuste_aplicado",
                        "movimiento_inventario",
                        "movimiento_producto_cedis",
                    ]
                )
            conteo.estatus = ConteoFisicoMensual.ESTATUS_CERRADO
            conteo.cerrado_en = timezone.now()
            metadata = dict(conteo.metadata or {})
            metadata["cerrado_por"] = getattr(usuario, "username", str(usuario))
            conteo.metadata = metadata
            conteo.save(update_fields=["estatus", "cerrado_en", "metadata"])
        ConsumoInsumoAuditService().calcular_periodo(conteo.periodo, dry_run=False)
        return conteo

    def _crear_lineas_insumos(self, conteo: ConteoFisicoMensual, rows: list[ExistenciaInsumo], periodo: date) -> None:
        _, period_end = period_bounds(periodo)
        costs = self._latest_insumo_costs([row.insumo_id for row in rows], period_end)
        lineas = []
        for row in rows:
            unit = row.insumo.unidad_base.codigo if row.insumo.unidad_base_id else ""
            cost = costs.get(row.insumo_id, DECIMAL_ZERO)
            lineas.append(
                LineaConteoFisico(
                    conteo=conteo,
                    insumo=row.insumo,
                    nombre=row.insumo.display_name,
                    unidad=unit,
                    stock_teorico=_q3(row.stock_actual),
                    costo_unitario=_money(cost),
                )
            )
        LineaConteoFisico.objects.bulk_create(lineas, batch_size=500)

    def _crear_lineas_productos(self, conteo: ConteoFisicoMensual, rows: list[InventarioCedisProducto]) -> None:
        cost_map = get_total_cost_map([row.receta_id for row in rows])
        lineas = []
        for row in rows:
            lineas.append(
                LineaConteoFisico(
                    conteo=conteo,
                    producto=row.receta,
                    nombre=row.receta.nombre,
                    unidad="pza",
                    stock_teorico=_q3(row.stock_actual),
                    costo_unitario=_money(cost_map.get(row.receta_id, DECIMAL_ZERO)),
                )
            )
        LineaConteoFisico.objects.bulk_create(lineas, batch_size=500)

    def _latest_insumo_costs(self, insumo_ids: list[int], periodo: date) -> dict[int, Decimal]:
        costs: dict[int, Decimal] = {}
        for row in (
            CostoInsumo.objects.filter(insumo_id__in=insumo_ids, fecha__lte=periodo)
            .exclude(raw__source="RECETA_PREPARACION")
            .order_by("insumo_id", "-fecha", "-id")
        ):
            costs.setdefault(int(row.insumo_id), Decimal(str(row.costo_unitario or 0)))
        return costs

    def _recalcular_linea(self, linea: LineaConteoFisico) -> None:
        counted = Decimal(str(linea.stock_contado or 0))
        theoretical = Decimal(str(linea.stock_teorico or 0))
        linea.diferencia = _q3(counted - theoretical)
        linea.costo_diferencia = _money(linea.diferencia * Decimal(str(linea.costo_unitario or 0)))

    def _crear_ajuste_insumo(self, linea: LineaConteoFisico, conteo: ConteoFisicoMensual) -> MovimientoInventario:
        source_hash = self._source_hash("CONTEO_FISICO_INSUMO", conteo.id, linea.id)
        movement, _ = MovimientoInventario.objects.update_or_create(
            source_hash=source_hash,
            defaults={
                "fecha": self._movement_datetime(conteo.fecha_conteo),
                "tipo": MovimientoInventario.TIPO_AJUSTE,
                "insumo": linea.insumo,
                "cantidad": linea.diferencia,
                "referencia": f"CONTEO-FISICO:{conteo.periodo:%Y-%m}",
            },
        )
        return movement

    def _crear_ajuste_producto(self, linea: LineaConteoFisico, conteo: ConteoFisicoMensual) -> MovimientoProductoCedis:
        source_hash = self._source_hash("CONTEO_FISICO_PRODUCTO", conteo.id, linea.id)
        movement, _ = MovimientoProductoCedis.objects.update_or_create(
            source_hash=source_hash,
            defaults={
                "fecha": self._movement_datetime(conteo.fecha_conteo),
                "tipo": MovimientoProductoCedis.TIPO_AJUSTE,
                "receta": linea.producto,
                "cantidad": linea.diferencia,
                "referencia": f"CONTEO-FISICO:{conteo.periodo:%Y-%m}",
            },
        )
        return movement

    def _movement_datetime(self, value: date):
        return timezone.make_aware(datetime.combine(value, time.min))

    def _source_hash(self, *parts: object) -> str:
        raw = "|".join(str(part) for part in parts)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_conteo_period(value: str) -> date:
    return parse_period(value)
