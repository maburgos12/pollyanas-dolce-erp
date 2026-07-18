"""Consumo por venta de recetas de servicio (rebanadas/sabores sin producción Point)."""
from datetime import date
from decimal import Decimal

from django.test import TestCase

from inventario.models import MovimientoInventario
from inventario.services_auditoria_insumos import ConsumoInsumoAuditService
from inventario.services_consumo_bom import ConsumoInsumoAutoService
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointProductionLine
from recetas.models import LineaReceta, Receta, VentaHistorica


class ConsumoVentaServicioTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.pza = UnidadMedida.objects.create(codigo="pza", nombre="Pieza")
        cls.plato = Insumo.objects.create(
            nombre="Rebanada Triangular RP25",
            codigo_point="RP25",
            unidad_base=cls.pza,
            nombre_normalizado="rebanada triangular rp25",
        )
        cls.rebanada = Receta.objects.create(
            nombre="Pay de Queso Rebanada",
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
        )
        LineaReceta.objects.create(
            receta=cls.rebanada,
            insumo=cls.plato,
            insumo_texto="Rebanada Triangular RP25",
            cantidad=Decimal("1"),
            unidad=cls.pza,
            unidad_texto="PZA",
        )
        cls.branch = PointBranch.objects.create(external_id="1", name="ALMACEN")

    def test_venta_sin_produccion_genera_movimiento_consumo(self):
        VentaHistorica.objects.create(
            receta=self.rebanada, fecha=date(2026, 6, 5), cantidad=Decimal("40")
        )
        VentaHistorica.objects.create(
            receta=self.rebanada, fecha=date(2026, 6, 6), cantidad=Decimal("10")
        )
        summary = ConsumoInsumoAutoService().generar_consumos_produccion(
            date(2026, 6, 1), date(2026, 6, 30)
        )
        self.assertEqual(summary.recetas_venta_servicio_procesadas, 1)
        movs = MovimientoInventario.objects.filter(
            insumo=self.plato, tipo=MovimientoInventario.TIPO_CONSUMO
        )
        self.assertEqual(movs.count(), 2)
        self.assertEqual(sum(m.cantidad for m in movs), Decimal("50"))
        self.assertTrue(all(m.referencia.startswith("VENTA-SERV-") for m in movs))

    def test_receta_con_produccion_point_queda_fuera(self):
        VentaHistorica.objects.create(
            receta=self.rebanada, fecha=date(2026, 6, 5), cantidad=Decimal("40")
        )
        PointProductionLine.objects.create(
            branch=self.branch,
            receta=self.rebanada,
            production_external_id="900",
            detail_external_id="1",
            source_hash="hash-prod-900",
            production_date=date(2026, 6, 4),
            item_name="Pay de Queso Rebanada",
            produced_quantity=Decimal("40"),
        )
        # La vía de venta no debe duplicar lo que ya consume la vía de producción:
        # solo debe existir el movimiento PROD (40), no uno VENTA-SERV adicional.
        ConsumoInsumoAutoService().generar_consumos_produccion(date(2026, 6, 1), date(2026, 6, 30))
        movs = MovimientoInventario.objects.filter(
            insumo=self.plato, tipo=MovimientoInventario.TIPO_CONSUMO
        )
        self.assertEqual(movs.count(), 1)
        self.assertTrue(movs.first().referencia.startswith("PROD-"))

    def test_rerun_es_idempotente(self):
        VentaHistorica.objects.create(
            receta=self.rebanada, fecha=date(2026, 6, 5), cantidad=Decimal("40")
        )
        service = ConsumoInsumoAutoService()
        service.generar_consumos_produccion(date(2026, 6, 1), date(2026, 6, 30))
        summary = service.generar_consumos_produccion(date(2026, 6, 1), date(2026, 6, 30))
        self.assertEqual(summary.movimientos_creados, 0)
        self.assertEqual(summary.movimientos_sin_cambio, 1)
        movs = MovimientoInventario.objects.filter(insumo=self.plato)
        self.assertEqual(movs.count(), 1)
        self.assertEqual(movs.first().cantidad, Decimal("40"))

    def test_rerun_actualiza_movimiento_cuando_cambia_el_bom(self):
        VentaHistorica.objects.create(
            receta=self.rebanada, fecha=date(2026, 6, 5), cantidad=Decimal("40")
        )
        service = ConsumoInsumoAutoService()
        service.generar_consumos_produccion(date(2026, 6, 1), date(2026, 6, 30))
        LineaReceta.objects.filter(receta=self.rebanada).update(cantidad=Decimal("2"))
        summary = service.generar_consumos_produccion(date(2026, 6, 1), date(2026, 6, 30))
        self.assertEqual(summary.movimientos_actualizados, 1)
        mov = MovimientoInventario.objects.get(insumo=self.plato)
        self.assertEqual(mov.cantidad, Decimal("80"))

    def test_teorico_incluye_ventas_de_servicio(self):
        VentaHistorica.objects.create(
            receta=self.rebanada, fecha=date(2026, 6, 5), cantidad=Decimal("40")
        )
        resumen = ConsumoInsumoAuditService().calcular_periodo(date(2026, 6, 1), dry_run=True)
        fila = next(r for r in resumen.rows if r.insumo_id == self.plato.id)
        self.assertEqual(fila.consumo_teorico, Decimal("40.0000"))
