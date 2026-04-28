from datetime import date, timedelta
from decimal import Decimal

from django.test import TestCase

from control.models import MermaMensualSucursal
from core.models import Sucursal
from recetas.models import Receta, VentaHistorica
from ventas.services.sales_canonical_source import POINT_BRIDGE_SALES_SOURCE

from .models import ProyeccionProduccion
from .services import ProyeccionProduccionService


class ProyeccionProduccionServiceTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="CRUCERO", nombre="Crucero", activa=True)
        self.receta = Receta.objects.create(
            nombre="Pastel proyección test",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            codigo_point="PPT",
            hash_contenido="hash-pastel-proyeccion-test",
        )

    def test_proyectar_dia_persists_from_sales_and_waste_factor(self):
        target = date(2026, 4, 28)
        for offset in range(1, 15):
            day = target - timedelta(days=offset)
            if day.weekday() == 6:
                continue
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=self.sucursal,
                fecha=day,
                cantidad=Decimal("10"),
                fuente=POINT_BRIDGE_SALES_SOURCE,
            )
        MermaMensualSucursal.objects.create(
            periodo=date(2026, 4, 1),
            sucursal=self.sucursal,
            receta=self.receta,
            nombre_producto=self.receta.nombre,
            unidades_merma=Decimal("10"),
            unidades_vendidas=Decimal("100"),
            costo_merma=Decimal("50"),
        )

        summary = ProyeccionProduccionService().proyectar_dia(target, dry_run=False)

        self.assertEqual(summary.created, 1)
        row = ProyeccionProduccion.objects.get()
        self.assertEqual(row.periodo, target)
        self.assertEqual(row.sucursal, self.sucursal)
        self.assertEqual(row.receta, self.receta)
        self.assertEqual(row.factor_merma, Decimal("0.1000"))
        self.assertEqual(row.unidades_proyectadas_ajustadas, Decimal("11.000"))
        self.assertEqual(row.confianza, ProyeccionProduccion.CONFIANZA_MEDIA)

    def test_excludes_products_with_less_than_three_history_days(self):
        target = date(2026, 4, 28)
        for offset in (1, 2):
            VentaHistorica.objects.create(
                receta=self.receta,
                sucursal=self.sucursal,
                fecha=target - timedelta(days=offset),
                cantidad=Decimal("10"),
                fuente=POINT_BRIDGE_SALES_SOURCE,
            )

        summary = ProyeccionProduccionService().proyectar_dia(target, dry_run=True)

        self.assertEqual(summary.rows, [])
        self.assertEqual(summary.skipped, 1)
