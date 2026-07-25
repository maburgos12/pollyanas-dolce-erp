from decimal import Decimal

from django.test import TestCase

from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida
from recetas.models import Receta
from recetas.views import matching, recetas


class RecetasDisponibilidadMultiubicacionTests(TestCase):
    def setUp(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-multiubicacion",
            nombre="Kilogramo multiubicación",
            tipo=UnidadMedida.TIPO_MASA,
        )
        self.insumo = Insumo.objects.create(
            nombre="Harina disponibilidad total",
            codigo_point="HAR-TOTAL-001",
            unidad_base=unidad,
            activo=True,
        )
        self.receta = Receta.objects.create(
            nombre="Receta disponibilidad total",
            hash_contenido="receta-disponibilidad-total",
        )
        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            almacen="CFP_1_1",
            stock_actual=Decimal("8"),
        )
        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            almacen="ARMADO",
            stock_actual=Decimal("2"),
        )

    def test_contextos_generales_suman_stock_de_todas_las_ubicaciones(self):
        for module in (recetas, matching):
            with self.subTest(module=module.__name__):
                stock_by_insumo = module._stock_total_by_insumo({self.insumo.id})
                self.assertEqual(stock_by_insumo[self.insumo.id], Decimal("10"))
