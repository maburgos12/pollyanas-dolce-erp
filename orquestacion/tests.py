from decimal import Decimal

from django.test import TestCase

from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida
from orquestacion.services.rule_runners import _build_chain_purchase_summary
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta


class OrquestacionModuleTests(TestCase):
    def test_placeholder(self):
        self.assertTrue(True)


class ChainPurchaseSummaryMultiubicacionTests(TestCase):
    def test_suma_stock_de_todas_las_ubicaciones(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-chain-multi",
            nombre="Kilogramo chain multi",
            tipo=UnidadMedida.TIPO_MASA,
        )
        insumo = Insumo.objects.create(
            nombre="Insumo chain multi",
            codigo_point="CHAIN-MULTI-001",
            unidad_base=unidad,
            activo=True,
        )
        receta = Receta.objects.create(nombre="Receta chain multi", hash_contenido="chain-multi")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=insumo,
            insumo_texto=insumo.nombre,
            cantidad=Decimal("10"),
            unidad=unidad,
            unidad_texto=unidad.codigo,
            match_status=LineaReceta.STATUS_AUTO,
        )
        plan = PlanProduccion.objects.create(nombre="Plan chain multi")
        PlanProduccionItem.objects.create(plan=plan, receta=receta, cantidad=Decimal("1"))
        ExistenciaInsumo.objects.create(insumo=insumo, almacen="CFP_1_1", stock_actual=Decimal("4"))
        ExistenciaInsumo.objects.create(insumo=insumo, almacen="ARMADO", stock_actual=Decimal("3"))

        summary = _build_chain_purchase_summary(plan=plan)

        row = summary["details"]["shortage_rows"][0]
        self.assertEqual(Decimal(row["stock_actual"]), Decimal("7"))
        self.assertEqual(Decimal(row["shortage_qty"]), Decimal("3"))
