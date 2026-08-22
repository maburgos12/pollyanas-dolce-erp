from datetime import datetime
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from inventario.models import (
    ExistenciaInsumo,
    MovimientoInventario,
    UBICACION_ALMACEN,
    UBICACION_CEDIS,
)
from inventario.services_point_reconciliation import reconcile_insumo_from_point
from inventario.units import from_presentation_quantity, presentation_quantity
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch
from pos_bridge.services.live_inventory_lookup_service import (
    PointLiveInventoryLookupError,
    PointLiveInventoryResult,
)


class InventoryPresentationUnitsTests(TestCase):
    def setUp(self):
        self.gram = UnidadMedida.objects.create(
            codigo="g", nombre="Gramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1
        )
        self.milliliter = UnidadMedida.objects.create(
            codigo="ml", nombre="Mililitro", tipo=UnidadMedida.TIPO_VOLUMEN, factor_to_base=1
        )

    def test_grams_and_milliliters_are_presented_as_kg_and_liters(self):
        self.assertEqual(
            presentation_quantity(Decimal("169669.245"), self.gram),
            (Decimal("169.669245"), "kg"),
        )
        self.assertEqual(
            presentation_quantity(Decimal("11217.150"), self.milliliter),
            (Decimal("11.21715"), "L"),
        )

    def test_presentation_input_is_converted_back_to_base_units(self):
        self.assertEqual(from_presentation_quantity(Decimal("1.25"), self.gram), Decimal("1250.00"))
        self.assertEqual(from_presentation_quantity(Decimal("2.5"), self.milliliter), Decimal("2500.0"))

    def test_existencias_form_stores_reorder_inputs_as_grams_without_editing_point_stock(self):
        user = get_user_model().objects.create_superuser(
            username="inventario-unidades", email="inventario@example.com", password="test"
        )
        insumo = Insumo.objects.create(
            codigo_point="UNIT-G",
            nombre="Insumo en gramos",
            categoria="PRUEBA",
            unidad_base=self.gram,
        )
        self.client.force_login(user)

        response = self.client.post(
            reverse("inventario:existencias"),
            {
                "ubicacion": "almacen",
                "insumo_id": str(insumo.id),
                "stock_actual": "1.25",
                "stock_minimo": "0.5",
                "stock_maximo": "2",
                "inventario_promedio": "1",
                "dias_llegada_pedido": "1",
                "consumo_diario_promedio": "0.1",
                "punto_reorden": "",
            },
        )

        self.assertEqual(response.status_code, 302)
        existencia = ExistenciaInsumo.objects.get(insumo=insumo, almacen=UBICACION_ALMACEN)
        self.assertEqual(existencia.stock_actual, Decimal("0.000"))
        self.assertEqual(existencia.stock_minimo, Decimal("500.000"))
        self.assertEqual(existencia.stock_maximo, Decimal("2000.000"))
        self.assertEqual(existencia.consumo_diario_promedio, Decimal("100.000"))

        page = self.client.get(reverse("inventario:existencias"))
        row = next(item for item in page.context["existencias"] if item.insumo.id == insumo.id)
        self.assertEqual(row.display_unit, "kg")
        self.assertIsNone(row.stock_actual_display)
        self.assertContains(page, "Fuente madre: Point")


class _FakeLiveInventoryService:
    def __init__(self, quantities=None, failing_branch=None):
        self.quantities = quantities or {"9": Decimal("-5"), "8": Decimal("-4.67")}
        self.failing_branch = failing_branch
        self.calls = []

    def get_stock(self, *, product_codes, sucursal, point_branch):
        self.calls.append((sucursal.codigo, point_branch.external_id, tuple(product_codes)))
        if point_branch.external_id == self.failing_branch:
            raise PointLiveInventoryLookupError("fallo controlado de Point")
        qty = self.quantities[point_branch.external_id]
        return PointLiveInventoryResult(
            product_code=product_codes[0],
            product_name="AZUCAR MASCABADO",
            point_product_id="71",
            point_branch_id=point_branch.external_id,
            point_branch_name=point_branch.name,
            stock_qty=qty,
            captured_at=timezone.make_aware(datetime(2026, 8, 5, 10, 0)),
            raw_payload={"Codigo": product_codes[0], "Cantidad": str(qty), "Unidad": "KG"},
        )


class PointInventoryReconciliationTests(TestCase):
    def setUp(self):
        self.gram = UnidadMedida.objects.create(
            codigo="g", nombre="Gramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1
        )
        UnidadMedida.objects.create(
            codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1000
        )
        self.insumo = Insumo.objects.create(
            codigo_point="011",
            nombre="AZUCAR MASCABADO",
            categoria="AZUCARES",
            unidad_base=self.gram,
        )
        self.almacen_branch, _ = Sucursal.objects.update_or_create(
            codigo="ALMACEN", defaults={"nombre": "Almacen", "activa": False}
        )
        self.cedis_branch, _ = Sucursal.objects.update_or_create(
            codigo="CEDIS", defaults={"nombre": "CEDIS", "activa": True}
        )

        # Aliases históricos: no deben ganar sobre los IDs numéricos oficiales.
        PointBranch.objects.create(
            external_id="Almacen", name="Almacen", erp_branch=self.almacen_branch
        )
        PointBranch.objects.create(external_id="CEDIS", name="CEDIS", erp_branch=self.cedis_branch)
        PointBranch.objects.create(external_id="9", name="Almacen")
        PointBranch.objects.create(external_id="8", name="CEDIS", erp_branch=self.cedis_branch)

        ExistenciaInsumo.objects.create(
            insumo=self.insumo, almacen=UBICACION_ALMACEN, stock_actual=Decimal("-169669.245")
        )
        ExistenciaInsumo.objects.create(
            insumo=self.insumo, almacen=UBICACION_CEDIS, stock_actual=Decimal("11217.150")
        )

    def test_preview_uses_separate_numeric_point_branches(self):
        self.assertFalse(self.almacen_branch.activa)  # Es ubicación logística, no sucursal comercial.
        live = _FakeLiveInventoryService()

        results = reconcile_insumo_from_point(insumo=self.insumo, apply=False, live_service=live)

        self.assertEqual(live.calls, [("ALMACEN", "9", ("011",)), ("CEDIS", "8", ("011",))])
        self.assertEqual(
            [(row.ledger, row.target_qty, row.delta) for row in results],
            [
                (UBICACION_ALMACEN, Decimal("-5000.000"), Decimal("164669.245")),
                (UBICACION_CEDIS, Decimal("-4670.000"), Decimal("-15887.150")),
            ],
        )
        self.assertFalse(MovimientoInventario.objects.exists())

    def test_apply_sets_each_ledger_to_its_own_point_stock_and_is_idempotent(self):
        live = _FakeLiveInventoryService()

        first = reconcile_insumo_from_point(insumo=self.insumo, apply=True, live_service=live)

        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=self.insumo, almacen=UBICACION_ALMACEN).stock_actual,
            Decimal("-5000.000"),
        )
        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=self.insumo, almacen=UBICACION_CEDIS).stock_actual,
            Decimal("-4670.000"),
        )
        movements = list(MovimientoInventario.objects.order_by("almacen"))
        self.assertEqual(len(movements), 2)
        self.assertEqual(
            [(m.almacen, m.tipo, m.cantidad) for m in movements],
            [
                (UBICACION_ALMACEN, MovimientoInventario.TIPO_AJUSTE, Decimal("164669.245")),
                (UBICACION_CEDIS, MovimientoInventario.TIPO_AJUSTE, Decimal("-15887.150")),
            ],
        )
        self.assertTrue(all(row.applied and row.movement_id for row in first))

        second = reconcile_insumo_from_point(insumo=self.insumo, apply=True, live_service=live)

        self.assertEqual(MovimientoInventario.objects.count(), 2)
        self.assertTrue(all(row.delta == 0 and row.movement_id is None for row in second))

    def test_point_failure_does_not_partially_update_almacen(self):
        live = _FakeLiveInventoryService(failing_branch="8")

        with self.assertRaises(PointLiveInventoryLookupError):
            reconcile_insumo_from_point(insumo=self.insumo, apply=True, live_service=live)

        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=self.insumo, almacen=UBICACION_ALMACEN).stock_actual,
            Decimal("-169669.245"),
        )
        self.assertFalse(MovimientoInventario.objects.exists())
