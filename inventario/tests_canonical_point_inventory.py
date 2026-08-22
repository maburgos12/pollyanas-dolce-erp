from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from inventario.canonical_point_inventory import (
    CanonicalInventoryUnavailable,
    CanonicalPointInventoryService,
    InventoryFreshness,
    InventoryLocation,
    display_quantity,
    require_inventory_location,
)
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob


class CanonicalPointInventoryContractTests(SimpleTestCase):
    def test_location_is_required(self):
        with self.assertRaisesMessage(ValueError, "ubicación de inventario es obligatoria"):
            require_inventory_location(None)

    def test_only_business_locations_are_accepted(self):
        self.assertEqual(require_inventory_location("ALMACEN"), InventoryLocation.ALMACEN)
        self.assertEqual(require_inventory_location("CEDIS"), InventoryLocation.CEDIS)
        with self.assertRaises(ValueError):
            require_inventory_location("CFP")

    def test_base_units_are_presented_as_kg_liters_or_pieces(self):
        self.assertEqual(
            display_quantity(Decimal("169669.245"), SimpleNamespace(codigo="g")),
            (Decimal("169.669245"), "kg"),
        )
        self.assertEqual(
            display_quantity(Decimal("11217.150"), SimpleNamespace(codigo="ml")),
            (Decimal("11.21715"), "L"),
        )
        self.assertEqual(
            display_quantity(Decimal("7"), SimpleNamespace(codigo="pza")),
            (Decimal("7"), "pza"),
        )


@override_settings(POINT_INVENTORY_CANONICAL_MAX_AGE_MINUTES=720)
class CanonicalPointInventoryServiceTests(TestCase):
    def setUp(self):
        self.now = timezone.now()
        self.gram = UnidadMedida.objects.create(
            codigo="g",
            nombre="Gramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1"),
        )
        UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(
            codigo_point="AZM",
            nombre="AZUCAR MASCABADO",
            unidad_base=self.gram,
        )
        self.insumo_without_code = Insumo.objects.create(
            nombre="INSUMO SIN CODIGO POINT",
            unidad_base=self.gram,
        )
        self.almacen = PointBranch.objects.create(external_id="9", name="Almacen")
        self.cedis = PointBranch.objects.create(external_id="8", name="CEDIS")
        self.product = PointProduct.objects.create(
            external_id="AZM",
            sku="AZM",
            name="AZUCAR MASCABADO",
        )
        self.job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            finished_at=self.now,
        )
        PointInventorySnapshot.objects.create(
            branch=self.almacen,
            product=self.product,
            stock=Decimal("20"),
            captured_at=self.now,
            sync_job=self.job,
            raw_payload={"Unidad": "KG"},
        )
        PointInventorySnapshot.objects.create(
            branch=self.cedis,
            product=self.product,
            stock=Decimal("-14.140"),
            captured_at=self.now,
            sync_job=self.job,
            raw_payload={"Unidad": "KG"},
        )
        self.service = CanonicalPointInventoryService()

    def test_read_many_never_compensates_almacen_with_cedis(self):
        almacen = self.service.read_many([self.insumo], location="ALMACEN", now=self.now)
        cedis = self.service.read_many([self.insumo], location="CEDIS", now=self.now)

        self.assertEqual(almacen[self.insumo.id].quantity_base, Decimal("20000.000000"))
        self.assertEqual(almacen[self.insumo.id].display_quantity, Decimal("20.000000"))
        self.assertEqual(cedis[self.insumo.id].quantity_base, Decimal("-14140.000000"))
        self.assertEqual(cedis[self.insumo.id].display_quantity, Decimal("-14.140000"))

    def test_missing_point_code_is_not_reported_as_zero(self):
        reading = self.service.read_many(
            [self.insumo_without_code],
            location="ALMACEN",
            now=self.now,
        )[self.insumo_without_code.id]

        self.assertIsNone(reading.quantity_base)
        self.assertEqual(reading.freshness, InventoryFreshness.MISSING)

    def test_stale_snapshot_is_visible_but_not_usable_for_decisions(self):
        stale_now = self.now + timedelta(hours=13)
        reading = self.service.read_many([self.insumo], location="ALMACEN", now=stale_now)[self.insumo.id]

        self.assertEqual(reading.display_quantity, Decimal("20.000000"))
        self.assertEqual(reading.freshness, InventoryFreshness.STALE)
        with self.assertRaises(CanonicalInventoryUnavailable):
            self.service.require_fresh([self.insumo], location="ALMACEN", now=stale_now)

    def test_missing_snapshot_is_not_reported_as_zero(self):
        self.product.snapshots.filter(branch=self.almacen).delete()

        reading = self.service.read_many([self.insumo], location="ALMACEN", now=self.now)[self.insumo.id]

        self.assertIsNone(reading.quantity_base)
        self.assertEqual(reading.freshness, InventoryFreshness.MISSING)
