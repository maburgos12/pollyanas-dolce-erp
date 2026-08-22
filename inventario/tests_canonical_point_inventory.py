from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

from django.test import SimpleTestCase, TestCase, override_settings
from django.contrib.auth import get_user_model
from django.urls import reverse
from django.utils import timezone

from inventario.canonical_point_inventory import (
    CanonicalInventoryUnavailable,
    CanonicalPointInventoryService,
    InventoryFreshness,
    InventoryLocation,
    display_quantity,
    require_inventory_location,
    canonical_point_inventory_report_rows,
)
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointInsumoInventorySnapshot, PointSyncJob
from inventario.models import ExistenciaInsumo, UBICACION_ALMACEN


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
        self.job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            finished_at=self.now,
            parameters={"canonical_insumo_inventory": True, "locations": ["ALMACEN", "CEDIS"]},
        )
        PointInsumoInventorySnapshot.objects.create(
            branch=self.almacen,
            insumo=self.insumo,
            point_code="AZM",
            point_name="AZUCAR MASCABADO",
            point_quantity=Decimal("20"),
            point_unit="KG",
            quantity_base=Decimal("20000"),
            captured_at=self.now,
            sync_job=self.job,
            raw_payload={"row": {"Unidad": "KG"}},
        )
        PointInsumoInventorySnapshot.objects.create(
            branch=self.cedis,
            insumo=self.insumo,
            point_code="AZM",
            point_name="AZUCAR MASCABADO",
            point_quantity=Decimal("-14.140"),
            point_unit="KG",
            quantity_base=Decimal("-14140"),
            captured_at=self.now,
            sync_job=self.job,
            raw_payload={"row": {"Unidad": "KG"}},
        )
        self.service = CanonicalPointInventoryService()

    def test_read_many_never_compensates_almacen_with_cedis(self):
        almacen = self.service.read_many([self.insumo], location="ALMACEN", now=self.now)
        cedis = self.service.read_many([self.insumo], location="CEDIS", now=self.now)

        self.assertEqual(almacen[self.insumo.id].quantity_base, Decimal("20000.000000"))
        self.assertEqual(almacen[self.insumo.id].display_quantity, Decimal("20.000000"))
        self.assertEqual(cedis[self.insumo.id].quantity_base, Decimal("-14140.000000"))
        self.assertEqual(cedis[self.insumo.id].display_quantity, Decimal("-14.140000"))

    def test_variant_reads_the_canonical_point_snapshot_instead_of_internal_stock(self):
        variant = Insumo.objects.create(
            nombre="azucar mascabado",
            unidad_base=self.gram,
        )

        readings = self.service.read_many([variant], location="ALMACEN", now=self.now)

        self.assertEqual(readings[variant.id].quantity_base, Decimal("20000.000000"))
        self.assertEqual(readings[variant.id].insumo_id, self.insumo.id)

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
        PointInsumoInventorySnapshot.objects.filter(insumo=self.insumo, branch=self.almacen).delete()

        reading = self.service.read_many([self.insumo], location="ALMACEN", now=self.now)[self.insumo.id]

        self.assertIsNone(reading.quantity_base)
        self.assertEqual(reading.freshness, InventoryFreshness.MISSING)

    def test_report_rows_use_point_stock_but_keep_erp_reorder_policy(self):
        ExistenciaInsumo.objects.update_or_create(
            insumo=self.insumo,
            almacen=UBICACION_ALMACEN,
            defaults={"stock_actual": Decimal("999999"), "punto_reorden": Decimal("25000")},
        )

        rows = canonical_point_inventory_report_rows(location="ALMACEN", insumos=[self.insumo])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].stock_actual, Decimal("20000.000000"))
        self.assertEqual(rows[0].punto_reorden, Decimal("25000.000"))
        self.assertTrue(rows[0].inventory_decision_ready)

    def test_failed_or_partial_cycles_are_never_fresh(self):
        for status in (
            PointSyncJob.STATUS_FAILED,
            PointSyncJob.STATUS_PARTIAL,
        ):
            with self.subTest(status=status):
                self.job.status = status
                self.job.save(update_fields=["status"])
                reading = self.service.read_many([self.insumo], location="ALMACEN", now=self.now)[self.insumo.id]
                self.assertEqual(reading.freshness, InventoryFreshness.ERROR)
                with self.assertRaises(CanonicalInventoryUnavailable):
                    self.service.require_fresh([self.insumo], location="ALMACEN", now=self.now)

    def test_running_cycle_does_not_hide_last_completed_capture(self):
        running_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_RUNNING,
            started_at=self.now + timedelta(minutes=5),
            parameters={"canonical_insumo_inventory": True, "locations": ["ALMACEN", "CEDIS"]},
        )

        reading = self.service.read_many(
            [self.insumo],
            location="ALMACEN",
            now=self.now + timedelta(minutes=10),
        )[self.insumo.id]

        self.assertEqual(reading.freshness, InventoryFreshness.FRESH)
        self.assertEqual(reading.sync_job_id, self.job.id)
        self.assertNotEqual(reading.sync_job_id, running_job.id)

    def test_latest_failed_cycle_invalidates_older_successful_capture(self):
        failed_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_FAILED,
            started_at=self.now + timedelta(minutes=5),
            finished_at=self.now + timedelta(minutes=6),
            parameters={"canonical_insumo_inventory": True, "locations": ["ALMACEN", "CEDIS"]},
        )

        reading = self.service.read_many(
            [self.insumo],
            location="ALMACEN",
            now=self.now + timedelta(minutes=10),
        )[self.insumo.id]

        self.assertEqual(reading.freshness, InventoryFreshness.ERROR)
        self.assertEqual(reading.sync_job_id, failed_job.id)

    def test_partial_cycle_keeps_confirmed_location_usable(self):
        partial_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_PARTIAL,
            started_at=self.now + timedelta(minutes=5),
            finished_at=self.now + timedelta(minutes=6),
            parameters={"canonical_insumo_inventory": True, "locations": ["ALMACEN", "CEDIS"]},
            result_summary={
                "complete": False,
                "locations": {
                    "ALMACEN": {"rows": 1, "snapshots": 1},
                    "CEDIS": {"rows": 0, "snapshots": 0},
                },
            },
        )
        PointInsumoInventorySnapshot.objects.create(
            branch=self.almacen,
            insumo=self.insumo,
            point_code="AZM",
            point_name=self.insumo.nombre,
            point_quantity=Decimal("21"),
            point_unit="KG",
            quantity_base=Decimal("21000"),
            captured_at=self.now + timedelta(minutes=5),
            sync_job=partial_job,
        )

        almacen = self.service.read_many(
            [self.insumo], location="ALMACEN", now=self.now + timedelta(minutes=10)
        )[self.insumo.id]
        cedis = self.service.read_many(
            [self.insumo], location="CEDIS", now=self.now + timedelta(minutes=10)
        )[self.insumo.id]

        self.assertEqual(almacen.freshness, InventoryFreshness.FRESH)
        self.assertEqual(almacen.quantity_base, Decimal("21000.000000"))
        self.assertEqual(cedis.freshness, InventoryFreshness.ERROR)


@override_settings(POINT_INVENTORY_CANONICAL_MAX_AGE_MINUTES=300)
class CanonicalPointInventoryViewTests(TestCase):
    def setUp(self):
        self.now = timezone.now()
        self.user = get_user_model().objects.create_superuser(
            username="inventario-point-canonico",
            email="inventario-point-canonico@example.com",
            password="test",
        )
        self.client.force_login(self.user)
        self.gram = UnidadMedida.objects.create(
            codigo="g",
            nombre="Gramo vista Point",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1"),
        )
        self.insumo = Insumo.objects.create(
            codigo_point="AZM-VIEW",
            nombre="AZUCAR MASCABADO POINT",
            unidad_base=self.gram,
            activo=True,
        )
        self.internal = ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            almacen=UBICACION_ALMACEN,
            stock_actual=Decimal("-169669.245"),
            punto_reorden=Decimal("5000"),
        )
        self.almacen = PointBranch.objects.create(external_id="109", name="Almacen")
        self.cedis = PointBranch.objects.create(external_id="108", name="CEDIS")
        self.job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            started_at=self.now,
            finished_at=self.now,
            parameters={"canonical_insumo_inventory": True, "locations": ["ALMACEN", "CEDIS"]},
        )
        PointInsumoInventorySnapshot.objects.create(
            branch=self.almacen,
            insumo=self.insumo,
            point_code="AZM-VIEW",
            point_name="AZUCAR MASCABADO POINT",
            point_quantity=Decimal("53.471"),
            point_unit="KG",
            quantity_base=Decimal("53471"),
            captured_at=self.now,
            sync_job=self.job,
        )
        PointInsumoInventorySnapshot.objects.create(
            branch=self.cedis,
            insumo=self.insumo,
            point_code="AZM-VIEW",
            point_name="AZUCAR MASCABADO POINT",
            point_quantity=Decimal("-14.140"),
            point_unit="KG",
            quantity_base=Decimal("-14140"),
            captured_at=self.now,
            sync_job=self.job,
        )

    def _row(self, response):
        return next(row for row in response.context["existencias"] if row.insumo.id == self.insumo.id)

    def test_view_uses_point_as_only_stock_source_and_keeps_locations_separate(self):
        almacen_response = self.client.get(reverse("inventario:existencias"))
        cedis_response = self.client.get(reverse("inventario:existencias"), {"ubicacion": "cedis"})

        almacen_row = self._row(almacen_response)
        cedis_row = self._row(cedis_response)
        self.assertEqual(almacen_row.stock_actual, Decimal("53471.000000"))
        self.assertEqual(almacen_row.stock_actual_display, Decimal("53.471000"))
        self.assertEqual(cedis_row.stock_actual, Decimal("-14140.000000"))
        self.assertEqual(cedis_row.stock_actual_display, Decimal("-14.140000"))
        self.assertEqual(almacen_row.inventory_source, "POINT")
        self.assertEqual(almacen_row.inventory_freshness, InventoryFreshness.FRESH)
        self.assertContains(almacen_response, "Fuente madre: Point")
        self.assertNotContains(almacen_response, "-169,669.245")

    def test_failed_latest_cycle_never_falls_back_to_internal_stock(self):
        PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_FAILED,
            started_at=self.now + timedelta(minutes=1),
            finished_at=self.now + timedelta(minutes=2),
            parameters={"canonical_insumo_inventory": True, "locations": ["ALMACEN", "CEDIS"]},
        )

        response = self.client.get(reverse("inventario:existencias"))
        row = self._row(response)

        self.assertIsNone(row.stock_actual)
        self.assertIsNone(row.stock_actual_display)
        self.assertEqual(row.inventory_freshness, InventoryFreshness.ERROR)
        self.assertContains(response, "No disponible")
        self.assertNotContains(response, "-169,669.245")

        dashboard = self.client.get(reverse("inventario:dashboard"))
        alerts = self.client.get(reverse("inventario:alertas"))
        self.assertEqual(dashboard.context["critical_out_count"], 0)
        self.assertEqual(dashboard.context["inventory_unavailable_count"], 1)
        self.assertEqual(alerts.context["criticos_count"], 0)
        self.assertEqual(alerts.context["inventory_unavailable_count"], 1)
        self.assertContains(alerts, "Point no disponible")

    def test_manual_form_cannot_overwrite_point_stock(self):
        response = self.client.post(
            reverse("inventario:existencias"),
            {
                "ubicacion": "almacen",
                "insumo_id": str(self.insumo.id),
                "stock_actual": "999",
                "stock_minimo": "2",
                "stock_maximo": "80",
                "inventario_promedio": "20",
                "dias_llegada_pedido": "1",
                "consumo_diario_promedio": "3",
            },
            follow=True,
        )

        self.internal.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.internal.stock_actual, Decimal("-169669.245"))
        self.assertContains(response, "El stock operativo proviene exclusivamente de Point")
