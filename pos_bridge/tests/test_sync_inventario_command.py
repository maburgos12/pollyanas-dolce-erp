from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.core.management import CommandError, call_command
from django.test import TestCase

from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, UnidadMedida


class SyncInventarioDesdePointCommandTests(TestCase):
    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointInventoryCostCaptureService")
    def test_corte_aborta_si_point_no_entrega_insumos(self, capture_class):
        capture_class.return_value.capture_all_rows.return_value = []

        with self.assertRaises(CommandError):
            call_command("sync_inventario_desde_point", "--apply")

        self.assertFalse(AlmacenSyncRun.objects.filter(message__startswith="POINT_ALMACEN_BASELINE|").exists())

    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointRecipeIdentityService")
    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointInventoryCostCaptureService")
    def test_escritura_absoluta_actualiza_solo_almacen_1(self, capture_class, identity_class):
        unidad, _ = UnidadMedida.objects.get_or_create(
            codigo="kg",
            defaults={"nombre": "Kilogramo", "tipo": UnidadMedida.TIPO_MASA},
        )
        insumo = Insumo.objects.create(
            nombre="Insumo sync Point door",
            unidad_base=unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ALMACEN_1",
            stock_actual=Decimal("2"),
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ARMADO",
            stock_actual=Decimal("7"),
        )
        capture_class.return_value.capture_all_rows.return_value = [
            SimpleNamespace(
                point_code="SYNC-DOOR-001",
                point_name=insumo.nombre,
                quantity=Decimal("5"),
                unit="kg",
                kind="supply",
            )
        ]
        identity_class.return_value.resolve_insumo.return_value = SimpleNamespace(
            insumo=insumo,
            method="POINT_CODE",
            score=100.0,
        )

        call_command("sync_inventario_desde_point", "--apply")

        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ALMACEN_1").stock_actual,
            Decimal("5"),
        )
        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ARMADO").stock_actual,
            Decimal("7"),
        )

    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointRecipeIdentityService")
    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointInventoryCostCaptureService")
    def test_corte_incluye_cero_excluye_productos_y_deja_marcador(self, capture_class, identity_class):
        unidad, _ = UnidadMedida.objects.get_or_create(
            codigo="kg",
            defaults={"nombre": "Kilogramo", "tipo": UnidadMedida.TIPO_MASA},
        )
        insumo = Insumo.objects.create(nombre="Azúcar corte Point", unidad_base=unidad, activo=True)
        existencia = ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ALMACEN_1",
            stock_actual=Decimal("12"),
        )
        capture_class.return_value.capture_all_rows.return_value = [
            SimpleNamespace(
                point_code="AZ-CORTE-1",
                point_name=insumo.nombre,
                quantity=Decimal("0"),
                unit="kg",
                kind="supply",
            ),
            SimpleNamespace(
                point_code="PRODUCTO-1",
                point_name="Pastel que no es insumo",
                quantity=Decimal("99"),
                unit="pza",
                kind="product",
            ),
        ]
        identity_class.return_value.resolve_insumo.return_value = SimpleNamespace(
            insumo=insumo,
            method="POINT_CODE",
            score=100.0,
        )

        call_command("sync_inventario_desde_point", "--apply")

        existencia.refresh_from_db()
        self.assertEqual(existencia.stock_actual, Decimal("0"))
        self.assertEqual(existencia.trazabilidad_stock["details"]["point_stock"], "0")
        self.assertEqual(identity_class.return_value.resolve_insumo.call_count, 1)
        movimiento = MovimientoInventario.objects.get(insumo=insumo, tipo=MovimientoInventario.TIPO_AJUSTE)
        self.assertEqual(movimiento.cantidad, Decimal("-12"))
        run = AlmacenSyncRun.objects.get(message__startswith="POINT_ALMACEN_BASELINE|")
        self.assertEqual(run.rows_stock_read, 1)
        self.assertEqual(run.existencias_updated, 1)

    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointRecipeIdentityService")
    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointInventoryCostCaptureService")
    def test_corte_aborta_si_dos_filas_del_mismo_insumo_tienen_saldos_distintos(
        self,
        capture_class,
        identity_class,
    ):
        unidad = UnidadMedida.objects.create(
            codigo="kg-point-conflict",
            nombre="Kilogramo sync Point conflict",
            tipo=UnidadMedida.TIPO_MASA,
        )
        insumo = Insumo.objects.create(nombre="Harina conflicto Point", unidad_base=unidad, activo=True)
        existencia = ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ALMACEN_1",
            stock_actual=Decimal("8"),
        )
        capture_class.return_value.capture_all_rows.return_value = [
            SimpleNamespace(
                point_code="HARINA-A",
                point_name=insumo.nombre,
                quantity=Decimal("3"),
                unit="kg",
                kind="supply",
            ),
            SimpleNamespace(
                point_code="HARINA-B",
                point_name=insumo.nombre,
                quantity=Decimal("4"),
                unit="kg",
                kind="supply",
            ),
        ]
        identity_class.return_value.resolve_insumo.return_value = SimpleNamespace(
            insumo=insumo,
            method="EXACT",
            score=100.0,
        )

        with self.assertRaises(CommandError):
            call_command("sync_inventario_desde_point", "--apply")

        existencia.refresh_from_db()
        self.assertEqual(existencia.stock_actual, Decimal("8"))
        self.assertFalse(AlmacenSyncRun.objects.filter(message__startswith="POINT_ALMACEN_BASELINE|").exists())
        self.assertFalse(MovimientoInventario.objects.filter(insumo=insumo).exists())
