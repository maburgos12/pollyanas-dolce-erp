from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida


class SyncInventarioDesdePointCommandTests(TestCase):
    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointRecipeIdentityService")
    @patch("pos_bridge.management.commands.sync_inventario_desde_point.PointInventoryCostCaptureService")
    def test_escritura_absoluta_actualiza_solo_almacen_1(self, capture_class, identity_class):
        unidad = UnidadMedida.objects.create(
            codigo="kg-sync-point-door",
            nombre="Kilogramo sync Point door",
            tipo=UnidadMedida.TIPO_MASA,
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
            )
        ]
        identity_class.return_value.resolve_insumo.return_value = SimpleNamespace(insumo=insumo)

        call_command("sync_inventario_desde_point", "--apply")

        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ALMACEN_1").stock_actual,
            Decimal("5"),
        )
        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ARMADO").stock_actual,
            Decimal("7"),
        )
