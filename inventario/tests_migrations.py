from decimal import Decimal

from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase

from maestros.models import Insumo, UnidadMedida


class ExistenciasPorUbicacionMigrationTests(TransactionTestCase):
    migrate_from = ("inventario", "0012_alter_existenciainsumo_options_and_more")
    migrate_to = ("inventario", "0013_existencias_por_ubicacion")

    def setUp(self):
        super().setUp()
        unidad = UnidadMedida.objects.create(
            codigo="kg-mig-location",
            nombre="Kilogramo migration location",
            tipo=UnidadMedida.TIPO_MASA,
        )
        first = Insumo.objects.create(nombre="Migración ubicación uno", unidad_base=unidad, activo=True)
        second = Insumo.objects.create(nombre="Migración ubicación dos", unidad_base=unidad, activo=True)
        self.insumo_ids = (first.id, second.id)

        self.executor = MigrationExecutor(connection)
        self.executor.migrate([self.migrate_from])
        old_apps = self.executor.loader.project_state([self.migrate_from]).apps
        ExistenciaInsumo = old_apps.get_model("inventario", "ExistenciaInsumo")
        ExistenciaInsumo.objects.create(
            insumo_id=self.insumo_ids[0],
            almacen="ALMACEN_CASA_1",
            stock_actual=Decimal("8"),
        )
        ExistenciaInsumo.objects.create(
            insumo_id=self.insumo_ids[1],
            almacen="CUARTO_FRIO",
            stock_actual=Decimal("2"),
        )

        self.executor = MigrationExecutor(connection)
        self.executor.migrate([self.migrate_to])
        self.apps = self.executor.loader.project_state([self.migrate_to]).apps

    def tearDown(self):
        executor = MigrationExecutor(connection)
        executor.migrate(executor.loader.graph.leaf_nodes())
        super().tearDown()

    def test_preserva_stock_y_almacen_de_filas_existentes(self):
        ExistenciaInsumo = self.apps.get_model("inventario", "ExistenciaInsumo")
        rows = {
            row["insumo_id"]: (row["almacen"], row["stock_actual"])
            for row in ExistenciaInsumo.objects.filter(insumo_id__in=self.insumo_ids).values(
                "insumo_id", "almacen", "stock_actual"
            )
        }
        self.assertEqual(rows[self.insumo_ids[0]], ("ALMACEN_CASA_1", Decimal("8")))
        self.assertEqual(rows[self.insumo_ids[1]], ("CUARTO_FRIO", Decimal("2")))


class LotesYOrigenMovimientosMigrationTests(TransactionTestCase):
    migrate_from = ("inventario", "0013_existencias_por_ubicacion")
    migrate_to = ("inventario", "0014_lotes_y_origen_movimientos")

    def setUp(self):
        super().setUp()
        unidad = UnidadMedida.objects.create(
            codigo="kg-mig-lote",
            nombre="Kilogramo migracion lote",
            tipo=UnidadMedida.TIPO_MASA,
        )
        insumo = Insumo.objects.create(
            codigo_point="MIG-LOTE-1",
            nombre="Insumo historico migracion lote",
            unidad_base=unidad,
        )
        executor = MigrationExecutor(connection)
        executor.migrate([self.migrate_from])
        old_apps = executor.loader.project_state([self.migrate_from]).apps
        MovimientoInventario = old_apps.get_model("inventario", "MovimientoInventario")
        movimiento = MovimientoInventario.objects.create(
            tipo="ENTRADA",
            insumo_id=insumo.id,
            cantidad=Decimal("4.250"),
            referencia="MOV-HIST-001",
            almacen="ALMACEN_1",
            registrado_por="Usuario historico",
            source_hash="movement-history-before-lots",
        )
        self.movimiento_id = movimiento.id

        executor = MigrationExecutor(connection)
        executor.migrate([self.migrate_to])
        self.apps = executor.loader.project_state([self.migrate_to]).apps

    def tearDown(self):
        executor = MigrationExecutor(connection)
        executor.migrate(executor.loader.graph.leaf_nodes())
        super().tearDown()

    def test_preserva_movimiento_historico_y_agrega_origenes_vacios(self):
        MovimientoInventario = self.apps.get_model("inventario", "MovimientoInventario")

        movimiento = MovimientoInventario.objects.get(pk=self.movimiento_id)

        self.assertEqual(movimiento.tipo, "ENTRADA")
        self.assertEqual(movimiento.cantidad, Decimal("4.250"))
        self.assertEqual(movimiento.referencia, "MOV-HIST-001")
        self.assertEqual(movimiento.registrado_por, "Usuario historico")
        self.assertEqual(movimiento.source_hash, "movement-history-before-lots")
        self.assertIsNone(movimiento.lote_id)
        self.assertIsNone(movimiento.linea_bitacora_id)
        self.assertIsNone(movimiento.registrado_por_usuario_id)
        self.assertEqual(movimiento.trazabilidad, {})
