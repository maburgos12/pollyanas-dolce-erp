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
