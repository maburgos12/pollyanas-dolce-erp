from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from django.test import TestCase

from control.models import MermaPOS
from core.models import Sucursal
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, PointPendingMatch, UnidadMedida
from pos_bridge.models import PointProductionLine, PointTransferLine, PointWasteLine
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from recetas.models import InventarioCedisProducto, MovimientoProductoCedis, Receta


@dataclass
class FakeWasteLine:
    branch: dict
    movement_external_id: str
    movement_at: datetime
    responsible: str
    item_name: str
    item_code: str
    quantity: Decimal
    unit: str
    unit_cost: Decimal
    total_cost: Decimal
    justification: str
    raw_payload: dict
    source_hash: str


@dataclass
class FakeProductionLine:
    branch: dict
    production_external_id: str
    detail_external_id: str
    production_date: date
    responsible: str
    item_name: str
    item_code: str
    unit: str
    unit_cost: Decimal
    requested_quantity: Decimal
    produced_quantity: Decimal
    is_insumo: bool
    raw_payload: dict
    source_hash: str


@dataclass
class FakeTransferLine:
    origin_branch: dict
    destination_branch: dict
    transfer_external_id: str
    detail_external_id: str
    registered_at: datetime
    sent_at: datetime | None
    received_at: datetime | None
    requested_by: str
    sent_by: str
    received_by: str
    item_name: str
    item_code: str
    unit: str
    unit_cost: Decimal
    requested_quantity: Decimal
    sent_quantity: Decimal
    received_quantity: Decimal
    is_insumo: bool
    is_received: bool
    is_cancelled: bool
    is_finalized: bool
    raw_payload: dict
    source_hash: str
    is_open: bool = False


class FakeWasteExtractor:
    def __init__(self, rows):
        self.rows = rows

    def extract(self, **kwargs):
        return list(self.rows)


class FakeProductionExtractor:
    def __init__(self, rows):
        self.rows = rows

    def extract(self, **kwargs):
        return list(self.rows)


class FakeTransferExtractor:
    def __init__(self, rows):
        self.rows = rows

    def extract(self, **kwargs):
        return list(self.rows)


class PointMovementSyncServiceTests(TestCase):
    def setUp(self):
        self.unit = UnidadMedida.objects.create(codigo="pza", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA)
        self.branch_matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        self.branch_cedis, _ = Sucursal.objects.update_or_create(
            codigo="CEDIS",
            defaults={"nombre": "CEDIS", "activa": True},
        )
        self.branch_produccion_crucero = Sucursal.objects.create(codigo="PRODUCCION_CRUCERO", nombre="Produccion Crucero")

    def test_point_branch_matches_unique_sucursal_name_fragment(self):
        leyva = Sucursal.objects.create(codigo="LEYVA", nombre="Sucursal Leyva")
        service = PointMovementSyncService(transfer_extractor=FakeTransferExtractor([]))

        branch = service._upsert_branch({"external_id": "leyva-point", "name": "Leyva", "status": "ACTIVE", "metadata": {}})

        self.assertEqual(branch.erp_branch_id, leyva.id)

    def test_run_waste_sync_persists_merma_pos(self):
        receta = Receta.objects.create(nombre="Pastel de 3 Pecados R", codigo_point="0108", hash_contenido="hash-receta-waste")
        waste_line = FakeWasteLine(
            branch={"external_id": "Matriz", "name": "Matriz", "status": "ACTIVE", "metadata": {}},
            movement_external_id="1510899",
            movement_at=datetime(2026, 3, 20, 20, 48, 40),
            responsible="Alondra Alvarado",
            item_name=receta.nombre,
            item_code="",
            quantity=Decimal("2.000"),
            unit="PZA",
            unit_cost=Decimal("0"),
            total_cost=Decimal("0"),
            justification="Merma desde la caja",
            raw_payload={"detail": {"Articulo": receta.nombre}},
            source_hash="waste-hash-1",
        )
        service = PointMovementSyncService(waste_extractor=FakeWasteExtractor([waste_line]))

        job = service.run_waste_sync(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(PointWasteLine.objects.count(), 1)
        merma = MermaPOS.objects.get(source_hash="waste-hash-1")
        self.assertEqual(merma.sucursal, self.branch_matriz)
        self.assertEqual(merma.receta, receta)
        self.assertEqual(merma.producto_texto, receta.nombre)
        self.assertEqual(merma.cantidad, Decimal("2.000"))
        self.assertEqual(merma.fuente, PointMovementSyncService.WASTE_SOURCE)
        self.assertEqual(merma.responsable_texto, "Alondra Alvarado")

    def test_run_waste_sync_uses_local_operational_date_for_aware_timestamp(self):
        receta = Receta.objects.create(nombre="Bollo Red Velvet", codigo_point="BOLLO-RV", hash_contenido="hash-receta-waste-tz")
        waste_line = FakeWasteLine(
            branch={"external_id": "EL TUNEL", "name": "EL TUNEL", "status": "ACTIVE", "metadata": {}},
            movement_external_id="1510900",
            movement_at=datetime(2026, 3, 21, 3, 13, 4, tzinfo=timezone.utc),
            responsible="Cesar Gastelum",
            item_name=receta.nombre,
            item_code="",
            quantity=Decimal("1.000"),
            unit="PZA",
            unit_cost=Decimal("9.07"),
            total_cost=Decimal("9.07"),
            justification="Merma nocturna",
            raw_payload={"detail": {"Articulo": receta.nombre}},
            source_hash="waste-hash-local-date",
        )
        service = PointMovementSyncService(waste_extractor=FakeWasteExtractor([waste_line]))

        service.run_waste_sync(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        merma = MermaPOS.objects.get(source_hash="waste-hash-local-date")
        self.assertEqual(merma.fecha, date(2026, 3, 20))

    def test_run_production_sync_creates_inventory_entry_for_insumo(self):
        insumo = Insumo.objects.create(
            nombre="Betún Dream Whip Pastel",
            nombre_point="Betún Dream Whip Pastel",
            codigo_point="01DW01",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        production_line = FakeProductionLine(
            branch={"external_id": "CEDIS", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            production_external_id="21216",
            detail_external_id="77615",
            production_date=date(2026, 3, 20),
            responsible="Roxana Rivas",
            item_name=insumo.nombre,
            item_code=insumo.codigo_point,
            unit="KG",
            unit_cost=Decimal("91.501586"),
            requested_quantity=Decimal("167.340"),
            produced_quantity=Decimal("167.340"),
            is_insumo=True,
            raw_payload={"detail": {"Codigo": insumo.codigo_point}},
            source_hash="prod-insumo-1",
        )
        service = PointMovementSyncService(production_extractor=FakeProductionExtractor([production_line]))

        job = service.run_production_sync(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(PointProductionLine.objects.count(), 1)
        movimiento = MovimientoInventario.objects.get(source_hash="prod-insumo-1")
        self.assertEqual(movimiento.insumo, insumo)
        self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_ENTRADA)
        self.assertEqual(movimiento.almacen, "CUARTO_FRIO")
        existencia = ExistenciaInsumo.objects.get(insumo=insumo, almacen="CUARTO_FRIO")
        self.assertEqual(existencia.stock_actual, Decimal("167.340"))

    def test_actualiza_transferencia_en_ubicacion_existente_no_default(self):
        insumo = Insumo.objects.create(
            nombre="Betún transferencia Armado",
            codigo_point="TRANS-ARMADO-001",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ARMADO",
            stock_actual=Decimal("2"),
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ALMACEN_1",
            stock_actual=Decimal("7"),
        )
        MovimientoInventario.objects.create(
            source_hash="transfer-armado-existente",
            insumo=insumo,
            almacen="ARMADO",
            tipo=MovimientoInventario.TIPO_ENTRADA,
            cantidad=Decimal("2"),
        )
        line = SimpleNamespace(
            source_hash="transfer-armado-existente",
            received_at=datetime(2026, 3, 20, 15, 0, tzinfo=timezone.utc),
            sent_at=None,
            registered_at=datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc),
            insumo=insumo,
            insumo_id=insumo.id,
            received_quantity=Decimal("5"),
            unit=None,
            transfer_external_id="TR-ARMADO-001",
            destination_branch=SimpleNamespace(name="ARMADO", metadata={}),
        )

        PointMovementSyncService()._upsert_transfer_inventory_movement(line=line)

        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ARMADO").stock_actual,
            Decimal("5"),
        )
        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ALMACEN_1").stock_actual,
            Decimal("7"),
        )
        self.assertEqual(
            MovimientoInventario.objects.get(source_hash="transfer-armado-existente").almacen,
            "ARMADO",
        )

    def test_actualiza_transferencia_con_almacen_vacio_en_almacen_1(self):
        insumo = Insumo.objects.create(
            nombre="Betún transferencia almacén vacío",
            codigo_point="TRANS-VACIO-001",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ALMACEN_1",
            stock_actual=Decimal("4"),
        )
        MovimientoInventario.objects.create(
            source_hash="transfer-almacen-vacio",
            insumo=insumo,
            almacen="",
            tipo=MovimientoInventario.TIPO_ENTRADA,
            cantidad=Decimal("4"),
        )
        line = SimpleNamespace(
            source_hash="transfer-almacen-vacio",
            received_at=datetime(2026, 3, 20, 15, 0, tzinfo=timezone.utc),
            sent_at=None,
            registered_at=datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc),
            insumo=insumo,
            insumo_id=insumo.id,
            received_quantity=Decimal("6"),
            unit=None,
            transfer_external_id="TR-VACIO-001",
            destination_branch=SimpleNamespace(
                name="CEDIS",
                metadata={"inventario_ubicacion": "ALMACEN_1"},
            ),
        )

        PointMovementSyncService()._upsert_transfer_inventory_movement(line=line)

        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ALMACEN_1").stock_actual,
            Decimal("6"),
        )
        self.assertFalse(ExistenciaInsumo.objects.filter(insumo=insumo, almacen="").exists())
        self.assertEqual(
            MovimientoInventario.objects.get(source_hash="transfer-almacen-vacio").almacen,
            "ALMACEN_1",
        )

    def test_point_puede_ajustar_transferencia_aunque_el_saldo_quede_negativo(self):
        """Regresión: un ajuste de Point con delta negativo no debe abortar el sync.

        CUARTO_FRIO puede no tener stock de apertura (inventario por secciones en
        construcción). Si Point baja la cantidad de una transferencia ya
        sincronizada, el delta negativo dejaba el job en FAILED y bloqueaba la
        recarga CEDIS de las rutas. El flujo automático debe tolerar saldo
        negativo, igual que el consumo por venta.
        """
        insumo = Insumo.objects.create(
            nombre="Betún transferencia ajustada",
            codigo_point="TRANS-NEG-001",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        MovimientoInventario.objects.create(
            source_hash="transfer-ajuste-negativo",
            insumo=insumo,
            almacen="CUARTO_FRIO",
            tipo=MovimientoInventario.TIPO_ENTRADA,
            cantidad=Decimal("41"),
        )
        line = SimpleNamespace(
            source_hash="transfer-ajuste-negativo",
            received_at=datetime(2026, 7, 28, 15, 0, tzinfo=timezone.utc),
            sent_at=None,
            registered_at=datetime(2026, 7, 28, 14, 0, tzinfo=timezone.utc),
            insumo=insumo,
            insumo_id=insumo.id,
            received_quantity=Decimal("1"),
            unit=None,
            transfer_external_id="TR-NEG-001",
            destination_branch=SimpleNamespace(name="CEDIS", metadata={}),
        )

        PointMovementSyncService()._upsert_transfer_inventory_movement(line=line)

        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="CUARTO_FRIO").stock_actual,
            Decimal("-40"),
        )
        self.assertEqual(
            MovimientoInventario.objects.get(source_hash="transfer-ajuste-negativo").cantidad,
            Decimal("1"),
        )

    def test_normaliza_almacen_vacio_aunque_la_cantidad_no_cambie(self):
        insumo = Insumo.objects.create(
            nombre="Betún transferencia no-op",
            codigo_point="TRANS-NOOP-001",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ALMACEN_1",
            stock_actual=Decimal("4"),
        )
        MovimientoInventario.objects.create(
            source_hash="transfer-almacen-vacio-noop",
            insumo=insumo,
            almacen="",
            tipo=MovimientoInventario.TIPO_ENTRADA,
            cantidad=Decimal("4"),
        )
        line = SimpleNamespace(
            source_hash="transfer-almacen-vacio-noop",
            received_at=datetime(2026, 3, 20, 15, 0, tzinfo=timezone.utc),
            sent_at=None,
            registered_at=datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc),
            insumo=insumo,
            insumo_id=insumo.id,
            received_quantity=Decimal("4"),
            unit=None,
            transfer_external_id="TR-NOOP-001",
            destination_branch=SimpleNamespace(
                name="CEDIS",
                metadata={"inventario_ubicacion": "ALMACEN_1"},
            ),
        )

        PointMovementSyncService()._upsert_transfer_inventory_movement(line=line)

        movimiento = MovimientoInventario.objects.get(source_hash="transfer-almacen-vacio-noop")
        self.assertEqual(movimiento.almacen, "ALMACEN_1")
        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="ALMACEN_1").stock_actual,
            Decimal("4"),
        )

    def test_run_production_sync_creates_cedis_entry_for_finished_product(self):
        receta = Receta.objects.create(
            nombre="Pastel Fresas Con Crema Mediano",
            codigo_point="0100",
            hash_contenido="hash-receta-prod",
        )
        production_line = FakeProductionLine(
            branch={"external_id": "CEDIS", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            production_external_id="30001",
            detail_external_id="90001",
            production_date=date(2026, 3, 20),
            responsible="Roxana Rivas",
            item_name=receta.nombre,
            item_code=receta.codigo_point,
            unit="PZA",
            unit_cost=Decimal("0"),
            requested_quantity=Decimal("10"),
            produced_quantity=Decimal("10"),
            is_insumo=False,
            raw_payload={"detail": {"Codigo": receta.codigo_point}},
            source_hash="prod-receta-1",
        )
        service = PointMovementSyncService(production_extractor=FakeProductionExtractor([production_line]))

        job = service.run_production_sync(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(job.status, "SUCCESS")
        movimiento = MovimientoProductoCedis.objects.get(source_hash="prod-receta-1")
        self.assertEqual(movimiento.receta, receta)
        inventario = InventarioCedisProducto.objects.get(receta=receta)
        self.assertEqual(inventario.stock_actual, Decimal("10"))

    def test_run_transfer_sync_creates_inventory_entry_for_received_insumo_into_cedis(self):
        insumo = Insumo.objects.create(
            nombre="Betún de chocolate",
            nombre_point="Betún de chocolate",
            codigo_point="01BC03",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        transfer_line = FakeTransferLine(
            origin_branch={"external_id": "10", "name": "Produccion Crucero", "status": "ACTIVE", "metadata": {}},
            destination_branch={
                "external_id": "8",
                "name": "CEDIS",
                "status": "ACTIVE",
                "metadata": {},
            },
            transfer_external_id="32292",
            detail_external_id="474444",
            registered_at=datetime(2026, 3, 20, 8, 0, tzinfo=timezone.utc),
            sent_at=datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc),
            received_at=datetime(2026, 3, 20, 15, 9, tzinfo=timezone.utc),
            requested_by="Johana López",
            sent_by="Produccion Crucero",
            received_by="CEDIS",
            item_name=insumo.nombre,
            item_code=insumo.codigo_point,
            unit="KG",
            unit_cost=Decimal("86.195541"),
            requested_quantity=Decimal("15.270"),
            sent_quantity=Decimal("15.270"),
            received_quantity=Decimal("15.270"),
            is_insumo=True,
            is_received=True,
            is_cancelled=False,
            is_finalized=True,
            raw_payload={"detail": {"Codigo": insumo.codigo_point}},
            source_hash="transfer-insumo-1",
        )
        service = PointMovementSyncService(transfer_extractor=FakeTransferExtractor([transfer_line]))

        job = service.run_transfer_sync(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(PointTransferLine.objects.count(), 1)
        movimiento = MovimientoInventario.objects.get(source_hash="transfer-insumo-1")
        self.assertEqual(movimiento.insumo, insumo)
        self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_ENTRADA)
        self.assertEqual(movimiento.almacen, "CUARTO_FRIO")
        existencia = ExistenciaInsumo.objects.get(insumo=insumo, almacen="CUARTO_FRIO")
        self.assertEqual(existencia.stock_actual, Decimal("15.270"))
        self.assertEqual(job.result_summary["inventory_entries_created"], 1)
        self.assertEqual(job.result_summary["skipped_non_storage_branch"], 0)

    def test_modo_snapshot_refresca_lineas_sin_escribir_inventario(self):
        """Contrato logística/inventario: la ruta se guía por Point (snapshot);
        el libro de inventario lo escribe después el proceso programado."""
        insumo = Insumo.objects.create(
            nombre="Betún snapshot",
            nombre_point="Betún snapshot",
            codigo_point="01BS01",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        transfer_line = FakeTransferLine(
            origin_branch={"external_id": "10", "name": "Produccion Crucero", "status": "ACTIVE", "metadata": {}},
            destination_branch={"external_id": "8", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            transfer_external_id="32293",
            detail_external_id="474445",
            registered_at=datetime(2026, 3, 20, 8, 0, tzinfo=timezone.utc),
            sent_at=datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc),
            received_at=datetime(2026, 3, 20, 15, 9, tzinfo=timezone.utc),
            requested_by="Johana López",
            sent_by="Produccion Crucero",
            received_by="CEDIS",
            item_name=insumo.nombre,
            item_code=insumo.codigo_point,
            unit="KG",
            unit_cost=Decimal("86.195541"),
            requested_quantity=Decimal("15.270"),
            sent_quantity=Decimal("15.270"),
            received_quantity=Decimal("15.270"),
            is_insumo=True,
            is_received=True,
            is_cancelled=False,
            is_finalized=True,
            raw_payload={"detail": {"Codigo": insumo.codigo_point}},
            source_hash="transfer-snapshot-1",
        )
        sin_mapear = FakeTransferLine(
            origin_branch={"external_id": "10", "name": "Produccion Crucero", "status": "ACTIVE", "metadata": {}},
            destination_branch={"external_id": "8", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            transfer_external_id="32294",
            detail_external_id="474446",
            registered_at=datetime(2026, 3, 20, 8, 0, tzinfo=timezone.utc),
            sent_at=datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc),
            received_at=datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc),
            requested_by="Johana López",
            sent_by="Produccion Crucero",
            received_by="CEDIS",
            item_name="Producto nuevo sin mapear",
            item_code="SIN-MAPEO-999",
            unit="PZA",
            unit_cost=Decimal("10"),
            requested_quantity=Decimal("2"),
            sent_quantity=Decimal("2"),
            received_quantity=Decimal("2"),
            is_insumo=True,
            is_received=True,
            is_cancelled=False,
            is_finalized=True,
            raw_payload={"detail": {"Codigo": "SIN-MAPEO-999"}},
            source_hash="transfer-snapshot-sin-mapeo",
        )
        service = PointMovementSyncService(
            transfer_extractor=FakeTransferExtractor([transfer_line, sin_mapear])
        )

        job = service.run_transfer_sync(
            start_date=date(2026, 3, 20),
            end_date=date(2026, 3, 20),
            apply_inventory=False,
        )

        self.assertEqual(job.status, "SUCCESS")
        self.assertIs(job.parameters["apply_inventory"], False)
        self.assertEqual(PointTransferLine.objects.count(), 2)
        self.assertFalse(MovimientoInventario.objects.exists())
        self.assertFalse(ExistenciaInsumo.objects.exists())
        self.assertEqual(job.result_summary["inventory_entries_created"], 0)
        # La cola de matching se siembra también en modo snapshot: un producto
        # Point sin mapear debe hacerse visible al equipo el mismo día.
        self.assertEqual(job.result_summary["unmatched_items"], 1)
        self.assertEqual(
            PointPendingMatch.objects.filter(point_codigo="SIN-MAPEO-999").count(),
            1,
        )

        # El proceso programado (modo completo, default) aplica el inventario
        # diferido de forma idempotente sobre el mismo snapshot.
        job_completo = service.run_transfer_sync(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(job_completo.status, "SUCCESS")
        movimiento = MovimientoInventario.objects.get(source_hash="transfer-snapshot-1")
        self.assertEqual(movimiento.almacen, "CUARTO_FRIO")
        self.assertEqual(
            ExistenciaInsumo.objects.get(insumo=insumo, almacen="CUARTO_FRIO").stock_actual,
            Decimal("15.270"),
        )

    def test_run_transfer_sync_creates_cedis_entry_for_received_finished_product_into_cedis(self):
        receta = Receta.objects.create(
            nombre="Empanada de Manzana",
            codigo_point="0135",
            hash_contenido="hash-receta-transfer",
        )
        transfer_line = FakeTransferLine(
            origin_branch={"external_id": "10", "name": "Produccion Crucero", "status": "ACTIVE", "metadata": {}},
            destination_branch={"external_id": "8", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            transfer_external_id="32292",
            detail_external_id="474441",
            registered_at=datetime(2026, 3, 20, 8, 0, tzinfo=timezone.utc),
            sent_at=datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc),
            received_at=datetime(2026, 3, 20, 15, 9, tzinfo=timezone.utc),
            requested_by="Johana López",
            sent_by="Produccion Crucero",
            received_by="CEDIS",
            item_name=receta.nombre,
            item_code=receta.codigo_point,
            unit="PZA",
            unit_cost=Decimal("2.029549"),
            requested_quantity=Decimal("24"),
            sent_quantity=Decimal("24"),
            received_quantity=Decimal("24"),
            is_insumo=False,
            is_received=True,
            is_cancelled=False,
            is_finalized=True,
            raw_payload={"detail": {"Codigo": receta.codigo_point}},
            source_hash="transfer-receta-1",
        )
        service = PointMovementSyncService(transfer_extractor=FakeTransferExtractor([transfer_line]))

        job = service.run_transfer_sync(start_date=date(2026, 3, 20), end_date=date(2026, 3, 20))

        self.assertEqual(job.status, "SUCCESS")
        movimiento = MovimientoProductoCedis.objects.get(source_hash="transfer-receta-1")
        self.assertEqual(movimiento.receta, receta)
        inventario = InventarioCedisProducto.objects.get(receta=receta)
        self.assertEqual(inventario.stock_actual, Decimal("24"))

    def test_point_sync_links_matching_bitacora_movement_without_second_delta(self):
        # Test exact match case: Point finds exact bitacora movement and links without creating delta
        from operacion.models import BitacoraOperativa, BitacoraOperativaLinea

        insumo = Insumo.objects.create(
            nombre="Relleno Crunch",
            codigo_point="RC-001",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        receta = Receta.objects.create(nombre="Crunch", codigo_point="RC-001", hash_contenido="hash-rc")

        # Create initial bitacora movement
        bitacora = BitacoraOperativa.objects.create(tipo="HORNOS", fecha=date(2026, 7, 11))
        linea = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora,
            receta=receta,
        )
        bitacora_move = MovimientoInventario.objects.create(
            linea_bitacora=linea,
            insumo=insumo,
            tipo=MovimientoInventario.TIPO_ENTRADA,
            almacen="CFP_1_1",
            cantidad=Decimal("8"),
            fecha=datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc),
            referencia=f"BITACORA:HORNOS:{linea.id}",
            trazabilidad={},
        )
        from inventario.services_existencias import aplicar_delta
        aplicar_delta(insumo, "CFP_1_1", Decimal("8"))

        before = ExistenciaInsumo.objects.get(insumo=insumo, almacen="CFP_1_1").stock_actual

        # Now Point has the same entry - should link without duplicating
        production_line = FakeProductionLine(
            branch={"external_id": "CEDIS", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            production_external_id="21216",
            detail_external_id="77615",
            production_date=date(2026, 7, 11),
            responsible="Test",
            item_name=insumo.nombre,
            item_code=insumo.codigo_point,
            unit="PZA",
            unit_cost=Decimal("0"),
            requested_quantity=Decimal("8"),
            produced_quantity=Decimal("8"),
            is_insumo=True,
            raw_payload={"detail": {}},
            source_hash="prod-reconciled-1",
        )
        service = PointMovementSyncService(production_extractor=FakeProductionExtractor([production_line]))

        job = service.run_production_sync(start_date=date(2026, 7, 11), end_date=date(2026, 7, 11))

        # Stock should not change
        after = ExistenciaInsumo.objects.get(insumo=insumo, almacen="CFP_1_1").stock_actual
        self.assertEqual(after, before, "Stock should not change on reconciliation")

        # Should only have one movement
        self.assertEqual(MovimientoInventario.objects.filter(insumo=insumo).count(), 1,
                         "Should not create duplicate movement")

        # Bitacora movement should now have reconciliation marks
        bitacora_move.refresh_from_db()
        self.assertIn("point_source_hash", bitacora_move.trazabilidad)
        self.assertEqual(bitacora_move.trazabilidad.get("point_source_hash"), "prod-reconciled-1")

    def test_point_sync_creates_entry_when_no_bitacora_match(self):
        # Test zero matches: Point creates normal entry when no bitacora found
        insumo = Insumo.objects.create(
            nombre="Betún Dream Whip",
            codigo_point="01DW01",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        production_line = FakeProductionLine(
            branch={"external_id": "CEDIS", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            production_external_id="21216",
            detail_external_id="77615",
            production_date=date(2026, 7, 11),
            responsible="Test",
            item_name=insumo.nombre,
            item_code=insumo.codigo_point,
            unit="KG",
            unit_cost=Decimal("91.5"),
            requested_quantity=Decimal("50"),
            produced_quantity=Decimal("50"),
            is_insumo=True,
            raw_payload={"detail": {}},
            source_hash="prod-new-entry-1",
        )
        service = PointMovementSyncService(production_extractor=FakeProductionExtractor([production_line]))

        job = service.run_production_sync(start_date=date(2026, 7, 11), end_date=date(2026, 7, 11))

        self.assertEqual(job.status, "SUCCESS")
        movimiento = MovimientoInventario.objects.get(source_hash="prod-new-entry-1")
        self.assertEqual(movimiento.insumo, insumo)
        existencia = ExistenciaInsumo.objects.get(insumo=insumo, almacen="CUARTO_FRIO")
        self.assertEqual(existencia.stock_actual, Decimal("50"))

    def test_point_sync_creates_pending_match_on_quantity_discrepancy(self):
        # Test discrepancy: Point finds bitacora but quantity doesn't match
        from maestros.models import PointPendingMatch
        from operacion.models import BitacoraOperativa, BitacoraOperativaLinea

        insumo = Insumo.objects.create(
            nombre="Relleno Discrepancia",
            codigo_point="RD-001",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        receta = Receta.objects.create(nombre="Test", codigo_point="RD-001", hash_contenido="hash-rd")

        # Create bitacora entry with quantity 5
        bitacora = BitacoraOperativa.objects.create(tipo="HORNOS", fecha=date(2026, 7, 11))
        linea = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora,
            receta=receta,
        )
        MovimientoInventario.objects.create(
            linea_bitacora=linea,
            insumo=insumo,
            tipo=MovimientoInventario.TIPO_ENTRADA,
            almacen="CFP_1_1",
            cantidad=Decimal("5"),
            fecha=datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc),
            referencia=f"BITACORA:HORNOS:{linea.id}",
            trazabilidad={},
        )
        from inventario.services_existencias import aplicar_delta
        aplicar_delta(insumo, "CFP_1_1", Decimal("5"))

        # Point has different quantity (8 instead of 5)
        production_line = FakeProductionLine(
            branch={"external_id": "CEDIS", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            production_external_id="21216",
            detail_external_id="77615",
            production_date=date(2026, 7, 11),
            responsible="Test",
            item_name=insumo.nombre,
            item_code=insumo.codigo_point,
            unit="PZA",
            unit_cost=Decimal("0"),
            requested_quantity=Decimal("8"),
            produced_quantity=Decimal("8"),
            is_insumo=True,
            raw_payload={"detail": {}},
            source_hash="prod-discrepancy-1",
        )
        service = PointMovementSyncService(production_extractor=FakeProductionExtractor([production_line]))

        job = service.run_production_sync(start_date=date(2026, 7, 11), end_date=date(2026, 7, 11))

        # Should create PointPendingMatch for reconciliation
        pending = PointPendingMatch.objects.filter(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo=insumo.codigo_point,
            method="BITACORA_MOVEMENT_RECONCILIATION"
        ).first()
        self.assertIsNotNone(pending, "Should create PointPendingMatch for quantity discrepancy")

        # Stock should not change
        existencia = ExistenciaInsumo.objects.get(insumo=insumo, almacen="CFP_1_1")
        self.assertEqual(existencia.stock_actual, Decimal("5"), "Stock should remain unchanged on discrepancy")

    def test_point_sync_retries_dont_duplicate_pending_match(self):
        # Test retries: repeated calls don't create multiple pending matches
        from maestros.models import PointPendingMatch
        from operacion.models import BitacoraOperativa, BitacoraOperativaLinea

        insumo = Insumo.objects.create(
            nombre="Relleno Retry",
            codigo_point="RR-001",
            unidad_base=self.unit,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        receta = Receta.objects.create(nombre="Retry", codigo_point="RR-001", hash_contenido="hash-rr")

        bitacora = BitacoraOperativa.objects.create(tipo="HORNOS", fecha=date(2026, 7, 11))
        linea = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora,
            receta=receta,
        )
        MovimientoInventario.objects.create(
            linea_bitacora=linea,
            insumo=insumo,
            tipo=MovimientoInventario.TIPO_ENTRADA,
            almacen="CFP_1_1",
            cantidad=Decimal("3"),
            fecha=datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc),
            referencia=f"BITACORA:HORNOS:{linea.id}",
            trazabilidad={},
        )
        from inventario.services_existencias import aplicar_delta
        aplicar_delta(insumo, "CFP_1_1", Decimal("3"))

        production_line = FakeProductionLine(
            branch={"external_id": "CEDIS", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            production_external_id="21216",
            detail_external_id="77615",
            production_date=date(2026, 7, 11),
            responsible="Test",
            item_name=insumo.nombre,
            item_code=insumo.codigo_point,
            unit="PZA",
            unit_cost=Decimal("0"),
            requested_quantity=Decimal("7"),
            produced_quantity=Decimal("7"),
            is_insumo=True,
            raw_payload={"detail": {}},
            source_hash="prod-retry-1",
        )
        service = PointMovementSyncService(production_extractor=FakeProductionExtractor([production_line]))

        # Run twice
        job1 = service.run_production_sync(start_date=date(2026, 7, 11), end_date=date(2026, 7, 11))
        job2 = service.run_production_sync(start_date=date(2026, 7, 11), end_date=date(2026, 7, 11))

        # Should still only have one PointPendingMatch
        pending_count = PointPendingMatch.objects.filter(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo=insumo.codigo_point,
            method="BITACORA_MOVEMENT_RECONCILIATION"
        ).count()
        self.assertEqual(pending_count, 1, "Retries should not duplicate PointPendingMatch")

    def test_transfer_sync_desactiva_detalles_ausentes_del_snapshot_actual_del_mismo_folio(self):
        pendiente = FakeTransferLine(
            origin_branch={"external_id": "8", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
            destination_branch={"external_id": "25", "name": "Sucursal Leyva", "status": "ACTIVE", "metadata": {}},
            transfer_external_id="36654",
            detail_external_id="519914",
            registered_at=datetime(2026, 7, 13, 18, 0, tzinfo=timezone.utc),
            sent_at=None,
            received_at=None,
            requested_by="Sucursal Leyva",
            sent_by="",
            received_by="",
            item_name="Pay de Queso Grande",
            item_code="0001",
            unit="PZA",
            unit_cost=Decimal("0"),
            requested_quantity=Decimal("2"),
            sent_quantity=Decimal("0"),
            received_quantity=Decimal("0"),
            is_insumo=False,
            is_received=False,
            is_cancelled=False,
            is_finalized=False,
            is_open=True,
            raw_payload={"transfer": {"isEnviado": False}, "detail": {"PK_Transf_Det": 519914}},
            source_hash="point-36654-519914",
        )
        pendiente_2 = replace(
            pendiente,
            detail_external_id="519915",
            item_name="Pay de Queso Mediano",
            item_code="0002",
            source_hash="point-36654-519915",
        )
        extractor = FakeTransferExtractor([pendiente, pendiente_2])
        service = PointMovementSyncService(transfer_extractor=extractor)
        service.run_transfer_sync(start_date=date(2026, 7, 13), end_date=date(2026, 7, 13))

        enviada = replace(
            pendiente,
            detail_external_id="520176",
            sent_at=datetime(2026, 7, 13, 19, 50, tzinfo=timezone.utc),
            received_at=datetime(2026, 7, 14, 18, 0, tzinfo=timezone.utc),
            requested_quantity=Decimal("3"),
            sent_quantity=Decimal("3"),
            received_quantity=Decimal("3"),
            is_received=True,
            is_finalized=True,
            is_open=False,
            raw_payload={"transfer": {"isEnviado": True}, "detail": {"PK_Transf_Det": 520176}},
            source_hash="point-36654-520176",
        )
        enviada_2 = replace(
            enviada,
            detail_external_id="520177",
            item_name="Pay de Queso Mediano",
            item_code="0002",
            source_hash="point-36654-520177",
        )
        extractor.rows = [enviada, enviada_2]

        service.run_transfer_sync(start_date=date(2026, 7, 13), end_date=date(2026, 7, 14))

        self.assertEqual(PointTransferLine.objects.filter(transfer_external_id="36654").count(), 4)
        anterior = PointTransferLine.objects.get(source_hash=pendiente.source_hash)
        anterior_2 = PointTransferLine.objects.get(source_hash=pendiente_2.source_hash)
        self.assertFalse(anterior.is_open)
        self.assertFalse(anterior.is_current_snapshot)
        self.assertFalse(anterior_2.is_open)
        self.assertFalse(anterior_2.is_current_snapshot)
        self.assertEqual(
            set(
                PointTransferLine.objects.filter(
                    transfer_external_id="36654",
                    sent_at__isnull=False,
                ).values_list("detail_external_id", flat=True)
            ),
            {"520176", "520177"},
        )
