from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

from django.test import TestCase

from control.models import MermaPOS
from core.models import Sucursal
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, UnidadMedida
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
        self.branch_cedis = Sucursal.objects.create(codigo="CEDIS", nombre="CEDIS")
        self.branch_produccion_crucero = Sucursal.objects.create(codigo="PRODUCCION_CRUCERO", nombre="Produccion Crucero")

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
        existencia = ExistenciaInsumo.objects.get(insumo=insumo)
        self.assertEqual(existencia.stock_actual, Decimal("167.340"))

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
            destination_branch={"external_id": "8", "name": "CEDIS", "status": "ACTIVE", "metadata": {}},
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
        existencia = ExistenciaInsumo.objects.get(insumo=insumo)
        self.assertEqual(existencia.stock_actual, Decimal("15.270"))

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
