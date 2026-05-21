from __future__ import annotations

from django.test import TestCase

from pos_bridge.models import PointProduct
from pos_bridge.services.point_purchase_resale_cost_service import PointPurchaseResaleCostSyncService
from reportes.models import ProductoReventaCosto


class PointPurchaseResaleCostSyncServiceTests(TestCase):
    def test_sync_purchase_payloads_creates_resale_cost_from_point_purchase_detail(self):
        product = PointProduct.objects.create(
            external_id="169",
            sku="0169",
            name="Pirotecnia Alegría G",
            category="Alegría",
            active=True,
        )
        service = PointPurchaseResaleCostSyncService()

        result = service.sync_purchase_payloads(
            apply=True,
            purchases=[
                {
                    "FK_Movimiento": "1570485",
                    "Folio": "F-1",
                    "Proveedor": "ALEGRIA DE GUADALAJARA S.A. DE C.V.",
                    "Sucursal": "Almacen",
                    "Fecha_compra": "2026-05-19T07:00:00",
                }
            ],
            details_by_purchase={
                "1570485": [
                    {
                        "Articulo": "Pirotecnia Alegría G",
                        "Cantidad": 1500,
                        "Unidad": "PZA",
                        "Costo_unitario": 13.27,
                        "Costo_total": 19909.19,
                    }
                ]
            },
        )

        self.assertEqual(result.created, 1)
        row = ProductoReventaCosto.objects.get(producto_point=product)
        self.assertEqual(row.fuente, ProductoReventaCosto.FUENTE_POINT_HISTORIAL)
        self.assertEqual(str(row.costo_unitario), "13.270000")
        self.assertEqual(row.fecha_vigencia.isoformat(), "2026-05-19")
        self.assertEqual(row.proveedor_nombre, "ALEGRIA DE GUADALAJARA S.A. DE C.V.")

    def test_sync_purchase_payloads_dry_run_does_not_write(self):
        PointProduct.objects.create(
            external_id="1005",
            sku="1250",
            name="ESPAGUETI DORADA",
            category="Granmark",
            active=True,
        )
        service = PointPurchaseResaleCostSyncService()

        result = service.sync_purchase_payloads(
            apply=False,
            purchases=[{"FK_Movimiento": "1570459", "Fecha_compra": "2026-05-18T07:00:00"}],
            details_by_purchase={
                "1570459": [
                    {
                        "Articulo": "ESPAGUETI DORADA",
                        "Cantidad": 120,
                        "Unidad": "PZA",
                        "Costo_unitario": 29.84,
                    }
                ]
            },
        )

        self.assertEqual(result.dry_run_created, 1)
        self.assertEqual(ProductoReventaCosto.objects.count(), 0)

    def test_sync_purchase_payloads_resolves_mothers_day_card_purchase_alias(self):
        product = PointProduct.objects.create(
            external_id="1032",
            sku="78421",
            name="TARJETA DE REGALO DIA DE LAS MADRES",
            category="Otros postres",
            active=True,
        )
        service = PointPurchaseResaleCostSyncService()

        result = service.sync_purchase_payloads(
            apply=True,
            purchases=[
                {
                    "FK_Movimiento": "1556640",
                    "Folio": "A15597",
                    "Proveedor": "IMPRENTA MARCOPOLO",
                    "Sucursal": "Almacen",
                    "Fecha_compra": "2026-05-08T07:00:00",
                }
            ],
            details_by_purchase={
                "1556640": [
                    {
                        "Articulo": "TARJETA HAPPY MOTHERS DAY",
                        "Cantidad": 200,
                        "Unidad": "PZA",
                        "Costo_unitario": 4.18,
                        "Costo_total": 835.2,
                    }
                ]
            },
        )

        self.assertEqual(result.created, 1)
        row = ProductoReventaCosto.objects.get(producto_point=product)
        self.assertEqual(str(row.costo_unitario), "4.180000")
        self.assertEqual(row.proveedor_nombre, "IMPRENTA MARCOPOLO")
