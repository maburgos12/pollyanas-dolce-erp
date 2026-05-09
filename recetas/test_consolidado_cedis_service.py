from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.models import Receta, SolicitudReabastoCedis, SolicitudReabastoCedisLinea
from recetas.services.consolidado_service import ConsolidadoNocturnoCedisService


class ConsolidadoNocturnoCedisServiceTests(TestCase):
    def test_skip_inventory_sync_when_cedis_snapshot_is_fresh(self):
        user = get_user_model().objects.create_user(username="tester")
        sucursal = Sucursal.objects.create(nombre="Matriz", codigo="MATRIZ", activa=True)
        receta = Receta.objects.create(nombre="Pastel prueba", codigo_point="P001")
        solicitud = SolicitudReabastoCedis.objects.create(
            fecha_operacion=timezone.localdate(),
            sucursal=sucursal,
            estado=SolicitudReabastoCedis.ESTADO_ENVIADA,
            creado_por=user,
        )
        SolicitudReabastoCedisLinea.objects.create(
            solicitud=solicitud,
            receta=receta,
            solicitado=Decimal("3"),
            sugerido=Decimal("3"),
        )
        branch = PointBranch.objects.create(external_id="8", name="CEDIS")
        product = PointProduct.objects.create(external_id="P001", sku="P001", name="Pastel prueba")
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
        )
        PointInventorySnapshot.objects.create(
            branch=branch,
            product=product,
            stock=Decimal("0"),
            captured_at=timezone.now() - timedelta(minutes=10),
            sync_job=sync_job,
        )

        with override_settings(CONSOLIDADO_CEDIS_INVENTORY_FRESHNESS_MINUTES=180), patch(
            "recetas.services.consolidado_service.run_inventory_sync"
        ) as run_inventory_mock:
            open_transfer_sync_service = type(
                "FakeOpenTransferSyncService",
                (),
                {"sync_open_transfers": lambda self, **kwargs: None},
            )()
            consolidado = ConsolidadoNocturnoCedisService(
                open_transfer_sync_service=open_transfer_sync_service
            ).consolidar(
                fecha_operacion=timezone.localdate(),
                usuario=user,
                sincronizar_point=True,
                sincronizar_inventario_cedis=True,
                forzar_recalculo=True,
            )

        run_inventory_mock.assert_not_called()
        self.assertEqual(consolidado.productos_consolidados, 1)
        self.assertIn("snapshot_cedis_fresco", consolidado.metadata["inventory_sync_skipped_reason"])
