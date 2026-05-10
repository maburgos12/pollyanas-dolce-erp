from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch
from types import SimpleNamespace

from django.contrib.auth import get_user_model
from django.core import mail
from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone
from openpyxl import Workbook

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.models import ConsolidadoNocturnoCEDIS, Receta, SolicitudReabastoCedis, SolicitudReabastoCedisLinea
from recetas.services.consolidado_service import ConsolidadoNocturnoCedisService
from recetas.tasks.consolidado_nocturno import consolidado_nocturno_cedis, enviar_solicitudes_sucursal_cedis


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
            class FakeOpenTransferSyncService:
                def __init__(self):
                    self.calls = []

                def sync_open_transfers(self, **kwargs):
                    self.calls.append(kwargs)
                    return None

            open_transfer_sync_service = FakeOpenTransferSyncService()
            fecha_operacion = timezone.localdate()
            consolidado = ConsolidadoNocturnoCedisService(
                open_transfer_sync_service=open_transfer_sync_service
            ).consolidar(
                fecha_operacion=fecha_operacion,
                usuario=user,
                sincronizar_point=True,
                sincronizar_inventario_cedis=True,
                forzar_recalculo=True,
            )

        run_inventory_mock.assert_not_called()
        self.assertEqual(open_transfer_sync_service.calls[0]["fecha"], fecha_operacion - timedelta(days=1))
        self.assertEqual(consolidado.productos_consolidados, 1)
        self.assertIn("snapshot_cedis_fresco", consolidado.metadata["inventory_sync_skipped_reason"])
        self.assertEqual(consolidado.metadata["transfer_request_date"], (fecha_operacion - timedelta(days=1)).isoformat())

    def test_consolidar_invalidates_empty_consolidado_cache(self):
        user = get_user_model().objects.create_user(username="cache_tester")
        sucursal = Sucursal.objects.create(nombre="Matriz", codigo="MATRIZ", activa=True)
        receta = Receta.objects.create(nombre="Pastel cache", codigo_point="P002")
        fecha = timezone.localdate()
        cache.set(f"reabasto:consolidado:{fecha.isoformat()}", [], 300)
        solicitud = SolicitudReabastoCedis.objects.create(
            fecha_operacion=fecha,
            sucursal=sucursal,
            estado=SolicitudReabastoCedis.ESTADO_ENVIADA,
            creado_por=user,
        )
        SolicitudReabastoCedisLinea.objects.create(
            solicitud=solicitud,
            receta=receta,
            solicitado=Decimal("4"),
            sugerido=Decimal("4"),
        )
        ConsolidadoNocturnoCEDIS.objects.create(
            fecha_operacion=fecha,
            metadata={
                "solicitudes_sucursal_email_sent_at": "2026-05-09T05:30:24-07:00",
                "solicitudes_sucursal_email_recipients": ["produccion.carolina@pollyanasdolce.com"],
            },
        )

        consolidado = ConsolidadoNocturnoCedisService().consolidar(
            fecha_operacion=fecha,
            usuario=user,
            sincronizar_point=False,
            sincronizar_inventario_cedis=False,
            forzar_recalculo=True,
        )

        self.assertEqual(consolidado.productos_consolidados, 1)
        self.assertEqual(consolidado.total_solicitado, Decimal("4"))
        self.assertEqual(consolidado.metadata["solicitudes_sucursal_email_sent_at"], "2026-05-09T05:30:24-07:00")
        self.assertEqual(consolidado.metadata["solicitudes_sucursal_email_recipients"], ["produccion.carolina@pollyanasdolce.com"])

    def test_task_uses_previous_day_as_transfer_request_date_for_explicit_plan_date(self):
        captured = {}

        class FakeService:
            def consolidar(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(
                    id=99,
                    fecha_operacion=kwargs["fecha_operacion"],
                    estado="PLAN_GENERADO",
                    plan_produccion_id=10,
                    metadata={},
                    sync_job_id=20,
                    sucursales_esperadas=9,
                    sucursales_con_solicitud=8,
                    productos_consolidados=40,
                    total_plan_produccion=Decimal("78"),
                )

        with patch("recetas.tasks.consolidado_nocturno.ConsolidadoNocturnoCedisService", return_value=FakeService()):
            result = consolidado_nocturno_cedis(
                fecha_operacion="2026-05-09",
                sincronizar_point=True,
                enviar_excel_carolina=False,
            )

        self.assertEqual(captured["fecha_operacion"].isoformat(), "2026-05-09")
        self.assertEqual(captured["fecha_transferencias"].isoformat(), "2026-05-08")
        self.assertEqual(result["fecha_operacion"], "2026-05-09")

    @override_settings(
        EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
        DEFAULT_FROM_EMAIL="erp@pollyanasdolce.com",
        CONSOLIDADO_CEDIS_EXPORT_CC=["mauricio@pollyanasdolce.com"],
    )
    def test_email_solicitudes_copies_director_and_stores_audit_metadata(self):
        get_user_model().objects.create_user(
            username="carolina.cayetano",
            email="produccion.carolina@pollyanasdolce.com",
        )
        consolidado = ConsolidadoNocturnoCEDIS.objects.create(
            fecha_operacion=date(2026, 5, 10),
            productos_consolidados=37,
            sucursales_con_solicitud=7,
            sucursales_esperadas=9,
            metadata={"transfer_request_date": "2026-05-09"},
        )

        with patch("recetas.tasks.consolidado_nocturno._build_solicitudes_sucursal_workbook", return_value=Workbook()):
            result = enviar_solicitudes_sucursal_cedis(consolidado=consolidado, forzar_envio=True)

        self.assertEqual(result["recipients"], ["produccion.carolina@pollyanasdolce.com"])
        self.assertEqual(result["cc"], ["mauricio@pollyanasdolce.com"])
        self.assertEqual(len(mail.outbox), 2)
        self.assertEqual(mail.outbox[0].to, ["produccion.carolina@pollyanasdolce.com"])
        self.assertEqual(mail.outbox[0].cc, [])
        self.assertEqual(mail.outbox[1].to, ["mauricio@pollyanasdolce.com"])
        consolidado.refresh_from_db()
        self.assertEqual(consolidado.metadata["solicitudes_sucursal_email_cc"], ["mauricio@pollyanasdolce.com"])
        self.assertEqual(consolidado.metadata["solicitudes_sucursal_email_recipients"], ["produccion.carolina@pollyanasdolce.com"])
        self.assertEqual(
            [row["email"] for row in consolidado.metadata["solicitudes_sucursal_email_deliveries"]],
            ["produccion.carolina@pollyanasdolce.com", "mauricio@pollyanasdolce.com"],
        )
