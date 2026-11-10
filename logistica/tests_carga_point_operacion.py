import json
from datetime import timedelta
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from config.celery import app
from logistica.models import RutaEntrega
from logistica.services_carga_ruta import MODO_SYNC_CARGA_REPARTIDOR, VIGENCIA_SYNC_CARGA_REPARTIDOR
from logistica.tasks import actualizar_carga_point_repartidor
from pos_bridge.models import PointSyncJob


class ColaCargaPointTests(SimpleTestCase):
    def test_carga_point_tiene_cola_distinta_de_ventas(self):
        router = app.amqp.router
        self.assertEqual(
            router.route({}, actualizar_carga_point_repartidor.name)["queue"].name,
            "logistica_point",
        )
        self.assertEqual(router.route({}, "pos_bridge.daily_sales_sync")["queue"].name, "celery")


class AlertaCargaPointTests(TestCase):
    def test_reporta_solo_solicitudes_vencidas_incluso_despues_de_un_reintento(self):
        ruta = RutaEntrega.objects.create(nombre="Ruta prueba")
        vencimiento = timezone.now() - VIGENCIA_SYNC_CARGA_REPARTIDOR
        parametros = {"mode": MODO_SYNC_CARGA_REPARTIDOR, "ruta_id": ruta.pk, "recarga_cedis_id": 42}

        def solicitud(status, started_at, **fields):
            return PointSyncJob.objects.create(
                job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
                status=status,
                started_at=started_at,
                parameters=fields.pop("parameters", parametros),
                **fields,
            )

        pendiente = solicitud(PointSyncJob.STATUS_PENDING, vencimiento)
        ejecutando = solicitud(PointSyncJob.STATUS_RUNNING, vencimiento)
        reintento = solicitud(
            PointSyncJob.STATUS_FAILED, vencimiento,
            error_message="La actualización venció en espera. Vuelve a intentarlo.",
        )
        solicitud(PointSyncJob.STATUS_PENDING, timezone.now())
        solicitud(PointSyncJob.STATUS_PENDING, vencimiento - timedelta(days=2))
        solicitud(PointSyncJob.STATUS_PENDING, vencimiento, parameters={"mode": "open_transfers"})
        solicitud(
            PointSyncJob.STATUS_SUCCESS, vencimiento + timedelta(minutes=1), finished_at=timezone.now(),
        )
        otra_recarga = solicitud(
            PointSyncJob.STATUS_PENDING, vencimiento, parameters={**parametros, "recarga_cedis_id": 43},
        )
        solicitud(PointSyncJob.STATUS_FAILED, vencimiento, error_message="El tramo cambió.")

        output = StringIO()
        call_command("listar_cargas_point_vencidas", stdout=output)
        alertas = json.loads(output.getvalue())
        self.assertEqual(
            [item["id"] for item in alertas], [pendiente.id, ejecutando.id, reintento.id, otra_recarga.id],
        )
        self.assertTrue(alertas[0]["sync_posterior"])
        self.assertFalse(alertas[-1]["sync_posterior"])
        self.assertEqual(alertas[0]["folio"], ruta.folio)
        self.assertEqual(alertas[0]["recarga_id"], 42)
        pendiente.refresh_from_db()
        self.assertEqual(pendiente.status, PointSyncJob.STATUS_PENDING)


    def test_carga_vencida_esperando_sesion_no_consulta_point(self):
        ruta = RutaEntrega.objects.create(nombre="Ruta prueba")
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            parameters={"mode": MODO_SYNC_CARGA_REPARTIDOR, "ruta_id": ruta.pk},
        )
        with (
            patch("logistica.tasks.point_account_session_lock") as lock,
            patch("logistica.tasks.sincronizar_checklist_carga_desde_point") as sync,
        ):
            lock.return_value.__enter__.return_value = False
            actualizar_carga_point_repartidor.run(job_id=job.pk)

        self.assertTrue(lock.call_args.kwargs["wait"])
        self.assertGreater(lock.call_args.kwargs["timeout_seconds"], 0)
        sync.assert_not_called()
        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        self.assertIn("venció en espera", job.error_message)
