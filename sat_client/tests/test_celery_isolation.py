from django.conf import settings
from django.test import SimpleTestCase

from config.celery import app


class SatCeleryIsolationTests(SimpleTestCase):
    def test_sat_does_not_block_sales_and_daily_report_pipeline(self):
        router = app.amqp.Router()
        sat_queue = router.route({}, "sat_client.ejecutar_descarga_sat_nocturna")["queue"].name
        self.assertEqual(sat_queue, "sat")
        for task in (
            "pos_bridge.daily_sales_sync",
            "reportes.operations_automation_cycle",
            "reportes.refresh_dg_operacion_snapshot",
            "reportes.enviar_reporte_diario",
        ):
            with self.subTest(task=task):
                self.assertEqual(router.route({}, task)["queue"].name, "celery")

    def test_redis_does_not_redeliver_a_four_hour_sat_download(self):
        timeout = settings.CELERY_VISIBILITY_TIMEOUT
        self.assertGreaterEqual(timeout, 24 * 60 * 60)
        self.assertEqual(settings.CELERY_BROKER_TRANSPORT_OPTIONS["visibility_timeout"], timeout)
        self.assertEqual(settings.CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS["visibility_timeout"], timeout)
