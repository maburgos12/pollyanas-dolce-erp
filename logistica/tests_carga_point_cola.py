import json
import threading
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase, TransactionTestCase
from django.db import close_old_connections, connection
from django.utils import timezone
from django.urls import reverse

from core.models import UserModuleAccess
from logistica import tests_carga_sucursal as fixtures
from pos_bridge.models import PointSyncJob
from logistica.tasks import actualizar_carga_point_repartidor
from logistica.services_carga_point import contexto_carga_point
from logistica.services_contexto_operativo import construir_contexto_operativo
from logistica.services_carga_sucursal import guardar_carga_sucursal


class CargaPointErpColaTests(TestCase):
    def setUp(self):
        fixtures.PersistenciaCargaSucursalTests.setUp(self)
        UserModuleAccess.objects.create(user=self.jefe, module="logistica", access="manage")
        self.client.force_login(self.jefe)
        self.url = reverse("logistica:ruta_detail", args=[self.ruta.pk])

    def test_erp_encola_sin_consultar_point_en_el_request_y_reutiliza_solicitud(self):
        with (
            patch("logistica.tasks.actualizar_carga_point_repartidor.apply_async") as enqueue,
            patch("logistica.services_carga_ruta.OpenTransferSyncService") as point,
        ):
            response = self.client.post(self.url, {"action": "sync_carga_point"}, HTTP_ACCEPT="application/json")
            self.assertEqual(response.status_code, 202)
            job_id = response.json()["solicitud_id"]
            again = self.client.post(self.url, {"action": "sync_carga_point"}, HTTP_ACCEPT="application/json")
        self.assertEqual(again.json()["solicitud_id"], job_id)
        enqueue.assert_called_once()
        point.assert_not_called()
        job = PointSyncJob.objects.get(pk=job_id)
        self.assertEqual(job.parameters["origen"], "erp")
        self.assertEqual(job.parameters["ruta_id"], self.ruta.pk)
        status = self.client.get(self.url, {"carga_point_estado": job_id})
        self.assertEqual(status.status_code, 202)

    def test_fallo_broker_erp_queda_registrado_y_se_incluye_en_alerta(self):
        with patch("logistica.tasks.actualizar_carga_point_repartidor.apply_async", side_effect=ConnectionError):
            response = self.client.post(self.url, {"action": "sync_carga_point"}, HTTP_ACCEPT="application/json")
        self.assertEqual(response.json()["estado"], "ERROR")
        output = StringIO()
        call_command("listar_cargas_point_vencidas", stdout=output)
        alertas = json.loads(output.getvalue())
        self.assertEqual(alertas[0]["ruta_id"], self.ruta.pk)
        self.assertEqual(alertas[0]["estado"], "ERROR")

    def test_consulta_estado_no_expone_solicitudes_de_otra_ruta(self):
        job = PointSyncJob.objects.create(job_type="transfers", parameters={"ruta_id": self.ruta.pk + 1})
        self.assertEqual(self.client.get(self.url, {"carga_point_estado": job.pk}).status_code, 404)

    def test_cambio_de_fecha_en_espera_no_importa_otro_dia(self):
        with patch("logistica.tasks.actualizar_carga_point_repartidor.apply_async"):
            response = self.client.post(self.url, {"action": "sync_carga_point"}, HTTP_ACCEPT="application/json")
        self.ruta.fecha_ruta -= timedelta(days=1)
        self.ruta.save(update_fields=["fecha_ruta"])
        with patch("logistica.services_carga_ruta.OpenTransferSyncService") as point:
            actualizar_carga_point_repartidor.run(job_id=response.json()["solicitud_id"])
        point.assert_not_called()
        job = PointSyncJob.objects.get(pk=response.json()["solicitud_id"])
        self.assertEqual(job.status, "FAILED")
        self.assertIn("fecha cambiaron", job.error_message)


class CargaPointContencionTests(TransactionTestCase):
    def setUp(self):
        fixtures.PersistenciaCargaSucursalTests.setUp(self)
        UserModuleAccess.objects.create(user=self.jefe, module="logistica", access="manage")

    def test_erp_y_app_esperan_el_candado_real_y_permiten_guardar_sin_tocar_capturas(self):
        from pos_bridge.services.point_account_session_lock import POINT_ACCOUNT_SESSION_LOCK_ID
        point = fixtures.GuardarCargaSucursalTests.asociar_point(self, self.linea, enviada=True, sent="10")
        # Captura y entrega previas de una parada anterior al CEDIS actual.
        from logistica.models import ParadaRuta, RutaCargaChecklistLinea
        anterior = ParadaRuta.objects.create(
            ruta=self.ruta, punto=self.punto, orden=0, estado="VISITADA", entrega_estado="ENTREGADA",
        )
        previa = RutaCargaChecklistLinea.objects.create(
            checklist=self.checklist, parada=anterior, source_hash="capturada-previa",
            item_name="Captura anterior", cantidad_cargada=3, cantidad_enviada_esperada=3,
            estatus="CARGADA", validado_por=self.user, validado_en=timezone.now(),
        )
        antes_linea = RutaCargaChecklistLinea.objects.filter(pk=previa.pk).values().get()
        antes_parada = ParadaRuta.objects.filter(pk=anterior.pk).values().get()
        sync_job = PointSyncJob.objects.create(job_type="transfers", status="SUCCESS")
        for origen, actor in (("erp", self.jefe), ("app", self.user)):
            params = {"mode": "logistica_carga_repartidor", "ruta_id": self.ruta.pk}
            if origen == "erp":
                params.update(origen="erp", contexto=contexto_carga_point(self.ruta))
            job = PointSyncJob.objects.create(job_type="transfers", triggered_by=actor, parameters=params)
            attempted = threading.Event()

            def worker():
                close_old_connections()
                try:
                    def observe(execute, sql, params, many, context):
                        result = execute(sql, params, many, context)
                        if "pg_try_advisory_lock" in sql:
                            attempted.set()
                        return result
                    with connection.execute_wrapper(observe):
                        actualizar_carga_point_repartidor.run(job_id=job.pk)
                finally:
                    connection.close()

            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(%s)", [POINT_ACCOUNT_SESSION_LOCK_ID])
            try:
                with patch("logistica.services_carga_ruta.OpenTransferSyncService") as service:
                    service.return_value.sync_open_transfers.return_value = sync_job
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        future = pool.submit(worker)
                        try:
                            self.assertTrue(attempted.wait(3), "El worker debe intentar el candado ocupado de otra conexión")
                            self.assertFalse(future.done())
                            service.return_value.sync_open_transfers.assert_not_called()
                        finally:
                            with connection.cursor() as cursor:
                                cursor.execute("SELECT pg_advisory_unlock(%s)", [POINT_ACCOUNT_SESSION_LOCK_ID])
                        future.result(timeout=10)
                    self.assertEqual(service.return_value.sync_open_transfers.call_count, 2)
            finally:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(%s)", [POINT_ACCOUNT_SESSION_LOCK_ID])
            job.refresh_from_db()
            self.assertEqual(job.status, "SUCCESS", job.error_message)
            self.assertEqual(job.result_summary["estado"], "ACTUALIZADA")
            self.assertEqual(RutaCargaChecklistLinea.objects.filter(pk=previa.pk).values().get(), antes_linea)
            self.assertEqual(ParadaRuta.objects.filter(pk=anterior.pk).values().get(), antes_parada)
        linea = RutaCargaChecklistLinea.objects.get(checklist=self.checklist, point_transfer_line=point)
        contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user)
        receipt = guardar_carga_sucursal(
            actor=self.user, ruta=self.ruta, contexto_token=contexto.token,
            parada_id=self.parada.pk, client_event_id="despues-de-esperar",
            lineas=[{"linea_id": linea.pk, "source_hash": linea.source_hash, "cantidad_cargada": "10"}],
        )
        self.assertEqual(receipt["lineas_guardadas"], 1)
        self.assertEqual(RutaCargaChecklistLinea.objects.filter(pk=previa.pk).values().get(), antes_linea)
