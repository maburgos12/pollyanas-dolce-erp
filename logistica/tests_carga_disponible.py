"""Regresión: una solicitud sin despacho no invalida carga enviada del tramo."""
import json
from datetime import timedelta
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import UserModuleAccess
from logistica import tests_carga_sucursal as fixtures
from logistica.models import EntregaRuta, ParadaRuta, RutaCargaChecklistLinea
from logistica.services_carga_point import contexto_carga_point
from logistica.services_carga_ruta import estado_sync_carga_repartidor
from logistica.services_contexto_operativo import construir_contexto_operativo
from logistica.tasks import actualizar_carga_point_repartidor
from pos_bridge.models import PointSyncJob


class CargaDisponiblePointTests(TestCase):
    def setUp(self):
        fixtures.PersistenciaCargaSucursalTests.setUp(self)
        UserModuleAccess.objects.create(user=self.jefe, module="logistica", access="manage")
        self.enviada = fixtures.GuardarCargaSucursalTests.asociar_point(self, self.linea, enviada=True, sent="10")
        self.job = PointSyncJob.objects.create(
            job_type="transfers", triggered_by=self.user,
            parameters={"mode": "logistica_carga_repartidor", "ruta_id": self.ruta.pk},
        )
        self.read = PointSyncJob.objects.create(job_type="transfers", status="SUCCESS", triggered_by=self.user)
        self.pendiente = self.enviada.__class__.objects.create(
            origin_branch=self.enviada.origin_branch, destination_branch=self.enviada.destination_branch,
            erp_destination_branch=self.sucursal, transfer_external_id="41839",
            detail_external_id="pendiente-1", source_hash="pendiente-1", registered_at=timezone.now(),
            is_open=True, sync_job=self.read, item_name="Solicitud aún sin despacho", requested_quantity=2, sent_quantity=0,
            raw_payload={"transfer": {"isEnviado": False}},
        )

    def test_carga_enviada_actualiza_aunque_otra_solicitud_no_este_enviada(self):
        self.assertEqual(estado_sync_carga_repartidor(self.ruta, solicitud=self.job), "ACTUALIZADA")

    def test_sin_carga_enviada_conserva_pendiente_sin_inventar_cantidades(self):
        self.linea.delete()
        self.assertEqual(estado_sync_carga_repartidor(self.ruta, solicitud=self.job), "PENDIENTE_ENVIO")
        self.pendiente.refresh_from_db()
        self.assertIsNone(self.pendiente.sent_at)
        self.assertEqual(self.pendiente.sent_quantity, 0)

    def test_solicitud_de_otro_dia_no_cambia_el_resultado_del_tramo(self):
        self.linea.delete()
        self.pendiente.registered_at -= timedelta(days=3)
        self.pendiente.save(update_fields=["registered_at"])
        self.assertEqual(estado_sync_carga_repartidor(self.ruta, solicitud=self.job), "SIN_CARGA")

    def test_pendiente_de_otro_tramo_no_afecta_actualizacion(self):
        from core.models import Sucursal
        otra = Sucursal.objects.create(codigo="FUTURA", nombre="Otra parada")
        self.pendiente.erp_destination_branch = otra
        self.pendiente.save(update_fields=["erp_destination_branch"])
        self.linea.delete()
        self.assertEqual(estado_sync_carga_repartidor(self.ruta, solicitud=self.job), "SIN_CARGA")

    def test_captura_y_entrega_previas_son_identicas_tras_actualizar_y_capturar_disponible(self):
        anterior = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=0,
                                            estado="VISITADA", entrega_estado="ENTREGADA")
        previa = RutaCargaChecklistLinea.objects.create(
            checklist=self.checklist, parada=anterior, source_hash="previa", item_name="Ya capturado",
            cantidad_enviada_esperada=3, cantidad_cargada=3, estatus="CARGADA",
            validado_por=self.user, validado_en=timezone.now(), client_event_id="previo",
        )
        entrega = EntregaRuta.objects.create(ruta=self.ruta, estatus="ENTREGADA", cliente_nombre="Prueba previa")
        antes = RutaCargaChecklistLinea.objects.filter(pk=previa.pk).values().get()
        antes_entrega = EntregaRuta.objects.filter(pk=entrega.pk).values().get()
        self.assertEqual(estado_sync_carga_repartidor(self.ruta, solicitud=self.job), "ACTUALIZADA")
        contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user)
        self.client.force_login(self.user)
        response = self.client.post(
            reverse("api_logistica_ruta_carga_sucursal", args=[self.ruta.pk, self.parada.pk]),
            data=json.dumps(dict(contexto_token=contexto.token, version_checklist=contexto.version_checklist, client_event_id="solo-disponible",
                                 lineas=[dict(linea_id=self.linea.pk, source_hash=self.linea.source_hash,
                                              cantidad_cargada="10")])),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200, response.content)
        recibo = response.json()
        self.assertEqual(recibo["lineas_guardadas"], 1)
        self.assertEqual(RutaCargaChecklistLinea.objects.filter(pk=previa.pk).values().get(), antes)
        self.assertEqual(EntregaRuta.objects.filter(pk=entrega.pk).values().get(), antes_entrega)
        self.assertFalse(RutaCargaChecklistLinea.objects.filter(point_transfer_line=self.pendiente).exists())

    def test_erp_y_app_reciben_actualizada_con_folio_pendiente_sin_alerta_de_fallo(self):
        for origen, actor in (("app", self.user), ("erp", self.jefe)):
            with self.subTest(origen=origen):
                parametros = {"mode": "logistica_carga_repartidor", "ruta_id": self.ruta.pk}
                if origen == "erp":
                    parametros.update(origen="erp", contexto=contexto_carga_point(self.ruta))
                job = PointSyncJob.objects.create(job_type="transfers", triggered_by=actor, parameters=parametros)
                self.read.triggered_by = actor
                self.read.started_at = timezone.now()
                self.read.save(update_fields=["triggered_by", "started_at"])
                with patch("logistica.services_carga_ruta.OpenTransferSyncService") as point:
                    point.return_value.sync_open_transfers.return_value = self.read
                    actualizar_carga_point_repartidor.run(job_id=job.pk)
                job.refresh_from_db()
                self.assertEqual(job.status, "SUCCESS", job.error_message)
                self.assertEqual(job.result_summary["estado"], "ACTUALIZADA")
                self.assertEqual(job.result_summary["pendientes_envio"][0]["folio"], "41839")
                self.client.force_login(actor)
                if origen == "erp":
                    response = self.client.get(reverse("logistica:ruta_detail", args=[self.ruta.pk]),
                                               {"carga_point_estado": job.pk})
                else:
                    response = self.client.get(reverse("api_logistica_ruta_carga_point_repartidor", args=[self.ruta.pk]))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["estado"], "ACTUALIZADA")
                output = StringIO()
                call_command("listar_cargas_point_vencidas", stdout=output)
                self.assertNotIn(job.pk, [a["id"] for a in json.loads(output.getvalue())])

    def test_pendiente_entre_sucursales_no_se_atribuye_a_carga_cedis(self):
        self.linea.delete()
        self.pendiente.origin_branch.name = "Sucursal de origen ajena"
        self.pendiente.origin_branch.save(update_fields=["name"])
        self.assertEqual(estado_sync_carga_repartidor(self.ruta, solicitud=self.job), "SIN_CARGA")

    def test_carga_ya_entregada_en_tramo_no_desaparece_del_resultado(self):
        self.parada.entrega_estado = "ENTREGADA"
        self.parada.save(update_fields=["entrega_estado"])
        self.assertEqual(estado_sync_carga_repartidor(self.ruta, solicitud=self.job), "ACTUALIZADA")
