from decimal import Decimal
import json

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from logistica.models import (
    DiscrepanciaLogistica,
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaCargaSucursalEvento,
    RutaEntrega,
    Unidad,
)
from logistica.services_carga_sucursal import (
    CargaSucursalError,
    ConflictoIdempotencia,
    guardar_carga_sucursal,
)
from logistica.services_contexto_operativo import construir_contexto_operativo
from pos_bridge.models import PointBranch, PointTransferLine


User = get_user_model()


class PersistenciaCargaSucursalTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="AT-SUC", nombre="Sucursal Atómica", activa=True)
        self.unidad = Unidad.objects.create(codigo="AT-01", descripcion="Unidad atómica", sucursal=self.sucursal)
        self.user = User.objects.create_user(username="chofer.atomico")
        self.jefe = User.objects.create_user(username="jefe.atomico")
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal)
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta atómica",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre=self.sucursal.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
        )
        self.punto_cedis = PuntoLogistico.objects.create(
            nombre="CEDIS Atómico",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.560000",
            longitud="-108.460000",
        )
        self.cedis = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto_cedis, orden=1)
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=2)
        self.checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)
        self.linea = RutaCargaChecklistLinea.objects.create(
            checklist=self.checklist,
            parada=self.parada,
            transfer_external_id="AT-T-1",
            detail_external_id="AT-D-1",
            source_hash="at-source-1",
            item_name="Pastel atómico",
            unit="PZA",
            cantidad_solicitada=Decimal("10"),
            cantidad_enviada_esperada=Decimal("10"),
        )

    def test_evento_cliente_es_unico_por_ruta(self):
        RutaCargaSucursalEvento.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            client_event_id="evt-1",
            payload_hash="a" * 64,
            contexto_version="b" * 64,
            respuesta={"ok": True},
            creado_por=self.user,
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                RutaCargaSucursalEvento.objects.create(
                    ruta=self.ruta,
                    parada=self.parada,
                    client_event_id="evt-1",
                    payload_hash="c" * 64,
                    contexto_version="b" * 64,
                    respuesta={},
                    creado_por=self.user,
                )

    def test_discrepancias_de_carga_y_recepcion_son_eventos_separados(self):
        carga = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_CARGA,
            cantidad_enviada=Decimal("10"),
            cantidad_cargada=Decimal("8"),
            motivo="faltante_fisico",
            asignado_a=self.jefe,
            creado_por=self.user,
        )
        recepcion = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
            cantidad_enviada=Decimal("10"),
            cantidad_cargada=Decimal("8"),
            cantidad_recibida=Decimal("7"),
            motivo="faltante_fisico",
            asignado_a=self.jefe,
            creado_por=self.user,
        )

        self.assertNotEqual(carga.id, recepcion.id)
        self.assertEqual(carga.estado, DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE)
        self.assertEqual(recepcion.estado, DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE)

    def test_no_permite_dos_discrepancias_abiertas_del_mismo_origen(self):
        datos = {
            "ruta": self.ruta,
            "parada": self.parada,
            "linea_carga": self.linea,
            "origen": DiscrepanciaLogistica.ORIGEN_CARGA,
            "cantidad_enviada": Decimal("10"),
            "cantidad_cargada": Decimal("8"),
            "motivo": "faltante_fisico",
            "asignado_a": self.jefe,
            "creado_por": self.user,
        }
        DiscrepanciaLogistica.objects.create(**datos)

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                DiscrepanciaLogistica.objects.create(**datos)


class GuardarCargaSucursalTests(PersistenciaCargaSucursalTests):
    def setUp(self):
        super().setUp()
        self.linea_2 = RutaCargaChecklistLinea.objects.create(
            checklist=self.checklist,
            parada=self.parada,
            transfer_external_id="AT-T-2",
            detail_external_id="AT-D-2",
            source_hash="at-source-2",
            item_name="Pay atómico",
            unit="PZA",
            cantidad_solicitada=Decimal("5"),
            cantidad_enviada_esperada=Decimal("5"),
        )
        self.contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user)

    def payload(self, *, cantidad_1="10", cantidad_2="5", motivo_1="", linea_2_id=None):
        return [
            {
                "linea_id": self.linea.id,
                "source_hash": self.linea.source_hash,
                "cantidad_cargada": Decimal(cantidad_1),
                "motivo_diferencia": motivo_1,
                "notas": "",
            },
            {
                "linea_id": linea_2_id or self.linea_2.id,
                "source_hash": self.linea_2.source_hash,
                "cantidad_cargada": Decimal(cantidad_2),
                "motivo_diferencia": "",
                "notas": "",
            },
        ]

    def guardar(self, *, event_id="evt-guardar-1", lineas=None):
        return guardar_carga_sucursal(
            actor=self.user,
            ruta=self.ruta,
            contexto_token=self.contexto.token,
            parada_id=self.parada.id,
            client_event_id=event_id,
            lineas=lineas or self.payload(),
        )

    def asociar_point(self, linea, *, enviada, sent="0"):
        origin = PointBranch.objects.create(external_id=f"AT-O-{linea.id}", name="CEDIS")
        destination = PointBranch.objects.create(
            external_id=f"AT-D-{linea.id}",
            name=self.sucursal.nombre,
            erp_branch=self.sucursal,
        )
        point_line = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_destination_branch=self.sucursal,
            transfer_external_id=f"AT-P-{linea.id}",
            detail_external_id=f"AT-PD-{linea.id}",
            source_hash=f"at-point-{linea.id}",
            registered_at=timezone.now(),
            sent_at=timezone.now() if enviada else None,
            item_name=linea.item_name,
            item_code=linea.item_code,
            unit=linea.unit,
            requested_quantity=linea.cantidad_solicitada,
            sent_quantity=sent,
            raw_payload={"transfer": {"isEnviado": enviada}},
        )
        linea.point_transfer_line = point_line
        linea.save(update_fields=["point_transfer_line", "actualizado_en"])
        return point_line

    def test_guarda_todas_las_lineas_de_la_sucursal(self):
        respuesta = self.guardar()

        self.linea.refresh_from_db()
        self.linea_2.refresh_from_db()
        self.assertEqual(self.linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(self.linea_2.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(respuesta["parada_id"], self.parada.id)
        self.assertEqual(respuesta["lineas_guardadas"], 2)

    def test_producto_pendiente_point_no_bloquea_los_que_ya_estan_enviados(self):
        self.asociar_point(self.linea, enviada=False, sent="10")
        self.contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user)

        respuesta = self.guardar(lineas=[self.payload()[1]])

        self.linea.refresh_from_db()
        self.linea_2.refresh_from_db()
        self.assertEqual(self.linea.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertEqual(self.linea_2.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(respuesta["lineas_guardadas"], 1)
        self.assertEqual(RutaCargaSucursalEvento.objects.count(), 1)

    def test_enviado_cero_permanece_visible_sin_convertirse_en_cargada(self):
        self.asociar_point(self.linea, enviada=True, sent="0")
        self.linea.cantidad_enviada_esperada = Decimal("0")
        self.linea.cantidad_cargada = Decimal("0")
        self.linea.estatus = RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED
        self.linea.save(update_fields=["cantidad_enviada_esperada", "cantidad_cargada", "estatus", "actualizado_en"])
        self.contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user)

        self.guardar(lineas=[self.payload(cantidad_1="0")[1]])

        self.linea.refresh_from_db()
        self.assertEqual(self.linea.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(self.linea.cantidad_cargada, Decimal("0"))

    def test_diferencia_exige_motivo_y_revierte_toda_la_sucursal(self):
        with self.assertRaises(CargaSucursalError) as error:
            self.guardar(lineas=self.payload(cantidad_1="8"))

        self.linea.refresh_from_db()
        self.linea_2.refresh_from_db()
        self.assertEqual(error.exception.codigo, "motivo_diferencia_requerido")
        self.assertEqual(self.linea.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertEqual(self.linea_2.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertFalse(RutaCargaSucursalEvento.objects.exists())

    def test_linea_ajena_revierte_toda_la_sucursal(self):
        with self.assertRaises(CargaSucursalError) as error:
            self.guardar(lineas=self.payload(linea_2_id=999999))

        self.assertEqual(error.exception.codigo, "producto_no_vigente")
        self.assertFalse(RutaCargaSucursalEvento.objects.exists())
        self.assertFalse(
            RutaCargaChecklistLinea.objects.filter(
                pk__in=[self.linea.id, self.linea_2.id],
                estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            ).exists()
        )

    def test_reintento_identico_devuelve_resultado_sin_duplicar(self):
        primera = self.guardar()
        segunda = self.guardar()

        self.assertEqual(primera, segunda)
        self.assertEqual(RutaCargaSucursalEvento.objects.count(), 1)


class CargaSucursalApiTests(GuardarCargaSucursalTests):
    def url(self):
        return reverse(
            "api_logistica_ruta_carga_sucursal",
            kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id},
        )

    def api_payload(self, *, token=None, event_id="evt-api-1"):
        return {
            "contexto_token": token or self.contexto.token,
            "version_checklist": self.contexto.version_checklist,
            "client_event_id": event_id,
            "lineas": [
                {
                    "linea_id": self.linea.id,
                    "source_hash": self.linea.source_hash,
                    "cantidad_cargada": "10",
                    "motivo_diferencia": "",
                    "notas": "",
                },
                {
                    "linea_id": self.linea_2.id,
                    "source_hash": self.linea_2.source_hash,
                    "cantidad_cargada": "5",
                    "motivo_diferencia": "",
                    "notas": "",
                },
            ],
        }

    def test_api_guarda_sucursal_completa(self):
        self.client.force_login(self.user)

        response = self.client.post(
            self.url(),
            data=json.dumps(self.api_payload()),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["lineas_guardadas"], 2)
        self.assertEqual(RutaCargaSucursalEvento.objects.count(), 1)

    def test_api_checklist_incluye_contexto_operativo(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse("api_logistica_ruta_carga_checklist", kwargs={"ruta_id": self.ruta.id})
        )

        self.assertEqual(response.status_code, 200, response.content)
        contexto = response.json()["contexto_operativo"]
        self.assertEqual(contexto["ruta_id"], self.ruta.id)
        self.assertEqual(contexto["chofer_autorizado_id"], self.repartidor.id)
        self.assertEqual(contexto["unidad_id"], self.unidad.id)
        self.assertTrue(contexto["token"])

    def test_api_ruta_activa_incluye_contexto_para_pwa(self):
        self.client.force_login(self.user)

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200, response.content)
        contexto = response.json()["contexto_operativo"]
        self.assertEqual(contexto["ruta_id"], self.ruta.id)
        self.assertEqual(contexto["tramo_id"], self.contexto.tramo_id)

    def test_api_acompanante_no_puede_guardar(self):
        user_acompanante = User.objects.create_user(username="acompanante.api")
        acompanante = Repartidor.objects.create(user=user_acompanante, sucursal=self.sucursal)
        self.ruta.acompanante = acompanante
        self.ruta.save(update_fields=["acompanante", "updated_at"])
        self.client.force_login(user_acompanante)

        response = self.client.post(
            self.url(),
            data=json.dumps(self.api_payload()),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403, response.content)
        self.assertFalse(RutaCargaSucursalEvento.objects.exists())

    def test_api_contexto_obsoleto_devuelve_conflicto_sin_escribir(self):
        self.client.force_login(self.user)
        self.linea.cantidad_enviada_esperada = Decimal("11")
        self.linea.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])

        response = self.client.post(
            self.url(),
            data=json.dumps(self.api_payload()),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 409, response.content)
        self.assertEqual(response.json()["error"], "checklist_actualizado")
        self.assertFalse(RutaCargaSucursalEvento.objects.exists())

    def test_reintento_distinto_rechaza_conflicto(self):
        self.guardar()

        with self.assertRaises(ConflictoIdempotencia):
            self.guardar(lineas=self.payload(cantidad_1="9", motivo_1=RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO))

        self.assertEqual(RutaCargaSucursalEvento.objects.count(), 1)
