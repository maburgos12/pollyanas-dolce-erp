from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from api.logistica_serializers import ParadaRutaSerializer
from api.omnichannel_views import _serialize_delivery_detail
from core.models import Sucursal
from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import (
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaEntrega,
    SolicitudDomicilio,
    UbicacionRuta,
    Unidad,
)
from logistica.services_domicilio_assignment import assign_domicilio
from logistica.services_domicilio_route import sync_linked_domicilios_on_route_start


class DomicilioRouteLinkTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="ruta-domicilios-manager",
            email="ruta@example.com",
            password="unused",
        )
        self.sucursal = Sucursal.objects.create(
            codigo="DOM-RUTA",
            nombre="Domicilios Ruta",
        )
        self.unidad = Unidad.objects.create(
            codigo="DOM-RUTA-U1",
            descripcion="Unidad domicilios ruta",
            sucursal=self.sucursal,
        )
        self.repartidor_user = get_user_model().objects.create_user(
            username="ruta-domicilios-driver",
            password="unused",
        )
        self.repartidor = Repartidor.objects.create(
            user=self.repartidor_user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )

    def _solicitud(self, suffix: str, *, latitud="24.809064", longitud="-107.394011"):
        cliente = Cliente.objects.create(
            nombre=f"Cliente {suffix}",
            telefono=f"6671234{suffix[-3:]:0>3}",
        )
        direccion = DireccionCliente.objects.create(
            cliente=cliente,
            alias="Casa",
            direccion=f"Av. Obregón {suffix}",
            referencias=f"Portón {suffix}",
            latitud=Decimal(latitud),
            longitud=Decimal(longitud),
            place_id=f"place-{suffix}",
        )
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            direccion_entrega=direccion,
            descripcion=f"Pedido {suffix}",
            monto_estimado=Decimal("565.00"),
            canal=PedidoCliente.CANAL_TELEFONO,
            point_note_id=f"POINT-{suffix}",
            point_note_folio=f"F-{suffix}",
            point_note_snapshot={
                "pk_nota": f"POINT-{suffix}",
                "lines": [
                    {
                        "point_code": "P001",
                        "description": "Pastel",
                        "quantity": "1",
                        "unit_price": "565.00",
                        "discount": "0.00",
                        "line_total": "565.00",
                    }
                ],
            },
        )
        solicitud = SolicitudDomicilio.objects.create(
            pedido_cliente=pedido,
            cliente=cliente,
            direccion_cliente=direccion,
            cliente_nombre=cliente.nombre,
            cliente_telefono=cliente.telefono,
            direccion=direccion.direccion,
            canal_origen=PedidoCliente.CANAL_TELEFONO,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        solicitud.estatus = SolicitudDomicilio.ESTATUS_PREPARANDO
        solicitud.save(update_fields=["estatus"])
        solicitud.estatus = SolicitudDomicilio.ESTATUS_LISTO
        solicitud.save(update_fields=["estatus"])
        return solicitud

    def test_assignment_creates_one_canonical_route_stop(self):
        solicitud = self._solicitud("101")

        result = assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )

        solicitud.refresh_from_db()
        self.assertTrue(result["route_linked"])
        self.assertEqual(result["ruta_id"], solicitud.parada_ruta.ruta_id)
        self.assertEqual(result["parada_id"], solicitud.parada_ruta_id)
        self.assertEqual(result["ruta_folio"], solicitud.parada_ruta.ruta.folio)
        self.assertEqual(solicitud.route_sequence, solicitud.parada_ruta.orden)
        self.assertEqual(solicitud.parada_ruta.punto.tipo, PuntoLogistico.TIPO_DOMICILIO)
        self.assertEqual(
            solicitud.parada_ruta.punto.direccion_cliente_id,
            solicitud.direccion_cliente_id,
        )
        self.assertEqual(RutaEntrega.objects.count(), 1)
        self.assertEqual(ParadaRuta.objects.count(), 1)
        self.assertEqual(
            PuntoLogistico.objects.filter(
                tipo=PuntoLogistico.TIPO_DOMICILIO,
            ).count(),
            1,
        )

    def test_assignment_replay_and_second_order_do_not_duplicate_or_split_route(self):
        first = self._solicitud("201")
        second = self._solicitud(
            "202",
            latitud="24.809064",
            longitud="-107.394011",
        )

        first_result = assign_domicilio(
            solicitud_id=first.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        replay = assign_domicilio(
            solicitud_id=first.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        second_result = assign_domicilio(
            solicitud_id=second.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )

        self.assertTrue(replay["idempotent"])
        self.assertEqual(replay["parada_id"], first_result["parada_id"])
        self.assertEqual(second_result["ruta_id"], first_result["ruta_id"])
        self.assertNotEqual(second_result["parada_id"], first_result["parada_id"])
        self.assertEqual(RutaEntrega.objects.count(), 1)
        self.assertEqual(ParadaRuta.objects.count(), 2)
        self.assertEqual(
            PuntoLogistico.objects.filter(
                tipo=PuntoLogistico.TIPO_DOMICILIO,
            ).count(),
            2,
        )

    def test_reassignment_before_departure_moves_same_stop_to_new_driver_route(self):
        solicitud = self._solicitud("301")
        first_result = assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        second_unit = Unidad.objects.create(
            codigo="DOM-RUTA-U2",
            descripcion="Unidad domicilios ruta 2",
            sucursal=self.sucursal,
        )
        second_user = get_user_model().objects.create_user(
            username="ruta-domicilios-driver-2",
            password="unused",
        )
        second_driver = Repartidor.objects.create(
            user=second_user,
            sucursal=self.sucursal,
            unidad_asignada=second_unit,
        )

        moved = assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=second_driver.id,
            audit_user=self.user,
        )

        solicitud.refresh_from_db()
        self.assertNotEqual(moved["ruta_id"], first_result["ruta_id"])
        self.assertEqual(moved["parada_id"], first_result["parada_id"])
        self.assertEqual(solicitud.parada_ruta.ruta.repartidor_id, second_driver.id)
        self.assertFalse(
            RutaEntrega.objects.get(pk=first_result["ruta_id"]).paradas.exists()
        )
        self.assertEqual(ParadaRuta.objects.count(), 1)
        self.assertEqual(
            PuntoLogistico.objects.filter(
                tipo=PuntoLogistico.TIPO_DOMICILIO,
            ).count(),
            1,
        )

    def test_driver_route_stop_contains_order_and_customer_information(self):
        solicitud = self._solicitud("401")
        assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        solicitud.refresh_from_db()

        parada = ParadaRuta.objects.select_related(
            "solicitud_domicilio",
            "solicitud_domicilio__direccion_cliente",
            "solicitud_domicilio__pedido_cliente",
        ).get(pk=solicitud.parada_ruta_id)
        payload = ParadaRutaSerializer(parada).data

        self.assertEqual(payload["domicilio"]["id"], solicitud.id)
        self.assertEqual(payload["domicilio"]["cliente_nombre"], solicitud.cliente_nombre)
        self.assertEqual(payload["domicilio"]["cliente_telefono"], solicitud.cliente_telefono)
        self.assertEqual(payload["domicilio"]["direccion"], solicitud.direccion)
        self.assertEqual(payload["domicilio"]["folio_point"], "F-401")
        self.assertEqual(payload["domicilio"]["productos"][0]["codigo"], "P001")
        self.assertEqual(payload["domicilio"]["total"], "565.00")

    def test_driver_active_route_supports_domicilio_only_route_without_point_transfer_sync(self):
        solicitud = self._solicitud("451")
        assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        solicitud.refresh_from_db()
        self.client.force_login(self.repartidor_user)

        response = self.client.get("/api/logistica/rutas/activa/")

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["ruta"]["id"], solicitud.parada_ruta.ruta_id)
        self.assertEqual(response.json()["paradas"][0]["domicilio"]["id"], solicitud.id)

    def test_driver_mobile_route_does_not_overlay_footer_or_show_branch_load_for_domicilio_only(self):
        source = (
            Path(__file__).resolve().parent
            / "templates"
            / "logistica"
            / "pwa.html"
        ).read_text(encoding="utf-8")

        self.assertIn("position: static;", source)
        self.assertIn(
            "const soloDomicilios = paradas.length > 0 && paradas.every",
            source,
        )
        self.assertIn('${soloDomicilios ? "" : `', source)

    def test_call_center_detail_contains_live_route_without_other_customer_data(self):
        solicitud = self._solicitud("501")
        assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        solicitud.refresh_from_db()
        ruta = solicitud.parada_ruta.ruta
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.hora_inicio_real = timezone.now()
        ruta.save(update_fields=["estatus", "hora_inicio_real"])
        UbicacionRuta.objects.create(
            ruta=ruta,
            repartidor=self.repartidor,
            unidad=self.unidad,
            latitud=Decimal("24.810000"),
            longitud=Decimal("-107.395000"),
            precision_metros=Decimal("7.50"),
        )

        payload = _serialize_delivery_detail(solicitud)

        self.assertEqual(payload["ruta"]["id"], ruta.id)
        self.assertEqual(payload["ruta"]["folio"], ruta.folio)
        self.assertEqual(payload["ruta"]["estatus"], RutaEntrega.ESTATUS_EN_RUTA)
        self.assertEqual(payload["ruta"]["parada"]["id"], solicitud.parada_ruta_id)
        self.assertEqual(payload["ruta"]["parada"]["orden"], 1)
        self.assertEqual(payload["ruta"]["ultima_ubicacion"]["latitud"], Decimal("24.810000"))
        self.assertEqual(payload["ruta"]["ultima_ubicacion"]["longitud"], Decimal("-107.395000"))
        self.assertNotIn("cliente_nombre", payload["ruta"])

    def test_route_start_moves_linked_ready_order_to_en_ruta_once(self):
        solicitud = self._solicitud("601")
        assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        solicitud.refresh_from_db()
        ruta = solicitud.parada_ruta.ruta

        first = sync_linked_domicilios_on_route_start(
            ruta=ruta,
            actor=self.repartidor_user,
        )
        replay = sync_linked_domicilios_on_route_start(
            ruta=ruta,
            actor=self.repartidor_user,
        )

        solicitud.refresh_from_db()
        self.assertEqual(first, 1)
        self.assertEqual(replay, 0)
        self.assertEqual(solicitud.estatus, SolicitudDomicilio.ESTATUS_EN_RUTA)

    def test_pwa_delivery_button_closes_same_canonical_order(self):
        solicitud = self._solicitud("701")
        assign_domicilio(
            solicitud_id=solicitud.id,
            repartidor_id=self.repartidor.id,
            audit_user=self.user,
        )
        solicitud.refresh_from_db()
        ruta = solicitud.parada_ruta.ruta
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.hora_inicio_real = timezone.now()
        ruta.save(update_fields=["estatus", "hora_inicio_real"])
        sync_linked_domicilios_on_route_start(
            ruta=ruta,
            actor=self.repartidor_user,
        )
        self.client.force_login(self.user)

        response = self.client.post(
            f"/api/logistica/rutas/{ruta.id}/paradas/{solicitud.parada_ruta_id}/entrega/",
            data={
                "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                "notas": "Entrega confirmada sin geocerca validada.",
                "client_event_id": str(uuid4()),
                "client_context": {
                    "causa": "GPS_SIN_SENAL",
                    "client_timestamp": timezone.now().isoformat(),
                    "client_version": "test-domicilio-route",
                },
                "evidencias": [],
            },
            content_type="application/json",
        )

        solicitud.refresh_from_db()
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(solicitud.estatus, SolicitudDomicilio.ESTATUS_ENTREGADO)
        self.assertIsNotNone(solicitud.entregado_en)
