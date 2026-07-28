from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from threading import Barrier
from uuid import uuid4

from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.db import IntegrityError, close_old_connections, connection, transaction
from django.test import Client, RequestFactory, TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from core.access import ROLE_LOGISTICA, ROLE_VENTAS
from core.models import Sucursal
from integraciones.models import PublicApiClient
from logistica.models import (
    EntregaEcommerce,
    Repartidor,
    SolicitudDomicilio,
    SolicitudDomicilioStatusOperation,
    Unidad,
)
from pos_bridge.models import PointBranch

from .models import Cliente, DireccionCliente, PedidoCliente, SeguimientoPedido
from .services import SucursalResolutionError, resolve_sucursal


class _CRMViewsTestBase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="ventas", password="pass123")
        ventas_group, _ = Group.objects.get_or_create(name=ROLE_VENTAS)
        self.user.groups.add(ventas_group)
        self.client.login(username="ventas", password="pass123")
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.point_branch = PointBranch.objects.create(
            external_id="1",
            name="Matriz Point",
            erp_branch=self.sucursal,
            status=PointBranch.STATUS_ACTIVE,
        )


class PedidoDomicilioDetailTests(TestCase):
    def setUp(self):
        self.ventas = User.objects.create_user(username="ventas-domicilio", password="pass123")
        ventas_group, _ = Group.objects.get_or_create(name=ROLE_VENTAS)
        self.ventas.groups.add(ventas_group)
        self.logistica = User.objects.create_user(username="logistica-domicilio", password="pass123")
        logistica_group, _ = Group.objects.get_or_create(name=ROLE_LOGISTICA)
        self.logistica.groups.add(logistica_group)
        self.sin_acceso = User.objects.create_user(username="sin-acceso", password="pass123")
        self.sucursal = Sucursal.objects.create(codigo="DOM", nombre="Centro", activa=True)
        self.cliente = Cliente.objects.create(
            nombre="María López",
            telefono="6671234567",
            email="maria@example.com",
        )
        self.direccion = DireccionCliente.objects.create(
            cliente=self.cliente,
            alias="Casa",
            direccion="Av. Central 123",
            referencias="Portón blanco",
            latitud=Decimal("25.790466"),
            longitud=Decimal("-108.985886"),
        )
        self.pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            direccion_entrega=self.direccion,
            descripcion="Pedido Point 18452",
            sucursal_ref=self.sucursal,
            canal=PedidoCliente.CANAL_FACEBOOK,
            monto_estimado=Decimal("565.00"),
            point_note_id="900001",
            point_note_folio="18452",
            point_note_snapshot={
                "pk_nota": "900001",
                "folio": "18452",
                "branch_name": "Centro Point",
                "sold_at": "2026-07-27T18:30:00-07:00",
                "total": "565.00",
                "invoiced": True,
                "payment_type": "TARJETA",
                "point_channel": "MOSTRADOR",
                "source_endpoint": "/Clientes/get_detalle_nota/",
                "lines": [
                    {
                        "point_code": "P001",
                        "description": "Pastel tres leches",
                        "quantity": "1",
                        "unit_price": "550.00",
                        "discount": "10.00",
                        "line_total": "540.00",
                    },
                    {
                        "point_code": "V001",
                        "description": "Velas",
                        "quantity": "1",
                        "unit_price": "25.00",
                        "discount": "0.00",
                        "line_total": "25.00",
                    },
                ],
            },
            point_note_fetched_at=timezone.now(),
            point_note_sold_at=timezone.now(),
        )
        self.unidad = Unidad.objects.create(
            codigo="MOTO-01",
            descripcion="Motocicleta reparto",
            sucursal=self.sucursal,
            placa="ABC-123",
        )
        self.repartidor_user = User.objects.create_user(
            username="repartidor-domicilio",
            first_name="Luis",
            last_name="Pérez",
        )
        self.repartidor = Repartidor.objects.create(
            user=self.repartidor_user,
            unidad_asignada=self.unidad,
            telefono="6677654321",
            sucursal=self.sucursal,
        )
        self.solicitud = SolicitudDomicilio.objects.create(
            pedido_cliente=self.pedido,
            cliente=self.cliente,
            direccion_cliente=self.direccion,
            cliente_nombre=self.cliente.nombre,
            cliente_telefono=self.cliente.telefono,
            direccion=self.direccion.direccion,
            canal_origen=PedidoCliente.CANAL_FACEBOOK,
            instrucciones_entrega="Tocar el timbre",
            repartidor=self.repartidor,
            unidad=self.unidad,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
            created_by=self.ventas,
        )
        self.api_client, _ = PublicApiClient.create_with_generated_key(
            nombre="Centro Operativo",
            created_by=self.ventas,
        )
        SolicitudDomicilioStatusOperation.objects.create(
            solicitud=self.solicitud,
            operation_id=uuid4(),
            api_client=self.api_client,
            repartidor=self.repartidor,
            requested_status=SolicitudDomicilio.ESTATUS_EN_RUTA,
            final_status=SolicitudDomicilio.ESTATUS_EN_RUTA,
            actor_id="call-center-1",
            actor_nombre="Call center",
            result_snapshot={"estatus": SolicitudDomicilio.ESTATUS_EN_RUTA},
        )

    def test_domicilio_detail_uses_single_canonical_order(self):
        self.client.force_login(self.ventas)
        response = self.client.get(
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["pedido"], self.pedido)
        self.assertEqual(response.context["solicitud"], self.solicitud)
        self.assertContains(response, "18452")
        self.assertContains(response, "Pastel tres leches")
        self.assertContains(response, "$565.00")
        self.assertContains(response, "Centro Point")
        self.assertContains(response, "TARJETA")
        self.assertContains(response, "MOSTRADOR")
        self.assertContains(response, "Facturada")
        self.assertContains(response, "/Clientes/get_detalle_nota/")
        self.assertContains(response, self.solicitud.get_estatus_display())
        self.assertContains(response, "Av. Central 123")
        self.assertContains(response, "Call center")
        self.assertNotContains(response, "name=\"point_note")
        self.assertNotContains(response, "Registrar seguimiento")

    def test_domicilio_detail_uses_snapshot_total_not_legacy_estimated_amount(self):
        PedidoCliente.objects.filter(pk=self.pedido.pk).update(monto_estimado=Decimal("999.99"))
        self.pedido.refresh_from_db()
        self.client.force_login(self.ventas)

        response = self.client.get(
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]),
        )

        self.assertContains(response, "Total Point")
        self.assertContains(response, "$565.00")
        self.assertNotContains(response, "$999.99")
        self.assertFalse(response.context["point_snapshot"]["is_legacy_fallback"])

    def test_domicilio_detail_marks_legacy_snapshot_fallback_explicitly(self):
        legacy_order = PedidoCliente.objects.create(
            cliente=self.cliente,
            direccion_entrega=self.direccion,
            descripcion="Pedido histórico",
            monto_estimado=Decimal("321.00"),
            point_note_id="LEGACY-1",
            point_note_folio="LEGACY-1",
            point_note_snapshot={"pk_nota": "LEGACY-1", "total": "321.00"},
        )
        legacy_delivery = SolicitudDomicilio.objects.create(
            pedido_cliente=legacy_order,
            cliente=self.cliente,
            direccion_cliente=self.direccion,
            cliente_nombre=self.cliente.nombre,
            direccion=self.direccion.direccion,
            repartidor=self.repartidor,
            unidad=self.unidad,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE crm_pedidocliente
                SET point_note_id = '', point_note_folio = '',
                    point_note_snapshot = '{}'::jsonb,
                    point_note_fetched_at = NULL, point_note_sold_at = NULL
                WHERE id = %s
                """,
                [legacy_order.id],
            )
            cursor.execute(
                """
                UPDATE logistica_solicituddomicilio
                SET legacy_without_point = TRUE, estatus = 'ENTREGADO',
                    entregado_en = NOW()
                WHERE id = %s
                """,
                [legacy_delivery.id],
            )
        self.client.force_login(self.ventas)

        response = self.client.get(
            reverse("crm:pedido_domicilio_detail", args=[legacy_order.id]),
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registro histórico sin snapshot Point")
        self.assertContains(response, "Monto estimado legacy")
        self.assertTrue(response.context["point_snapshot"]["is_legacy_fallback"])

    def test_domicilios_inbox_filters_and_links_each_row_to_exact_detail(self):
        non_delivery = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido sin domicilio",
        )
        self.client.force_login(self.ventas)

        response = self.client.get(reverse("crm:pedidos_domicilios"))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["domicilios_only"])
        self.assertContains(response, self.pedido.folio)
        self.assertNotContains(response, non_delivery.folio)
        self.assertContains(
            response,
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]),
        )
        self.assertContains(response, 'class="ops-grid-2 is-single-column"')
        self.assertContains(
            response,
            "crm-templates-crm-pedidos.css?v=20260727-domicilios-v2",
        )
        self.assertNotContains(
            response,
            f'href="{reverse("crm:pedido_detail", args=[self.pedido.id])}"',
        )
        self.assertNotContains(response, "Nuevo pedido")

    def test_generic_orders_keeps_generic_rows_and_commercial_detail_links(self):
        non_delivery = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido comercial normal",
        )
        self.client.force_login(self.ventas)

        response = self.client.get(reverse("crm:pedidos"))

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["domicilios_only"])
        self.assertContains(response, self.pedido.folio)
        self.assertContains(response, non_delivery.folio)
        self.assertNotContains(response, 'class="ops-grid-2 is-single-column"')
        self.assertContains(
            response,
            f'href="{reverse("crm:pedido_detail", args=[self.pedido.id])}"',
        )
        self.assertContains(response, "Nuevo pedido")

    def test_domicilios_inbox_uses_delivery_status_and_point_snapshot_values(self):
        self.pedido.estatus = PedidoCliente.ESTATUS_CANCELADO
        self.pedido.monto_estimado = Decimal("999.99")
        self.pedido.save(update_fields=["estatus", "monto_estimado", "updated_at"])
        self.client.force_login(self.ventas)

        response = self.client.get(
            reverse("crm:pedidos_domicilios"),
            {"estatus": SolicitudDomicilio.ESTATUS_CONFIRMADO},
        )

        row = response.context["pedidos"][0]
        self.assertEqual(row.domicilio_canonico.estatus, SolicitudDomicilio.ESTATUS_CONFIRMADO)
        self.assertEqual(row.point_snapshot_display["total"], Decimal("565.00"))
        self.assertContains(response, "Confirmado")
        self.assertContains(response, "18452")
        self.assertContains(response, "Centro Point")
        self.assertContains(response, "$565.00")
        self.assertNotContains(response, "$999.99")

        excluded = self.client.get(
            reverse("crm:pedidos_domicilios"),
            {"estatus": SolicitudDomicilio.ESTATUS_EN_RUTA},
        )
        self.assertEqual(list(excluded.context["pedidos"]), [])

    def test_domicilios_inbox_hides_crm_tabs_from_logistics_but_keeps_them_for_crm(self):
        url = reverse("crm:pedidos_domicilios")
        self.client.force_login(self.logistica)

        logistics_response = self.client.get(url)

        self.assertEqual(logistics_response.status_code, 200)
        self.assertNotContains(logistics_response, 'aria-label="Submódulos CRM"')
        self.assertNotContains(logistics_response, f'href="{reverse("crm:home")}"')
        self.assertNotContains(logistics_response, f'href="{reverse("crm:clientes")}"')
        self.assertNotContains(logistics_response, f'href="{reverse("crm:pedidos")}"')

        self.client.force_login(self.ventas)
        crm_response = self.client.get(url)
        self.assertContains(crm_response, 'aria-label="Submódulos CRM"')
        self.assertContains(crm_response, f'href="{reverse("crm:home")}"')
        self.assertContains(crm_response, f'href="{reverse("crm:clientes")}"')

    def test_domicilios_metrics_and_dates_use_canonical_delivery_not_commercial_order(self):
        old_order = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido comercial creado hoy, domicilio anterior",
            prioridad=PedidoCliente.PRIORIDAD_URGENTE,
            point_note_id="POINT-OLD-DELIVERY",
            point_note_snapshot={"pk_nota": "POINT-OLD-DELIVERY"},
        )
        old_delivery = SolicitudDomicilio.objects.create(
            pedido_cliente=old_order,
            cliente=self.cliente,
            direccion_cliente=self.direccion,
            cliente_nombre=self.cliente.nombre,
            direccion=self.direccion.direccion,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        yesterday = timezone.now() - timedelta(days=1)
        SolicitudDomicilio._base_manager.filter(pk=old_delivery.pk).update(
            created_at=yesterday,
        )
        delivered_order = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Domicilio entregado hoy",
            point_note_id="POINT-DELIVERED-TODAY",
            point_note_snapshot={"pk_nota": "POINT-DELIVERED-TODAY", "total": "50.00"},
        )
        delivered = SolicitudDomicilio.objects.create(
            pedido_cliente=delivered_order,
            cliente=self.cliente,
            direccion_cliente=self.direccion,
            cliente_nombre=self.cliente.nombre,
            direccion=self.direccion.direccion,
            repartidor=self.repartidor,
            unidad=self.unidad,
            estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        )
        for next_status in (
            SolicitudDomicilio.ESTATUS_PREPARANDO,
            SolicitudDomicilio.ESTATUS_LISTO,
            SolicitudDomicilio.ESTATUS_EN_RUTA,
            SolicitudDomicilio.ESTATUS_ENTREGADO,
        ):
            delivered.estatus = next_status
            if next_status == SolicitudDomicilio.ESTATUS_ENTREGADO:
                delivered.entregado_en = timezone.now()
                delivered.save(update_fields=["estatus", "entregado_en"])
            else:
                delivered.save(update_fields=["estatus"])
        legacy_order = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Domicilio histórico excluido de métricas",
        )
        legacy = SolicitudDomicilio.objects.create(
            pedido_cliente=legacy_order,
            cliente=self.cliente,
            direccion_cliente=self.direccion,
            cliente_nombre=self.cliente.nombre,
            direccion=self.direccion.direccion,
        )
        SolicitudDomicilio._base_manager.filter(pk=legacy.pk).update(
            estatus=SolicitudDomicilio.ESTATUS_CANCELADO,
            cancelacion_motivo="Histórico",
            legacy_without_point=True,
        )
        self.client.force_login(self.logistica)

        response = self.client.get(reverse("crm:pedidos_domicilios"))

        self.assertEqual(response.context["pedidos_abiertos"], 2)
        self.assertEqual(response.context["pedidos_hoy"], 2)
        self.assertContains(response, "Servicios hoy 2")
        self.assertContains(response, "Activos 2")

        today = timezone.localdate().isoformat()
        today_response = self.client.get(
            reverse("crm:pedidos_domicilios"),
            {"date_from": today, "date_to": today},
        )
        visible_ids = {order.id for order in today_response.context["pedidos"]}
        self.assertNotIn(old_order.id, visible_ids)
        self.assertIn(self.pedido.id, visible_ids)
        self.assertIn(delivered_order.id, visible_ids)

    def test_domicilios_ignores_hidden_commercial_priority_and_focus_params(self):
        self.pedido.prioridad = PedidoCliente.PRIORIDAD_BAJA
        self.pedido.estatus = PedidoCliente.ESTATUS_CANCELADO
        self.pedido.save(update_fields=["prioridad", "estatus", "updated_at"])
        self.client.force_login(self.logistica)

        response = self.client.get(
            reverse("crm:pedidos_domicilios"),
            {
                "prioridad": PedidoCliente.PRIORIDAD_URGENTE,
                "enterprise_focus": "PRODUCCION",
            },
        )

        self.assertContains(response, self.pedido.folio)
        self.assertEqual(response.context["prioridad"], "")
        self.assertEqual(response.context["enterprise_focus"], "")
        self.assertNotContains(response, 'name="prioridad"')
        self.assertNotContains(response, 'name="enterprise_focus"')

    def test_domicilio_detail_allows_logistics_owner_and_rejects_unrelated_role(self):
        url = reverse("crm:pedido_domicilio_detail", args=[self.pedido.id])
        self.client.force_login(self.logistica)
        self.assertEqual(self.client.get(url).status_code, 200)

        self.client.force_login(self.sin_acceso)
        self.assertEqual(self.client.get(url).status_code, 403)

    def test_domicilio_actions_are_exact_and_hidden_by_role(self):
        url = reverse("crm:pedido_domicilio_detail", args=[self.pedido.id])

        self.client.force_login(self.logistica)
        logistics_response = self.client.get(url)
        self.assertContains(logistics_response, 'data-operation-owner="centro-operativo-api"')
        self.assertNotContains(logistics_response, reverse("crm:editar_cliente", args=[self.cliente.id]))
        self.assertNotContains(
            logistics_response,
            f'href="{reverse("crm:pedido_detail", args=[self.pedido.id])}"',
        )
        self.assertNotContains(logistics_response, 'aria-label="Submódulos CRM"')
        self.assertNotContains(logistics_response, "Abrir operación logística")

        self.client.force_login(self.ventas)
        crm_response = self.client.get(url)
        self.assertContains(crm_response, reverse("crm:editar_cliente", args=[self.cliente.id]))
        self.assertContains(
            crm_response,
            f'href="{reverse("crm:pedido_detail", args=[self.pedido.id])}"',
        )
        self.assertNotContains(crm_response, "Abrir operación logística")

    def test_domicilio_detail_does_not_leak_existing_order_to_unauthorized_user(self):
        self.client.force_login(self.sin_acceso)
        existing = self.client.get(
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]),
        )
        missing = self.client.get(
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id + 9999]),
        )
        self.assertEqual(existing.status_code, 403)
        self.assertEqual(missing.status_code, 403)

    def test_domicilio_detail_renders_without_n_plus_one_queries(self):
        from crm.views import pedido_domicilio_detail

        request = RequestFactory().get(
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]),
        )
        request.user = self.ventas
        with CaptureQueriesContext(connection) as queries:
            response = pedido_domicilio_detail(request, self.pedido.id)

        self.assertEqual(response.status_code, 200)
        # The canonical ERP actions load the scoped driver and unit catalogs in
        # addition to the read-only order graph. Keep the bound fixed so the
        # status-operation history cannot introduce N+1 queries.
        self.assertLessEqual(len(queries), 24)

    def test_domicilio_detail_has_responsive_semantics_and_touch_contract(self):
        self.client.force_login(self.ventas)
        response = self.client.get(
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]),
        )
        self.assertContains(response, 'class="delivery-detail-grid"')
        self.assertContains(response, "@media (max-width: 767px)")
        self.assertContains(response, "min-height: 44px")
        self.assertContains(response, 'aria-label="Estado operativo del domicilio"')
        self.assertContains(response, 'aria-label="Productos de la nota Point"')

    def test_domicilio_detail_post_uses_canonical_assignment_and_transition_services(self):
        url = reverse("crm:pedido_domicilio_detail", args=[self.pedido.id])
        self.client.force_login(self.logistica)
        assigned = self.client.post(
            url,
            {
                "action": "assign",
                "repartidor_id": self.repartidor.id,
                "unidad_id": self.unidad.id,
            },
        )
        advanced = self.client.post(
            url,
            {
                "action": "advance",
                "requested_status": SolicitudDomicilio.ESTATUS_PREPARANDO,
            },
        )
        self.assertRedirects(assigned, url, fetch_redirect_response=False)
        self.assertRedirects(advanced, url, fetch_redirect_response=False)
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_PREPARANDO)

        self.client.force_login(self.ventas)
        forbidden = self.client.post(
            url,
            {
                "action": "cancel",
                "cancelacion_motivo": "No autorizado",
            },
        )
        self.assertEqual(forbidden.status_code, 403)

    def test_domicilio_detail_post_requires_csrf(self):
        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(self.logistica)
        response = csrf_client.post(
            reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]),
            {
                "action": "advance",
                "requested_status": SolicitudDomicilio.ESTATUS_PREPARANDO,
            },
        )
        self.assertIn(response.status_code, {302, 403})
        self.solicitud.refresh_from_db()
        self.assertEqual(self.solicitud.estatus, SolicitudDomicilio.ESTATUS_CONFIRMADO)

    def test_legacy_ecommerce_page_never_creates_second_delivery(self):
        self.client.force_login(self.logistica)
        before = EntregaEcommerce.objects.count()
        response = self.client.post(
            reverse("logistica:domicilios_ecommerce"),
            {
                "order_id": "999",
                "order_number": "18452",
                "cliente_nombre": self.cliente.nombre,
                "direccion": self.direccion.direccion,
                "repartidor": str(self.repartidor.id),
                "unidad_operativa": str(self.unidad.id),
            },
        )

        self.assertRedirects(
            response,
            reverse("crm:pedidos_domicilios"),
            fetch_redirect_response=False,
        )
        self.assertEqual(EntregaEcommerce.objects.count(), before)
        self.assertEqual(
            SolicitudDomicilio.objects.filter(pedido_cliente=self.pedido).count(),
            1,
        )
        self.assertEqual(
            self.client.get(reverse("crm:pedidos_domicilios")).status_code,
            200,
        )

    def test_legacy_general_delivery_page_is_read_only_redirect_for_get_and_post(self):
        self.client.force_login(self.logistica)
        before = SolicitudDomicilio.objects.count()

        get_response = self.client.get(reverse("logistica:domicilios_generales"))
        post_response = self.client.post(
            reverse("logistica:domicilios_generales"),
            {
                "accion": "crear",
                "cliente_nombre": "Duplicado libre",
                "direccion": "Sin pedido Point",
                "canal_origen": SolicitudDomicilio.CANAL_TELEFONO,
            },
        )

        canonical_url = reverse("crm:pedidos_domicilios")
        self.assertRedirects(get_response, canonical_url, fetch_redirect_response=False)
        self.assertRedirects(post_response, canonical_url, fetch_redirect_response=False)
        self.assertEqual(SolicitudDomicilio.objects.count(), before)

    def test_database_rejects_second_delivery_for_same_order(self):
        with self.assertRaises(IntegrityError), transaction.atomic():
            SolicitudDomicilio.objects.create(
                pedido_cliente=self.pedido,
                cliente=self.cliente,
                direccion_cliente=self.direccion,
                cliente_nombre=self.cliente.nombre,
                direccion=self.direccion.direccion,
            )


class CRMViewsTests(_CRMViewsTestBase):
    def test_dashboard_view_renders_executive_surface(self):
        cliente = Cliente.objects.create(nombre="Cliente Dashboard")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido dashboard",
            estatus=PedidoCliente.ESTATUS_EN_PRODUCCION,
            canal=PedidoCliente.CANAL_WHATSAPP,
            prioridad=PedidoCliente.PRIORIDAD_ALTA,
            monto_estimado=1250,
        )
        SeguimientoPedido.objects.create(
            pedido=pedido,
            estatus_nuevo=PedidoCliente.ESTATUS_EN_PRODUCCION,
            comentario="Producción confirmada",
            created_by=self.user,
        )

        resp = self.client.get(reverse("crm:home"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Clientes y pedidos en control")
        self.assertContains(resp, "Distribución por estatus")
        self.assertContains(resp, "Origen comercial")
        self.assertContains(resp, "Últimos 7 días")

    def test_dashboard_view_supports_real_status_and_query_filters(self):
        cliente = Cliente.objects.create(nombre="Cliente Filtrado", codigo="CLT-001")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido filtrado",
            estatus=PedidoCliente.ESTATUS_EN_PRODUCCION,
            monto_estimado=800,
        )
        SeguimientoPedido.objects.create(
            pedido=pedido,
            estatus_nuevo=PedidoCliente.ESTATUS_EN_PRODUCCION,
            comentario="Filtro activo",
            created_by=self.user,
        )

        resp = self.client.get(
            reverse("crm:home"),
            {"estatus": PedidoCliente.ESTATUS_EN_PRODUCCION, "q": "Filtrado"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["selected_estatus"], PedidoCliente.ESTATUS_EN_PRODUCCION)
        self.assertEqual(resp.context["selected_q"], "Filtrado")
        self.assertContains(resp, "Cliente o folio")

    def test_dashboard_view_supports_real_channel_and_priority_filters(self):
        cliente = Cliente.objects.create(nombre="Cliente Canal", codigo="CLT-CHAN-1")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido canal",
            estatus=PedidoCliente.ESTATUS_CONFIRMADO,
            canal=PedidoCliente.CANAL_WHATSAPP,
            prioridad=PedidoCliente.PRIORIDAD_URGENTE,
            monto_estimado=900,
        )
        SeguimientoPedido.objects.create(
            pedido=pedido,
            estatus_nuevo=PedidoCliente.ESTATUS_CONFIRMADO,
            comentario="Seguimiento canal",
            created_by=self.user,
        )

        resp = self.client.get(
            reverse("crm:home"),
            {"canal": PedidoCliente.CANAL_WHATSAPP, "prioridad": PedidoCliente.PRIORIDAD_URGENTE},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["selected_canal"], PedidoCliente.CANAL_WHATSAPP)
        self.assertEqual(resp.context["selected_prioridad"], PedidoCliente.PRIORIDAD_URGENTE)
        self.assertContains(resp, "Canal")
        self.assertContains(resp, "Prioridad")

    def test_clientes_view_and_create(self):
        resp = self.client.get(reverse("crm:clientes"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "CRM · Clientes")
        self.assertContains(resp, "Base comercial")
        self.assertContains(resp, "Alta de cliente")
        self.assertContains(resp, "Clientes registrados")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Ruta crítica ERP")
        self.assertNotContains(resp, "Mesa de gobierno ERP")
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("crm:clientes"),
            {
                "nombre": "Cliente Demo",
                "telefono": "6671230000",
                "email": "demo@example.com",
                "tipo_cliente": "Mostrador",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertTrue(Cliente.objects.filter(nombre="Cliente Demo").exists())

    def test_cliente_detail_edit_deactivate_and_reactivate(self):
        cliente = Cliente.objects.create(
            nombre="Cliente Editable",
            telefono="6670000000",
            email="viejo@example.com",
            tipo_cliente="Mostrador",
        )
        PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido cliente editable",
            sucursal_ref=self.sucursal,
            monto_estimado=500,
        )

        detail_url = reverse("crm:cliente_detail", kwargs={"cliente_id": cliente.id})
        resp_detail = self.client.get(detail_url)
        self.assertEqual(resp_detail.status_code, 200)
        self.assertContains(resp_detail, "Cliente Editable")
        self.assertContains(resp_detail, "Historial de pedidos")
        self.assertContains(resp_detail, "Pedido cliente editable")

        edit_url = reverse("crm:editar_cliente", kwargs={"cliente_id": cliente.id})
        resp_edit = self.client.post(
            edit_url,
            {
                "accion": "editar",
                "nombre": "Cliente Editado",
                "telefono": "6671111111",
                "email": "nuevo@example.com",
                "tipo_cliente": "Evento",
                "sucursal_referencia": "Matriz",
                "notas": "Cliente actualizado desde test",
            },
            follow=True,
        )
        self.assertEqual(resp_edit.status_code, 200)
        cliente.refresh_from_db()
        self.assertEqual(cliente.nombre, "Cliente Editado")
        self.assertEqual(cliente.nombre_normalizado, "cliente editado")
        self.assertEqual(cliente.email, "nuevo@example.com")

        resp_deactivate = self.client.post(edit_url, {"accion": "desactivar"}, follow=True)
        self.assertEqual(resp_deactivate.status_code, 200)
        cliente.refresh_from_db()
        self.assertFalse(cliente.activo)

        resp_activate = self.client.post(edit_url, {"accion": "activar"}, follow=True)
        self.assertEqual(resp_activate.status_code, 200)
        cliente.refresh_from_db()
        self.assertTrue(cliente.activo)

    def test_pedidos_create_and_tracking(self):
        cliente = Cliente.objects.create(nombre="Cliente Pedido")
        resp_index = self.client.get(reverse("crm:pedidos"))
        self.assertEqual(resp_index.status_code, 200)
        self.assertContains(resp_index, "Nuevo pedido")
        self.assertContains(resp_index, "Seguimiento de pedidos")
        self.assertContains(resp_index, "Focos operativos")
        self.assertNotContains(resp_index, "Centro de mando ERP")
        self.assertNotContains(resp_index, "Cadena documental ERP")
        self.assertNotContains(resp_index, "Ruta crítica ERP")
        self.assertNotContains(resp_index, "Mesa de gobierno ERP")
        self.assertTrue(resp_index.context["focus_cards"])
        self.assertTrue(resp_index.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_index.context["enterprise_chain"][0])
        self.assertTrue(resp_index.context["operational_health_cards"])
        resp_post = self.client.post(
            reverse("crm:pedidos"),
            {
                "cliente_id": cliente.id,
                "descripcion": "Pastel cumpleaños 30 personas",
                "sucursal": self.point_branch.external_id,
                "canal": PedidoCliente.CANAL_WHATSAPP,
                "prioridad": PedidoCliente.PRIORIDAD_ALTA,
                "estatus": PedidoCliente.ESTATUS_NUEVO,
                "monto_estimado": "1450.00",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)

        pedido = PedidoCliente.objects.get(cliente=cliente)
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_NUEVO)
        self.assertEqual(pedido.monto_estimado, 1450)
        self.assertEqual(pedido.sucursal_ref_id, self.sucursal.id)
        self.assertEqual(pedido.sucursal, self.sucursal.nombre)
        self.assertTrue(SeguimientoPedido.objects.filter(pedido=pedido).exists())

        detail_url = reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id})
        resp_detail = self.client.post(
            detail_url,
            {
                "estatus_nuevo": PedidoCliente.ESTATUS_CONFIRMADO,
                "comentario": "Cliente confirma anticipo",
            },
            follow=True,
        )
        self.assertEqual(resp_detail.status_code, 200)
        self.assertContains(resp_detail, "Ficha del pedido")
        self.assertContains(resp_detail, "Registrar seguimiento")
        self.assertContains(resp_detail, "Bitácora del pedido")
        self.assertNotContains(resp_detail, "Centro de mando ERP")
        self.assertNotContains(resp_detail, "Cadena documental ERP")
        self.assertNotContains(resp_detail, "Mesa de gobierno ERP")
        self.assertTrue(resp_detail.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_detail.context["enterprise_chain"][0])
        self.assertTrue(resp_detail.context["operational_health_cards"])
        pedido.refresh_from_db()
        self.assertEqual(pedido.estatus, PedidoCliente.ESTATUS_CONFIRMADO)
        self.assertTrue(
            SeguimientoPedido.objects.filter(
                pedido=pedido,
                estatus_nuevo=PedidoCliente.ESTATUS_CONFIRMADO,
                comentario__icontains="anticipo",
            ).exists()
        )

    def test_pedidos_can_focus_operational_subset(self):
        cliente = Cliente.objects.create(nombre="Cliente Foco")
        PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido en producción",
            estatus=PedidoCliente.ESTATUS_EN_PRODUCCION,
        )

        resp = self.client.get(reverse("crm:pedidos"), {"enterprise_focus": "PRODUCCION"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "PRODUCCION")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_redirects_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("crm:pedidos"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)


class PedidoClienteSaveTests(TestCase):
    def setUp(self):
        self.cliente = Cliente.objects.create(nombre="Cliente Save")
        self.sucursal_matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.sucursal_norte = Sucursal.objects.create(codigo="NORTE", nombre="Norte", activa=True)

    def test_create_fills_sucursal_from_ref_when_blank(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido nuevo",
            sucursal_ref=self.sucursal_matriz,
        )

        self.assertEqual(pedido.sucursal, "Matriz")

    def test_update_refreshes_sucursal_when_it_still_matches_previous_canonical_value(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido canonico",
            sucursal_ref=self.sucursal_matriz,
        )

        pedido.sucursal_ref = self.sucursal_norte
        pedido.save()
        pedido.refresh_from_db()

        self.assertEqual(pedido.sucursal, "Norte")

    def test_update_preserves_manual_sucursal_value(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido manual",
            sucursal_ref=self.sucursal_matriz,
            sucursal="Alias histórico",
        )

        pedido.descripcion = "Pedido manual actualizado"
        pedido.save()
        pedido.refresh_from_db()

        self.assertEqual(pedido.sucursal, "Alias histórico")

    def test_update_fields_persists_safe_fill_when_sucursal_is_blank(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido blank",
            sucursal_ref=self.sucursal_matriz,
        )
        PedidoCliente.objects.filter(pk=pedido.pk).update(sucursal="")

        pedido.refresh_from_db()
        pedido.estatus = PedidoCliente.ESTATUS_CONFIRMADO
        pedido.save(update_fields=["estatus", "updated_at"])
        pedido.refresh_from_db()

        self.assertEqual(pedido.sucursal, "Matriz")


class PedidoClientePointLinkTests(TestCase):
    def setUp(self):
        self.cliente = Cliente.objects.create(nombre="Cliente Point")
        self.otro_cliente = Cliente.objects.create(nombre="Otro cliente Point")

    def test_pedido_accepts_social_channels(self):
        self.assertIn(("FACEBOOK", "Facebook"), PedidoCliente.CANAL_CHOICES)
        self.assertIn(("INSTAGRAM", "Instagram"), PedidoCliente.CANAL_CHOICES)

    def test_point_note_identity_is_unique_when_present(self):
        PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                PedidoCliente.objects.create(
                    cliente=self.otro_cliente,
                    descripcion="Dos",
                    point_note_id="900001",
                    point_note_snapshot={"pk_nota": "900001"},
                )

    def test_empty_point_note_identity_is_not_unique(self):
        PedidoCliente.objects.create(cliente=self.cliente, descripcion="Uno")
        PedidoCliente.objects.create(cliente=self.otro_cliente, descripcion="Dos")

        self.assertEqual(PedidoCliente.objects.filter(point_note_id="").count(), 2)

    def test_point_note_snapshot_is_immutable(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )
        pedido.point_note_snapshot = {"pk_nota": "alterada"}

        with self.assertRaisesMessage(
            ValidationError,
            "La procedencia Point es inmutable.",
        ):
            pedido.save()

        pedido.refresh_from_db()
        self.assertEqual(pedido.point_note_snapshot, {"pk_nota": "900001"})

    def test_point_note_provenance_allows_explicit_legacy_backfill_once(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Legacy",
        )
        fetched_at = timezone.now()

        pedido.backfill_point_note_provenance(
            point_note_id="900001",
            point_note_folio="18452",
            point_note_snapshot={"pk_nota": "900001"},
            point_note_fetched_at=fetched_at,
        )
        pedido.refresh_from_db()

        self.assertEqual(pedido.point_note_id, "900001")
        self.assertEqual(pedido.point_note_folio, "18452")
        self.assertEqual(pedido.point_note_snapshot, {"pk_nota": "900001"})
        self.assertEqual(pedido.point_note_fetched_at, fetched_at)

        with self.assertRaisesMessage(ValidationError, "La procedencia Point es inmutable."):
            pedido.backfill_point_note_provenance(
                point_note_id="900001",
                point_note_folio="otro",
                point_note_snapshot={"pk_nota": "900001"},
                point_note_fetched_at=fetched_at,
            )

    def test_point_note_provenance_fields_are_immutable_as_a_set(self):
        fetched_at = timezone.now()
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_folio="18452",
            point_note_snapshot={"pk_nota": "900001"},
            point_note_fetched_at=fetched_at,
        )
        changes = {
            "point_note_id": "900002",
            "point_note_folio": "18453",
            "point_note_snapshot": {"pk_nota": "900001", "folio": "otro"},
            "point_note_fetched_at": fetched_at + timedelta(seconds=1),
        }

        for field_name, changed_value in changes.items():
            with self.subTest(field=field_name):
                pedido.refresh_from_db()
                setattr(pedido, field_name, changed_value)
                with self.assertRaisesMessage(ValidationError, "La procedencia Point es inmutable."):
                    pedido.save()

    def test_save_kwargs_cannot_authorize_point_note_backfill(self):
        forbidden_kwargs = (
            "_allow_point_note_backfill",
            "_allow_point_note_snapshot_backfill",
        )

        for forbidden_kwarg in forbidden_kwargs:
            with self.subTest(kwarg=forbidden_kwarg):
                pedido = PedidoCliente.objects.create(
                    cliente=self.cliente,
                    descripcion=f"Legacy {forbidden_kwarg}",
                )
                pedido.point_note_id = "900001"
                pedido.point_note_snapshot = {"pk_nota": "900001"}

                with self.assertRaisesMessage(
                    TypeError,
                    "solo puede ejecutarse mediante backfill_point_note_provenance",
                ):
                    pedido.save(**{forbidden_kwarg: True})

    def test_point_note_snapshot_identity_must_match_point_note_id(self):
        with self.assertRaisesMessage(ValidationError, "no coincide"):
            PedidoCliente.objects.create(
                cliente=self.cliente,
                descripcion="Mismatch",
                point_note_id="900001",
                point_note_snapshot={"pk_nota": "900002"},
            )

    def test_point_note_snapshot_accepts_real_uppercase_identity_key(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Contrato real",
            point_note_id="900001",
            point_note_snapshot={"PK_NOTA": "900001"},
        )

        self.assertEqual(pedido.point_note_id, "900001")

    def test_queryset_update_cannot_overwrite_immutable_snapshots_or_point_provenance(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )

        forbidden_updates = (
            {"payload_snapshot": {"alterado": True}},
            {"point_note_snapshot": {"pk_nota": "900002"}},
            {"point_note_id": "900002"},
            {"point_note_folio": "otro"},
            {"point_note_fetched_at": timezone.now()},
        )
        for update_kwargs in forbidden_updates:
            with self.subTest(fields=tuple(update_kwargs)):
                with self.assertRaisesMessage(ValidationError, "actualización masiva"):
                    PedidoCliente.objects.filter(pk=pedido.pk).update(**update_kwargs)

    def test_bulk_update_cannot_overwrite_immutable_snapshots_or_point_provenance(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Uno",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )
        pedido.point_note_snapshot = {"pk_nota": "900002"}

        with self.assertRaisesMessage(ValidationError, "actualización masiva"):
            PedidoCliente.objects.bulk_update([pedido], ["point_note_snapshot"])

    def test_bulk_create_validates_point_note_identity(self):
        pedido = PedidoCliente(
            cliente=self.cliente,
            descripcion="Mismatch masivo",
            point_note_id="900001",
            point_note_snapshot={"PK_NOTA": "900002"},
        )

        with self.assertRaisesMessage(ValidationError, "no coincide"):
            PedidoCliente.objects.bulk_create([pedido])

    def test_payload_snapshot_remains_immutable_after_refactor(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Omnicanal",
            payload_snapshot={"external_id": "web-001"},
        )
        pedido.payload_snapshot = {"external_id": "alterado"}

        with self.assertRaisesMessage(
            ValidationError,
            "El snapshot omnicanal es inmutable.",
        ):
            pedido.save()

        pedido.refresh_from_db()
        self.assertEqual(pedido.payload_snapshot, {"external_id": "web-001"})

    def test_payload_snapshot_allows_legacy_backfill_only_once(self):
        pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Omnicanal legacy",
        )
        pedido.payload_snapshot = {"external_id": "web-001"}
        pedido.save(_allow_payload_snapshot_backfill=True)
        pedido.refresh_from_db()
        self.assertEqual(pedido.payload_snapshot, {"external_id": "web-001"})

        pedido.payload_snapshot = {"external_id": "web-002"}
        with self.assertRaisesMessage(
            ValidationError,
            "El snapshot omnicanal es inmutable.",
        ):
            pedido.save(_allow_payload_snapshot_backfill=True)


class PedidoClientePointBackfillConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self.cliente = Cliente.objects.create(nombre="Cliente concurrencia Point")
        self.pedido = PedidoCliente.objects.create(
            cliente=self.cliente,
            descripcion="Pedido legacy concurrente",
        )

    def test_two_concurrent_backfills_cannot_overwrite_each_other(self):
        ready = Barrier(2)

        def attempt_backfill(point_note_id):
            close_old_connections()
            try:
                pedido = PedidoCliente.objects.get(pk=self.pedido.pk)
                ready.wait(timeout=10)
                pedido.backfill_point_note_provenance(
                    point_note_id=point_note_id,
                    point_note_folio=f"F-{point_note_id}",
                    point_note_snapshot={"pk_nota": point_note_id},
                    point_note_fetched_at=timezone.now(),
                )
                return ("won", point_note_id)
            except ValidationError:
                return ("rejected", point_note_id)
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(attempt_backfill, "900001"),
                executor.submit(attempt_backfill, "900002"),
            ]
            results = [future.result(timeout=20) for future in futures]

        winners = [point_note_id for result, point_note_id in results if result == "won"]
        rejected = [point_note_id for result, point_note_id in results if result == "rejected"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(rejected), 1)

        self.pedido.refresh_from_db()
        self.assertEqual(self.pedido.point_note_id, winners[0])
        self.assertEqual(self.pedido.point_note_snapshot["pk_nota"], winners[0])
        self.assertEqual(self.pedido.point_note_folio, f"F-{winners[0]}")


class SucursalResolutionTests(TestCase):
    def test_resolve_sucursal_excludes_col_duplicate_and_keeps_canonical_branch(self):
        canonical = Sucursal.objects.create(codigo="COLOSIO", nombre="Colosio", activa=True)
        Sucursal.objects.create(codigo="COL", nombre="Colosio", activa=True)
        PointBranch.objects.create(
            external_id="5",
            name="COLOSIO",
            erp_branch=canonical,
            status=PointBranch.STATUS_ACTIVE,
        )

        resolution = resolve_sucursal("Colosio")

        self.assertEqual(resolution.sucursal.id, canonical.id)
        self.assertEqual(resolution.sucursal.codigo, "COLOSIO")

    def test_resolve_sucursal_rejects_excluded_duplicate_code(self):
        # El duplicado fantasma COL se remedia con datos (comando
        # purge_ghost_branch_col, 0ba49d89), no con la lista de exclusión del
        # resolver; el contrato vigente del resolver es rechazar códigos de
        # EXCLUDED_BRANCH_CODES (core.branch_catalog) aunque existan activos.
        Sucursal.objects.create(codigo="COLOSIO", nombre="Colosio", activa=True)
        Sucursal.objects.create(codigo="TMP1", nombre="Colosio", activa=True)

        with self.assertRaises(SucursalResolutionError):
            resolve_sucursal("TMP1")
