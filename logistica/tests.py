import json
from decimal import Decimal
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.db import IntegrityError, transaction
from django.test import SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE, ACCESS_VIEW
from core.email_rendering import render_email_to_string
from core.models import Sucursal, UserModuleAccess
from crm.models import Cliente, PedidoCliente
from logistica.models import (
    BitacoraSalidaLlegada,
    EntregaRuta,
    EventoRuta,
    ParadaEntregaEvidencia,
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
    UbicacionRuta,
    Unidad,
)
from logistica.services_carga_ruta import sincronizar_checklist_carga_desde_point, sincronizar_recepcion_desde_point
from logistica.services_google_roads import snap_gps_path_to_roads
from logistica.services_rutas_control import distancia_metros, registrar_ubicacion_ruta, resumen_control_rutas
from logistica.services_tiempos_ruta import resumen_tiempos_ruta
from logistica.tasks import _emails_de_grupo, detectar_gps_perdido_rutas
from api.logistica_views import _can_operate_pwa
from pos_bridge.models import PointBranch, PointSyncJob, PointTransferLine


class LogisticaEmailTemplateTests(SimpleTestCase):
    def test_email_sources_do_not_use_manual_inline_styles(self):
        email_dir = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "emails"

        for template_path in email_dir.glob("*.html"):
            with self.subTest(template=template_path.name):
                source = template_path.read_text(encoding="utf-8")
                self.assertNotIn("style=", source)
                self.assertNotIn("<style", source.lower())

    def test_email_renderer_compiles_source_classes_to_inline_styles(self):
        unidad = SimpleNamespace(codigo="QA-LOG-1", descripcion="Unidad QA", placa="QA-123")

        html = render_email_to_string(
            "logistica/emails/alerta_lavado.html",
            {
                "unidad": unidad,
                "dias_sin_lavar": None,
                "ultimo_lavado": None,
            },
        )

        self.assertIn("Alerta de lavado pendiente", html)
        self.assertIn("style=", html)
        self.assertIn("background: #8b2252", html)
        self.assertNotIn("<style", html.lower())


class LogisticaControlRutasTemplateTests(SimpleTestCase):
    def test_programmed_google_polyline_uses_route_source_not_pipe_character(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "control_rutas.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn("function decodeRoutePolyline(value, source)", source)
        self.assertIn("decodeRoutePolyline(route.programada_polyline, route.programada_fuente)", source)
        self.assertIn("const hasSegmentPayload = Array.isArray(route.ubicaciones_segmentos);", source)
        self.assertIn("function drawActualRoute(coords, color, tooltip)", source)
        self.assertIn('map.createPane("actualRoutePane");', source)
        self.assertIn("segment.estado === \"fuera_geocerca\" ? \"#d82424\" : \"#e0007a\"", source)
        self.assertIn('dashArray: "1 12"', source)
        self.assertIn("background: repeating-linear-gradient(90deg, #e0007a", source)
        self.assertIn("L.circleMarker(point", source)
        self.assertNotIn('if (value.includes("|")) return parseFallbackPolyline(value);', source)


VALID_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!"
    b"\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00"
    b"\x00\x02\x02D\x01\x00;"
)


class LogisticaGroupAliasCompatibilityTests(TestCase):
    def test_emails_de_grupo_legacy_dg_uses_canonical_group(self):
        user = User.objects.create_user(
            username="dg.logistica.alias",
            email="dg.logistica@example.com",
        )
        user.groups.add(Group.objects.get_or_create(name="DG")[0])

        self.assertEqual(_emails_de_grupo("dg"), ["dg.logistica@example.com"])


class LogisticaSeedPuntosPollyanasTests(TestCase):
    def test_seed_puntos_logisticos_pollyanas_es_idempotente(self):
        output = StringIO()
        call_command("seed_puntos_logisticos_pollyanas", stdout=output)
        call_command("seed_puntos_logisticos_pollyanas", stdout=output)

        self.assertEqual(Sucursal.objects.filter(codigo__in=["MATRIZ", "GUAMUCHIL"]).count(), 2)
        self.assertEqual(PuntoLogistico.objects.filter(tipo=PuntoLogistico.TIPO_SUCURSAL, activo=True).count(), 9)
        self.assertTrue(PuntoLogistico.objects.filter(nombre="Sucursal Matriz", radio_geocerca_metros=120).exists())
        self.assertTrue(PuntoLogistico.objects.filter(nombre="Sucursal Guamuchil", notas__contains="Blvd. Rosales 627").exists())


class LogisticaSeedRepartidoresTests(TestCase):
    def test_seed_repartidores_pwa_asigna_jorge_a_matriz_por_default(self):
        matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        Sucursal.objects.create(codigo="COLOSIO", nombre="Colosio", activa=True)

        output = StringIO()
        call_command("seed_repartidores_pwa", stdout=output)

        repartidor = Repartidor.objects.select_related("sucursal", "user").get(user__username="compras.jorge@pollyanasdolce.com")
        self.assertEqual(repartidor.sucursal, matriz)
        self.assertEqual(repartidor.sucursal.codigo, "MATRIZ")


class LogisticaViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="logistica", password="pass123")
        group, _ = Group.objects.get_or_create(name="LOGISTICA")
        self.user.groups.add(group)
        self.client.login(username="logistica", password="pass123")

    def test_dashboard_view_renders_executive_surface(self):
        cliente = Cliente.objects.create(nombre="Cliente Logística")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido de reparto",
            estatus=PedidoCliente.ESTATUS_CONFIRMADO,
            monto_estimado=950,
        )
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Centro",
            fecha_ruta=timezone.localdate(),
            chofer="Mario",
            unidad="Van 1",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_EN_CAMINO,
            monto_estimado=950,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Logística en control")
        self.assertContains(resp, "Distribución de estatus de ruta")
        self.assertContains(resp, "Distribución de estatus de entrega")
        self.assertContains(resp, "Últimos 7 días")

    def test_dashboard_view_supports_real_focus_filter(self):
        cliente = Cliente.objects.create(nombre="Cliente Incidencia")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido con incidencia", monto_estimado=500)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Incidencia",
            fecha_ruta="2026-03-25",
            chofer="Mario",
            unidad="Van 1",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_INCIDENCIA,
            monto_estimado=500,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"), {"enterprise_focus": "INCIDENCIAS"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["enterprise_focus"], "INCIDENCIAS")
        self.assertIsNotNone(resp.context["focus_summary"])
        self.assertContains(resp, "Foco")

    def test_dashboard_view_supports_real_search_filter(self):
        cliente = Cliente.objects.create(nombre="Cliente Busqueda Logística")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido filtro", monto_estimado=500)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Busqueda",
            fecha_ruta="2026-03-25",
            chofer="Mario",
            unidad="Van 99",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_PENDIENTE,
            monto_estimado=500,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"), {"q": "Van 99"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["selected_q"], "Van 99")
        self.assertContains(resp, "Ruta o pedido")

    def test_rutas_view_and_create(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal Centro", codigo="SC01")
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Centro",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            sucursal=sucursal,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Logística · Rutas")
        self.assertContains(resp, "Planear ruta del día")
        self.assertContains(resp, "Sucursales o puntos a visitar")
        self.assertContains(resp, "Filtro operativo")
        self.assertContains(resp, "Rutas de entrega")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Ruta crítica ERP")
        self.assertNotContains(resp, "Mesa de gobierno ERP")
        self.assertTrue(resp.context["focus_cards"])
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta Centro",
                "fecha_ruta": "2026-02-24",
                "chofer": "Mario",
                "unidad": "Van 1",
                "km_estimado": "18.5",
                "puntos_ruta": [str(punto.id)],
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Detalle de ruta")
        self.assertContains(resp_post, "Agregar entrega")
        self.assertContains(resp_post, "Entregas de ruta")
        self.assertContains(resp_post, "Sucursal Centro")
        ruta = RutaEntrega.objects.get(nombre="Ruta Centro")
        self.assertTrue(ruta.paradas.filter(punto=punto, orden=1).exists())
        self.assertNotContains(resp_post, "Centro de mando ERP")
        self.assertNotContains(resp_post, "Cadena documental ERP")
        self.assertNotContains(resp_post, "Mesa de gobierno ERP")

    def test_rutas_selector_omite_repartidores_con_usuario_inactivo(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal Centro", codigo="SC01")
        activo_user = User.objects.create_user(username="rep.activo", password="pass123")
        inactivo_user = User.objects.create_user(username="rep.inactivo", password="pass123", is_active=False)
        activo = Repartidor.objects.create(user=activo_user, sucursal=sucursal)
        inactivo = Repartidor.objects.create(user=inactivo_user, sucursal=sucursal)

        resp = self.client.get(reverse("logistica:rutas"))

        ids = {repartidor.id for repartidor in resp.context["repartidores"]}
        self.assertIn(activo.id, ids)
        self.assertNotIn(inactivo.id, ids)

    def test_rutas_create_requires_route_points(self):
        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta sin paradas",
                "fecha_ruta": "2026-02-24",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Selecciona al menos una sucursal o punto")
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta sin paradas").exists())

    def test_rutas_create_respects_visit_order(self):
        punto_sur = PuntoLogistico.objects.create(
            nombre="Sucursal Sur",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.560000",
            longitud="-108.460000",
            radio_geocerca_metros=120,
        )
        punto_norte = PuntoLogistico.objects.create(
            nombre="Sucursal Norte",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta con orden",
                "fecha_ruta": "2026-02-24",
                "puntos_ruta": [str(punto_sur.id), str(punto_norte.id)],
                f"punto_orden_{punto_sur.id}": "2",
                f"punto_orden_{punto_norte.id}": "1",
            },
            follow=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        ruta = RutaEntrega.objects.get(nombre="Ruta con orden")
        paradas = list(ruta.paradas.order_by("orden").values_list("punto_id", "orden"))
        self.assertEqual(paradas, [(punto_norte.id, 1), (punto_sur.id, 2)])
        self.assertEqual(ruta.ruta_programada_fuente, "FALLBACK")
        self.assertGreater(ruta.ruta_programada_distancia_metros, 0)
        self.assertGreater(ruta.ruta_programada_duracion_segundos, 0)
        self.assertGreater(ruta.km_estimado, 0)

    def test_rutas_create_deduplicates_repeated_route_points(self):
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Repetida",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta sin duplicados",
                "fecha_ruta": "2026-02-24",
                "puntos_ruta": [str(punto.id), str(punto.id)],
                f"punto_orden_{punto.id}": "1",
            },
            follow=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Se ignoraron puntos repetidos")
        ruta = RutaEntrega.objects.get(nombre="Ruta sin duplicados")
        self.assertEqual(ruta.paradas.filter(punto=punto).count(), 1)

    def test_ruta_detail_add_entrega(self):
        cliente = Cliente.objects.create(nombre="Cliente Logística")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pastel para entrega")
        ruta = RutaEntrega.objects.create(nombre="Ruta Norte", fecha_ruta="2026-02-24")
        resp = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "add_entrega",
                "pedido_id": str(pedido.id),
                "secuencia": "1",
                "cliente_nombre": "",
                "direccion": "Sucursal Centro",
                "estatus": "PENDIENTE",
                "monto_estimado": "950",
            },
            follow=True,
        )
        self.assertEqual(resp.status_code, 200)
        ruta.refresh_from_db()
        self.assertEqual(ruta.total_entregas, 1)
        self.assertContains(resp, "Agregar entrega")
        self.assertContains(resp, "Entregas de ruta")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Madurez ERP de logística")
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertTrue(resp.context["operational_health_cards"])

    def test_rutas_view_can_focus_enterprise_subset(self):
        cliente = Cliente.objects.create(nombre="Cliente Focus")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Entrega foco")
        ruta = RutaEntrega.objects.create(nombre="Ruta Focus", fecha_ruta="2026-02-24")
        self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "add_entrega",
                "pedido_id": str(pedido.id),
                "secuencia": "1",
                "direccion": "Centro",
                "estatus": "INCIDENCIA",
                "monto_estimado": "100",
            },
            follow=True,
        )

        resp = self.client.get(reverse("logistica:rutas"), {"enterprise_focus": "INCIDENCIAS"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Focos operativos")
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "INCIDENCIAS")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_redirect_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)


class LogisticaPwaApiTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="repartidor.api", password="pass123")
        group, _ = Group.objects.get_or_create(name="repartidor")
        self.user.groups.add(group)
        self.sucursal = Sucursal.objects.create(codigo="QA-LOG", nombre="QA Logística", activa=True)
        self.unidad = Unidad.objects.create(codigo="QA-LOG-1", descripcion="Unidad QA", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.repartidor.licencia_expiracion = timezone.localdate() + timezone.timedelta(days=30)
        self.repartidor.save(update_fields=["licencia_expiracion"])
        self.client.force_login(self.user)

    def test_bitacora_activa_without_open_shift_returns_null_not_404(self):
        response = self.client.get(reverse("api_logistica_bitacora_salida_activa"))

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")

    def test_session_token_bridges_unified_app_login_to_logistica_pwa(self):
        response = self.client.get(reverse("api_logistica_auth_session_token"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("access", payload)
        self.assertIn("refresh", payload)

        perfil = self.client.get(reverse("api_logistica_mi_perfil"), HTTP_AUTHORIZATION=f"Bearer {payload['access']}")
        self.assertEqual(perfil.status_code, 200)
        self.assertEqual(perfil.json()["user"]["username"], self.user.username)

        self.client.logout()

    def test_bitacora_salida_bloquea_unidad_distinta_si_hay_ruta_activa(self):
        unidad_ruta = Unidad.objects.create(codigo="QA-RUTA", descripcion="Unidad ruta", sucursal=self.sucursal)
        RutaEntrega.objects.create(
            nombre="Ruta Unidad Correcta",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=unidad_ruta,
        )

        response = self.client.post(
            reverse("api_logistica_bitacora_salida"),
            {
                "unidad": self.unidad.id,
                "km_salida": "1000",
                "nivel_gas_salida": "lleno",
                "latitud_salida": "25.570000",
                "longitud_salida": "-108.470000",
                "foto_tablero_salida": SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "unidad_ruta_distinta")
        self.assertEqual(response.json()["unidad_requerida"]["codigo"], "QA-RUTA")
        self.assertFalse(BitacoraSalidaLlegada.objects.filter(repartidor=self.repartidor).exists())

    def test_bitacora_salida_permite_unidad_asignada_a_ruta_activa(self):
        RutaEntrega.objects.create(
            nombre="Ruta Unidad Correcta",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )

        response = self.client.post(
            reverse("api_logistica_bitacora_salida"),
            {
                "unidad": self.unidad.id,
                "km_salida": "1000",
                "nivel_gas_salida": "lleno",
                "latitud_salida": "25.570000",
                "longitud_salida": "-108.470000",
                "foto_tablero_salida": SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
            },
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["unidad"], self.unidad.id)

    def test_occasional_driver_with_repartidor_profile_can_operate_without_repartidor_group(self):
        user = User.objects.create_user(username="conductora.ocasional", password="pass123")
        user.groups.add(Group.objects.get_or_create(name="PRODUCCION")[0])
        Repartidor.objects.create(
            user=user,
            sucursal=self.sucursal,
            tipo_identidad=Repartidor.TIPO_EMPLEADO_CONDUCTOR_OCASIONAL,
            motivo_autorizacion="Uso ocasional de unidades",
            autorizado_por="Direccion",
        )

        self.assertTrue(_can_operate_pwa(user))

    def test_session_token_rejects_users_without_logistica_access(self):
        self.client.logout()
        other = User.objects.create_user(username="sin.logistica", password="pass123")
        self.client.force_login(other)

        response = self.client.get(reverse("api_logistica_auth_session_token"))

        self.assertEqual(response.status_code, 403)

    def test_mantenimiento_user_can_open_logistica_pwa_without_logistica_module(self):
        self.client.logout()
        user = User.objects.create_user(username="mant.pwa", password="pass123")
        UserModuleAccess.objects.create(user=user, module="mantenimiento", access=ACCESS_MANAGE)
        self.client.force_login(user)

        response = self.client.get(reverse("logistica:pwa_app"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Logística")


class LogisticaControlRutasTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="ruta.control", password="pass123")
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.user.groups.add(Group.objects.get_or_create(name="LOGISTICA")[0])
        self.sucursal = Sucursal.objects.create(codigo="CTRL-LOG", nombre="Control Logística", activa=True)
        self.unidad = Unidad.objects.create(codigo="CTRL-01", descripcion="Unidad control", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.bitacora = BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", b"gif", content_type="image/gif"),
        )
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta Control",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
            bitacora_salida=self.bitacora,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Control",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=1)

    def _crear_transferencia_point_abierta(self, *, sucursal=None, item_name="Pastel Snicker chico", source_hash="transfer-snicker-1", registered_at=None):
        sucursal = sucursal or self.sucursal
        origin = PointBranch.objects.create(external_id=f"CEDIS-{source_hash}", name="CEDIS", erp_branch=None)
        destination = PointBranch.objects.create(external_id=f"SUC-{source_hash}", name=sucursal.nombre, erp_branch=sucursal)
        return PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_origin_branch=None,
            erp_destination_branch=sucursal,
            transfer_external_id=f"T-{source_hash}",
            detail_external_id=f"D-{source_hash}",
            source_hash=source_hash,
            registered_at=registered_at or timezone.now(),
            sent_at=timezone.now(),
            item_name=item_name,
            item_code="SNICK-CH",
            unit="pz",
            requested_quantity="5.000",
            sent_quantity="5.000",
            received_quantity="0.000",
            is_open=True,
            is_received=False,
            is_cancelled=False,
            is_finalized=False,
        )

    def _crear_linea_carga_con_transferencia_recibida(
        self,
        *,
        source_hash="transfer-recibida-1",
        sent_quantity="5.000",
        loaded_quantity="5.000",
        received_quantity="5.000",
        is_received=True,
    ):
        origin = PointBranch.objects.create(external_id=f"CEDIS-R-{source_hash}", name="CEDIS", erp_branch=None)
        destination = PointBranch.objects.create(external_id=f"SUC-R-{source_hash}", name=self.sucursal.nombre, erp_branch=self.sucursal)
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
        )
        transfer_line = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_origin_branch=None,
            erp_destination_branch=self.sucursal,
            sync_job=sync_job,
            transfer_external_id=f"T-R-{source_hash}",
            detail_external_id=f"D-R-{source_hash}",
            source_hash=source_hash,
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            received_at=timezone.now() if is_received else None,
            item_name="Pastel Snicker chico",
            item_code="SNICK-CH",
            unit="pz",
            requested_quantity=sent_quantity,
            sent_quantity=sent_quantity,
            received_quantity=received_quantity,
            is_open=not is_received,
            is_received=is_received,
            is_cancelled=False,
            is_finalized=is_received,
        )
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta, estatus=RutaCargaChecklist.ESTATUS_CONFIRMADA)
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            point_transfer_line=transfer_line,
            transfer_external_id=transfer_line.transfer_external_id,
            detail_external_id=transfer_line.detail_external_id,
            source_hash=transfer_line.source_hash,
            item_code=transfer_line.item_code,
            item_name=transfer_line.item_name,
            unit=transfer_line.unit,
            cantidad_solicitada=transfer_line.requested_quantity,
            cantidad_enviada_esperada=transfer_line.sent_quantity,
            cantidad_cargada=loaded_quantity,
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        return checklist, linea, transfer_line

    def _crear_ruta_planeada_para_carga(self):
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Carga Point",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)
        return ruta, parada

    def test_distancia_metros_detecta_punto_cercano(self):
        self.assertLess(distancia_metros("25.570010", "-108.470010", self.punto.latitud, self.punto.longitud), 5)

    @override_settings(LOGISTICA_FALLBACK_SPEED_KMH=36)
    def test_ruta_programada_fallback_calcula_tiempo_por_velocidad_configurada(self):
        punto_2 = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Control Dos",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )
        ruta = RutaEntrega.objects.create(nombre="Ruta Fallback Tiempo", fecha_ruta=timezone.localdate())
        ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)
        ParadaRuta.objects.create(ruta=ruta, punto=punto_2, orden=2)

        from logistica.services_google_routes import recalcular_ruta_programada

        recalcular_ruta_programada(ruta)
        ruta.refresh_from_db()

        self.assertEqual(ruta.ruta_programada_fuente, "FALLBACK")
        self.assertGreater(ruta.ruta_programada_distancia_metros, 0)
        self.assertEqual(ruta.ruta_programada_duracion_segundos, int(round((ruta.ruta_programada_distancia_metros / 1000) / 36 * 3600)))

    def test_resumen_tiempos_ruta_usa_promedio_historico_en_punto(self):
        llegada = timezone.now() - timezone.timedelta(hours=2)
        ruta_historica = RutaEntrega.objects.create(nombre="Ruta Histórica", fecha_ruta=timezone.localdate() - timezone.timedelta(days=1))
        ParadaRuta.objects.create(
            ruta=ruta_historica,
            punto=self.punto,
            orden=1,
            hora_llegada_real=llegada,
            hora_salida_real=llegada + timezone.timedelta(minutes=24),
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.ruta.ruta_programada_duracion_segundos = 1800
        self.ruta.save(update_fields=["ruta_programada_duracion_segundos", "updated_at"])

        resumen = resumen_tiempos_ruta(self.ruta)

        self.assertEqual(resumen.transito_programado_minutos, 30)
        self.assertEqual(resumen.surtido_estimado_minutos, 24)
        self.assertEqual(resumen.total_operativo_estimado_minutos, 54)
        self.assertEqual(resumen.paradas[0].promedio_surtido_minutos, 24)

    def test_resumen_tiempos_no_contamina_promedio_con_parada_actual(self):
        llegada = timezone.now() - timezone.timedelta(hours=2)
        ruta_historica = RutaEntrega.objects.create(nombre="Ruta Histórica Promedio", fecha_ruta=timezone.localdate() - timezone.timedelta(days=1))
        ParadaRuta.objects.create(
            ruta=ruta_historica,
            punto=self.punto,
            orden=1,
            hora_llegada_real=llegada,
            hora_salida_real=llegada + timezone.timedelta(minutes=24),
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.parada.hora_llegada_real = llegada + timezone.timedelta(hours=1)
        self.parada.hora_salida_real = self.parada.hora_llegada_real + timezone.timedelta(minutes=60)
        self.parada.save(update_fields=["hora_llegada_real", "hora_salida_real", "actualizado_en"])

        resumen = resumen_tiempos_ruta(self.ruta)

        self.assertEqual(resumen.paradas[0].permanencia_real_minutos, 60)
        self.assertEqual(resumen.paradas[0].promedio_surtido_minutos, 24)

    def test_ruta_detail_muestra_tiempos_de_transito_y_surtido(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        llegada = timezone.now() - timezone.timedelta(hours=2)
        ruta_historica = RutaEntrega.objects.create(nombre="Ruta Histórica Detail", fecha_ruta=timezone.localdate() - timezone.timedelta(days=1))
        ParadaRuta.objects.create(
            ruta=ruta_historica,
            punto=self.punto,
            orden=1,
            hora_llegada_real=llegada,
            hora_salida_real=llegada + timezone.timedelta(minutes=24),
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.ruta.ruta_programada_duracion_segundos = 1800
        self.ruta.save(update_fields=["ruta_programada_duracion_segundos", "updated_at"])

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tránsito programado")
        self.assertContains(response, "Promedio surtido")
        self.assertContains(response, "24 min")
        self.assertContains(response, "54")

    def test_registrar_ubicacion_marca_geocerca_visitada(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "precision_metros": 0,
                "velocidad_kmh": 0,
                "bateria_porcentaje": 0,
            },
            ip_registro="127.0.0.1",
        )

        self.parada.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertFalse(ubicacion.fuera_de_geocerca)
        self.assertEqual(ubicacion.precision_metros, 0)
        self.assertEqual(ubicacion.velocidad_kmh, 0)
        self.assertEqual(ubicacion.bateria_porcentaje, 0)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, 100)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).exists())

    def test_parada_conserva_geocerca_planeada_si_punto_maestro_cambia(self):
        self.assertEqual(self.parada.punto_nombre_snapshot, "Sucursal Control")
        self.assertEqual(self.parada.radio_geocerca_metros, 120)

        self.punto.nombre = "Sucursal Control Reubicada"
        self.punto.latitud = "25.900000"
        self.punto.longitud = "-108.900000"
        self.punto.radio_geocerca_metros = 20
        self.punto.save(update_fields=["nombre", "latitud", "longitud", "radio_geocerca_metros", "actualizado_en"])

        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )

        self.parada.refresh_from_db()
        self.assertFalse(ubicacion.fuera_de_geocerca)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertEqual(self.parada.punto_nombre_snapshot, "Sucursal Control")
        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE)
        self.assertIn("Sucursal Control", evento.descripcion)
        self.assertNotIn("Reubicada", evento.descripcion)

    def test_registrar_ubicacion_fuera_de_geocerca_crea_desvio(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
                "fuera_de_ruta_confirmado": True,
                "desvio_motivo": "Entrega urgente",
            },
        )

        self.assertTrue(ubicacion.fuera_de_geocerca)
        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO, severidad=EventoRuta.SEVERIDAD_CRITICA)
        self.assertEqual(evento.metadata["motivo"], "Entrega urgente")

    def test_registrar_ubicacion_fuera_de_geocerca_crea_desvio_automatico_sin_confirmacion(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
            },
        )

        self.assertTrue(ubicacion.fuera_de_geocerca)
        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO, severidad=EventoRuta.SEVERIDAD_CRITICA)
        self.assertEqual(evento.metadata["origen"], "automatico_geocerca")
        self.assertEqual(evento.metadata["motivo"], "Desvío detectado automáticamente por GPS fuera de geocerca.")

    def test_tracking_automatico_fuera_de_geocerca_no_crea_desvio_critico(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.assertTrue(ubicacion.fuera_de_geocerca)
        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO).exists())

    def test_tracking_precision_baja_no_marca_parada_visitada(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "precision_metros": "180",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIn("precision_baja", ubicacion._alertas_tracking)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PRECISION_BAJA).exists())

    def test_tracking_ubicacion_tardia_crea_alerta_y_no_marca_parada(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "timestamp_dispositivo": timezone.now() - timezone.timedelta(minutes=12),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIn("ubicacion_tardia", ubicacion._alertas_tracking)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_UBICACION_TARDIA).exists())

    def test_tracking_salto_imposible_crea_alerta(self):
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "timestamp_dispositivo": timezone.now(),
            },
        )
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.900000",
                "longitud": "-108.900000",
                "timestamp_dispositivo": timezone.now() + timezone.timedelta(seconds=10),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.assertIn("salto_imposible", ubicacion._alertas_tracking)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALTO_IMPOSIBLE).exists())

    def test_tracking_payload_duplicado_por_timestamp_no_duplica_ubicacion(self):
        timestamp = timezone.now()
        payload = {
            "latitud": "25.570010",
            "longitud": "-108.470010",
            "timestamp_dispositivo": timestamp,
            "tracking_origen": "automatico_pwa",
        }

        primera = registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)
        segunda = registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)

        self.assertEqual(primera.id, segunda.id)
        self.assertEqual(self.ruta.ubicaciones.count(), 1)

    def test_tracking_api_rechaza_desvio_confirmado_sin_motivo(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            json.dumps(
                {
                    "latitud": "25.590000",
                    "longitud": "-108.490000",
                    "fuera_de_ruta_confirmado": True,
                    "desvio_motivo": "   ",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO).exists())

    def test_tracking_api_rechaza_coordenadas_invalidas(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"latitud": "120.000000", "longitud": "-108.470010"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_tracking_api_rechaza_precision_negativa(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"latitud": "25.570010", "longitud": "-108.470010", "precision_metros": "-1"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_tracking_api_rechaza_ruta_de_otro_repartidor(self):
        other_user = User.objects.create_user(username="otro.repartidor", password="pass123")
        other_user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        Repartidor.objects.create(user=other_user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.client.force_login(other_user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            '{"latitud": "25.570010", "longitud": "-108.470010"}',
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    def test_api_ruta_activa_devuelve_paradas_del_repartidor(self):
        self.client.force_login(self.user)

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ruta"]["id"], self.ruta.id)
        self.assertEqual(response.json()["paradas"][0]["id"], self.parada.id)
        self.assertEqual(response.json()["paradas"][0]["punto_nombre_snapshot"], "Sucursal Control")

    def test_tracking_api_registra_ubicacion_de_ruta_activa(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            '{"latitud": "25.570010", "longitud": "-108.470010", "velocidad_kmh": 0}',
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertFalse(response.json()["fuera_de_geocerca"])

    def test_tracking_api_no_expone_gps_historico_a_consulta_general(self):
        viewer = User.objects.create_user(username="visor.rutas", password="pass123")
        UserModuleAccess.objects.create(user=viewer, module="logistica", access=ACCESS_VIEW)
        self.client.force_login(viewer)

        response = self.client.get(reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}))

        self.assertEqual(response.status_code, 403)

    def test_eventos_api_rechaza_eventos_automaticos_manuales(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_eventos", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"tipo": EventoRuta.TIPO_SALIDA, "severidad": EventoRuta.SEVERIDAD_INFO, "descripcion": "Salida manual"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_eventos_api_rechaza_evento_manual_en_ruta_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        response = self.client.post(
            reverse("api_logistica_ruta_eventos", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"tipo": EventoRuta.TIPO_INCIDENCIA_MANUAL, "severidad": EventoRuta.SEVERIDAD_ALERTA, "descripcion": "Evento tardío"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_eventos_api_rechaza_coordenadas_invalidas(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_eventos", kwargs={"ruta_id": self.ruta.id}),
            json.dumps(
                {
                    "tipo": EventoRuta.TIPO_INCIDENCIA_MANUAL,
                    "severidad": EventoRuta.SEVERIDAD_ALERTA,
                    "descripcion": "Incidencia manual",
                    "latitud": "0.000000",
                    "longitud": "0.000000",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_resumen_control_no_materializa_gps_perdido(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )
        UbicacionRuta = ubicacion.__class__
        UbicacionRuta.objects.filter(pk=ubicacion.pk).update(timestamp_servidor=timezone.now() - timezone.timedelta(minutes=20))

        resumen_control_rutas(fecha=self.ruta.fecha_ruta)

        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO).exists())

    def test_task_detecta_gps_perdido_por_ultima_senal_y_es_idempotente(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )
        UbicacionRuta = ubicacion.__class__
        UbicacionRuta.objects.filter(pk=ubicacion.pk).update(timestamp_servidor=timezone.now() - timezone.timedelta(minutes=20))

        resultado = detectar_gps_perdido_rutas(umbral_minutos=10)
        detectar_gps_perdido_rutas(umbral_minutos=10)

        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO)
        self.assertEqual(resultado["eventos_gps_perdido"], 1)
        self.assertEqual(evento.ubicacion_id, ubicacion.id)
        self.assertEqual(evento.metadata["detectado_por"], "celery")
        self.assertEqual(evento.metadata["ultima_ubicacion_id"], ubicacion.id)

    def test_task_detecta_gps_perdido_sin_primera_senal_y_es_idempotente(self):
        self.ruta.hora_inicio_real = timezone.now() - timezone.timedelta(minutes=20)
        self.ruta.save(update_fields=["hora_inicio_real", "updated_at"])

        resultado = detectar_gps_perdido_rutas(umbral_minutos=10)
        detectar_gps_perdido_rutas(umbral_minutos=10)

        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO)
        self.assertEqual(resultado["eventos_gps_perdido"], 1)
        self.assertTrue(evento.metadata["sin_primera_senal"])
        self.assertEqual(evento.metadata["detectado_por"], "celery")

    def test_task_gps_perdido_ignora_ruta_completada(self):
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.hora_inicio_real = timezone.now() - timezone.timedelta(minutes=20)
        self.ruta.save(update_fields=["estatus", "hora_inicio_real", "updated_at"])

        resultado = detectar_gps_perdido_rutas(umbral_minutos=10)

        self.assertEqual(resultado["rutas_revisadas"], 0)
        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO).exists())

    @patch("logistica.views.snap_gps_path_to_roads")
    def test_control_rutas_view_renderiza_panel_interno(self, snap_mock):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.ruta_programada_polyline = "25.570000,-108.470000|25.571000,-108.471000"
        self.ruta.ruta_programada_fuente = "FALLBACK"
        self.ruta.save(update_fields=["ruta_programada_polyline", "ruta_programada_fuente", "updated_at"])
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010", "precision_metros": "12"},
        )
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570050", "longitud": "-108.470050", "precision_metros": "12"},
        )
        snap_mock.return_value.coordinates = [(25.57001, -108.47001), (25.57050, -108.47050)]
        snap_mock.return_value.source = "GOOGLE_ROADS"
        snap_mock.return_value.warning = ""

        response = self.client.get(reverse("logistica:control_rutas"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Control interno de rutas")
        self.assertContains(response, "Rutas del día")
        self.assertContains(response, "Mapa real · OpenStreetMap")
        self.assertContains(response, "route-control-map-data")
        self.assertNotContains(response, "Vista esquemática, no evidencia GPS")
        self.assertContains(response, "Filtrar")
        mapa = response.context["mapa_rutas"]
        self.assertEqual(mapa["routes"][0]["paradas"][0]["nombre"], "Sucursal Control")
        self.assertEqual(mapa["routes"][0]["ubicaciones"][0]["lat"], 25.57001)
        self.assertEqual(mapa["routes"][0]["ubicaciones_snapped_fuente"], "GOOGLE_ROADS")
        self.assertEqual(mapa["routes"][0]["ubicaciones_segmentos"][0]["fuente"], "GOOGLE_ROADS")
        self.assertEqual(mapa["routes"][0]["ubicaciones_snapped"][1]["lng"], -108.4705)
        self.assertEqual(mapa["routes"][0]["programada_polyline"], "25.570000,-108.470000|25.571000,-108.471000")

    @patch("logistica.views.snap_gps_path_to_roads")
    def test_control_rutas_mapa_no_conecta_puntos_gps_descartados(self, snap_mock):
        def snap_side_effect(*, ruta_id, coords):
            return SimpleNamespace(coordinates=coords, source="RAW", warning="")

        snap_mock.side_effect = snap_side_effect
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        base = timezone.now()
        ubicaciones = [
            ("25.570000", "-108.470000", base),
            ("25.570100", "-108.470100", base + timezone.timedelta(seconds=45)),
            ("25.590000", "-108.490000", base + timezone.timedelta(seconds=90)),
            ("25.571000", "-108.471000", base + timezone.timedelta(seconds=135)),
            ("25.571100", "-108.471100", base + timezone.timedelta(seconds=180)),
        ]
        creadas = [
            UbicacionRuta.objects.create(
                ruta=self.ruta,
                repartidor=self.repartidor,
                unidad=self.unidad,
                latitud=lat,
                longitud=lng,
                timestamp_servidor=timestamp,
            )
            for lat, lng, timestamp in ubicaciones
        ]
        EventoRuta.objects.create(
            ruta=self.ruta,
            ubicacion=creadas[2],
            tipo=EventoRuta.TIPO_GPS_PRECISION_BAJA,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion="Precisión GPS baja: 180 m.",
            latitud=creadas[2].latitud,
            longitud=creadas[2].longitud,
        )

        response = self.client.get(reverse("logistica:control_rutas"))

        self.assertEqual(response.status_code, 200)
        route = response.context["mapa_rutas"]["routes"][0]
        self.assertEqual(len(route["ubicaciones_segmentos"]), 2)
        self.assertEqual([len(segment["coords"]) for segment in route["ubicaciones_segmentos"]], [2, 2])
        descartadas = [point for point in route["ubicaciones"] if point["trazo_descartado"]]
        self.assertEqual(len(descartadas), 1)
        self.assertEqual(descartadas[0]["alertas_tracking"], ["precision_baja"])
        self.assertEqual(snap_mock.call_count, 2)

    @override_settings(GOOGLE_SERVER_API_KEY="server-key", GOOGLE_ROADS_SNAP_ENABLED=True)
    @patch("logistica.services_google_roads.cache")
    @patch("logistica.services_google_roads.requests.get")
    def test_snap_gps_path_to_roads_usa_google_roads(self, get_mock, cache_mock):
        cache_mock.get.return_value = None
        get_mock.return_value.status_code = 200
        get_mock.return_value.json.return_value = {
            "snappedPoints": [
                {"location": {"latitude": 25.57001, "longitude": -108.47001}},
                {"location": {"latitude": 25.57020, "longitude": -108.47020}},
                {"location": {"latitude": 25.57050, "longitude": -108.47050}},
            ]
        }

        result = snap_gps_path_to_roads(
            ruta_id=self.ruta.id,
            coords=[(25.57001, -108.47001), (25.57050, -108.47050)],
        )

        self.assertEqual(result.source, "GOOGLE_ROADS")
        self.assertEqual(len(result.coordinates), 3)
        self.assertIn("snapToRoads", get_mock.call_args.args[0])
        self.assertEqual(get_mock.call_args.kwargs["params"]["interpolate"], "true")
        cache_mock.set.assert_called_once()

    @override_settings(GOOGLE_SERVER_API_KEY="server-key", GOOGLE_ROADS_SNAP_ENABLED=True)
    @patch("logistica.services_google_roads.cache")
    @patch("logistica.services_google_roads.requests.get")
    def test_snap_gps_path_to_roads_fallback_si_google_bloquea(self, get_mock, cache_mock):
        cache_mock.get.return_value = None
        get_mock.return_value.status_code = 403
        get_mock.return_value.json.return_value = {"error": {"status": "PERMISSION_DENIED"}}
        coords = [(25.57001, -108.47001), (25.57050, -108.47050)]

        result = snap_gps_path_to_roads(ruta_id=self.ruta.id, coords=coords)

        self.assertEqual(result.source, "RAW")
        self.assertEqual(result.warning, "google_roads_http_403")
        self.assertEqual(result.coordinates, coords)

    def test_control_rutas_filtra_por_ruta_repartidor_o_unidad(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.get(reverse("logistica:control_rutas"), {"q": "Control"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ruta Control")
        self.assertContains(response, "Filtro activo: Control")

        response = self.client.get(reverse("logistica:control_rutas"), {"q": "No existe"})

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Ruta Control")
        self.assertContains(response, "No hay rutas programadas para esta fecha.")

    def test_control_rutas_filtra_evidencia_por_tipo(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        EventoRuta.objects.create(
            ruta=self.ruta,
            tipo=EventoRuta.TIPO_SALIDA,
            severidad=EventoRuta.SEVERIDAD_INFO,
            descripcion="Salida visible",
        )
        EventoRuta.objects.create(
            ruta=self.ruta,
            tipo=EventoRuta.TIPO_DESVIO,
            severidad=EventoRuta.SEVERIDAD_CRITICA,
            descripcion="Desvio filtrado",
        )

        response = self.client.get(reverse("logistica:control_rutas"), {"evento": EventoRuta.TIPO_DESVIO})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Desvio filtrado")
        self.assertNotContains(response, "Salida visible")
        self.assertContains(response, f'value="{EventoRuta.TIPO_DESVIO}" selected')

    def test_pwa_tracking_declara_cola_offline_reintento_y_cache_versionado(self):
        from pathlib import Path

        base_dir = Path(__file__).resolve().parent
        pwa_html = (base_dir / "templates" / "logistica" / "pwa.html").read_text(encoding="utf-8")
        sw_js = (base_dir / "static" / "logistica" / "pwa" / "sw.js").read_text(encoding="utf-8")

        self.assertIn("pd_logistica_tracking_queue", pwa_html)
        self.assertIn("enqueueRutaTracking", pwa_html)
        self.assertIn("flushRutaTrackingQueue", pwa_html)
        self.assertIn("Sin conexión: seguimiento guardado para reintento.", pwa_html)
        self.assertIn("route-control-v22", pwa_html)
        self.assertIn("logistica:pwa_sw", pwa_html)
        self.assertIn('scope: "/logistica/"', pwa_html)
        self.assertIn("pollyanas-logistica-pwa-v22-token-sesion", sw_js)
        self.assertIn("const ROUTE_AUTO_TRACKING_INTERVAL_MS = 45 * 1000;", pwa_html)
        self.assertIn('activo: "Activo cada 45 s"', pwa_html)
        self.assertIn('gps_sin_senal: "GPS sin señal"', pwa_html)
        self.assertIn('pendiente_offline: "Sin internet, guardado"', pwa_html)
        self.assertIn('gps_denegado: "Ubicación bloqueada"', pwa_html)
        self.assertNotIn('Activo cada 90 s', pwa_html)
        self.assertNotIn('Pendiente local', pwa_html)
        self.assertIn("TRACKING_QUEUE_TTL_MS", pwa_html)
        self.assertIn("normalizeRutaTrackingQueue", pwa_html)
        self.assertIn("purgeRutaTrackingQueue", pwa_html)
        self.assertIn("velocidad_kmh: Number.isFinite(position.coords.speed)", pwa_html)
        self.assertIn("document.addEventListener(\"visibilitychange\"", pwa_html)
        self.assertIn("window.addEventListener(\"pagehide\"", pwa_html)
        self.assertIn("automatico_pwa", pwa_html)
        self.assertIn("Auto-tracking", pwa_html)
        self.assertIn("let payload = null;", pwa_html)
        self.assertIn("enqueueRutaTracking({ ruta_id: rutaId, payload });", pwa_html)
        self.assertIn("sessionStorage.getItem(ACCESS_TOKEN_KEY)", pwa_html)
        self.assertIn("sessionStorage.setItem(ACCESS_TOKEN_KEY, data.access)", pwa_html)
        self.assertIn("sessionStorage.removeItem(ACCESS_TOKEN_KEY);", pwa_html)
        self.assertIn("const response = await fetch(`${API}/auth/session-token/`", pwa_html)
        self.assertIn('error.error === "unidad_ruta_distinta"', pwa_html)
        self.assertIn('await showScreen("ruta_activa")', pwa_html)
        self.assertNotIn('localStorage.setItem("pd_logistica_refresh"', pwa_html)
        self.assertNotIn("localStorage.setItem(REFRESH_TOKEN_KEY", pwa_html)

    def test_pwa_sw_se_sirve_sin_cache_de_borde(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.get(reverse("logistica:pwa_sw"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("no-cache", response["Cache-Control"])
        self.assertIn("no-store", response["Cache-Control"])
        self.assertIn("pollyanas-logistica-pwa-v22-token-sesion", response.content.decode("utf-8"))

    def test_pwa_mi_ruta_declara_prototipo_operativo(self):
        from pathlib import Path

        base_dir = Path(__file__).resolve().parent
        pwa_html = (base_dir / "templates" / "logistica" / "pwa.html").read_text(encoding="utf-8")

        self.assertIn("route-hero", pwa_html)
        self.assertIn("route-dashboard-card", pwa_html)
        self.assertNotIn("grid-column: 1 / -1", pwa_html)
        self.assertIn("route-signal-grid", pwa_html)
        self.assertIn("route-progress-card", pwa_html)
        self.assertIn("Capturar ubicación GPS", pwa_html)
        self.assertIn("Reportar desvío", pwa_html)
        self.assertIn("Paradas de reparto", pwa_html)
        self.assertIn("Recepción Point pendiente", pwa_html)
        self.assertIn("Point recibió", pwa_html)
        self.assertIn("La ruta puede continuar; cierre final espera recepción Point.", pwa_html)
        self.assertIn('draft.geoStatus === "idle" ? "" : geoOverlay(draft, "capturarUbicacionRuta")', pwa_html)

    def test_puntos_logisticos_crea_punto_manual(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Sucursal Nueva",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "sucursal": self.sucursal.id,
                "latitud": "25.571000",
                "longitud": "-108.471000",
                "radio_geocerca_metros": "90",
                "activo": "on",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(PuntoLogistico.objects.filter(nombre="Sucursal Nueva", radio_geocerca_metros=90).exists())
        self.assertContains(response, "Puntos existentes")

    def test_punto_logistico_edit_y_toggle(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        punto_libre = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Punto Libre",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.573000",
            longitud="-108.473000",
            radio_geocerca_metros=80,
        )

        response = self.client.post(
            reverse("logistica:punto_logistico_edit", kwargs={"pk": punto_libre.id}),
            {
                "nombre": "Sucursal Control Editada",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "sucursal": self.sucursal.id,
                "latitud": "25.575000",
                "longitud": "-108.475000",
                "radio_geocerca_metros": "150",
                "activo": "on",
                "notas": "Entrada por estacionamiento",
            },
            follow=True,
        )

        punto_libre.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(punto_libre.nombre, "Sucursal Control Editada")
        self.assertEqual(punto_libre.radio_geocerca_metros, 150)
        self.assertTrue(punto_libre.activo)

        toggle = self.client.post(reverse("logistica:punto_logistico_toggle", kwargs={"pk": punto_libre.id}), follow=True)
        punto_libre.refresh_from_db()
        self.assertEqual(toggle.status_code, 200)
        self.assertFalse(punto_libre.activo)

    def test_punto_logistico_no_desactiva_si_esta_en_ruta_abierta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(reverse("logistica:punto_logistico_toggle", kwargs={"pk": self.punto.id}), follow=True)

        self.punto.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.punto.activo)
        self.assertContains(response, "No se puede desactivar")

    def test_punto_logistico_edit_no_desactiva_si_esta_en_ruta_abierta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:punto_logistico_edit", kwargs={"pk": self.punto.id}),
            {
                "nombre": self.punto.nombre,
                "tipo": self.punto.tipo,
                "sucursal": self.sucursal.id,
                "latitud": self.punto.latitud,
                "longitud": self.punto.longitud,
                "radio_geocerca_metros": self.punto.radio_geocerca_metros,
            },
        )

        self.punto.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.punto.activo)
        self.assertContains(response, "No se puede desactivar")

    def test_punto_logistico_rechaza_radio_invalido(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Punto Inválido",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "latitud": "25.571000",
                "longitud": "-108.471000",
                "radio_geocerca_metros": "abc",
                "activo": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "El radio debe ser un número entero.")
        self.assertFalse(PuntoLogistico.objects.filter(nombre="Punto Inválido").exists())

    def test_punto_logistico_rechaza_coordenadas_fuera_de_rango(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Punto Fuera",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "latitud": "120.000000",
                "longitud": "-108.471000",
                "radio_geocerca_metros": "80",
                "activo": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "La latitud debe estar entre -90 y 90.")
        self.assertFalse(PuntoLogistico.objects.filter(nombre="Punto Fuera").exists())

    def test_punto_logistico_rechaza_punto_activo_demasiado_cercano(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Sucursal Duplicada",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "sucursal": self.sucursal.id,
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "radio_geocerca_metros": "80",
                "activo": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ya existe un punto activo")
        self.assertFalse(PuntoLogistico.objects.filter(nombre="Sucursal Duplicada").exists())

    def test_ruta_detail_planea_paradas_y_libera_ruta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        ruta = RutaEntrega.objects.create(nombre="Ruta Manual", fecha_ruta=timezone.localdate())

        blocked = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )
        ruta.refresh_from_db()
        self.assertEqual(blocked.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(blocked, "No se puede liberar la ruta")

        self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "update_plan",
                "nombre": "Ruta Manual",
                "fecha_ruta": timezone.localdate().isoformat(),
                "repartidor": self.repartidor.id,
                "unidad_operativa": self.unidad.id,
                "km_estimado": "12.5",
            },
            follow=True,
        )
        self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "add_parada", "punto": self.punto.id, "orden": "1"},
            follow=True,
        )
        released = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(released.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertEqual(ruta.repartidor, self.repartidor)
        self.assertEqual(ruta.unidad_operativa, self.unidad)
        self.assertTrue(ruta.paradas.filter(punto=self.punto).exists())
        self.assertTrue(EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_SALIDA).exists())

    def test_ruta_en_ruta_no_permite_editar_paradas(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "delete_parada", "parada_id": self.parada.id},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(ParadaRuta.objects.filter(pk=self.parada.id).exists())
        self.assertContains(response, "planeación queda congelada")

    def test_ruta_en_ruta_no_permite_actualizar_entrega_oculta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        entrega = EntregaRuta.objects.create(ruta=self.ruta, secuencia=1, cliente_nombre="Cliente", estatus=EntregaRuta.ESTATUS_PENDIENTE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "entrega_status", "entrega_id": entrega.id, "estatus": EntregaRuta.ESTATUS_ENTREGADA},
            follow=True,
        )

        entrega.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(entrega.estatus, EntregaRuta.ESTATUS_PENDIENTE)
        self.assertContains(response, "planeación queda congelada")

    def test_ruta_model_clean_rechaza_en_ruta_sin_estructura(self):
        ruta = RutaEntrega(nombre="Ruta Inválida", fecha_ruta=timezone.localdate(), estatus=RutaEntrega.ESTATUS_EN_RUTA)

        with self.assertRaises(ValidationError) as ctx:
            ruta.full_clean()

        self.assertIn("repartidor", ctx.exception.message_dict)
        self.assertIn("unidad_operativa", ctx.exception.message_dict)
        self.assertIn("estatus", ctx.exception.message_dict)

    def test_ruta_status_bloquea_segunda_ruta_activa_mismo_repartidor(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Duplicada",
            fecha_ruta=timezone.localdate(),
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(response, "el repartidor ya tiene otra ruta en curso")

    def test_ruta_status_bloquea_completar_con_paradas_pendientes(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "hay paradas pendientes")

    def test_ruta_status_bloquea_completar_con_entrega_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "hay paradas sin entrega confirmada")

    def test_ruta_status_no_reabre_ruta_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_PLANEADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        self.assertContains(response, "no puede reabrirse")

    def test_ruta_status_no_regresa_en_ruta_a_planeada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_PLANEADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "no puede regresar a planeada")

    def test_ruta_status_no_completa_ruta_planeada_vacia(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta = RutaEntrega.objects.create(nombre="Ruta Vacía", fecha_ruta=timezone.localdate())

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(response, "Solo puedes completar una ruta que ya está en seguimiento")

    def test_api_ruta_status_no_reabre_ruta_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_PLANEADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_api_ruta_status_no_regresa_en_ruta_a_planeada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_PLANEADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)

    def test_api_ruta_status_bloquea_completar_con_entrega_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_COMPLETADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertIn("entrega confirmada", response.json()["detail"])

    def test_api_ruta_status_bloquea_completar_con_diferencia_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "entrega_estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_COMPLETADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertIn("diferencias", response.json()["detail"])

    def test_api_confirma_entrega_de_parada_con_evidencia_idempotente(self):
        self.client.force_login(self.user)
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="T-ENTREGA-1",
            detail_external_id="D-ENTREGA-1",
            source_hash="entrega-source-1",
            item_code="SNICK-CH",
            item_name="Pastel Snicker chico",
            unit="pz",
            cantidad_solicitada="5.000",
            cantidad_enviada_esperada="5.000",
        )
        payload = {
            "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
            "notas": "Recibido completo",
            "evidencias": [
                {
                    "linea_carga_id": linea.id,
                    "tipo": ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                    "cantidad_entregada": "5.000",
                    "comentario": "Sucursal confirma piezas completas",
                    "latitud": "25.570010",
                    "longitud": "-108.470010",
                    "precision_metros": "12.00",
                    "client_event_id": "evt-entrega-1",
                }
            ],
        }

        url = reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id})
        response = self.client.post(url, json.dumps(payload), content_type="application/json")
        retry = self.client.post(url, json.dumps(payload), content_type="application/json")

        self.parada.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.entrega_confirmada_por, self.user)
        self.assertEqual(ParadaEntregaEvidencia.objects.filter(parada=self.parada, client_event_id="evt-entrega-1").count(), 1)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, Decimal("0.00"))

    def test_api_entrega_completa_exige_evidencia(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_ENTREGADA, "evidencias": []}),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertIn("evidencia", response.json()["detail"])

    def test_api_confirma_entrega_con_diferencia_exige_motivo(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_CON_DIFERENCIA, "evidencias": []}),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_api_no_confirma_entrega_de_otro_repartidor(self):
        other_user = User.objects.create_user(username="otro.entrega", password="pass123")
        other_user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        Repartidor.objects.create(user=other_user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.client.force_login(other_user)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_ENTREGADA, "notas": "Intento ajeno"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    def test_api_mantenimiento_sin_repartidor_no_confirma_entrega_de_ruta(self):
        mantenimiento = User.objects.create_user(username="mant.sin.repartidor", password="pass123")
        UserModuleAccess.objects.create(user=mantenimiento, module="mantenimiento", access=ACCESS_VIEW)
        self.client.force_login(mantenimiento)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_ENTREGADA, "evidencias": [{"comentario": "Intento"}]}),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_sincronizar_recepcion_desde_point_confirma_parada_recibida(self):
        self._crear_linea_carga_con_transferencia_recibida()

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertEqual(resumen.paradas_actualizadas, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.entrega_confirmada_por, self.user)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, Decimal("0.00"))
        evidencia = ParadaEntregaEvidencia.objects.get(parada=self.parada)
        self.assertEqual(evidencia.cantidad_entregada, Decimal("5.000"))
        self.assertEqual(evidencia.metadata["origen"], "point_transfer")

    def test_sincronizar_recepcion_importa_transferencia_point_recibida_post_salida(self):
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-post-salida")
        transferencia.is_open = False
        transferencia.is_received = True
        transferencia.is_finalized = True
        transferencia.received_quantity = Decimal("5.000")
        transferencia.received_at = timezone.now()
        transferencia.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertTrue(RutaCargaChecklistLinea.objects.filter(checklist__ruta=self.ruta, source_hash=transferencia.source_hash).exists())
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)

    def test_sincronizar_recepcion_desde_point_no_inventa_si_point_no_recibio(self):
        self._crear_linea_carga_con_transferencia_recibida(is_received=False, received_quantity="0.000")

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertEqual(resumen.lineas_pendientes_point, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertFalse(ParadaEntregaEvidencia.objects.filter(parada=self.parada).exists())

    def test_sincronizar_recepcion_desde_point_marca_diferencia_si_recibido_no_cuadra(self):
        self._crear_linea_carga_con_transferencia_recibida(loaded_quantity="5.000", received_quantity="3.000")

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_CON_DIFERENCIA)

    def test_api_sincroniza_recepcion_point_y_devuelve_parada_actualizada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        _, _, transfer_line = self._crear_linea_carga_con_transferencia_recibida()
        sync_job = transfer_line.sync_job

        with patch("logistica.services_carga_ruta.PointMovementSyncService") as service_cls:
            service_cls.return_value.run_transfer_sync.return_value = sync_job
            response = self.client.post(
                reverse("api_logistica_ruta_recepcion_point_sync", kwargs={"ruta_id": self.ruta.id}),
                "{}",
                content_type="application/json",
            )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["paradas_actualizadas"], 1)
        self.assertEqual(response.json()["lineas_recibidas"], 1)
        self.assertEqual(response.json()["paradas"][0]["entrega_estado"], ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)

    def test_ruta_detail_muestra_recepcion_point_por_producto(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self._crear_linea_carga_con_transferencia_recibida()

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Recepción Point por parada")
        self.assertContains(response, "Sincronizar carga Point")
        self.assertContains(response, 'value="sync_carga_point"')
        self.assertContains(response, "Sincronizar recepción Point")
        self.assertContains(response, "Pastel Snicker chico")
        self.assertContains(response, "Esperado")
        self.assertContains(response, "Cargado")
        self.assertContains(response, "Recibido Point")
        self.assertContains(response, "Recibido correcto")

    def test_ruta_detail_sincroniza_recepcion_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        _, _, transfer_line = self._crear_linea_carga_con_transferencia_recibida()
        sync_job = transfer_line.sync_job

        with patch("logistica.services_carga_ruta.PointMovementSyncService") as service_cls:
            service_cls.return_value.run_transfer_sync.return_value = sync_job
            response = self.client.post(
                reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
                {"action": "sync_recepcion_point"},
                follow=True,
            )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertContains(response, "Recepción Point sincronizada")
        self.assertContains(response, "Recibido correcto")

    def test_ruta_detail_bloquea_salida_si_checklist_carga_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(response, "confirma la carga")

    def test_ruta_detail_bloquea_completar_con_diferencia_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "entrega_estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "diferencias o entregas no recibidas")

    def test_api_ruta_activa_expone_recepcion_point_por_producto(self):
        self.client.force_login(self.user)
        self._crear_linea_carga_con_transferencia_recibida()

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        linea = response.json()["checklist_carga"]["lineas"][0]
        self.assertTrue(linea["point_is_received"])
        self.assertEqual(linea["point_received_quantity"], "5.000")
        self.assertEqual(linea["point_recepcion_estado"], "RECIBIDO_OK")
        self.assertIn("entrega_estado", response.json()["paradas"][0])

    def test_db_bloquea_dos_rutas_en_ruta_mismo_repartidor(self):
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                RutaEntrega.objects.create(
                    nombre="Ruta Activa Duplicada",
                    fecha_ruta=timezone.localdate(),
                    estatus=RutaEntrega.ESTATUS_EN_RUTA,
                    repartidor=self.repartidor,
                    unidad_operativa=self.unidad,
                )

    def test_api_no_crea_ruta_directamente_en_ruta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps({
                "nombre": "Ruta API Directa",
                "fecha_ruta": timezone.localdate().isoformat(),
                "estatus": RutaEntrega.ESTATUS_EN_RUTA,
                "repartidor": self.repartidor.id,
                "unidad_operativa": self.unidad.id,
            }),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta API Directa").exists())

    def test_checklist_carga_se_genera_desde_transferencia_point_abierta(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta()

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.point_transfer_line, transferencia)
        self.assertEqual(linea.item_name, "Pastel Snicker chico")
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal(str(transferencia.sent_quantity)))
        self.assertEqual(linea.source_hash, transferencia.source_hash)

    def test_checklist_carga_incluye_transferencia_del_dia_anterior(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta(
            source_hash="transfer-vespertina-previa",
            registered_at=timezone.now() - timezone.timedelta(days=1),
        )

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.point_transfer_line, transferencia)

    def test_checklist_carga_desbloquea_si_luego_encuentra_transferencias(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(
            ruta=ruta,
            estatus=RutaCargaChecklist.ESTATUS_BLOQUEADA,
            notas="No se encontraron transferencias abiertas de Point para las sucursales de esta ruta.",
        )
        self._crear_transferencia_point_abierta(source_hash="transfer-desbloquea")

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        checklist.refresh_from_db()
        self.assertEqual(resumen.creadas, 1)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_EN_REVISION)
        self.assertEqual(checklist.notas, "")

    def test_checklist_carga_no_duplica_transferencia_point_en_otra_ruta(self):
        ruta_uno, _ = self._crear_ruta_planeada_para_carga()
        ruta_dos, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta()

        primero = sincronizar_checklist_carga_desde_point(ruta=ruta_uno, user=self.user, ejecutar_sync=False)
        segundo = sincronizar_checklist_carga_desde_point(ruta=ruta_dos, user=self.user, ejecutar_sync=False)

        self.assertEqual(primero.creadas, 1)
        self.assertEqual(segundo.creadas, 0)
        self.assertEqual(segundo.omitidas, 1)
        self.assertEqual(RutaCargaChecklistLinea.objects.filter(source_hash=transferencia.source_hash).count(), 1)

    def test_db_bloquea_source_hash_point_duplicado_entre_checklists(self):
        ruta_uno, parada_uno = self._crear_ruta_planeada_para_carga()
        ruta_dos, parada_dos = self._crear_ruta_planeada_para_carga()
        checklist_uno = RutaCargaChecklist.objects.create(ruta=ruta_uno)
        checklist_dos = RutaCargaChecklist.objects.create(ruta=ruta_dos)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist_uno,
            parada=parada_uno,
            transfer_external_id="T-DUP",
            detail_external_id="D-DUP-1",
            source_hash="source-duplicado-global",
            item_name="Pastel Snicker chico",
            cantidad_solicitada="5.000",
            cantidad_enviada_esperada="5.000",
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                RutaCargaChecklistLinea.objects.create(
                    checklist=checklist_dos,
                    parada=parada_dos,
                    transfer_external_id="T-DUP",
                    detail_external_id="D-DUP-2",
                    source_hash="source-duplicado-global",
                    item_name="Pastel Snicker chico",
                    cantidad_solicitada="5.000",
                    cantidad_enviada_esperada="5.000",
                )

    def test_api_ruta_status_bloquea_salida_si_checklist_carga_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_EN_RUTA}),
            content_type="application/json",
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIn("confirma la carga", response.json()["detail"])

    def test_api_repartidor_valida_linea_carga_e_idempotencia(self):
        self.client.force_login(self.user)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist
        linea = checklist.lineas.get()

        url = reverse("api_logistica_ruta_carga_linea_validar", kwargs={"ruta_id": ruta.id, "linea_id": linea.id})
        payload = {
            "cantidad_cargada": "5.000",
            "client_event_id": "evt-carga-1",
        }
        response = self.client.post(url, json.dumps(payload), content_type="application/json")
        second = self.client.post(url, json.dumps(payload), content_type="application/json")

        linea.refresh_from_db()
        checklist.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(linea.client_event_id, "evt-carga-1")
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CONFIRMADA)

    def test_api_no_crea_ruta_sin_paradas(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps({
                "nombre": "Ruta API Vacía",
                "fecha_ruta": timezone.localdate().isoformat(),
                "repartidor": self.repartidor.id,
                "unidad_operativa": self.unidad.id,
                "paradas": [],
            }),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta API Vacía").exists())

    def test_api_crea_ruta_con_paradas_ordenadas_y_deduplicadas(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        punto_sur = PuntoLogistico.objects.create(
            nombre="Sucursal API Sur",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.560000",
            longitud="-108.460000",
            radio_geocerca_metros=120,
        )

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps({
                "nombre": "Ruta API Ordenada",
                "fecha_ruta": timezone.localdate().isoformat(),
                "repartidor": self.repartidor.id,
                "unidad_operativa": self.unidad.id,
                "paradas": [
                    {"punto_id": punto_sur.id, "orden": 2},
                    {"punto_id": self.punto.id, "orden": 1},
                    {"punto_id": punto_sur.id, "orden": 3},
                ],
            }),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["ruta_programada_fuente"], "FALLBACK")
        self.assertGreater(data["ruta_programada_distancia_metros"], 0)
        self.assertGreater(data["ruta_programada_duracion_segundos"], 0)
        ruta = RutaEntrega.objects.get(nombre="Ruta API Ordenada")
        paradas = list(ruta.paradas.order_by("orden").values_list("punto_id", "orden"))
        self.assertEqual(paradas, [(self.punto.id, 1), (punto_sur.id, 2)])
        self.assertEqual(ruta.ruta_programada_fuente, "FALLBACK")
        self.assertGreater(ruta.ruta_programada_distancia_metros, 0)

    def test_api_no_crea_ruta_directamente_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps(
                {
                    "nombre": "Ruta API Cerrada",
                    "fecha_ruta": timezone.localdate().isoformat(),
                    "estatus": RutaEntrega.ESTATUS_COMPLETADA,
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta API Cerrada").exists())

    def test_api_no_agrega_entrega_a_ruta_en_ruta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_entregas", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"secuencia": 1, "cliente_nombre": "Cliente", "direccion": "Sucursal"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_ruta_detail_reordena_paradas_sin_integrity_error(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_PLANEADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        punto_2 = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Segunda",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.572000",
            longitud="-108.472000",
            radio_geocerca_metros=100,
        )
        parada_2 = ParadaRuta.objects.create(ruta=self.ruta, punto=punto_2, orden=2)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "move_parada", "parada_id": parada_2.id, "direction": "up"},
            follow=True,
        )

        self.parada.refresh_from_db()
        parada_2.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(parada_2.orden, 1)
        self.assertEqual(self.parada.orden, 2)

    def test_punto_logistico_toggle_no_activa_duplicado_cercano(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        punto_inactivo = PuntoLogistico.objects.create(
            nombre="Punto Inactivo Cercano",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570010",
            longitud="-108.470010",
            radio_geocerca_metros=80,
            activo=False,
        )

        response = self.client.post(reverse("logistica:punto_logistico_toggle", kwargs={"pk": punto_inactivo.id}), follow=True)

        punto_inactivo.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(punto_inactivo.activo)
        self.assertContains(response, "No se puede activar")
