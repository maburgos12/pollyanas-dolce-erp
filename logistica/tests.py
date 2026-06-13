import json
from io import StringIO

from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.db import IntegrityError, transaction
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE, ACCESS_VIEW
from core.models import Sucursal, UserModuleAccess
from crm.models import Cliente, PedidoCliente
from logistica.models import BitacoraSalidaLlegada, EntregaRuta, EventoRuta, ParadaRuta, PuntoLogistico, Repartidor, RutaEntrega, Unidad
from logistica.services_rutas_control import distancia_metros, registrar_ubicacion_ruta, resumen_control_rutas
from logistica.tasks import _emails_de_grupo
from api.logistica_views import _can_operate_pwa


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

    def test_distancia_metros_detecta_punto_cercano(self):
        self.assertLess(distancia_metros("25.570010", "-108.470010", self.punto.latitud, self.punto.longitud), 5)

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

    def test_resumen_control_materializa_gps_perdido(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )
        UbicacionRuta = ubicacion.__class__
        UbicacionRuta.objects.filter(pk=ubicacion.pk).update(timestamp_servidor=timezone.now() - timezone.timedelta(minutes=20))

        resumen_control_rutas(fecha=self.ruta.fecha_ruta)

        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO).exists())

    def test_resumen_control_materializa_gps_perdido_sin_primera_senal(self):
        self.ruta.hora_inicio_real = timezone.now() - timezone.timedelta(minutes=20)
        self.ruta.save(update_fields=["hora_inicio_real", "updated_at"])

        resumen_control_rutas(fecha=self.ruta.fecha_ruta)

        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO).exists())

    def test_control_rutas_view_renderiza_panel_interno(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.get(reverse("logistica:control_rutas"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Control interno de rutas")
        self.assertContains(response, "Rutas del día")

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
        ruta = RutaEntrega.objects.get(nombre="Ruta API Ordenada")
        paradas = list(ruta.paradas.order_by("orden").values_list("punto_id", "orden"))
        self.assertEqual(paradas, [(self.punto.id, 1), (punto_sur.id, 2)])

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
