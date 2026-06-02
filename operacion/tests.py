from pathlib import Path

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase, override_settings

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento
from core.access import ACCESS_MANAGE, ACCESS_VIEW, ROLE_DG, ROLE_LOGISTICA, ROLE_REPARTIDOR
from core.models import Sucursal, UserModuleAccess, UserProfile
from logistica.models import Repartidor, Unidad
from mermas.models import PersonalEnviosSucursal


@override_settings(SECURE_SSL_REDIRECT=False)
class OperacionAppTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.sucursal = Sucursal.objects.create(codigo="COL", nombre="Colosio", activa=True)

    def _user(self, username: str, *, sucursal=None):
        user = self.user_model.objects.create_user(username=username, password="test12345")
        UserProfile.objects.create(user=user, sucursal=sucursal)
        return user

    def _grant(self, user, module: str, access: str = ACCESS_VIEW):
        return UserModuleAccess.objects.create(user=user, module=module, access=access)

    def test_app_requires_login(self):
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/login/?next=/app/")

    def test_repartidor_only_sees_logistica_mobile_without_other_modules(self):
        group = Group.objects.create(name=ROLE_REPARTIDOR)
        user = self._user("jorge.repartidor", sucursal=self.sucursal)
        user.groups.add(group)
        unidad = Unidad.objects.create(codigo="GS-PM1", descripcion="Panel móvil", sucursal=self.sucursal)
        Repartidor.objects.create(user=user, sucursal=self.sucursal, unidad_asignada=unidad)
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Nuevo Reporte")
        self.assertContains(response, "Mis Reportes")
        self.assertContains(response, "Inspección")
        self.assertContains(response, "Lavado")
        self.assertContains(response, "Bitácora")
        self.assertContains(response, "/logistica/app/?pantalla=nuevo_reporte")
        self.assertContains(response, "/logistica/app/?pantalla=bitacora_salida")
        self.assertNotContains(response, "Registrar merma")
        self.assertNotContains(response, "Reportar falla")
        self.assertNotContains(response, "pd_logistica_access")
        self.assertEqual(dashboard.status_code, 302)
        self.assertEqual(dashboard["Location"], "/logistica/app/")

    def test_unified_app_home_exposes_django_logout(self):
        user = self._user("operacion.logout", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/logout/"')
        self.assertContains(response, "Cerrar sesión")

    def test_mermas_only_user_can_enter_unified_app_without_losing_guardrail(self):
        user = self._user("mermas.colosio", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registrar merma")
        self.assertContains(response, "/mermas/app/?modo=captura")
        self.assertNotContains(response, "Logística móvil")
        self.assertEqual(dashboard.status_code, 302)
        self.assertEqual(dashboard["Location"], "/mermas/app/")

    def test_sucursal_user_gets_only_assigned_operational_actions(self):
        user = self._user("sucursal.colosio", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self._grant(user, "fallas.reportar")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registrar merma")
        self.assertContains(response, "Reportar falla")
        self.assertContains(response, "Colosio")
        self.assertNotContains(response, "Flota")
        self.assertNotContains(response, "Mantenimiento vehicular")

    def test_branch_capture_only_can_enter_app_but_regular_erp_still_redirects(self):
        user = self._user("captura.reabasto", sucursal=self.sucursal)
        profile = user.userprofile
        profile.modo_captura_sucursal = True
        profile.save(update_fields=["modo_captura_sucursal"])
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Captura sucursal")
        self.assertContains(response, "/recetas/reabasto-cedis/captura/")
        self.assertEqual(dashboard.status_code, 302)
        self.assertEqual(dashboard["Location"], "/recetas/reabasto-cedis/captura/")

    def test_logistica_role_sees_logistica_management_without_sucursal_tiles(self):
        group = Group.objects.create(name=ROLE_LOGISTICA)
        user = self._user("logistica.supervisor")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tickets logística")
        self.assertContains(response, "Flota")
        self.assertContains(response, "Rutas")
        self.assertNotContains(response, "Logística móvil")
        self.assertNotContains(response, "Registrar merma")

    def test_logistica_view_only_user_does_not_see_management_only_tiles(self):
        user = self._user("logistica.lectura")
        self._grant(user, "logistica", access=ACCESS_VIEW)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Tickets logística")
        self.assertNotContains(response, "Logística móvil")

    def test_logistica_pwa_rejects_users_without_logistica_or_repartidor_access(self):
        user = self._user("logistica.sinpermiso")
        self.client.force_login(user)

        response = self.client.get("/logistica/app/")

        self.assertEqual(response.status_code, 403)

    def test_user_without_operational_access_gets_empty_state(self):
        user = self._user("sin.permisos")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tu usuario no tiene actividades operativas asignadas")
        self.assertNotContains(response, "Registrar merma")
        self.assertNotContains(response, "Logística móvil")
        self.assertNotContains(response, "Reportar falla")

    def test_personal_envios_sucursal_gets_cedis_reception_access(self):
        user = self._user("cedis.recepcion")
        PersonalEnviosSucursal.objects.create(user=user, activo=True)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Recibir merma")
        self.assertContains(response, "/mermas/app/?modo=recepcion")
        self.assertContains(response, "Recepción y validación")
        self.assertNotContains(response, "Registrar merma")
        self.assertNotContains(response, "Logística móvil")

        recepcion = self.client.get("/mermas/app/?modo=recepcion")
        self.assertEqual(recepcion.status_code, 200)
        self.assertContains(recepcion, 'href="/logout/"')
        self.assertContains(recepcion, "Salir")

    def test_explicit_cedis_manage_access_gets_reception_and_mermas_guardrail(self):
        user = self._user("cedis.manage")
        self._grant(user, "mermas.recepcion", access=ACCESS_MANAGE)
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Recibir merma")
        self.assertContains(response, "/mermas/app/?modo=recepcion")
        self.assertEqual(dashboard.status_code, 302)
        self.assertEqual(dashboard["Location"], "/mermas/app/")

    def test_fallas_mis_reportes_can_be_shown_without_reportar(self):
        user = self._user("fallas.consulta", sucursal=self.sucursal)
        self._grant(user, "fallas.mis_reportes")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mis reportes")
        self.assertNotContains(response, "Reportar falla")

    def test_fallas_branch_user_actions_return_to_unified_app_not_dashboard(self):
        user = self._user("fallas.sucursal", sucursal=self.sucursal)
        self._grant(user, "fallas.reportar")
        self._grant(user, "fallas.mis_reportes")
        self.client.force_login(user)

        reportar = self.client.get("/fallas/reportar/")
        mis_reportes = self.client.get("/fallas/mis-reportes/")

        self.assertEqual(reportar.status_code, 200)
        self.assertContains(reportar, 'href="/app/"')
        self.assertNotContains(reportar, 'href="/fallas/"')
        self.assertNotContains(reportar, "Categorías")
        self.assertEqual(mis_reportes.status_code, 200)
        self.assertNotContains(mis_reportes, 'href="/fallas/"')
        self.assertNotContains(mis_reportes, "Categorías")

    def test_fallas_dashboard_user_keeps_dashboard_navigation(self):
        user = self._user("fallas.dashboard")
        self._grant(user, "fallas.reportar")
        self._grant(user, "fallas.mis_reportes")
        self._grant(user, "fallas.dashboard")
        self.client.force_login(user)

        response = self.client.get("/fallas/reportar/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/fallas/"')
        self.assertNotContains(response, 'href="/app/"')

    def test_locked_logistica_profile_hides_logistica_tiles_even_with_role(self):
        group = Group.objects.create(name=ROLE_LOGISTICA)
        user = self._user("logistica.bloqueado")
        user.groups.add(group)
        profile = user.userprofile
        profile.lock_logistica = True
        profile.save(update_fields=["lock_logistica"])
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Logística móvil")
        self.assertContains(response, "Tu usuario no tiene actividades operativas asignadas")

    def test_superuser_gets_full_operational_surface(self):
        user = self.user_model.objects.create_superuser(
            username="admin.operacion",
            email="admin.operacion@example.com",
            password="test12345",
        )
        UserProfile.objects.create(user=user, sucursal=self.sucursal)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registrar merma")
        self.assertContains(response, "Recibir merma")
        self.assertContains(response, "Reportar falla")
        self.assertContains(response, "Mantenimiento")
        self.assertContains(response, "Flota")
        self.assertContains(response, "Rutas")
        self.assertNotContains(response, "Logística móvil")

    def test_repartidor_direct_urls_are_limited_to_app_and_logistica_pwa(self):
        group = Group.objects.create(name=ROLE_REPARTIDOR)
        user = self._user("repartidor.guardrail", sucursal=self.sucursal)
        user.groups.add(group)
        unidad = Unidad.objects.create(codigo="GS-PM2", descripcion="Panel móvil 2", sucursal=self.sucursal)
        Repartidor.objects.create(user=user, sucursal=self.sucursal, unidad_asignada=unidad)
        self.client.force_login(user)

        self.assertEqual(self.client.get("/app/").status_code, 200)
        self.assertEqual(self.client.get("/logistica/app/").status_code, 200)

        blocked = self.client.get("/mermas/app/")
        self.assertEqual(blocked.status_code, 302)
        self.assertEqual(blocked["Location"], "/logistica/app/")

    def test_mermas_only_direct_urls_are_limited_to_app_and_mermas(self):
        user = self._user("mermas.guardrail", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        self.assertEqual(self.client.get("/app/").status_code, 200)
        self.assertEqual(self.client.get("/mermas/app/").status_code, 200)

        blocked = self.client.get("/fallas/reportar/")
        self.assertEqual(blocked.status_code, 302)
        self.assertEqual(blocked["Location"], "/mermas/app/")

    def test_mermas_capture_cancel_returns_to_unified_app_for_branch_user(self):
        user = self._user("mermas.cancelar", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        response = self.client.get("/mermas/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/app/"')
        self.assertContains(response, 'href="/logout/"')
        self.assertContains(response, "Salir")
        self.assertNotContains(response, 'href="/mermas/"')

    def test_mermas_capture_cancel_keeps_panel_for_dashboard_user(self):
        user = self._user("mermas.panel", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self._grant(user, "mermas.dashboard")
        self.client.force_login(user)

        response = self.client.get("/mermas/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/mermas/"')
        self.assertContains(response, 'href="/logout/"')
        self.assertContains(response, "Panel")

    def test_mermas_detail_hidden_sidebar_keeps_logout_escape(self):
        template = Path(__file__).resolve().parents[1] / "mermas/templates/mermas/detalle.html"
        html = template.read_text(encoding="utf-8")

        self.assertIn("{% url 'logout' %}", html)
        self.assertIn(">Salir<", html)

    def test_dg_group_gets_management_surface(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.operacion")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mantenimiento")
        self.assertContains(response, "Tickets logística")
        self.assertNotContains(response, "Logística móvil")

    def test_dg_mermas_tiles_do_not_logout_and_respect_requested_mode(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mermas")
        user.groups.add(group)
        self.client.force_login(user)

        captura = self.client.get("/mermas/app/?modo=captura")
        recepcion = self.client.get("/mermas/app/?modo=recepcion")

        self.assertEqual(captura.status_code, 200)
        self.assertContains(captura, "Registrar merma")
        self.assertEqual(recepcion.status_code, 200)
        self.assertContains(recepcion, "Recepción CEDIS")

    def test_mantenimiento_pwa_requires_login_and_operational_group(self):
        anonymous = self.client.get("/mantenimiento/app/")
        self.assertEqual(anonymous.status_code, 302)
        self.assertEqual(anonymous["Location"], "/login/?next=/mantenimiento/app/")

        user = self._user("mantenimiento.sinpermiso")
        self.client.force_login(user)
        self.assertEqual(self.client.get("/mantenimiento/app/").status_code, 403)

        group = Group.objects.create(name=ROLE_DG)
        user.groups.add(group)
        self.assertEqual(self.client.get("/mantenimiento/app/").status_code, 200)

    def test_dg_group_can_call_mantenimiento_session_api(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mantenimiento.api")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/api/mantenimiento/me/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["username"], "dg.mantenimiento.api")

    def test_mantenimiento_session_token_uses_django_session_not_logistica_storage(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mantenimiento.token")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/api/mantenimiento/session-token/")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("access", payload)
        self.assertIn("refresh", payload)

        self.client.logout()
        perfil = self.client.get("/api/mantenimiento/me/", HTTP_AUTHORIZATION=f"Bearer {payload['access']}")
        self.assertEqual(perfil.status_code, 200)
        self.assertEqual(perfil.json()["username"], "dg.mantenimiento.token")

    def test_mantenimiento_pwa_uses_own_token_storage_and_hides_fleet_entry(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mantenimiento.preview")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/mantenimiento/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "pd_mantenimiento_access")
        self.assertNotContains(response, "pd_logistica_access")
        self.assertContains(response, "Bandeja de mantenimiento")
        self.assertContains(response, "Pendientes")
        self.assertContains(response, "Buscar equipo")
        self.assertContains(response, "Seguimiento")
        self.assertContains(response, "Reportes")
        self.assertContains(response, "Registrar mantenimiento")
        self.assertNotContains(response, "Vehículo de flota")

    def test_pwa_logout_buttons_redirect_to_django_logout(self):
        mantenimiento_group = Group.objects.create(name="MANTENIMIENTO")
        mantenimiento_user = self._user("tecnico.logout")
        mantenimiento_user.groups.add(mantenimiento_group)
        self.client.force_login(mantenimiento_user)
        mantenimiento = self.client.get("/mantenimiento/app/")
        self.assertEqual(mantenimiento.status_code, 200)
        self.assertContains(mantenimiento, 'window.location.href = "/logout/";')

        self.client.logout()
        logistica_group = Group.objects.create(name=ROLE_LOGISTICA)
        logistica_user = self._user("logistica.logout")
        logistica_user.groups.add(logistica_group)
        self.client.force_login(logistica_user)
        logistica = self.client.get("/logistica/app/")
        self.assertEqual(logistica.status_code, 200)
        self.assertContains(logistica, 'window.location.href = "/logout/";')

        self.client.logout()
        fallas_user = self._user("fallas.logout", sucursal=self.sucursal)
        self._grant(fallas_user, "fallas.reportar")
        self.client.force_login(fallas_user)
        fallas = self.client.get("/fallas/reportar/")
        self.assertEqual(fallas.status_code, 200)
        self.assertContains(fallas, 'href="/logout/"')

        fallas_pwa_template = Path(__file__).resolve().parents[1] / "fallas/templates/fallas/pwa_reporte.html"
        self.assertIn('window.location.href = "/logout/";', fallas_pwa_template.read_text(encoding="utf-8"))

    def test_operational_pwas_prefer_current_django_session_before_cached_token(self):
        mantenimiento_group = Group.objects.create(name="MANTENIMIENTO")
        mantenimiento_user = self._user("tecnico.token.actual")
        mantenimiento_user.groups.add(mantenimiento_group)
        self.client.force_login(mantenimiento_user)
        mantenimiento = self.client.get("/mantenimiento/app/")
        self.assertEqual(mantenimiento.status_code, 200)
        mantenimiento_html = mantenimiento.content.decode()
        mantenimiento_boot = mantenimiento_html[mantenimiento_html.index("async function boot()") :]
        self.assertLess(
            mantenimiento_boot.index("if (await useDjangoSessionToken())"),
            mantenimiento_boot.index("if (state.token)"),
        )

        self.client.logout()
        logistica_group = Group.objects.create(name=ROLE_LOGISTICA)
        logistica_user = self._user("logistica.token.actual")
        logistica_user.groups.add(logistica_group)
        self.client.force_login(logistica_user)
        logistica = self.client.get("/logistica/app/")
        self.assertEqual(logistica.status_code, 200)
        logistica_html = logistica.content.decode()
        logistica_boot = logistica_html[logistica_html.index("async function boot()") :]
        self.assertLess(
            logistica_boot.index("if (await useDjangoSessionToken())"),
            logistica_boot.index("if (state.token)"),
        )

    def test_mantenimiento_group_gets_app_tile_and_pwa_access(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.mantenimiento")
        user.groups.add(group)
        self.client.force_login(user)

        app = self.client.get("/app/")
        pwa = self.client.get("/mantenimiento/app/")
        token = self.client.get("/api/mantenimiento/session-token/")

        self.assertEqual(app.status_code, 200)
        self.assertContains(app, "Mantenimiento")
        self.assertEqual(pwa.status_code, 200)
        self.assertEqual(token.status_code, 200)

    def test_activos_access_gets_mantenimiento_app_and_api(self):
        user = self._user("activos.mantenimiento")
        self._grant(user, "activos", access=ACCESS_VIEW)
        self.client.force_login(user)

        app = self.client.get("/app/")
        pwa = self.client.get("/mantenimiento/app/")
        perfil = self.client.get("/api/mantenimiento/me/")

        self.assertEqual(app.status_code, 200)
        self.assertContains(app, "Mantenimiento")
        self.assertEqual(pwa.status_code, 200)
        self.assertEqual(perfil.status_code, 200)

    def test_mantenimiento_order_followup_updates_status_and_bitacora(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.seguimiento")
        user.groups.add(group)
        activo = Activo.objects.create(nombre="Batidora QA", sucursal=self.sucursal, categoria="Producción")
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_ALTA,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            descripcion="Ruido en motor",
        )
        self.client.force_login(user)

        response = self.client.patch(
            f"/api/mantenimiento/ordenes/{orden.id}/",
            data={
                "estatus": OrdenMantenimiento.ESTATUS_EN_PROCESO,
                "responsable": "Técnico QA",
                "comentario": "Se inició revisión del motor.",
                "costo_adicional": "125.50",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        orden.refresh_from_db()
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_EN_PROCESO)
        self.assertEqual(orden.responsable, "Técnico QA")
        self.assertIsNotNone(orden.fecha_inicio)
        self.assertEqual(BitacoraMantenimiento.objects.filter(orden=orden).count(), 1)
        self.assertIn("Se inició revisión del motor.", BitacoraMantenimiento.objects.get(orden=orden).comentario)

    def test_mantenimiento_can_create_maintainable_point_for_general_repairs(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.punto")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.post(
            "/api/mantenimiento/activos/rapido/",
            data={
                "sucursal": self.sucursal.id,
                "nombre": "Instalación hidrosanitaria - Cocina",
                "categoria": "Plomería",
                "ubicacion": "Cocina",
                "notas": "Alta desde reparación general: fuga debajo de tarja.",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        activo = Activo.objects.get(id=payload["id"])
        self.assertTrue(activo.codigo.startswith("PM-"))
        self.assertEqual(activo.sucursal, self.sucursal)
        self.assertEqual(activo.nombre, "Instalación hidrosanitaria - Cocina")
        self.assertEqual(activo.categoria, "Plomería")
        self.assertEqual(activo.ubicacion, "Cocina")
        self.assertIn("fuga debajo de tarja", activo.notas)

        orden = self.client.post(
            "/api/mantenimiento/ordenes/",
            data={
                "activo_ref": activo.id,
                "tipo": OrdenMantenimiento.TIPO_CORRECTIVO,
                "prioridad": OrdenMantenimiento.PRIORIDAD_ALTA,
                "descripcion": "Se reparó fuga y se selló conexión.",
            },
        )
        self.assertEqual(orden.status_code, 201)
        self.assertTrue(OrdenMantenimiento.objects.filter(activo_ref=activo).exists())

    def test_mantenimiento_sucursales_api_does_not_depend_on_existing_assets(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.sucursales")
        user.groups.add(group)
        Activo.objects.all().delete()
        self.client.force_login(user)

        response = self.client.get("/api/mantenimiento/sucursales/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(self.sucursal.id, [item["id"] for item in response.json()])

    def test_mantenimiento_pwa_exposes_maintainable_point_shortcut(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.punto.ui")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/mantenimiento/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No encuentro el equipo")
        self.assertContains(response, "Registrar punto mantenible")
