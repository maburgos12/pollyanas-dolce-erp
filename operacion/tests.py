from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase, override_settings

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
        self.assertContains(response, "Logística móvil")
        self.assertContains(response, "Tickets logística")
        self.assertContains(response, "Flota")
        self.assertContains(response, "Rutas")
        self.assertNotContains(response, "Registrar merma")

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
        self.assertContains(response, "Logística móvil")
        self.assertContains(response, "Flota")
        self.assertContains(response, "Rutas")

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
        self.assertNotContains(response, 'href="/mermas/"')

    def test_mermas_capture_cancel_keeps_panel_for_dashboard_user(self):
        user = self._user("mermas.panel", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self._grant(user, "mermas.dashboard")
        self.client.force_login(user)

        response = self.client.get("/mermas/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/mermas/"')
        self.assertContains(response, "Panel")

    def test_dg_group_gets_management_surface(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.operacion")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mantenimiento")
        self.assertContains(response, "Logística móvil")
        self.assertContains(response, "Tickets logística")

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
