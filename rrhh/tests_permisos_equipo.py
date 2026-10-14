from datetime import timedelta

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APIClient

from core.models import Notificacion
from core.navigation import build_nav_groups
from .models import Empleado, PermisoSalida


class PermisosEquipoTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.dg = User.objects.create_user(username="direccion", is_superuser=True)
        superior = Empleado.objects.create(nombre="Director", usuario_erp=self.dg)
        self.user = User.objects.create_user(username="yesenia.soto", is_staff=True)
        self.jefa = Empleado.objects.create(
            nombre="SOTO INZUNZA YESENIA", usuario_erp=self.user,
            departamento=Empleado.DEP_ADMINISTRACION, jefe_directo=superior,
        )
        self.equipo = Empleado.objects.create(nombre="Colaborador de Administración", jefe_directo=self.jefa)
        self.ajeno = Empleado.objects.create(nombre="Persona de otra jefatura")
        self.inactivo = Empleado.objects.create(nombre="Colaborador inactivo", jefe_directo=self.jefa, activo=False)
        self.inicio = timezone.now() + timedelta(days=2)
        self.permiso = self.crear(self.equipo)
        self.propio = self.crear(self.jefa)
        self.otro = self.crear(self.ajeno)
        self.url = reverse("rrhh:rrhh_permisos_list")
        self.client.force_login(self.user)
        self.api = APIClient()
        self.api.force_authenticate(self.user)

    def crear(self, empleado):
        return PermisoSalida.objects.create(
            empleado=empleado, tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=self.inicio, fecha_fin=self.inicio + timedelta(hours=1), motivo="Solicitud",
        )

    def payload(self, empleado):
        return {
            "action": "crear", "empleado": empleado.pk, "tipo": PermisoSalida.TIPO_PERMISO_HORA,
            "fecha_inicio": self.inicio.isoformat(), "fecha_fin": (self.inicio + timedelta(hours=1)).isoformat(),
            "motivo": "Salir una hora", "goce_sueldo": "false",
        }

    def test_bandeja_menu_y_formulario_limitados_al_equipo_y_propio(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.equipo.nombre)
        self.assertContains(response, self.jefa.nombre)
        self.assertNotContains(response, self.ajeno.nombre)
        self.assertNotContains(response, self.inactivo.nombre)
        self.assertEqual(response.context["stats"]["total"], 2)
        self.assertContains(response, 'value="preautorizar_jefe"', count=1)
        self.assertTrue(any(i["label"] == "Permisos de mi equipo" for g in build_nav_groups(self.user, self.url) for i in g["items"]))
        self.assertTrue(any(t["active"] for t in response.context["module_tabs"]))

    def test_crea_equipo_sin_autorizar_y_con_actor(self):
        response = self.client.post(self.url, self.payload(self.equipo))
        self.assertEqual(response.status_code, 302)
        permiso = PermisoSalida.objects.latest("id")
        self.assertEqual(permiso.empleado, self.equipo)
        self.assertEqual(permiso.creado_por, self.user)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
        self.assertFalse(permiso.goce_sueldo)

    def test_autoriza_y_rechaza_equipo_con_actor_y_fecha(self):
        for action, estado in [("preautorizar_jefe", PermisoSalida.ESTADO_APROBADO), ("rechazar_jefe", PermisoSalida.ESTADO_RECHAZADO)]:
            permiso = self.crear(self.equipo)
            response = self.client.post(self.url, {"action": action, "permiso_id": permiso.pk})
            self.assertEqual(response.status_code, 302)
            permiso.refresh_from_db()
            self.assertEqual(permiso.estado, estado)
            self.assertEqual(permiso.autorizado_jefe_por, self.user)
            self.assertIsNotNone(permiso.fecha_autorizacion_jefe)

    def test_no_resuelve_propio_ajeno_ni_direccion(self):
        for permiso, action, status in [(self.propio, "preautorizar_jefe", 403), (self.otro, "preautorizar_jefe", 404), (self.permiso, "autorizar_direccion", 403)]:
            response = self.client.post(self.url, {"action": action, "permiso_id": permiso.pk})
            self.assertEqual(response.status_code, status)
            permiso.refresh_from_db()
            self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)

    def test_creacion_rechaza_ajeno_inactivo_fechas_y_empleado_ausente(self):
        original = PermisoSalida.objects.count()
        datos = [self.payload(self.ajeno), self.payload(self.inactivo)]
        invertido = self.payload(self.equipo)
        invertido["fecha_fin"] = (self.inicio - timedelta(hours=1)).isoformat()
        ausente = self.payload(self.equipo)
        del ausente["empleado"]
        for payload in [*datos, invertido, ausente]:
            response = self.client.post(self.url, payload)
            self.assertEqual(response.status_code, 200)
            self.assertContains(response, "Revisa la solicitud")
            self.assertEqual(PermisoSalida.objects.count(), original)

    def test_api_lista_equipo_y_mis_conserva_solo_propios(self):
        response = self.api.get("/rrhh/api/permisos/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual({p["id"] for p in response.data}, {self.propio.pk, self.permiso.pk})
        response = self.api.get("/rrhh/api/permisos/?mis=true")
        self.assertEqual([p["id"] for p in response.data], [self.propio.pk])

    def test_api_crea_y_resuelve_equipo_y_no_expone_ajeno(self):
        response = self.api.post("/rrhh/api/permisos/", self.payload(self.equipo), format="json")
        self.assertEqual(response.status_code, 201)
        permiso_id = response.data["id"]
        self.assertEqual(self.api.post(f"/rrhh/api/permisos/{permiso_id}/autorizar-jefe/").status_code, 200)
        self.assertEqual(self.api.post(f"/rrhh/api/permisos/{self.permiso.pk}/rechazar-jefe/").status_code, 200)
        self.assertEqual(self.api.post(f"/rrhh/api/permisos/{self.propio.pk}/autorizar-jefe/").status_code, 403)
        self.assertEqual(self.api.get(f"/rrhh/api/permisos/{self.otro.pk}/").status_code, 404)
        self.assertEqual(self.api.post("/rrhh/api/permisos/", self.payload(self.ajeno), format="json").status_code, 400)

    def test_solicitud_propia_notifica_al_superior(self):
        response = self.api.post("/rrhh/api/permisos/", self.payload(self.jefa), format="json")
        self.assertEqual(response.status_code, 201)
        self.assertTrue(Notificacion.objects.filter(usuario=self.dg, objeto_id=response.data["id"], tipo=Notificacion.TIPO_PERMISO).exists())

    def test_autoservicio_conserva_identidad_legada_sin_usuario_erp(self):
        self.jefa.email = "jefa@example.test"
        self.jefa.usuario_erp = None
        self.jefa.save(update_fields=["email", "usuario_erp"])
        self.user.email = self.jefa.email
        self.user.save(update_fields=["email"])
        response = self.api.post("/rrhh/api/permisos/", {"tipo": "permiso_hora", "fecha_inicio": self.inicio.isoformat(), "motivo": "Propio"}, format="json")
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["empleado"], self.jefa.pk)
        self.assertEqual(self.api.post(f'/rrhh/api/permisos/{response.data["id"]}/autorizar-jefe/').status_code, 403)

    def test_rrhh_conserva_consulta_global_sin_poder_autorizar(self):
        rrhh = get_user_model().objects.create_user(username="rrhh")
        rrhh.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        self.client.force_login(rrhh)
        response = self.client.get(self.url)
        self.assertContains(response, self.ajeno.nombre)
        self.assertEqual(self.client.post(self.url, {"action": "preautorizar_jefe", "permiso_id": self.permiso.pk}).status_code, 403)

    def test_app_ofrece_acceso_a_bandeja(self):
        self.assertContains(self.client.get("/rrhh/app/permisos/"), "Abrir bandeja de permisos propios y de mi equipo")
