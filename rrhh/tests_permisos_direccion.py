from datetime import timedelta

from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.navigation import build_nav_groups
from rrhh.models import Empleado, PermisoSalida


class PermisosDireccionTests(TestCase):
    def setUp(self):
        self.dg = User.objects.create_user(username="direccion", is_superuser=True)
        self.carolina_user = User.objects.create_user(username="carolina")
        self.carolina = Empleado.objects.create(
            nombre="CAYETANO VALENZUELA CAROLINA", usuario_erp=self.carolina_user,
            departamento=Empleado.DEP_PRODUCCION, puesto="Jefe de Produccion",
        )
        self.hoy = timezone.localtime().replace(hour=15, minute=30, second=0, microsecond=0)
        self.permiso = PermisoSalida.objects.create(
            empleado=self.carolina, tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=self.hoy, fecha_fin=self.hoy + timedelta(hours=1), motivo="Salir temprano",
        )
        self.url = reverse("rrhh:rrhh_permisos_list")

    def test_direccion_ve_y_resuelve_en_un_solo_paso(self):
        self.client.force_login(self.dg)
        response = self.client.get(self.url)
        self.assertEqual(response.context["bandeja_activa"], "direccion")
        self.assertContains(response, 'value="autorizar_direccion"')
        self.assertNotContains(response, 'value="preautorizar_jefe"')
        response = self.client.post(self.url, {
            "permiso_id": self.permiso.pk, "action": "autorizar_direccion",
        })
        self.assertEqual(response.status_code, 302)
        self.permiso.refresh_from_db()
        self.assertEqual(self.permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertEqual(self.permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_AUTORIZADO)
        self.assertEqual(self.permiso.autorizado_direccion_por, self.dg)

    def test_anteriores_se_separan_sin_cambiar_estado_y_ongoing_sigue_vigente(self):
        anterior = PermisoSalida.objects.create(
            empleado=self.carolina, tipo=PermisoSalida.TIPO_PERMISO_DIA,
            fecha_inicio=self.hoy - timedelta(days=1), motivo="Anterior",
        )
        vigente = PermisoSalida.objects.create(
            empleado=self.carolina, tipo=PermisoSalida.TIPO_PERMISO_DIA,
            fecha_inicio=self.hoy - timedelta(days=1), fecha_fin=self.hoy + timedelta(days=1),
            motivo="Varios dias",
        )
        self.client.force_login(self.dg)
        response = self.client.get(self.url)
        columnas = {key: [p.pk for p in items] for key, _, items in response.context["columnas"]}
        self.assertEqual(set(columnas["direccion"]), {self.permiso.pk, vigente.pk})
        self.assertEqual(columnas["anteriores"], [anterior.pk])
        anterior.refresh_from_db()
        self.assertEqual(anterior.estado, PermisoSalida.ESTADO_SOLICITADO)

    def test_menu_direccion_y_rrhh_sin_autorizacion(self):
        groups = build_nav_groups(self.dg, "/dashboard/")
        items = next(g["items"] for g in groups if g["key"] == "mi_trabajo")
        item = next(i for i in items if i["label"] == "Permisos por autorizar")
        self.assertEqual(item["url"], self.url)
        self.assertEqual(item["badge_count"], 1)
        rrhh = User.objects.create_user(username="rrhh")
        rrhh.groups.add(Group.objects.create(name="RRHH"))
        groups = build_nav_groups(rrhh, "/dashboard/")
        self.assertFalse(any(i["label"] == "Permisos por autorizar" for g in groups for i in g["items"]))
        self.client.force_login(rrhh)
        response = self.client.get(self.url)
        self.assertNotContains(response, 'value="autorizar_direccion"')
        self.assertEqual(self.client.post(self.url, {
            "permiso_id": self.permiso.pk, "action": "autorizar_direccion",
        }).status_code, 403)

    def test_no_muestra_ni_permite_autorizacion_propia(self):
        self.carolina_user.is_superuser = True
        self.carolina_user.save(update_fields=["is_superuser"])
        self.client.force_login(self.carolina_user)
        response = self.client.get(self.url)
        self.assertNotContains(response, 'value="autorizar_direccion"')
        self.assertNotContains(response, 'value="preautorizar_jefe"')
        self.assertEqual(self.client.post(self.url, {
            "permiso_id": self.permiso.pk, "action": "autorizar_direccion",
        }).status_code, 403)
        groups = build_nav_groups(self.carolina_user, "/dashboard/")
        self.assertFalse(any(i["label"] == "Permisos por autorizar" for g in groups for i in g["items"]))
