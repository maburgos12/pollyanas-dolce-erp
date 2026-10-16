from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from rest_framework.test import APIClient

from core.models import AuditLog
from core.navigation import build_nav_groups
from .models import Empleado, HoraExtra, PermisoSalida, PermisoSalidaCambio


class MiEquipoTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="jefa", password="local-only")
        self.jefa = Empleado.objects.create(nombre="Jefa", usuario_erp=self.user)
        self.emp = Empleado.objects.create(nombre="Colaborador", jefe_directo=self.jefa)
        self.ajeno = Empleado.objects.create(nombre="Ajeno")
        self.inactivo = Empleado.objects.create(nombre="Inactivo", jefe_directo=self.jefa, activo=False)
        self.api = APIClient()
        self.api.force_authenticate(self.user)
        self.fecha = timezone.localdate() + timedelta(days=2)
        self.permisos = "/rrhh/api/mi-equipo/permisos/"
        self.extras = "/rrhh/api/mi-equipo/horas-extra/"

    def permiso(self, empleado=None):
        return {"empleado": (empleado or self.emp).pk, "tipo": "permiso_hora", "fecha_inicio": f"{self.fecha}T10:00:00-07:00", "fecha_fin": f"{self.fecha}T11:00:00-07:00", "motivo": "Consulta", "goce_sueldo": False}

    def extra(self, empleado=None):
        return {"empleado": (empleado or self.emp).pk, "fecha": str(self.fecha), "horas": "1.50", "notas": "Cierre de trabajo"}

    def test_pagina_y_menu_permanente_sin_solicitudes(self):
        self.client.force_login(self.user)
        response = self.client.get("/rrhh/mi-equipo/")
        self.assertContains(response, "Registrar una solicitud no la autoriza")
        items = [i for g in build_nav_groups(self.user, "/rrhh/mi-equipo/") for i in g["items"]]
        self.assertTrue(any(i["label"] == "Mi equipo" and i["active"] for i in items))
        self.emp.jefe_directo = None
        self.emp.save()
        self.assertEqual(self.client.get("/rrhh/mi-equipo/").status_code, 403)
        self.assertFalse(any(i["label"] == "Mi equipo" for g in build_nav_groups(self.user, "/") for i in g["items"]))

    def test_crea_corrige_permiso_con_historial_y_resuelve(self):
        response = self.api.post(self.permisos, self.permiso(), format="json")
        self.assertEqual(response.status_code, 201, response.data)
        pk = response.data["id"]
        permiso = PermisoSalida.objects.get(pk=pk)
        self.assertEqual(permiso.creado_por, self.user)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
        data = {**self.permiso(), "motivo": "Consulta corregida", "motivo_cambio": "Aclaración de la solicitud"}
        self.assertEqual(self.api.post(f"{self.permisos}{pk}/editar/", data, format="json").status_code, 200)
        cambio = PermisoSalidaCambio.objects.get(permiso=permiso)
        self.assertEqual(cambio.realizado_por, self.user)
        self.assertEqual(cambio.cambios["motivo"]["antes"], "Consulta")
        self.assertEqual(self.api.post(f"{self.permisos}{pk}/preautorizar/").status_code, 200)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)

    @patch("rrhh.views_equipo.notificar_hora_extra_solicitada")
    def test_crea_corrige_extra_preserva_auditoria_y_autoriza(self, notificar):
        response = self.api.post(self.extras, self.extra(), format="json")
        self.assertEqual(response.status_code, 201, response.data)
        pk = response.data["id"]
        notificar.assert_called_once()
        data = {**self.extra(), "horas": "2.00", "notas": "Corrección", "motivo_cambio": "Faltaba media hora"}
        response = self.api.post(f"{self.extras}{pk}/editar/", data, format="json")
        self.assertEqual(response.status_code, 200, response.data)
        audit = AuditLog.objects.get(model="rrhh.HoraExtra", object_id=str(pk))
        self.assertEqual(audit.payload["antes"]["notas"], "Cierre de trabajo")
        self.assertEqual(audit.payload["despues"]["horas"], "2.00")
        self.assertEqual(audit.user, self.user)
        self.assertEqual(self.api.post(f"{self.extras}{pk}/autorizar/").status_code, 200)
        self.assertEqual(HoraExtra.objects.get(pk=pk).estado, HoraExtra.ESTADO_AUTORIZADO)

    def test_equipo_ajeno_propio_inactivo_y_cambio_de_jefatura(self):
        for url, payload in [(self.permisos, self.permiso), (self.extras, self.extra)]:
            for empleado in (self.ajeno, self.jefa, self.inactivo):
                self.assertEqual(self.api.post(url, payload(empleado), format="json").status_code, 400)
            response = self.api.post(url, payload(), format="json")
            self.assertEqual(response.status_code, 201, response.data)
            pk = response.data["id"]
            self.assertEqual([e["id"] for e in self.api.get(url).data["empleados"]], [self.emp.pk])
            self.emp.jefe_directo = None
            self.emp.save()
            self.assertEqual(self.api.post(f"{url}{pk}/editar/", {**payload(), "motivo_cambio": "Cambio"}, format="json").status_code, 404)
            for action in ("preautorizar", "rechazar") if url == self.permisos else ("autorizar", "rechazar"):
                self.assertEqual(self.api.post(f"{url}{pk}/{action}/").status_code, 404)
            self.emp.jefe_directo = self.jefa
            self.emp.save()

    def test_validacion_fechas_horas_identificadores_y_motivo(self):
        for overrides in ({"fecha_fin": "invalida"}, {"fecha_fin": f"{self.fecha}T09:00:00-07:00"}, {"motivo": " "}, {"empleado": "x"}, {"empleado": None}):
            self.assertEqual(self.api.post(self.permisos, {**self.permiso(), **overrides}, format="json").status_code, 400)
        for overrides in ({"horas": "NaN"}, {"horas": "Infinity"}, {"horas": "-1"}, {"horas": "100"}, {"fecha": "2026-02-30"}, {"notas": " "}, {"empleado": "x"}):
            self.assertEqual(self.api.post(self.extras, {**self.extra(), **overrides}, format="json").status_code, 400)
        self.assertFalse(PermisoSalida.objects.exists())
        self.assertFalse(HoraExtra.objects.exists())

    def test_motivo_correccion_obligatorio_y_estados_terminales(self):
        for url, payload, model, estado in [(self.permisos, self.permiso, PermisoSalida, PermisoSalida.ESTADO_CANCELADO), (self.extras, self.extra, HoraExtra, HoraExtra.ESTADO_PAGADO)]:
            pk = self.api.post(url, payload(), format="json").data["id"]
            self.assertEqual(self.api.post(f"{url}{pk}/editar/", payload(), format="json").status_code, 400)
            model.objects.filter(pk=pk).update(estado=estado)
            self.assertIn(self.api.post(f"{url}{pk}/editar/", {**payload(), "motivo_cambio": "Corregir"}, format="json").status_code, (400, 403))
            self.assertEqual(self.api.post(f"{url}{pk}/eliminar/").status_code, 404)

    def test_no_duplicar_horas_extra_y_csrf(self):
        self.assertEqual(self.api.post(self.extras, self.extra(), format="json").status_code, 201)
        self.assertEqual(self.api.post(self.extras, self.extra(), format="json").status_code, 400)
        session = APIClient(enforce_csrf_checks=True)
        session.force_login(self.user)
        self.assertEqual(session.post(self.permisos, self.permiso(), format="json").status_code, 403)
        self.assertIn(APIClient().get(self.permisos).status_code, (401, 403))
