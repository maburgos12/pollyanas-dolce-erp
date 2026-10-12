from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from .models import CampanaClima, Empleado, ParticipanteClima


class PreparacionClimaTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser("clima_admin", "admin@example.com", "test-pass")
        self.other = get_user_model().objects.create_user("clima_other", password="test-pass")
        self.active = Empleado.objects.create(nombre="Persona activa", departamento=Empleado.DEP_PRODUCCION)
        self.inactive = Empleado.objects.create(nombre="Persona inactiva", activo=False)
        self.url = reverse("rrhh:clima_campanas")

    def test_campana_guardada_es_borrador_con_preguntas_y_foto_del_padron(self):
        self.client.force_login(self.admin)
        self.assertContains(self.client.get(self.url), "Contacto personal: Sin contacto")
        response = self.client.post(self.url, {
            "titulo": "Clima 2026", "fecha_cierre": (timezone.localdate() + timedelta(days=14)).isoformat(),
            "empleados": [str(self.active.pk), str(self.active.pk)],
        })
        self.assertEqual(response.status_code, 302)
        campana = CampanaClima.objects.get()
        participante = ParticipanteClima.objects.get(campana=campana)
        self.assertEqual(campana.participantes.count(), 1)
        self.assertEqual(participante.empleado, self.active)
        self.assertEqual(participante.departamento, Empleado.DEP_PRODUCCION)
        self.assertEqual(sum(q["type"] == "choice" for s in campana.cuestionario["sections"] for q in s["questions"]), 23)
        self.assertEqual(sum(q["type"] == "text" for s in campana.cuestionario["sections"] for q in s["questions"]), 3)

    def test_no_admite_empleado_inactivo(self):
        self.client.force_login(self.admin)
        response = self.client.post(self.url, {
            "titulo": "Clima 2026", "fecha_cierre": (timezone.localdate() + timedelta(days=14)).isoformat(),
            "empleados": [str(self.inactive.pk)],
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(CampanaClima.objects.count(), 0)

    def test_usuario_sin_permiso_no_ve_ni_crea_campana(self):
        self.client.force_login(self.other)
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(self.client.post(self.url).status_code, 403)
