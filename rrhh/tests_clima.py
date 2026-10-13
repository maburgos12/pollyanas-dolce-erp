from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core import signing
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from .models import CampanaClima, Empleado, ParticipanteClima, RespuestaClima
from .views_clima import QUESTIONNAIRE


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
        self.assertFalse(campana.recepcion_habilitada)
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

    def test_liga_y_qr_solo_administracion_y_caducidad_publica(self):
        campana = CampanaClima.objects.create(
            titulo="Clima 2026", fecha_cierre=timezone.localdate() + timedelta(days=14),
            cuestionario={}, creada_por=self.admin,
        )
        participante = ParticipanteClima.objects.create(
            campana=campana, empleado=self.active, nombre=self.active.nombre,
            departamento=self.active.departamento,
        )
        acceso = reverse("rrhh:clima_acceso_individual", args=[campana.pk, participante.pk])
        self.client.force_login(self.other)
        self.assertEqual(self.client.get(acceso).status_code, 403)
        self.client.force_login(self.admin)
        self.assertContains(self.client.get(acceso), "data:image/svg+xml;base64")
        token = signing.Signer(salt="rrhh.clima").sign(str(participante.pk))
        publica = reverse("rrhh:clima_acceso_publico", args=[token])
        self.client.logout()
        response = self.client.get(publica)
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, participante.nombre)
        self.assertEqual(self.client.get(reverse("rrhh:clima_acceso_publico", args=[token + "x"])).status_code, 404)
        campana.fecha_cierre = timezone.localdate() - timedelta(days=1)
        campana.save(update_fields=["fecha_cierre"])
        self.assertEqual(self.client.get(publica).status_code, 410)

    def test_respuesta_separada_y_solo_una_por_participante(self):
        campana = CampanaClima.objects.create(
            titulo="Prueba", fecha_cierre=timezone.localdate() + timedelta(days=14),
            cuestionario=QUESTIONNAIRE, creada_por=self.admin,
        )
        participante = ParticipanteClima.objects.create(
            campana=campana, empleado=self.active, nombre=self.active.nombre,
            departamento=self.active.departamento,
        )
        token = signing.Signer(salt="rrhh.clima").sign(str(participante.pk))
        url = reverse("rrhh:clima_acceso_publico", args=[token])
        self.assertContains(self.client.get(url), "todavía no está disponible")
        campana.recepcion_habilitada = True
        campana.save(update_fields=["recepcion_habilitada"])
        self.assertContains(self.client.get(url), "Encuesta de clima laboral")
        preguntas = [q for s in QUESTIONNAIRE["sections"] for q in s["questions"]]
        datos = {f"q_{i}": q["options"][0] for i, q in enumerate(preguntas) if q["type"] == "choice"}
        invalidos = dict(datos, q_0="opción inventada")
        self.assertEqual(self.client.post(url, invalidos).status_code, 200)
        self.assertEqual(RespuestaClima.objects.count(), 0)
        self.assertContains(self.client.post(url, datos), "respuesta quedó registrada")
        self.assertEqual(RespuestaClima.objects.count(), 1)
        self.assertTrue(ParticipanteClima.objects.get(pk=participante.pk).respondio)
        self.assertNotIn("empleado", [f.name for f in RespuestaClima._meta.fields])
        self.assertNotIn("participante", [f.name for f in RespuestaClima._meta.fields])
        self.assertContains(self.client.post(url, datos), "ya fue registrada")
        self.assertEqual(RespuestaClima.objects.count(), 1)

    def test_resultados_ocultos_con_menos_de_cinco_respuestas(self):
        campana = CampanaClima.objects.create(
            titulo="Prueba", fecha_cierre=timezone.localdate() + timedelta(days=14),
            cuestionario=QUESTIONNAIRE, creada_por=self.admin,
        )
        RespuestaClima.objects.create(campana=campana, departamento="PRODUCCION", respuestas={"q_0": "De acuerdo"})
        self.client.force_login(self.admin)
        response = self.client.get(reverse("rrhh:clima_detalle", args=[campana.pk]))
        self.assertContains(response, "al menos cinco respuestas")
        self.assertNotContains(response, "De acuerdo: 1")

    def test_agregados_solo_al_cerrar_y_sin_areas_pequenas(self):
        campana = CampanaClima.objects.create(
            titulo="Prueba", fecha_cierre=timezone.localdate() + timedelta(days=14),
            cuestionario=QUESTIONNAIRE, creada_por=self.admin, recepcion_habilitada=True,
        )
        for area in ["Producción"] * 5 + ["Ventas"]:
            RespuestaClima.objects.create(campana=campana, departamento=area, respuestas={"q_0": "De acuerdo"})
        self.client.force_login(self.admin)
        url = reverse("rrhh:clima_detalle", args=[campana.pk])
        self.assertNotContains(self.client.get(url), "Toda la campaña · 6 respuestas")
        campana.recepcion_habilitada = False
        campana.save(update_fields=["recepcion_habilitada"])
        response = self.client.get(url)
        self.assertContains(response, "Toda la campaña · 6 respuestas")
        self.assertNotContains(response, "Producción · 5 respuestas")
        self.assertNotContains(response, "Ventas · 1 respuesta")
