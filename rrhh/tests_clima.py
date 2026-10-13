from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core import signing
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from .models import AvisoClima, CampanaClima, Empleado, ParticipanteClima, RespuestaClima
from .views_clima import QUESTIONNAIRE
from .services_clima_avisos import procesar_avisos_clima
from core.whatsapp import ResultadoWhatsApp


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

    def test_cobertura_actual_detecta_contactos_compartidos_sin_mostrarlos(self):
        self.active.email = "Compartido@Example.com"
        self.active.save(update_fields=["email"])
        segundo = Empleado.objects.create(nombre="Segunda persona", email="compartido@example.com")
        tercero = Empleado.objects.create(nombre="Tercera persona", telefono="667 123 4567")
        sin_contacto = Empleado.objects.create(nombre="Cuarta persona")
        self.client.force_login(self.admin)
        principal = self.client.get(self.url)
        self.assertContains(principal, "1 de 4 personas")
        self.assertContains(principal, "1 no tiene contacto utilizable")
        self.assertContains(principal, "2 comparten correo o teléfono")
        campana = CampanaClima.objects.create(
            titulo="Ensayo", fecha_cierre=timezone.localdate() + timedelta(days=14),
            cuestionario=QUESTIONNAIRE, creada_por=self.admin,
        )
        for empleado in [self.active, segundo, tercero, sin_contacto, self.inactive]:
            ParticipanteClima.objects.create(
                campana=campana, empleado=empleado, nombre=empleado.nombre,
                departamento=empleado.departamento,
            )
        detalle = reverse("rrhh:clima_detalle", args=[campana.pk])
        self.client.force_login(self.other)
        self.assertEqual(self.client.get(detalle).status_code, 403)
        self.client.force_login(self.admin)
        response = self.client.get(detalle)
        self.assertContains(response, "5 personas pendientes: 1 con contacto")
        self.assertContains(response, "2 con dato compartido")
        self.assertContains(response, "1 que ya no están activas")
        self.assertNotContains(response, "Compartido@Example.com")
        self.assertNotContains(response, "667 123 4567")
        segundo.email = "unico@example.com"
        segundo.save(update_fields=["email"])
        self.assertContains(self.client.get(detalle), "5 personas pendientes: 3 con contacto")
        self.assertFalse(CampanaClima.objects.get(pk=campana.pk).recepcion_habilitada)


class AvisosClimaTests(TestCase):
    def setUp(self):
        admin = get_user_model().objects.create_superuser("clima_aviso_admin", "admin@example.com", "pass")
        self.campana = CampanaClima.objects.create(
            titulo="Prueba", fecha_cierre=timezone.localdate() + timedelta(days=14),
            cuestionario=QUESTIONNAIRE, creada_por=admin,
        )
        empleado = Empleado.objects.create(nombre="Ana Ejemplo", telefono="6671234567")
        self.persona = ParticipanteClima.objects.create(
            campana=self.campana, empleado=empleado, nombre=empleado.nombre,
            envio_whatsapp_autorizado=True,
        )

    @patch("rrhh.services_clima_avisos.whatsapp.enviar_plantilla")
    def test_cerrada_o_flag_apagado_nunca_envia(self, enviar):
        with patch.dict("os.environ", {"CLIMA_META_ENVIOS_HABILITADOS": "1"}):
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
        self.campana.recepcion_habilitada = True
        self.campana.save(update_fields=["recepcion_habilitada"])
        with patch.dict("os.environ", {"CLIMA_META_ENVIOS_HABILITADOS": ""}):
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
        enviar.assert_not_called()

    @patch("rrhh.services_clima_avisos.whatsapp.enviar_plantilla")
    def test_invitacion_una_vez_y_recordatorio_solo_si_sigue_pendiente(self, enviar):
        enviar.return_value = ResultadoWhatsApp("ENVIADO", referencia="wamid.test")
        self.campana.recepcion_habilitada = True
        self.campana.save(update_fields=["recepcion_habilitada"])
        with patch.dict("os.environ", {"CLIMA_META_ENVIOS_HABILITADOS": "1"}):
            self.assertEqual(procesar_avisos_clima()["procesados"], 1)
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
            invitacion = AvisoClima.objects.get(tipo=AvisoClima.INVITACION)
            invitacion.intentado_en = timezone.now() - timedelta(days=4)
            invitacion.save(update_fields=["intentado_en"])
            self.assertEqual(procesar_avisos_clima()["procesados"], 1)
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
        self.assertEqual(enviar.call_count, 2)
        self.assertEqual(enviar.call_args.kwargs["plantilla"], "clima_laboral_recordatorio_v1")
        self.assertEqual(AvisoClima.objects.count(), 2)

    @patch("rrhh.services_clima_avisos.whatsapp.enviar_plantilla")
    def test_no_recuerda_si_respondio_y_no_reintenta_incierto(self, enviar):
        enviar.return_value = ResultadoWhatsApp("INCIERTO")
        self.campana.recepcion_habilitada = True
        self.campana.save(update_fields=["recepcion_habilitada"])
        with patch.dict("os.environ", {"CLIMA_META_ENVIOS_HABILITADOS": "1"}):
            self.assertEqual(procesar_avisos_clima()["procesados"], 1)
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
            AvisoClima.objects.filter(participante=self.persona).update(
                estado="ENVIADO", intentado_en=timezone.now() - timedelta(days=4))
            self.persona.respondio = True
            self.persona.save(update_fields=["respondio"])
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
        enviar.assert_called_once()

    @patch("rrhh.services_clima_avisos.whatsapp.enviar_plantilla")
    def test_contacto_compartido_o_no_autorizado_se_omite(self, enviar):
        self.campana.recepcion_habilitada = True
        self.campana.save(update_fields=["recepcion_habilitada"])
        empleado = Empleado.objects.create(nombre="Otra persona", telefono="6671234567")
        ParticipanteClima.objects.create(campana=self.campana, empleado=empleado, nombre=empleado.nombre)
        with patch.dict("os.environ", {"CLIMA_META_ENVIOS_HABILITADOS": "1"}):
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
        enviar.assert_not_called()

    @patch("rrhh.services_clima_avisos.whatsapp.enviar_plantilla", side_effect=TimeoutError)
    def test_excepcion_conserva_estado_incierto_y_no_renvia(self, enviar):
        self.campana.recepcion_habilitada = True
        self.campana.save(update_fields=["recepcion_habilitada"])
        with patch.dict("os.environ", {"CLIMA_META_ENVIOS_HABILITADOS": "1"}):
            self.assertEqual(procesar_avisos_clima()["procesados"], 1)
            self.assertEqual(procesar_avisos_clima()["procesados"], 0)
        self.assertEqual(AvisoClima.objects.get().estado, "INCIERTO")
        enviar.assert_called_once()
