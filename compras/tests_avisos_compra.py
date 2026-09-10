"""Avisos automáticos al solicitante cuando Compras registra la compra."""
from datetime import date
from decimal import Decimal
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core import mail
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from compras.models import (AvisoCompraDepartamental, CompraRealizadaDepartamental,
                            CotizacionCompraDepartamental, ItemCompraDepartamental,
                            SolicitudCompraDepartamental)
from compras.services_avisos_compra import (contexto_mensaje, enviar_aviso, enviar_avisos_pendientes,
                                            programar_avisos, url_solicitud)
from compras.services_departamentales import seleccionar_cotizacion
from compras.services_edicion_compra import registrar_compra_realizada
from core.contactos import normalizar_telefono, resolver_correo, resolver_telefono
from core.models import UserProfile
from core.whatsapp import ResultadoWhatsApp
from maestros.models import Proveedor
from reportes.models import (AreaPresupuesto, AreaPresupuestoResponsable, LineaPresupuestoMensual,
                             RubroPresupuesto)
from rrhh.models import Empleado

WHATSAPP_ENV = {
    "WHATSAPP_ENABLED": "true",
    "META_WHATSAPP_TOKEN": "token-de-prueba",
    "META_WHATSAPP_PHONE_NUMBER_ID": "123456",
    "WHATSAPP_TEMPLATE_COMPRA_REALIZADA": "compra_realizada_v1",
}


class BaseAvisosMixin:
    """Solicitud autorizada y lista para registrar la compra."""

    def preparar(self):
        self.media = TemporaryDirectory()
        self.addCleanup(self.media.cleanup)
        self.override = override_settings(MEDIA_ROOT=self.media.name)
        self.override.enable()
        self.addCleanup(self.override.disable)
        User = get_user_model()
        self.compras_user = User.objects.create_superuser("compras-avisos", password="test")
        self.solicitante = User.objects.create_user(
            "carolina.prueba", password="test", email="carolina@pollyanasdolce.com",
            first_name="Carolina", last_name="Prueba",
        )
        self.area = AreaPresupuesto.objects.create(nombre="Pruebas avisos", codigo="prueba-avisos")
        # El solicitante ve su solicitud por el permiso de área que ya existía: el aviso no amplía acceso.
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=self.solicitante, puede_capturar=True)
        self.rubro = RubroPresupuesto.objects.create(area=self.area, concepto="Equipo")
        LineaPresupuestoMensual.objects.create(rubro=self.rubro, periodo=date(2026, 9, 1),
                                               monto_presupuesto=10000, monto_real=0)
        self.solicitud = SolicitudCompraDepartamental.objects.create(
            area=self.area, solicitante=self.solicitante, periodo=date(2026, 9, 1), estado="ENVIADA")
        self.item = ItemCompraDepartamental.objects.create(
            solicitud=self.solicitud, descripcion="Batidora industrial", cantidad=1, rubro=self.rubro)
        self.proveedor = Proveedor.objects.create(nombre="Proveedor avisos")
        self.quote = CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=self.proveedor, cantidad_ofertada=1, costo_unitario=200)
        seleccionar_cotizacion(self.quote, actor=self.compras_user)
        self.item.refresh_from_db()

    def registrar_compra(self):
        return registrar_compra_realizada(
            self.item, fecha_compra=timezone.localdate(), importe_final=Decimal("200.00"),
            numero_pedido="PEDIDO-1",
            comprobante=SimpleUploadedFile("c.pdf", b"%PDF-1.4\n%%EOF", content_type="application/pdf"),
            actor=self.compras_user, cotizacion_id=self.quote.pk, version=self.quote.version,
        )


class ResolucionContactosTests(BaseAvisosMixin, TestCase):
    """El ERP avisa al contacto de trabajo; el expediente de RRHH es personal."""

    def setUp(self):
        self.preparar()

    def test_correo_sale_de_la_cuenta_no_del_expediente(self):
        Empleado.objects.create(nombre="Carolina Prueba", email="personal@gmail.com",
                                telefono="6871234567", usuario_erp=self.solicitante)
        correo, motivo = resolver_correo(self.solicitante)
        self.assertEqual(correo, "carolina@pollyanasdolce.com")
        self.assertEqual(motivo, "")

    def test_sin_correo_de_trabajo_no_cae_al_personal_del_expediente(self):
        self.solicitante.email = ""
        self.solicitante.save(update_fields=["email"])
        Empleado.objects.create(nombre="Carolina Prueba", email="personal@gmail.com",
                                usuario_erp=self.solicitante)
        self.assertEqual(resolver_correo(self.solicitante), ("", "Sin correo de trabajo registrado"))

    def test_telefono_sale_del_perfil_que_guarda_la_linea_de_empresa(self):
        UserProfile.objects.create(user=self.solicitante, telefono="6871747006")
        Empleado.objects.create(nombre="Carolina Prueba", telefono="6871064285",
                                usuario_erp=self.solicitante)
        self.assertEqual(resolver_telefono(self.solicitante), ("526871747006", ""))

    def test_sin_linea_de_empresa_no_cae_al_celular_personal(self):
        """El celular del expediente es de Capital Humano, no un respaldo."""
        UserProfile.objects.create(user=self.solicitante, telefono="")
        Empleado.objects.create(nombre="Carolina Prueba", telefono="6871064285",
                                usuario_erp=self.solicitante)
        self.assertEqual(resolver_telefono(self.solicitante), ("", "Sin teléfono de trabajo registrado"))

    def test_sin_contacto_no_toma_el_de_otra_persona(self):
        sin_datos = get_user_model().objects.create_user("sin.datos", password="test")
        self.assertEqual(resolver_correo(sin_datos), ("", "Sin correo de trabajo registrado"))
        self.assertEqual(resolver_telefono(sin_datos), ("", "Sin teléfono de trabajo registrado"))

    def test_contacto_de_trabajo_invalido_se_reporta_como_invalido(self):
        malo = get_user_model().objects.create_user("malo", password="test", email="no-es-correo")
        UserProfile.objects.create(user=malo, telefono="671539723")
        self.assertEqual(resolver_correo(malo), ("", "Correo de trabajo con formato inválido"))
        self.assertEqual(resolver_telefono(malo), ("", "Teléfono de trabajo con formato inválido"))

    def test_normalizacion_de_telefono(self):
        self.assertEqual(normalizar_telefono("(687) 123-4567"), "526871234567")
        self.assertEqual(normalizar_telefono("+52 687 123 4567"), "526871234567")
        self.assertEqual(normalizar_telefono("whatsapp:+526871234567"), "526871234567")
        self.assertEqual(normalizar_telefono(""), "")
        self.assertEqual(normalizar_telefono("12345"), "")


class AvisosCompraTests(BaseAvisosMixin, TestCase):
    def setUp(self):
        self.preparar()

    def test_registrar_compra_encola_un_aviso_por_canal_al_solicitante(self):
        with patch("compras.services_avisos_compra._despachar") as despachar:
            compra = self.registrar_compra()
        despachar.assert_not_called()  # aún no hay commit dentro de TestCase
        avisos = list(compra.avisos.order_by("canal"))
        self.assertEqual([a.canal for a in avisos], ["CORREO", "WHATSAPP"])
        self.assertTrue(all(a.estado == "PENDIENTE" for a in avisos))
        self.assertTrue(all(a.destinatario_id == self.solicitante.pk for a in avisos))
        self.assertNotEqual(avisos[0].destinatario_id, self.compras_user.pk)

    def test_compra_y_entrega_siguen_separadas(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, ItemCompraDepartamental.ESTADO_COMPRADO)
        self.assertFalse(compra.item.recepciones.exists() if hasattr(compra.item, "recepciones") else False)

    def test_correo_usa_asunto_cuerpo_y_enlace_de_la_solicitud(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        with patch("core.whatsapp.enviar_plantilla",
                   return_value=ResultadoWhatsApp("SIN_CANAL", detalle="desactivado")):
            enviar_avisos_pendientes(compra)
        self.assertEqual(len(mail.outbox), 1)
        mensaje = mail.outbox[0]
        self.assertEqual(mensaje.subject, f"Compra realizada · {self.solicitud.folio}")
        self.assertEqual(mensaje.to, ["carolina@pollyanasdolce.com"])
        self.assertIn("Batidora industrial", mensaje.body)
        self.assertIn("Comprado, pendiente de entrega", mensaje.body)
        self.assertNotIn("entrega el", mensaje.body.lower())
        html = mensaje.alternatives[0][0]
        self.assertIn("Ver mi solicitud", html)
        self.assertIn(f"#item-{self.item.pk}", html)
        aviso = compra.avisos.get(canal="CORREO")
        self.assertEqual(aviso.estado, "ENVIADO")
        self.assertEqual(aviso.destino, "carolina@pollyanasdolce.com")

    def test_enlace_apunta_a_la_solicitud_y_su_articulo(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        esperado = reverse("compras:departamental_detalle", args=[self.solicitud.pk])
        self.assertTrue(url_solicitud(compra).endswith(f"{esperado}#item-{self.item.pk}"))
        self.assertEqual(contexto_mensaje(compra)["nombre"], "Carolina Prueba")

    def test_falla_de_whatsapp_no_afecta_al_correo(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        with patch("core.whatsapp.enviar_plantilla", side_effect=RuntimeError("canal caído")):
            UserProfile.objects.create(user=self.solicitante, telefono="6871234567")
            enviar_avisos_pendientes(compra)
        self.assertEqual(compra.avisos.get(canal="CORREO").estado, "ENVIADO")
        self.assertEqual(compra.avisos.get(canal="WHATSAPP").estado, "FALLIDO")
        self.assertEqual(len(mail.outbox), 1)
        self.assertTrue(CompraRealizadaDepartamental.objects.filter(pk=compra.pk).exists())

    def test_falla_de_correo_no_afecta_a_whatsapp(self):
        UserProfile.objects.create(user=self.solicitante, telefono="6871234567")
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        with patch("django.core.mail.EmailMultiAlternatives.send",
                   side_effect=RuntimeError("Resend API error (422): rechazado")), \
                patch("core.whatsapp.enviar_plantilla",
                      return_value=ResultadoWhatsApp("ENVIADO", referencia="wamid.1")):
            enviar_avisos_pendientes(compra)
        self.assertEqual(compra.avisos.get(canal="CORREO").estado, "FALLIDO")
        whatsapp = compra.avisos.get(canal="WHATSAPP")
        self.assertEqual(whatsapp.estado, "ENVIADO")
        self.assertEqual(whatsapp.referencia_externa, "wamid.1")
        self.assertEqual(whatsapp.destino, "526871234567")

    def test_sin_telefono_marca_sin_contacto_y_no_sustituye(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        with patch("core.whatsapp.enviar_plantilla") as enviar:
            enviar_avisos_pendientes(compra)
        enviar.assert_not_called()
        aviso = compra.avisos.get(canal="WHATSAPP")
        self.assertEqual(aviso.estado, "SIN_CONTACTO")
        self.assertEqual(aviso.detalle, "Sin teléfono de trabajo registrado")
        self.assertEqual(aviso.destino, "")

    def test_reenvio_no_duplica_un_aviso_ya_enviado(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        with patch("core.whatsapp.enviar_plantilla",
                   return_value=ResultadoWhatsApp("SIN_CANAL", detalle="desactivado")):
            enviar_avisos_pendientes(compra)
            enviar_avisos_pendientes(compra)
            enviar_avisos_pendientes(compra)
        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(compra.avisos.get(canal="CORREO").intentos, 1)

    def test_programar_avisos_dos_veces_no_crea_renglones_extra(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        programar_avisos(compra)
        programar_avisos(compra)
        self.assertEqual(compra.avisos.count(), 2)

    def test_resultado_incierto_de_whatsapp_no_se_marca_enviado(self):
        UserProfile.objects.create(user=self.solicitante, telefono="6871234567")
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        with patch("core.whatsapp.enviar_plantilla",
                   return_value=ResultadoWhatsApp("INCIERTO", detalle="Sin respuesta del canal")):
            enviar_avisos_pendientes(compra)
        aviso = compra.avisos.get(canal="WHATSAPP")
        self.assertEqual(aviso.estado, "INCIERTO")
        self.assertEqual(aviso.referencia_externa, "")

    def test_correo_sin_respuesta_queda_incierto_y_con_respuesta_de_error_queda_fallido(self):
        with patch("compras.services_avisos_compra._despachar"):
            compra = self.registrar_compra()
        aviso = compra.avisos.get(canal="CORREO")
        with patch("django.core.mail.EmailMultiAlternatives.send", side_effect=TimeoutError("sin respuesta")):
            enviar_aviso(aviso)
        aviso.refresh_from_db()
        self.assertEqual(aviso.estado, "INCIERTO")
        with patch("django.core.mail.EmailMultiAlternatives.send",
                   side_effect=RuntimeError("Resend API error (403): dominio no verificado")):
            enviar_aviso(aviso)
        aviso.refresh_from_db()
        self.assertEqual(aviso.estado, "FALLIDO")
        self.assertIn("403", aviso.detalle)

    def test_compras_existentes_no_reciben_avisos_retroactivos(self):
        """Una compra creada sin pasar por el servicio no genera cola ni envíos."""
        compra = CompraRealizadaDepartamental.objects.create(
            item=self.item, cotizacion=self.quote, fecha_compra=timezone.localdate(),
            importe_final=Decimal("200.00"),
            comprobante=SimpleUploadedFile("v.pdf", b"%PDF-1.4\n%%EOF"), registrado_por=self.compras_user)
        self.assertEqual(compra.avisos.count(), 0)
        self.assertEqual(len(mail.outbox), 0)


class WhatsAppCanalTests(TestCase):
    """El canal reporta el resultado real de la respuesta HTTP, no la ejecución."""

    def test_sin_credenciales_reporta_sin_canal_y_no_llama_a_meta(self):
        with patch.dict("os.environ", {"WHATSAPP_ENABLED": "false"}, clear=False), \
                patch("urllib.request.urlopen") as urlopen:
            resultado = ResultadoWhatsApp("", detalle="")
            from core import whatsapp
            resultado = whatsapp.enviar_plantilla(telefono="526871234567", plantilla="x", parametros=[])
        urlopen.assert_not_called()
        self.assertEqual(resultado.estado, "SIN_CANAL")

    def test_http_400_es_fallido_con_el_mensaje_de_meta(self):
        import urllib.error
        from core import whatsapp

        error = urllib.error.HTTPError(
            "url", 400, "Bad Request", {},
            __import__("io").BytesIO(b'{"error":{"message":"Template name does not exist"}}'))
        with patch.dict("os.environ", WHATSAPP_ENV, clear=False), \
                patch("urllib.request.urlopen", side_effect=error):
            resultado = whatsapp.enviar_plantilla(
                telefono="526871234567", plantilla="compra_realizada_v1", parametros=["a", "b", "c", "d"])
        self.assertEqual(resultado.estado, "FALLIDO")
        self.assertIn("Template name does not exist", resultado.detalle)

    def test_timeout_es_incierto(self):
        from core import whatsapp

        with patch.dict("os.environ", WHATSAPP_ENV, clear=False), \
                patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
            resultado = whatsapp.enviar_plantilla(
                telefono="526871234567", plantilla="compra_realizada_v1", parametros=["a"])
        self.assertEqual(resultado.estado, "INCIERTO")

    def test_200_sin_identificador_es_incierto(self):
        from core import whatsapp

        class Respuesta:
            def getcode(self):
                return 200

            def read(self):
                return b'{"messages":[]}'

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        with patch.dict("os.environ", WHATSAPP_ENV, clear=False), \
                patch("urllib.request.urlopen", return_value=Respuesta()):
            resultado = whatsapp.enviar_plantilla(
                telefono="526871234567", plantilla="compra_realizada_v1", parametros=["a"])
        self.assertEqual(resultado.estado, "INCIERTO")

    def test_200_con_identificador_es_enviado_y_usa_plantilla(self):
        from core import whatsapp

        capturado = {}

        class Respuesta:
            def getcode(self):
                return 200

            def read(self):
                return b'{"messages":[{"id":"wamid.ABC"}]}'

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def falso_urlopen(request, timeout=None):
            capturado["payload"] = __import__("json").loads(request.data.decode())
            return Respuesta()

        with patch.dict("os.environ", WHATSAPP_ENV, clear=False), \
                patch("urllib.request.urlopen", side_effect=falso_urlopen):
            resultado = whatsapp.enviar_plantilla(
                telefono="526871234567", plantilla="compra_realizada_v1", parametros=["Carolina", "Batidora"])
        self.assertEqual(resultado.estado, "ENVIADO")
        self.assertEqual(resultado.referencia, "wamid.ABC")
        self.assertEqual(capturado["payload"]["type"], "template")
        self.assertEqual(capturado["payload"]["template"]["name"], "compra_realizada_v1")


class ReintentoAvisoTests(BaseAvisosMixin, TestCase):
    def setUp(self):
        self.preparar()
        with patch("compras.services_avisos_compra._despachar"):
            self.compra = self.registrar_compra()
        self.aviso = self.compra.avisos.get(canal="CORREO")
        self.aviso.estado = AvisoCompraDepartamental.ESTADO_FALLIDO
        self.aviso.detalle = "Resend API error (500)"
        self.aviso.save(update_fields=["estado", "detalle"])
        self.url = reverse("compras:departamental_aviso_reintentar", args=[self.aviso.pk])
        self.headers = {"HTTP_ACCEPT": "application/json"}

    def test_solicitante_sin_permisos_no_puede_reintentar(self):
        self.client.force_login(self.solicitante)
        respuesta = self.client.post(self.url, **self.headers)
        self.assertEqual(respuesta.status_code, 403)
        self.assertEqual(len(mail.outbox), 0)

    def test_compras_reintenta_y_deja_traza(self):
        self.client.force_login(self.compras_user)
        respuesta = self.client.post(self.url, **self.headers)
        self.assertEqual(respuesta.status_code, 200)
        self.assertTrue(respuesta.json()["ok"])
        self.aviso.refresh_from_db()
        self.assertEqual(self.aviso.estado, "ENVIADO")
        self.assertEqual(self.aviso.intentos, 1)
        self.assertEqual(self.aviso.ultimo_intento_por_id, self.compras_user.pk)
        self.assertEqual(len(mail.outbox), 1)

    def test_reintento_de_aviso_ya_enviado_no_reenvia(self):
        self.aviso.estado = AvisoCompraDepartamental.ESTADO_ENVIADO
        self.aviso.save(update_fields=["estado"])
        self.client.force_login(self.compras_user)
        respuesta = self.client.post(self.url, **self.headers)
        self.assertEqual(respuesta.status_code, 200)
        self.assertIn("ya se había enviado", respuesta.json()["toast"]["message"])
        self.assertEqual(len(mail.outbox), 0)

    def test_incierto_se_reconcilia_antes_de_reenviar(self):
        self.aviso.estado = AvisoCompraDepartamental.ESTADO_INCIERTO
        self.aviso.referencia_externa = "resend-123"
        self.aviso.save(update_fields=["estado", "referencia_externa"])
        self.client.force_login(self.compras_user)
        with patch("config.email_backends.retrieve_resend_email", return_value={"id": "resend-123"}):
            respuesta = self.client.post(self.url, **self.headers)
        self.assertEqual(respuesta.status_code, 200)
        self.aviso.refresh_from_db()
        self.assertEqual(self.aviso.estado, "ENVIADO")
        self.assertEqual(len(mail.outbox), 0)

    def test_fallo_persistente_devuelve_409_con_motivo(self):
        self.client.force_login(self.compras_user)
        with patch("django.core.mail.EmailMultiAlternatives.send",
                   side_effect=RuntimeError("Resend API error (403): dominio no verificado")):
            respuesta = self.client.post(self.url, **self.headers)
        self.assertEqual(respuesta.status_code, 409)
        self.assertIn("403", respuesta.json()["toast"]["message"])

    def test_pantalla_muestra_estado_de_cada_canal(self):
        self.client.force_login(self.compras_user)
        html = self.client.get(
            reverse("compras:departamental_detalle", args=[self.solicitud.pk])).content.decode()
        self.assertIn("Aviso al solicitante", html)
        self.assertIn("Reintentar aviso", html)
        self.assertIn("data-async-action", html)
        self.assertNotIn("Entregado", html)

    def test_solicitante_ve_estado_pero_sin_boton_de_reintento(self):
        self.client.force_login(self.solicitante)
        html = self.client.get(
            reverse("compras:departamental_detalle", args=[self.solicitud.pk])).content.decode()
        self.assertIn("Aviso al solicitante", html)
        self.assertNotIn("Reintentar aviso", html)


class TransaccionRevertidaTests(BaseAvisosMixin, TransactionTestCase):
    """Si la compra no se confirma, no queda cola ni sale ningún envío."""

    def setUp(self):
        self.preparar()

    def test_rollback_no_deja_aviso_ni_envio(self):
        with patch("compras.tasks.enviar_avisos_compra_realizada.delay") as encolar:
            with self.assertRaises(ValidationError):
                with transaction.atomic():
                    self.registrar_compra()
                    raise ValidationError("falla posterior simulada")
        encolar.assert_not_called()
        self.assertEqual(AvisoCompraDepartamental.objects.count(), 0)
        self.assertEqual(CompraRealizadaDepartamental.objects.count(), 0)
        self.assertEqual(len(mail.outbox), 0)

    def test_commit_encola_el_envio_una_sola_vez(self):
        with patch("compras.tasks.enviar_avisos_compra_realizada.delay") as encolar:
            compra = self.registrar_compra()
        encolar.assert_called_once_with(compra.pk)
        self.assertEqual(AvisoCompraDepartamental.objects.count(), 2)
