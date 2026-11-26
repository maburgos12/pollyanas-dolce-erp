from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from logistica.models import ConfigAlertaFlota, DocumentoUnidad, Repartidor, Unidad
from logistica.tasks import alertar_documentos_por_vencer


@override_settings(
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    DEFAULT_FROM_EMAIL="ERP <avisos@pollyanasdolce.com>",
    LOGISTICA_VENCIMIENTOS_DESTINATARIOS=["maburgos12@pollyanasdolce.com"],
)
class AvisosVencimientoTests(TestCase):
    def setUp(self):
        self.hoy = timezone.localdate()
        self.sucursal = Sucursal.objects.create(codigo="VTEST", nombre="Pruebas de avisos")
        self.user = get_user_model().objects.create_user("conductor-prueba")
        self.repartidor = Repartidor.objects.create(
            user=self.user, sucursal=self.sucursal,
            licencia_expiracion=self.hoy + timedelta(days=7),
        )
        self.config = ConfigAlertaFlota.objects.create(tipo="documento_vencimiento")

    def documento(self, dias=7):
        unidad = Unidad.objects.create(codigo="VTEST-1", descripcion="Unidad de prueba", sucursal=self.sucursal)
        return DocumentoUnidad.objects.create(
            unidad=unidad, tipo="seguro", archivo="fixture.pdf",
            fecha_vencimiento=self.hoy + timedelta(days=dias),
        )

    def test_licencia_recupera_aviso_omitido_con_destino_institucional(self):
        resultado = alertar_documentos_por_vencer()
        self.assertEqual(resultado["enviadas"], 1)
        self.assertEqual(mail.outbox[0].to, ["maburgos12@pollyanasdolce.com"])
        self.assertIn("Licencia", mail.outbox[0].subject)
        self.assertIn("7 días", mail.outbox[0].body)
        alertar_documentos_por_vencer()
        self.assertEqual(len(mail.outbox), 1)

    def test_documento_recupera_aviso_fuera_de_fecha_exacta(self):
        self.user.is_active = False
        self.user.save()
        self.documento()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)

    def test_destinatario_personal_no_recibe_documentos(self):
        self.documento(15)
        personal = get_user_model().objects.create_user("personal", email="personal@gmail.com")
        self.config.destinatarios.add(personal)
        resultado = alertar_documentos_por_vencer()
        self.assertEqual(resultado["enviadas"], 0)
        self.assertEqual(resultado["motivo"], "sin_destinatarios_institucionales")

    def test_vencido_no_se_oculta_automaticamente(self):
        documento = self.documento(-3)
        alertar_documentos_por_vencer()
        documento.refresh_from_db()
        self.assertTrue(documento.vigente)

    def test_correo_rechazado_se_recupera_sin_duplicar_aceptados(self):
        with patch("logistica.services_vencimientos.send_mail", return_value=0):
            self.assertEqual(alertar_documentos_por_vencer()["pendientes"], 1)
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)
        self.assertEqual(len(mail.outbox), 1)

    def test_excepcion_de_red_es_incierta_y_no_se_reenvia_a_ciegas(self):
        from logistica.models import AvisoVencimientoFlota
        with patch("logistica.services_vencimientos.send_mail", side_effect=TimeoutError):
            alertar_documentos_por_vencer()
        self.assertEqual(AvisoVencimientoFlota.objects.get().estado_envio, "incierto")
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)

    def test_nueva_fecha_de_licencia_habilita_otro_aviso(self):
        alertar_documentos_por_vencer()
        self.repartidor.licencia_expiracion += timedelta(days=1)
        self.repartidor.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)

    def test_umbral_mas_urgente_sin_rafaga_de_avisos_atrasados(self):
        self.repartidor.licencia_expiracion = self.hoy + timedelta(days=28)
        self.repartidor.save()
        alertar_documentos_por_vencer()
        with patch("logistica.services_vencimientos.timezone.localdate", return_value=self.hoy + timedelta(days=22)):
            self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
            self.assertIn("6 días", mail.outbox[-1].body)
        self.assertEqual(len(mail.outbox), 2)

    def test_inactivos_y_cuentas_tecnicas_no_reciben_alerta_de_licencia(self):
        self.repartidor.tipo_identidad = Repartidor.TIPO_CUENTA_TECNICA
        self.repartidor.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)
        self.repartidor.tipo_identidad = Repartidor.TIPO_EMPLEADO_DOLCE
        self.repartidor.save()
        self.user.is_active = False
        self.user.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)

    def test_sin_configuracion_usa_respaldo_institucional(self):
        self.config.delete()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)

    def test_desactivacion_explicita_se_respeta_y_es_visible(self):
        self.config.activa = False
        self.config.save()
        self.assertEqual(alertar_documentos_por_vencer()["motivo"], "config_no_activa")
        self.login_direccion()
        response = self.client.get(reverse("logistica:dashboard_ejecutivo"))
        self.assertContains(response, "Los avisos de vencimiento están desactivados")

    def login_direccion(self):
        director = get_user_model().objects.create_superuser("direccion-avisos", "direccion@pollyanasdolce.com", "test-only")
        self.client.force_login(director)

    def test_panel_advierte_si_nunca_se_ejecuto_y_muestra_licencias(self):
        self.login_direccion()
        response = self.client.get(reverse("logistica:dashboard_ejecutivo"))
        self.assertContains(response, "Todavía no se ha completado una revisión")
        self.assertContains(response, "Licencia ·")
        self.assertContains(response, "maburgos12@pollyanasdolce.com")

    def test_sin_fecha_de_licencia_se_avisa_y_se_ve_en_panel(self):
        self.repartidor.licencia_expiracion = None
        self.repartidor.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
        self.assertIn("Sin fecha registrada", mail.outbox[0].body)
        self.login_direccion()
        self.assertContains(self.client.get(reverse("logistica:dashboard_ejecutivo")), "Sin fecha registrada")

    @override_settings(DEFAULT_FROM_EMAIL="personal@gmail.com")
    def test_remitente_personal_bloquea_y_explica(self):
        self.assertEqual(alertar_documentos_por_vencer()["motivo"], "remitente_no_institucional")

    def test_destinos_se_normalizan_deduplican_y_excluyen_inactivos(self):
        director = get_user_model().objects.create_user("director", email=" Director@pollyanasdolce.com ")
        repetido = get_user_model().objects.create_user("duplicado", email="director@pollyanasdolce.com")
        baja = get_user_model().objects.create_user("baja", email="baja@pollyanasdolce.com", is_active=False)
        self.config.destinatarios.add(director, repetido, baja)
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
        self.assertEqual(mail.outbox[0].to, ["director@pollyanasdolce.com"])

    def test_revision_atrasada_y_pendientes_no_parecen_operacion_sana(self):
        from logistica.services_vencimientos import resumen_avisos_vencimiento
        alertar_documentos_por_vencer()
        ConfigAlertaFlota.objects.filter(pk=self.config.pk).update(ultima_ejecucion=timezone.now() - timedelta(hours=27))
        self.assertEqual(resumen_avisos_vencimiento()["motivo"], "revision_atrasada")

    def test_panel_conserva_permisos(self):
        self.client.force_login(self.user)
        response = self.client.get(reverse("logistica:dashboard_ejecutivo"))
        self.assertIn(response.status_code, (302, 403))
        self.assertNotContains(response, "maburgos12@pollyanasdolce.com", status_code=response.status_code)

    def test_rechazo_explicito_no_bloquea_otro_destinatario_y_se_reintenta(self):
        from smtplib import SMTPRecipientsRefused
        from logistica.models import AvisoVencimientoFlota
        with patch("logistica.services_vencimientos.send_mail", side_effect=SMTPRecipientsRefused({"test": (550, b"rejected")})):
            self.assertEqual(alertar_documentos_por_vencer()["pendientes"], 1)
        self.assertEqual(AvisoVencimientoFlota.objects.get().estado_envio, "fallido")
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)

    def test_envio_interrumpido_antiguo_permanece_visible_sin_duplicar(self):
        from logistica.models import AvisoVencimientoFlota
        from logistica.services_vencimientos import resumen_avisos_vencimiento
        alertar_documentos_por_vencer()
        AvisoVencimientoFlota.objects.update(
            estado_envio="enviando", aceptado_en=None, actualizado_en=timezone.now() - timedelta(minutes=16),
        )
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)
        self.assertEqual(resumen_avisos_vencimiento()["inciertos"], 1)
        self.assertEqual(len(mail.outbox), 1)

    def test_documentos_retirados_y_unidades_inactivas_no_se_reactivan(self):
        self.user.is_active = False
        self.user.save()
        documento = self.documento(-3)
        documento.vigente = False
        documento.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)
        documento.vigente = True
        documento.save()
        documento.unidad.activa = False
        documento.unidad.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)

    def test_licencia_vencida_y_externo_autorizado_tambien_se_avisan(self):
        self.repartidor.tipo_identidad = Repartidor.TIPO_EXTERNO_AUTORIZADO
        self.repartidor.licencia_expiracion = self.hoy - timedelta(days=33)
        self.repartidor.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
        self.assertIn("Vencido", mail.outbox[-1].subject)

    def test_destinatario_nuevo_recupera_su_aviso_sin_repetir_el_anterior(self):
        alertar_documentos_por_vencer()
        anterior = get_user_model().objects.create_user("dg-2", email="maburgos12@pollyanasdolce.com")
        nuevo = get_user_model().objects.create_user("responsable", email="responsable@pollyanasdolce.com")
        self.config.destinatarios.add(anterior, nuevo)
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
        self.assertEqual(mail.outbox[-1].to, ["responsable@pollyanasdolce.com"])

    def test_sin_pendientes_no_afirma_entrega(self):
        alertar_documentos_por_vencer()
        self.login_direccion()
        response = self.client.get(reverse("logistica:dashboard_ejecutivo"))
        self.assertContains(response, "Esto no confirma su entrega al destinatario")
        self.assertNotContains(response, "Requieren atención.")

    @override_settings(IS_DEVELOPMENT_ENV=False, EMAIL_BACKEND="django.core.mail.backends.dummy.EmailBackend")
    def test_backend_que_descarta_correos_no_se_presenta_como_operativo(self):
        self.assertEqual(alertar_documentos_por_vencer()["motivo"], "correo_no_operativo")

    @override_settings(LOGISTICA_VENCIMIENTOS_DESTINATARIOS=[])
    def test_sin_destinatarios_explicitos_o_de_respaldo_advierte(self):
        from logistica.services_vencimientos import resumen_avisos_vencimiento
        self.assertEqual(alertar_documentos_por_vencer()["motivo"], "sin_destinatarios_institucionales")
        self.assertEqual(resumen_avisos_vencimiento()["destinatarios"], [])

    def test_antes_del_primer_umbral_no_adelanta_avisos(self):
        self.repartidor.licencia_expiracion = self.hoy + timedelta(days=31)
        self.repartidor.save()
        self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 0)

    def test_umbral_cero_obligatorio_aun_con_anticipaciones_personalizadas(self):
        self.config.dias_anticipacion_1 = 20
        self.config.dias_anticipacion_2 = 10
        self.config.dias_anticipacion_3 = 5
        self.config.save()
        self.repartidor.licencia_expiracion = self.hoy
        self.repartidor.save()
        alertar_documentos_por_vencer()
        self.assertIn("Vence hoy", mail.outbox[0].subject)

    def test_renovacion_durante_revision_no_envia_otra_licencia_obsoleta(self):
        otro = get_user_model().objects.create_user("zeta-conductor")
        Repartidor.objects.create(user=otro, sucursal=self.sucursal, licencia_expiracion=self.repartidor.licencia_expiracion)
        def aceptar_y_renovar(**kwargs):
            Repartidor.objects.update(licencia_expiracion=self.hoy + timedelta(days=365))
            return 1
        with patch("logistica.services_vencimientos.send_mail", side_effect=aceptar_y_renovar) as enviar:
            self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
            self.assertEqual(enviar.call_count, 1)

    def test_destinatario_retirado_durante_revision_no_recibe_aviso(self):
        primero = get_user_model().objects.create_user("a-responsable", email="a@pollyanasdolce.com")
        segundo = get_user_model().objects.create_user("b-responsable", email="b@pollyanasdolce.com")
        self.config.destinatarios.add(primero, segundo)
        def aceptar_y_retirar(**kwargs):
            self.config.destinatarios.remove(segundo)
            return 1
        with patch("logistica.services_vencimientos.send_mail", side_effect=aceptar_y_retirar) as enviar:
            self.assertEqual(alertar_documentos_por_vencer()["enviadas"], 1)
            self.assertEqual(enviar.call_count, 1)


@override_settings(
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    DEFAULT_FROM_EMAIL="avisos@pollyanasdolce.com",
    LOGISTICA_VENCIMIENTOS_DESTINATARIOS=["maburgos12@pollyanasdolce.com"],
)
class AvisosVencimientoConcurrenciaTests(TransactionTestCase):
    def test_dos_trabajadores_no_envian_dos_veces_el_mismo_aviso(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event
        from django.db import close_old_connections
        from logistica.models import AvisoVencimientoFlota
        from logistica.services_vencimientos import procesar_avisos_vencimiento

        sucursal = Sucursal.objects.create(codigo="VLOCK", nombre="Concurrencia")
        user = get_user_model().objects.create_user("conductor-lock")
        Repartidor.objects.create(user=user, sucursal=sucursal, licencia_expiracion=timezone.localdate())
        entro = Event()
        continuar = Event()

        def transporte(**kwargs):
            entro.set()
            if not continuar.wait(10):
                raise AssertionError("La prueba no liberó el transporte")
            return 1

        def trabajador():
            close_old_connections()
            try:
                return procesar_avisos_vencimiento()
            finally:
                close_old_connections()

        with patch("logistica.services_vencimientos.send_mail", side_effect=transporte) as enviar:
            with ThreadPoolExecutor(max_workers=1) as executor:
                primero = executor.submit(trabajador)
                try:
                    self.assertTrue(entro.wait(10))
                    segundo = procesar_avisos_vencimiento()
                    self.assertEqual(segundo["motivo"], "revision_en_curso")
                finally:
                    continuar.set()
                self.assertEqual(primero.result(timeout=10)["enviadas"], 1)
            self.assertEqual(enviar.call_count, 1)
        self.assertEqual(AvisoVencimientoFlota.objects.get().estado_envio, "aceptado")
