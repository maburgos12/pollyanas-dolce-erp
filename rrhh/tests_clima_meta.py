"""Recepción de estados Meta autenticados, duplicados y fuera de orden."""

import hashlib
import hmac
import json
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from .models import AvisoClima, CampanaClima, Empleado, EstadoMetaClimaPendiente, ParticipanteClima
from .services_clima_meta import conciliar_estado, conciliar_pendientes_recientes


class EstadosMetaClimaTests(TestCase):
    def setUp(self):
        admin = get_user_model().objects.create_superuser("meta_clima_qa", "meta@example.com", "pass")
        campana = CampanaClima.objects.create(
            titulo="Meta QA", fecha_cierre=timezone.localdate() + timedelta(days=7),
            cuestionario={}, creada_por=admin,
        )
        empleado = Empleado.objects.create(nombre="Persona QA", telefono="6671234567")
        persona = ParticipanteClima.objects.create(campana=campana, empleado=empleado, nombre=empleado.nombre)
        self.aviso = AvisoClima.objects.create(
            participante=persona, tipo=AvisoClima.INVITACION, estado="ENVIADO",
            referencia_externa="wamid.clima.qa", intentado_en=timezone.now(),
        )
        self.url = reverse("rrhh:clima_meta_estados")

    def webhook(self, *estados, phone_id="123"):
        cuerpo = json.dumps({
            "object": "whatsapp_business_account",
            "entry": [{"changes": [{"field": "messages", "value": {
                "metadata": {"phone_number_id": phone_id}, "statuses": [
                    {"id": referencia, "status": estado, "timestamp": str(timestamp)}
                    for referencia, estado, timestamp in estados
                ]
            }}]}],
        }).encode()
        firma = "sha256=" + hmac.new(b"secret-qa", cuerpo, hashlib.sha256).hexdigest()
        return self.client.post(self.url, data=cuerpo, content_type="application/json", HTTP_X_HUB_SIGNATURE_256=firma)

    def test_verificacion_y_firma_son_obligatorias(self):
        self.assertEqual(self.client.get(self.url).status_code, 503)
        with patch.dict("os.environ", {"CLIMA_META_WEBHOOK_VERIFY_TOKEN": "verify-qa"}):
            self.assertEqual(self.client.get(self.url, {"hub.mode": "subscribe", "hub.verify_token": "incorrecto", "hub.challenge": "123"}).status_code, 403)
            self.assertEqual(self.client.get(self.url, {"hub.mode": "subscribe", "hub.verify_token": "verify-qa", "hub.challenge": "123"}).content, b"123")
        self.assertEqual(self.webhook(("wamid.clima.qa", "delivered", 1700000000)).status_code, 503)
        with patch.dict("os.environ", {"META_APP_SECRET": "secret-qa", "META_WHATSAPP_PHONE_NUMBER_ID": "123"}):
            self.assertEqual(self.client.post(self.url, data=b"{}", content_type="application/json", HTTP_X_HUB_SIGNATURE_256="sha256=wrong").status_code, 403)
        self.assertEqual(AvisoClima.objects.get(pk=self.aviso.pk).entrega_estado, "")

    def test_entregado_leido_duplicado_y_fuera_de_orden(self):
        ahora = int(timezone.now().timestamp())
        with patch.dict("os.environ", {"META_APP_SECRET": "secret-qa", "META_WHATSAPP_PHONE_NUMBER_ID": "123"}):
            self.assertEqual(self.webhook(("wamid.clima.qa", "delivered", ahora)).status_code, 200)
            self.assertEqual(self.webhook(("wamid.clima.qa", "read", ahora + 1)).status_code, 200)
            self.assertEqual(self.webhook(("wamid.clima.qa", "read", ahora + 1)).status_code, 200)
            self.assertEqual(self.webhook(("wamid.clima.qa", "sent", ahora - 1)).status_code, 200)
            self.assertEqual(self.webhook(("wamid.clima.qa", "failed", ahora + 2)).status_code, 200)
        self.aviso.refresh_from_db()
        self.assertEqual(self.aviso.entrega_estado, "LEIDO")
        self.assertEqual(int(self.aviso.entrega_evento_en.timestamp()), ahora + 1)
        self.assertEqual(EstadoMetaClimaPendiente.objects.count(), 0)

    def test_numero_de_meta_distinto_no_modifica_aviso(self):
        ahora = int(timezone.now().timestamp())
        with patch.dict("os.environ", {"META_APP_SECRET": "secret-qa", "META_WHATSAPP_PHONE_NUMBER_ID": "123"}):
            self.assertEqual(self.webhook(("wamid.clima.qa", "read", ahora), phone_id="999").status_code, 200)
        self.assertEqual(AvisoClima.objects.get(pk=self.aviso.pk).entrega_estado, "")

    def test_evento_temprano_se_concilia_al_persistir_referencia(self):
        self.aviso.referencia_externa = ""
        self.aviso.estado = "INCIERTO"
        self.aviso.save(update_fields=["referencia_externa", "estado"])
        ahora = int(timezone.now().timestamp())
        with patch.dict("os.environ", {"META_APP_SECRET": "secret-qa", "META_WHATSAPP_PHONE_NUMBER_ID": "123"}):
            self.assertEqual(self.webhook(("wamid.clima.qa", "delivered", ahora)).status_code, 200)
        self.assertEqual(EstadoMetaClimaPendiente.objects.count(), 1)
        self.aviso.referencia_externa = "wamid.clima.qa"
        self.aviso.estado = "ENVIADO"
        self.aviso.save(update_fields=["referencia_externa", "estado"])
        self.assertEqual(conciliar_estado(self.aviso.pk), 1)
        self.assertEqual(EstadoMetaClimaPendiente.objects.count(), 0)
        self.assertEqual(AvisoClima.objects.get(pk=self.aviso.pk).entrega_estado, "ENTREGADO")

    def test_no_guarda_eventos_ajenos_sin_envio_reciente(self):
        self.aviso.intentado_en = timezone.now() - timedelta(hours=1)
        self.aviso.save(update_fields=["intentado_en"])
        ahora = int(timezone.now().timestamp())
        with patch.dict("os.environ", {"META_APP_SECRET": "secret-qa", "META_WHATSAPP_PHONE_NUMBER_ID": "123"}):
            self.assertEqual(self.webhook(("wamid.otro", "read", ahora)).status_code, 200)
        self.assertEqual(EstadoMetaClimaPendiente.objects.count(), 0)

    def test_barrido_recupera_evento_temprano_aun_si_se_cruzaron_transacciones(self):
        self.aviso.referencia_externa = ""
        self.aviso.estado = "INCIERTO"
        self.aviso.save(update_fields=["referencia_externa", "estado"])
        ahora = int(timezone.now().timestamp())
        with patch.dict("os.environ", {"META_APP_SECRET": "secret-qa", "META_WHATSAPP_PHONE_NUMBER_ID": "123"}):
            self.webhook(("wamid.clima.qa", "read", ahora))
        self.aviso.referencia_externa = "wamid.clima.qa"
        self.aviso.estado = "ENVIADO"
        self.aviso.save(update_fields=["referencia_externa", "estado"])
        self.assertEqual(conciliar_pendientes_recientes(), 1)
        self.assertEqual(AvisoClima.objects.get(pk=self.aviso.pk).entrega_estado, "LEIDO")
