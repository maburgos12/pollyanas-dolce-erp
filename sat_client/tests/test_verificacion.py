from __future__ import annotations

from datetime import date
from unittest.mock import patch

from django.test import TestCase

from sat_client.models import SolicitudDescarga
from sat_client.services.verificacion import verificar_hasta_terminar


class SatVerificacionServiceTests(TestCase):
    def test_verificar_hasta_terminar_polls_until_terminal_state(self):
        solicitud = SolicitudDescarga.objects.create(
            id_solicitud="sol-termina",
            fecha_inicial=date(2026, 5, 1),
            fecha_final=date(2026, 5, 31),
            rfc_solicitante="AAA010101AAA",
            direccion=SolicitudDescarga.DIRECCION_EMITIDOS,
        )
        llamadas = []
        pausas = []

        def fake_verificar(solicitud_obj, *, token, transport=None):
            llamadas.append(token)
            if len(llamadas) == 1:
                solicitud_obj.estado = SolicitudDescarga.ESTADO_EN_PROCESO
            else:
                solicitud_obj.estado = SolicitudDescarga.ESTADO_TERMINADA
                solicitud_obj.ids_paquetes = ["paquete-1"]
            solicitud_obj.save()
            return solicitud_obj

        with patch("sat_client.services.verificacion.verificar_solicitud", side_effect=fake_verificar):
            result = verificar_hasta_terminar(
                solicitud,
                obtener_token_func=lambda: "token",
                sleep_func=lambda seconds: pausas.append(seconds),
                max_intentos=3,
                intervalo_segundos=0,
            )

        self.assertEqual(result.estado, SolicitudDescarga.ESTADO_TERMINADA)
        self.assertEqual(result.ids_paquetes, ["paquete-1"])
        self.assertEqual(llamadas, ["token", "token"])
        self.assertEqual(pausas, [0])
