from __future__ import annotations

from datetime import date
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings

from sat_client.models import SolicitudDescarga


@override_settings(SAT_RFC="AAA010101AAA")
class DescargarSatMesCommandTests(TestCase):
    @patch("sat_client.management.commands.descargar_sat_mes._procesar_con_split")
    def test_descarga_mes_completo_ambas_direcciones(self, procesar):
        procesar.return_value = [{"solicitud_id": "x", "descargados": 5, "nuevos": 4}]
        out = StringIO()

        call_command("descargar_sat_mes", "--mes", "2026-01", stdout=out)

        self.assertEqual(procesar.call_count, 2)
        args_emitidos = procesar.call_args_list[0].args
        self.assertEqual(args_emitidos[0], date(2026, 1, 1))
        self.assertEqual(args_emitidos[1], date(2026, 1, 31))
        self.assertIn("8 CFDIs nuevos", out.getvalue())

    @patch("sat_client.management.commands.descargar_sat_mes._procesar_con_split")
    def test_omite_periodo_ya_registrado_sin_forzar(self, procesar):
        SolicitudDescarga.objects.create(
            id_solicitud="ya",
            fecha_inicial=date(2026, 1, 1),
            fecha_final=date(2026, 1, 31),
            rfc_solicitante="AAA010101AAA",
            tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
            direccion=SolicitudDescarga.DIRECCION_EMITIDOS,
            estado=SolicitudDescarga.ESTADO_TERMINADA,
        )
        procesar.return_value = [{"solicitud_id": "x", "descargados": 0, "nuevos": 0}]
        out = StringIO()

        call_command("descargar_sat_mes", "--mes", "2026-01", stdout=out)

        self.assertEqual(procesar.call_count, 1)
        self.assertIn("ya registrado", out.getvalue())

    def test_mes_invalido_lanza_error(self):
        with self.assertRaises(CommandError):
            call_command("descargar_sat_mes", "--mes", "enero")

    @patch("sat_client.management.commands.descargar_sat_mes.time.sleep")
    @patch("sat_client.management.commands.descargar_sat_mes._procesar_con_split")
    def test_error_transitorio_reintenta_y_continua_con_los_demas(self, procesar, _sleep):
        from sat_client.services.base import SatServiceError

        procesar.side_effect = [
            SatServiceError("Error no controlado.", code="404"),
            SatServiceError("Error no controlado.", code="404"),
            SatServiceError("Error no controlado.", code="404"),
            [{"solicitud_id": "ok", "descargados": 2, "nuevos": 2}],
        ]
        out, err = StringIO(), StringIO()

        call_command(
            "descargar_sat_mes", "--mes", "2026-01", "--reintentos", "3",
            stdout=out, stderr=err,
        )

        self.assertEqual(procesar.call_count, 4)
        self.assertIn("emitidos", err.getvalue())
        self.assertIn("Periodos fallidos", err.getvalue())
        self.assertIn("2 CFDIs nuevos", out.getvalue())
