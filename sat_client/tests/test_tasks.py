from __future__ import annotations

from datetime import date
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase, override_settings

from sat_client.models import LogDescargaSat, SolicitudDescarga
from sat_client.tasks import _solicitud_periodo_registrada, periodos_diarios_a_descargar
from sat_client.tasks import ejecutar_descarga_sat_nocturna


class SatTaskPeriodTests(SimpleTestCase):
    def test_periodos_diarios_a_descargar_uses_elapsed_days(self):
        periodos = periodos_diarios_a_descargar(1, hoy=date(2026, 6, 8))

        self.assertEqual(
            periodos,
            [
                (date(2026, 6, 1), date(2026, 6, 1)),
                (date(2026, 6, 2), date(2026, 6, 2)),
                (date(2026, 6, 3), date(2026, 6, 3)),
                (date(2026, 6, 4), date(2026, 6, 4)),
                (date(2026, 6, 5), date(2026, 6, 5)),
                (date(2026, 6, 6), date(2026, 6, 6)),
                (date(2026, 6, 7), date(2026, 6, 7)),
            ],
        )

    def test_periodos_diarios_a_descargar_includes_yesterday_on_month_start(self):
        self.assertEqual(
            periodos_diarios_a_descargar(1, hoy=date(2026, 6, 1)),
            [(date(2026, 5, 31), date(2026, 5, 31))],
        )


class SatTaskRegisteredPeriodTests(TestCase):
    @override_settings(SAT_RFC="AAA010101AAA")
    def test_solicitud_periodo_registrada_detects_existing_request(self):
        SolicitudDescarga.objects.create(
            id_solicitud="abc",
            fecha_inicial=date(2026, 6, 7),
            fecha_final=date(2026, 6, 7),
            rfc_solicitante="AAA010101AAA",
            tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
            direccion=SolicitudDescarga.DIRECCION_RECIBIDOS,
            estado=SolicitudDescarga.ESTADO_TERMINADA,
        )

        self.assertTrue(
            _solicitud_periodo_registrada(
                date(2026, 6, 7),
                date(2026, 6, 7),
                SolicitudDescarga.DIRECCION_RECIBIDOS,
            )
        )


class SatTaskRechazoDefinitivoTests(TestCase):
    @override_settings(SAT_RFC="AAA010101AAA")
    def test_rechazo_5004_cuenta_como_registrado(self):
        SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 4, 5),
            fecha_final=date(2026, 4, 5),
            rfc_solicitante="AAA010101AAA",
            tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
            direccion=SolicitudDescarga.DIRECCION_EMITIDOS,
            estado=SolicitudDescarga.ESTADO_RECHAZADA,
            codigo_estado="5004",
        )

        self.assertTrue(
            _solicitud_periodo_registrada(
                date(2026, 4, 5), date(2026, 4, 5), SolicitudDescarga.DIRECCION_EMITIDOS
            )
        )

    @override_settings(SAT_RFC="AAA010101AAA")
    def test_rechazo_con_otro_codigo_no_cuenta(self):
        SolicitudDescarga.objects.create(
            fecha_inicial=date(2026, 4, 5),
            fecha_final=date(2026, 4, 5),
            rfc_solicitante="AAA010101AAA",
            tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
            direccion=SolicitudDescarga.DIRECCION_EMITIDOS,
            estado=SolicitudDescarga.ESTADO_RECHAZADA,
            codigo_estado="300",
        )

        self.assertFalse(
            _solicitud_periodo_registrada(
                date(2026, 4, 5), date(2026, 4, 5), SolicitudDescarga.DIRECCION_EMITIDOS
            )
        )

    @override_settings(SAT_RFC="AAA010101AAA")
    def test_solicitud_mensual_cubre_dias_interiores(self):
        SolicitudDescarga.objects.create(
            id_solicitud="backfill-enero",
            fecha_inicial=date(2026, 1, 1),
            fecha_final=date(2026, 1, 31),
            rfc_solicitante="AAA010101AAA",
            tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
            direccion=SolicitudDescarga.DIRECCION_RECIBIDOS,
            estado=SolicitudDescarga.ESTADO_TERMINADA,
        )

        self.assertTrue(
            _solicitud_periodo_registrada(
                date(2026, 1, 15), date(2026, 1, 15), SolicitudDescarga.DIRECCION_RECIBIDOS
            )
        )


class SatTaskEnabledFlagTests(TestCase):
    @override_settings(SAT_DESCARGA_ENABLED=False)
    def test_task_exits_without_logs_when_disabled(self):
        result = ejecutar_descarga_sat_nocturna.run()

        self.assertEqual(result, {"status": "deshabilitada"})
        self.assertEqual(LogDescargaSat.objects.count(), 0)

    @override_settings(SAT_DESCARGA_ENABLED=True, SAT_DESCARGA_MESES_ATRAS=1, SAT_RFC="AAA010101AAA")
    @patch("sat_client.tasks.periodos_diarios_a_descargar", return_value=[(date(2026, 6, 7), date(2026, 6, 7))])
    @patch("sat_client.tasks._procesar_con_split", return_value=[{"solicitud_id": "new", "descargados": 0, "nuevos": 0}])
    def test_task_skips_registered_daily_request(self, procesar, _periodos):
        SolicitudDescarga.objects.create(
            id_solicitud="emitidos-ya",
            fecha_inicial=date(2026, 6, 7),
            fecha_final=date(2026, 6, 7),
            rfc_solicitante="AAA010101AAA",
            tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
            direccion=SolicitudDescarga.DIRECCION_EMITIDOS,
            estado=SolicitudDescarga.ESTADO_TERMINADA,
        )

        result = ejecutar_descarga_sat_nocturna.run()

        self.assertEqual(result["omitidos"], 1)
        procesar.assert_called_once_with(
            date(2026, 6, 7),
            date(2026, 6, 7),
            SolicitudDescarga.DIRECCION_RECIBIDOS,
        )

    @override_settings(SAT_DESCARGA_ENABLED=True, SAT_DESCARGA_MESES_ATRAS=1, SAT_RFC="AAA010101AAA")
    @patch(
        "sat_client.tasks.periodos_diarios_a_descargar",
        return_value=[(date(2026, 6, 7), date(2026, 6, 7)), (date(2026, 6, 8), date(2026, 6, 8))],
    )
    @patch("sat_client.tasks._alertar_error")
    @patch("sat_client.tasks._procesar_con_split")
    def test_un_periodo_fallido_no_aborta_el_run(self, procesar, alertar, _periodos):
        from sat_client.services.base import SatServiceError

        procesar.side_effect = [
            SatServiceError("Se han agotado las solicitudes de por vida", code="5002"),
            [{"solicitud_id": "ok", "descargados": 3, "nuevos": 3}],
            [{"solicitud_id": "ok2", "descargados": 2, "nuevos": 1}],
            [{"solicitud_id": "ok3", "descargados": 1, "nuevos": 0}],
        ]

        result = ejecutar_descarga_sat_nocturna.run()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["fallidos"], 1)
        self.assertEqual(result["descargados"], 6)
        self.assertEqual(procesar.call_count, 4)
        alertar.assert_called_once()
        self.assertTrue(
            LogDescargaSat.objects.filter(nivel=LogDescargaSat.NIVEL_ERROR).exists()
        )
