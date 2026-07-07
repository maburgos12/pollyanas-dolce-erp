from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from reportes.models import DgOperacionSnapshot
from reportes.services_reporte_diario import construir_y_enviar_reporte_diario

def _decimal(value: str) -> dict:
    """Mismo formato que reportes.services_dg_operacion_snapshot._json_safe
    usa para serializar Decimal en el payload real — el bug original era
    que el reporte no "hidrataba" este envoltorio antes de leer los valores."""
    return {"__type__": "decimal", "value": value}


PAYLOAD_COMPLETO = {
    "point_exec_summary": {
        "latest_sales_amount": _decimal("12345.67"),
        "latest_tickets": 80,
        "latest_avg_ticket": _decimal("154.32"),
        "active_branch_count": 9,
    },
    "resumen_cierre": {
        "detalle": [
            {
                "sucursal": {"__type__": "sucursal", "id": 1, "codigo": "COLOSIO", "nombre": "Colosio", "activa": True},
                "semaforo": "verde",
                "estado_label": "Cerrado",
            },
            {
                "sucursal": {"__type__": "sucursal", "id": 2, "codigo": "CRUCERO", "nombre": "Crucero", "activa": True},
                "semaforo": "rojo",
                "estado_label": "Por validar",
            },
        ]
    },
    "point_waste_summary": {
        "total_qty": _decimal("12.5"),
        "total_cost": _decimal("340.00"),
        "top_branches": [{"branch_name": "Crucero"}],
    },
}


@override_settings(DIRECTOR_EMAIL="director@example.com", DEFAULT_FROM_EMAIL="erp@example.com")
class ReporteDiarioTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="reporte-diario-dg",
            email="reporte-diario-dg@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

    def _snapshot(self, *, status=DgOperacionSnapshot.STATUS_READY, payload=None, fecha=None):
        return DgOperacionSnapshot.objects.create(
            fecha_operacion=fecha or date(2026, 5, 20),
            payload=payload if payload is not None else PAYLOAD_COMPLETO,
            status=status,
        )

    @patch("reportes.services_reporte_diario.send_mail")
    def test_snapshot_listo_envia_correo(self, mock_send_mail):
        self._snapshot()

        resultado = construir_y_enviar_reporte_diario()

        self.assertEqual(resultado["status"], "enviado")
        mock_send_mail.assert_called_once()
        kwargs = mock_send_mail.call_args.kwargs
        self.assertEqual(kwargs["recipient_list"], ["director@example.com"])
        cuerpo = kwargs["message"]
        self.assertIn("Crucero", cuerpo)
        # Regresión: los valores vienen envueltos como {"value":..,"__type__":"decimal"}
        # en el payload real — si no se "hidratan" antes de formatear, todo cae en N/D.
        self.assertNotIn("N/D", cuerpo)
        self.assertIn("$12,345.67", cuerpo)
        self.assertIn("$154.32", cuerpo)
        self.assertIn("Crucero: Por validar", cuerpo)

    @patch("reportes.services_reporte_diario.send_mail")
    def test_snapshot_con_error_no_envia(self, mock_send_mail):
        self._snapshot(status=DgOperacionSnapshot.STATUS_ERROR)

        resultado = construir_y_enviar_reporte_diario()

        self.assertEqual(resultado["status"], "omitido")
        mock_send_mail.assert_not_called()

    @patch("reportes.services_reporte_diario.send_mail")
    def test_sin_snapshot_no_envia(self, mock_send_mail):
        resultado = construir_y_enviar_reporte_diario()

        self.assertEqual(resultado["status"], "omitido")
        mock_send_mail.assert_not_called()

    @override_settings(DIRECTOR_EMAIL="", DEFAULT_FROM_EMAIL="", EMAIL_HOST_USER="")
    @patch("reportes.services_reporte_diario.send_mail")
    def test_sin_destinatario_no_envia(self, mock_send_mail):
        self._snapshot()

        resultado = construir_y_enviar_reporte_diario()

        self.assertEqual(resultado["status"], "omitido")
        self.assertEqual(resultado["reason"], "sin_destinatario")
        mock_send_mail.assert_not_called()

    @patch("reportes.services_reporte_diario.send_mail")
    def test_seccion_ausente_no_inventa_dato(self, mock_send_mail):
        payload_sin_merma = {k: v for k, v in PAYLOAD_COMPLETO.items() if k != "point_waste_summary"}
        self._snapshot(payload=payload_sin_merma)

        resultado = construir_y_enviar_reporte_diario()

        self.assertEqual(resultado["status"], "enviado")
        cuerpo = mock_send_mail.call_args.kwargs["message"]
        self.assertIn("Merma del día: no disponible.", cuerpo)

    @patch("reportes.services_reporte_diario.send_mail")
    def test_fecha_operacion_explicita_usa_ese_snapshot(self, mock_send_mail):
        self._snapshot(fecha=date(2026, 5, 18))
        self._snapshot(fecha=date(2026, 5, 20))

        resultado = construir_y_enviar_reporte_diario(fecha_operacion="2026-05-18")

        self.assertEqual(resultado["fecha_operacion"], "2026-05-18")

    @patch("reportes.services_reporte_diario.send_mail")
    def test_alertas_amarillas_tambien_aparecen_en_cierre(self, mock_send_mail):
        payload_con_tardia = {
            **PAYLOAD_COMPLETO,
            "resumen_cierre": {
                "detalle": [
                    {
                        "sucursal": {"__type__": "sucursal", "id": 1, "codigo": "COLOSIO", "nombre": "Colosio", "activa": True},
                        "semaforo": "amarillo",
                        "estado_label": "Enviada (tardía)",
                    }
                ]
            },
        }
        self._snapshot(payload=payload_con_tardia)

        construir_y_enviar_reporte_diario()

        cuerpo = mock_send_mail.call_args.kwargs["message"]
        self.assertIn("Colosio: Enviada (tardía)", cuerpo)

    @patch("reportes.services_reporte_diario.send_mail")
    def test_merma_fraccional_conserva_decimales(self, mock_send_mail):
        self._snapshot()

        construir_y_enviar_reporte_diario()

        cuerpo = mock_send_mail.call_args.kwargs["message"]
        self.assertIn("Cantidad total: 12.5", cuerpo)

    def test_reporte_diario_view_renders_preview(self):
        self._snapshot()

        response = self.client.get(reverse("reportes:reporte_diario"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Preview del reporte diario")
        self.assertContains(response, "Venta total: $12,345.67")
        self.assertContains(response, "Crucero: Por validar")

    @patch("reportes.views.construir_y_enviar_reporte_diario")
    def test_reporte_diario_view_can_resend_snapshot(self, mock_send):
        self._snapshot(fecha=date(2026, 5, 18))
        mock_send.return_value = {
            "status": "enviado",
            "fecha_operacion": "2026-05-18",
            "recipient": "director@example.com",
        }

        response = self.client.post(
            reverse("reportes:reporte_diario"),
            {"action": "resend", "fecha_operacion": "2026-05-18"},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        mock_send.assert_called_once_with(fecha_operacion="2026-05-18")
        self.assertContains(response, "Reporte diario reenviado para 2026-05-18")
