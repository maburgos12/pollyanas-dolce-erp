from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, override_settings
from openpyxl import Workbook

from recetas.tasks.daily_inventory_close import inventario_final_cierre_email


@override_settings(
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    DEFAULT_FROM_EMAIL="erp@pollyanasdolce.com",
    TIME_ZONE="America/Mazatlan",
)
class InventarioFinalCierreEmailTests(TestCase):
    def test_sends_excel_and_pdf_to_carolina_for_previous_operating_day(self):
        get_user_model().objects.create_user(
            username="carolina.cayetano",
            email="produccion.carolina@pollyanasdolce.com",
        )
        payload = {
            "fecha_operacion": date(2026, 5, 8),
            "timezone_name": "America/Mazatlan",
            "last_capture_at": datetime(2026, 5, 8, 23, 5, tzinfo=ZoneInfo("America/Mazatlan")),
            "rows": [{"stocks": {"MATRIZ": Decimal("1.000")}}],
            "branches": [{"code": "MATRIZ", "name": "Matriz"}],
            "missing_branch_codes": [],
        }
        service = Mock()
        service.build_close.return_value = payload
        service.build_workbook.return_value = Workbook()
        service.build_pdf_bytes.return_value = b"%PDF-1.4 test"

        with patch("recetas.tasks.daily_inventory_close.DailyInventoryCloseService", return_value=service):
            result = inventario_final_cierre_email(fecha_operacion="2026-05-08")

        self.assertEqual(result["status"], "enviado")
        self.assertEqual(result["recipients"], ["produccion.carolina@pollyanasdolce.com"])
        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(email.to, ["produccion.carolina@pollyanasdolce.com"])
        self.assertIn("Inventario final al cierre", email.subject)
        self.assertEqual(len(email.attachments), 2)
        self.assertEqual(email.attachments[0][0], "inventario_final_cierre_2026-05-08.xlsx")
        self.assertEqual(email.attachments[1][0], "inventario_final_cierre_2026-05-08.pdf")
