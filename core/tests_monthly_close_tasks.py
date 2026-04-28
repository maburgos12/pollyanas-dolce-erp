from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, override_settings

from core.tasks import cerrar_mes_anterior, verificar_datos_mes
from pos_bridge.models import PointBranch, PointProductionLine
from recetas.models import Receta, VentaHistorica
from reportes.models import EmpresaResultadoMensual, PresupuestoResumenMensual
from reportes.services_budget_vs_actual import BUDGET_VS_ACTUAL_SOURCE


@override_settings(DIRECTOR_EMAIL="director@example.com", DEFAULT_FROM_EMAIL="erp@example.com")
class MonthlyCloseTasksTests(TestCase):
    def test_cerrar_mes_anterior_runs_all_steps_and_sends_summary(self):
        EmpresaResultadoMensual.objects.create(
            periodo=date(2026, 3, 1),
            venta_total=Decimal("3326094.19"),
            utilidad_operativa_total=Decimal("1739359.06"),
        )
        PresupuestoResumenMensual.objects.create(
            period=date(2026, 3, 1),
            tipo=PresupuestoResumenMensual.TIPO_FUENTE,
            fuente_nombre=BUDGET_VS_ACTUAL_SOURCE,
            total_budget=Decimal("790261.53"),
            total_actual=Decimal("1739359.06"),
        )

        with patch("core.tasks.timezone.localdate", return_value=date(2026, 4, 28)), patch(
            "core.tasks.call_command"
        ) as call_command_mock, patch("core.tasks.send_mail", return_value=1) as send_mail_mock:
            result = cerrar_mes_anterior()

        commands = [call.args[0] for call in call_command_mock.call_args_list]
        self.assertEqual(
            commands,
            [
                "snapshot_operating_finance",
                "snapshot_budget_vs_actual",
                "consolidar_mermas",
                "clasificar_devoluciones",
                "generar_consumos_bom",
                "calcular_consumo_insumos",
                "generar_proyeccion",
            ],
        )
        self.assertEqual(result["period"], "2026-03")
        self.assertTrue(result["email_sent"])
        self.assertEqual(result["summary"]["ventas_totales"], "3326094.19")
        send_mail_mock.assert_called_once()

    def test_verificar_datos_mes_alerts_when_month_has_no_data(self):
        with patch("core.tasks.timezone.localdate", return_value=date(2026, 4, 28)), patch(
            "core.tasks.send_mail", return_value=1
        ) as send_mail_mock:
            result = verificar_datos_mes()

        self.assertFalse(result["ok"])
        self.assertEqual(result["period"], "2026-03")
        self.assertEqual(set(result["status"]["missing"]), {"ventas", "produccion"})
        send_mail_mock.assert_called_once()

    def test_verificar_datos_mes_passes_when_sales_and_production_exist(self):
        receta = Receta.objects.create(
            nombre="Producto cierre",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="producto-cierre",
        )
        VentaHistorica.objects.create(receta=receta, fecha=date(2026, 3, 10), cantidad=Decimal("2"))
        branch = PointBranch.objects.create(external_id="PB-CIERRE", name="Matriz")
        PointProductionLine.objects.create(
            branch=branch,
            production_external_id="P-CIERRE",
            detail_external_id="D-CIERRE",
            source_hash="hash-cierre",
            production_date=date(2026, 3, 10),
            item_name="Producto cierre",
            produced_quantity=Decimal("2"),
        )

        with patch("core.tasks.timezone.localdate", return_value=date(2026, 4, 28)), patch(
            "core.tasks.send_mail"
        ) as send_mail_mock:
            result = verificar_datos_mes()

        self.assertTrue(result["ok"])
        self.assertFalse(result["email_sent"])
        send_mail_mock.assert_not_called()
