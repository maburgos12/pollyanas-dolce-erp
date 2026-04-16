from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from django.core.management import CommandError, call_command
from django.test import SimpleTestCase

from core.management.commands.diagnose_erp_runtime_context import TableStatus, _evaluate_context


class DiagnoseErpRuntimeContextTests(SimpleTestCase):
    def test_evaluate_context_flags_missing_critical_tables(self):
        context = {
            "engine": "django.db.backends.postgresql",
            "name": "pollyana_db",
            "table_statuses": [
                TableStatus(name="pos_bridge_daily_sales", exists=False, count=None),
                TableStatus(name="ventas_ventaautoritativapoint", exists=True, count=0),
            ],
        }

        errors, warnings = _evaluate_context(context, require_data=False, required_db_name=None)

        self.assertTrue(errors)
        self.assertTrue(any("Faltan tablas críticas" in error for error in errors))
        self.assertTrue(any("pollyana_db" in warning for warning in warnings))

    def test_evaluate_context_flags_zero_counts_when_require_data(self):
        context = {
            "engine": "django.db.backends.postgresql",
            "name": "pastelerias_chat_native_validate",
            "table_statuses": [
                TableStatus(name="pos_bridge_daily_sales", exists=True, count=0),
                TableStatus(name="ventas_ventaautoritativapoint", exists=True, count=0),
            ],
        }

        errors, warnings = _evaluate_context(context, require_data=True, required_db_name=None)

        self.assertTrue(any("no demuestra actividad defendible" in error for error in errors))
        self.assertEqual(warnings, [])

    def test_command_strict_fails_on_risky_warning(self):
        fake_context = {
            "engine": "django.db.backends.postgresql",
            "name": "pollyana_db",
            "host": "127.0.0.1",
            "port": "5432",
            "user": "postgres",
            "database_url_present": False,
            "database_public_url_present": False,
            "table_statuses": [
                TableStatus(name="pos_bridge_daily_sales", exists=True, count=1),
                TableStatus(name="ventas_ventaautoritativapoint", exists=True, count=1),
                TableStatus(name="orquestacion_orchestrationrun", exists=True, count=1),
                TableStatus(name="recetas_movimientoproductocedis", exists=True, count=1),
            ],
        }

        with patch(
            "core.management.commands.diagnose_erp_runtime_context._collect_context",
            return_value=fake_context,
        ), patch(
            "core.management.commands.diagnose_erp_runtime_context._prefer_public_database_url_if_needed",
            return_value=None,
        ):
            with self.assertRaisesMessage(CommandError, "Contexto DB del ERP riesgoso en modo estricto."):
                call_command("diagnose_erp_runtime_context", "--strict")
