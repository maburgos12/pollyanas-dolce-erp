from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.test import TestCase
from openpyxl import Workbook

from core.models import AuditLog, Sucursal
from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from reportes.services_branch_real_operating_expense_import import BranchRealOperatingExpenseImportService
from reportes.services_operating_finance import OperatingFinanceBootstrapService
from reportes.services_operating_finance_io import (
    OperatingFinanceExpenseImportService, validar_reimportacion_gastos_documentados,
)


class GastosDocumentadosImportTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="GUAMUCHIL", nombre="Guamuchil")
        OperatingFinanceBootstrapService().bootstrap()
        self.centro = CentroCosto.objects.get(sucursal=self.sucursal, tipo="SUCURSAL_VENTA")
        self.categoria = CategoriaGasto.objects.get(codigo="RENTA_SUC")
        self.key = "2025-GUAMUCHIL-01-RENTA-REAL-CONSOLIDADO"
        self.gasto = GastoOperativoMensual.objects.create(
            periodo=date(2025, 1, 1), centro_costo=self.centro,
            categoria_gasto=self.categoria, monto=Decimal("20"), external_key=self.key,
        )
        AuditLog.objects.create(action="clasificar_gastos2025_documentados",
            model="reportes.GastoOperativoMensual", object_id="2025",
            payload={"protected_external_keys": [self.key, "2025-GUAMUCHIL-01-RENTA-Arrendamiento"]})

    def workbook(self, path, branch):
        wb = Workbook()
        ws = wb.active
        if branch:
            ws.title = "GastosSucursal"
            ws.append(["sucursal", "periodo", "monto", "tipo_dato", "categoria_gasto", "external_key"])
            ws.append(["GUAMUCHIL", "2025-02-01", 10, "REAL", "RENTA_SUC", "2025-LIBRE"])
            ws.append(["GUAMUCHIL", "2025-01-01", 999, "REAL", "RENTA_SUC", self.key])
        else:
            ws.title = "Gastos"
            ws.append(["external_key", "periodo", "centro_costo", "categoria_gasto", "monto"])
            ws.append(["2025-LIBRE", "2025-02", self.centro.codigo, "RENTA_SUC", 10])
            ws.append([self.key, "2025-01", self.centro.codigo, "RENTA_SUC", 999])
        wb.save(path)

    def test_reimportacion_rechaza_archivo_entero_en_ambas_entradas(self):
        antes = list(GastoOperativoMensual.objects.order_by("pk").values())
        for branch in (False, True):
            with self.subTest(branch=branch), TemporaryDirectory() as tmp:
                path = Path(tmp) / "reimportacion.xlsx"
                self.workbook(path, branch)
                with patch.object(OperatingFinanceBootstrapService, "bootstrap") as bootstrap:
                    with self.assertRaisesMessage(ValueError, "corregidos documentalmente"):
                        if branch:
                            BranchRealOperatingExpenseImportService().import_workbook(
                                path, target_year=2025, refresh_projects=False)
                        else:
                            OperatingFinanceExpenseImportService().import_workbook(path)
                    bootstrap.assert_not_called()
                self.assertEqual(list(GastoOperativoMensual.objects.order_by("pk").values()), antes)

    def test_clave_original_desglosada_tambien_esta_protegida(self):
        with self.assertRaisesMessage(ValueError, "corregidos documentalmente"):
            validar_reimportacion_gastos_documentados(["2025-GUAMUCHIL-01-RENTA-Arrendamiento"])

    def test_auditoria_incompleta_no_libera_reimportacion(self):
        AuditLog.objects.filter(action="clasificar_gastos2025_documentados").update(payload={})
        with self.assertRaisesMessage(ValueError, "auditoría de gastos 2025 está incompleta"):
            validar_reimportacion_gastos_documentados(["2025-LIBRE"])

    def test_claves_ajenas_siguen_disponibles(self):
        validar_reimportacion_gastos_documentados(["2025-LIBRE"])
        with self.assertNumQueries(0):
            validar_reimportacion_gastos_documentados(["2026-GUAMUCHIL-01-RENTA"])
