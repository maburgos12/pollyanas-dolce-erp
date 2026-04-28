from __future__ import annotations

import csv
import tempfile
from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from openpyxl import Workbook
from rest_framework.test import APIClient

from reportes.models import AreaPresupuesto, EmpresaResultadoMensual, LineaPresupuestoMensual, RubroPresupuesto
from reportes.services_presupuesto_maestro import PresupuestoMaestroImportService, PresupuestoMaestroService, seed_capex_guamuchil_2026


class PresupuestoMaestroTests(TestCase):
    def _csv_file(self, rows):
        tmp = tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False, encoding="utf-8")
        writer = csv.DictWriter(
            tmp,
            fieldnames=[
                "concepto",
                "tipo",
                "sucursal",
                "enero",
                "febrero",
                "marzo",
                "abril",
                "mayo",
                "junio",
                "julio",
                "agosto",
                "septiembre",
                "octubre",
                "noviembre",
                "diciembre",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
        tmp.close()
        return tmp.name

    def _sales_xlsx_file(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        tmp.close()
        wb = Workbook()
        ws = wb.active
        ws.title = "GENERAL"
        ws.append(["Producto", "enero", None, None, None, None, None, None, None, None, "febrero", None, None, None, None, None, None, None, None, "marzo", None, None, None, None, None, None, None, None])
        ws.append([None, "2024", None, "2025", None, "PROYECCIÓN 2026", None, "RESULTADO 2026", None, None, "2024", None, "2025", None, "PROYECCIÓN 2026", None, "RESULTADO 2026", None, None, "2024", None, "2025", None, "PROYECCIÓN 2026", None, "RESULTADO 2026", None])
        ws.append([None, "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA", "DIF", "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA", "DIF", "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA"])
        ws.append(["Pay de Queso Grande", 1, 100, 2, 200, 4, 1200, 3, 900, 0, 5, 500, 6, 600, 7, 2100, 8, 2400, 0, 9, 900, 10, 1000, 11, 3300, 12, 3600])
        ws.append(["TOTAL VENTAS", 1, 100, 2, 200, 4, 1200, 3, 900, 0, 5, 500, 6, 600, 7, 2100, 8, 2400, 0, 9, 900, 10, 1000, 11, 3300, 12, 3600])
        branch = wb.create_sheet("Sucursal 1")
        branch.append(["Producto", "enero", None, None, None, None, None, None, None])
        branch.append([None, "2024", None, "2025", None, "PROYECCIÓN 2026", None, "RESULTADO 2026", None])
        branch.append([None, "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA", "CANT", "VENTA"])
        branch.append(["No debe duplicar", 1, 100, 2, 200, 4, 9999, 3, 8888])
        wb.save(tmp.name)
        return tmp.name

    def test_importar_presupuesto_es_idempotente(self):
        path = self._csv_file(
            [
                {
                    "concepto": "Pay de Queso Grande",
                    "tipo": "INGRESO",
                    "sucursal": "",
                    "enero": "100.00",
                    "febrero": "200.00",
                    "marzo": "0",
                    "abril": "0",
                    "mayo": "0",
                    "junio": "0",
                    "julio": "0",
                    "agosto": "0",
                    "septiembre": "0",
                    "octubre": "0",
                    "noviembre": "0",
                    "diciembre": "0",
                }
            ]
        )

        service = PresupuestoMaestroImportService()
        first = service.import_file(archivo=path, area_code="ventas", version="ORIGINAL", year=2026)
        second = service.import_file(archivo=path, area_code="ventas", version="ORIGINAL", year=2026)

        self.assertEqual(first.lines_created, 12)
        self.assertEqual(second.lines_created, 0)
        self.assertEqual(second.lines_updated, 12)
        self.assertEqual(LineaPresupuestoMensual.objects.count(), 12)
        enero = LineaPresupuestoMensual.objects.get(periodo=date(2026, 1, 1))
        self.assertEqual(enero.monto_presupuesto, Decimal("100.00"))

    def test_consolidado_lee_real_desde_empresa_resultado(self):
        path = self._csv_file(
            [
                {
                    "concepto": "Pay de Queso Grande",
                    "tipo": "INGRESO",
                    "sucursal": "",
                    "enero": "90.00",
                    "febrero": "0",
                    "marzo": "0",
                    "abril": "0",
                    "mayo": "0",
                    "junio": "0",
                    "julio": "0",
                    "agosto": "0",
                    "septiembre": "0",
                    "octubre": "0",
                    "noviembre": "0",
                    "diciembre": "0",
                }
            ]
        )
        PresupuestoMaestroImportService().import_file(archivo=path, area_code="ventas", version="ORIGINAL", year=2026)
        EmpresaResultadoMensual.objects.create(periodo=date(2026, 1, 1), venta_total=Decimal("120.00"))

        summary = PresupuestoMaestroService().build_consolidado(periodo="2026-01", version="ORIGINAL")
        ventas = summary["areas"][0]["rubros"][0]

        self.assertEqual(ventas["real"], Decimal("120.00"))
        self.assertEqual(ventas["fuente_real"], "reportes.EmpresaResultadoMensual")
        self.assertEqual(ventas["varianza"], Decimal("30.00"))

    def test_importador_ventas_toma_venta_proyectada_y_no_cantidad(self):
        path = self._sales_xlsx_file()

        summary = PresupuestoMaestroImportService().import_file(archivo=path, area_code="ventas", version="ORIGINAL", year=2026)

        self.assertEqual(summary.rubros_created, 1)
        rubro = RubroPresupuesto.objects.get(area__codigo="ventas")
        self.assertEqual(rubro.concepto, "Pay de Queso Grande")
        enero = LineaPresupuestoMensual.objects.get(rubro=rubro, periodo=date(2026, 1, 1))
        febrero = LineaPresupuestoMensual.objects.get(rubro=rubro, periodo=date(2026, 2, 1))
        self.assertEqual(enero.monto_presupuesto, Decimal("1200.00"))
        self.assertEqual(enero.monto_real, Decimal("900.00"))
        self.assertEqual(febrero.monto_presupuesto, Decimal("2100.00"))
        self.assertEqual(RubroPresupuesto.objects.filter(concepto="No debe duplicar").count(), 0)

    def test_total_real_de_ventas_no_se_contamina_con_capex(self):
        path = self._csv_file(
            [
                {
                    "concepto": "Pay de Queso Grande",
                    "tipo": "INGRESO",
                    "sucursal": "",
                    "enero": "90.00",
                    "febrero": "0",
                    "marzo": "0",
                    "abril": "0",
                    "mayo": "0",
                    "junio": "0",
                    "julio": "0",
                    "agosto": "0",
                    "septiembre": "0",
                    "octubre": "0",
                    "noviembre": "0",
                    "diciembre": "0",
                }
            ]
        )
        PresupuestoMaestroImportService().import_file(archivo=path, area_code="ventas", version="ORIGINAL", year=2026)
        seed_capex_guamuchil_2026()
        EmpresaResultadoMensual.objects.create(periodo=date(2026, 1, 1), venta_total=Decimal("120.00"))

        summary = PresupuestoMaestroService().build_consolidado(periodo="2026-01", version="ORIGINAL", area="ventas")

        self.assertEqual(summary["totales"]["real"], Decimal("120.00"))

    def test_kpis_todas_usan_solo_ventas_y_real_empresa_resultado(self):
        ventas_path = self._csv_file(
            [
                {
                    "concepto": "Pay de Queso Grande",
                    "tipo": "INGRESO",
                    "sucursal": "",
                    "enero": "4525810.00",
                    "febrero": "4801150.00",
                    "marzo": "4770210.00",
                    "abril": "5000000.00",
                    "mayo": "5000000.00",
                    "junio": "5000000.00",
                    "julio": "5000000.00",
                    "agosto": "5000000.00",
                    "septiembre": "5000000.00",
                    "octubre": "5000000.00",
                    "noviembre": "5000000.00",
                    "diciembre": "7328919.00",
                }
            ]
        )
        admin_path = self._csv_file(
            [
                {
                    "concepto": "Gasto fijo",
                    "tipo": "EGRESO",
                    "sucursal": "",
                    "enero": "1000000.00",
                    "febrero": "0",
                    "marzo": "0",
                    "abril": "0",
                    "mayo": "0",
                    "junio": "0",
                    "julio": "0",
                    "agosto": "0",
                    "septiembre": "0",
                    "octubre": "0",
                    "noviembre": "0",
                    "diciembre": "0",
                }
            ]
        )
        PresupuestoMaestroImportService().import_file(archivo=ventas_path, area_code="ventas", version="ORIGINAL", year=2026)
        PresupuestoMaestroImportService().import_file(archivo=admin_path, area_code="administracion", version="ORIGINAL", year=2026)
        EmpresaResultadoMensual.objects.create(periodo=date(2026, 1, 1), venta_total=Decimal("3519375.91"))

        kpis = PresupuestoMaestroService().executive_kpis(year=2026, month=1, version="ORIGINAL")

        self.assertEqual(kpis["annual_budget"], Decimal("61426089.00"))
        self.assertEqual(kpis["monthly_budget"], Decimal("4525810.00"))
        self.assertEqual(kpis["real_month"], Decimal("3519375.91"))
        self.assertEqual(kpis["variance"], Decimal("-1006434.09"))
        self.assertEqual(kpis["budget_scope"], "ventas_ingreso")

    def test_kpis_area_especifica_usa_presupuesto_con_signo(self):
        admin_path = self._csv_file(
            [
                {
                    "concepto": "Gasto fijo",
                    "tipo": "EGRESO",
                    "sucursal": "",
                    "enero": "100.00",
                    "febrero": "200.00",
                    "marzo": "0",
                    "abril": "0",
                    "mayo": "0",
                    "junio": "0",
                    "julio": "0",
                    "agosto": "0",
                    "septiembre": "0",
                    "octubre": "0",
                    "noviembre": "0",
                    "diciembre": "0",
                }
            ]
        )
        PresupuestoMaestroImportService().import_file(archivo=admin_path, area_code="administracion", version="ORIGINAL", year=2026)
        EmpresaResultadoMensual.objects.create(periodo=date(2026, 1, 1), venta_total=Decimal("120.00"))

        kpis = PresupuestoMaestroService().executive_kpis(year=2026, month=1, version="ORIGINAL", area="administracion")

        self.assertEqual(kpis["annual_budget"], Decimal("-300.00"))
        self.assertEqual(kpis["monthly_budget"], Decimal("-100.00"))
        self.assertEqual(kpis["real_month"], Decimal("120.00"))
        self.assertEqual(kpis["variance"], Decimal("220.00"))
        self.assertEqual(kpis["budget_scope"], "area_signed")

    def test_api_actualiza_celda(self):
        User = get_user_model()
        user = User.objects.create_superuser(username="admin", email="admin@example.com", password="x")
        seed_capex_guamuchil_2026()
        line = LineaPresupuestoMensual.objects.filter(periodo=date(2026, 1, 1)).first()
        client = APIClient()
        client.force_authenticate(user=user)

        response = client.put(
            reverse("api_presupuesto_linea", kwargs={"line_id": line.id}),
            {"monto_presupuesto": "333.22"},
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        line.refresh_from_db()
        self.assertEqual(line.monto_presupuesto, Decimal("333.22"))
        self.assertEqual(AreaPresupuesto.objects.filter(codigo="capex").count(), 1)
        self.assertEqual(RubroPresupuesto.objects.filter(area__codigo="capex").count(), 3)
