from __future__ import annotations

import csv
import tempfile
from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
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

    def test_importar_presupuesto_es_idempotente(self):
        path = self._csv_file(
            [
                {
                    "concepto": "Ventas",
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
                    "concepto": "Ventas",
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
