from tempfile import NamedTemporaryFile

from django.test import SimpleTestCase
from openpyxl import Workbook

from rrhh.management.commands.importar_saldo_inicial_vacaciones import read_rows


class ImportarSaldoInicialVacacionesTests(SimpleTestCase):
    def test_read_rows_parsea_saldo_ciclo_y_goce(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "Saldo inicial"
        ws.append(
            [
                "Empleado",
                "Último aniversario cumplido",
                "Días saldo ciclo ERP",
                "Días periodo anterior/vencido",
                "¿Saldo confirmado por RRHH?",
                "Tratamiento para ERP",
            ]
        )
        ws.append(["CAYETANO VALENZUELA CAROLINA", "2026-03-07", 18, 7, "Si", "goce anterior"])
        with NamedTemporaryFile(suffix=".xlsx") as tmp:
            wb.save(tmp.name)
            rows = read_rows(tmp.name, 2026)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].empleado_nombre, "CAYETANO VALENZUELA CAROLINA")
        self.assertEqual(rows[0].saldo_ciclo, 18)
        self.assertEqual(rows[0].goce_anterior, 7)
        self.assertTrue(rows[0].confirmado)
