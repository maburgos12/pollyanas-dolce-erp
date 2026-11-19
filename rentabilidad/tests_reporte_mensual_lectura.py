"""Exact report semantics without repeating discarded monthly costing."""
from datetime import date
from decimal import Decimal as D
from unittest.mock import patch, Mock

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from control.models import MermaMensualSucursal
from pos_bridge.models import PointBranch, PointProduct, PointDailySale
from reportes.models import AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual
from rrhh.models import Empleado, NominaLinea, NominaPeriodo
from .models_rentabilidad import SucursalRentabilidad
from .services_resultado import registros_del_mes
from .services_reporte_mensual import build_reporte_mensual_consolidado, _rentabilidad_por_mes


class MonthlyReadTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.month = date(2026, 9, 1)
        cls.branch = Sucursal.objects.create(codigo="READ", nombre="Sucursal de lectura")
        cls.point = PointBranch.objects.create(external_id="READ", name="Lectura", erp_branch=cls.branch)
        cls.product = PointProduct.objects.create(external_id="READ", sku="READ", name="Producto")
        PointDailySale.objects.create(branch=cls.point, product=cls.product, sale_date=cls.month,
            quantity=1, gross_amount=100, total_amount=100,
            source_endpoint="/Report/PrintReportes?idreporte=3")
        period = NominaPeriodo.objects.create(fecha_inicio=cls.month, fecha_fin=date(2026, 9, 15), estatus="PAGADA")
        employee = Empleado.objects.create(nombre="Fixture", codigo="READ", departamento="VENTAS", sucursal_ref=cls.branch)
        NominaLinea.objects.create(periodo=period, empleado=employee, salario_base=10, bonos=2)

    def closure(self, branches=None):
        return {"errores": [], "detalle": [dict(mes=9, sucursal_id=b.pk, nombre=b.nombre,
            ventas=D("100"), costos=D("20"), produccion=D("5"), gasto_directo=D("10"),
            administracion=D("3"), logistica=D("2"), utilidad=D("60"), combustible=D("1"),
            etapa="Operativa") for b in (branches or [self.branch])]}

    def test_closed_month_never_costs_amounts_that_will_be_discarded(self):
        with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()):
            with patch("rentabilidad.services_resultado.registros_del_mes", side_effect=AssertionError("discarded costing")):
                result = _rentabilidad_por_mes(self.month, self.month, None)
        self.assertEqual(result[self.month][self.branch.pk]["nomina"], D("12.00"))
        self.assertEqual(result[self.month][self.branch.pk]["ingresos"], D("100"))
        self.assertEqual(result[self.month][self.branch.pk]["alcance"], "REPARTO_CONCILIADO")

    def test_closed_payroll_matches_full_reader_and_does_not_write(self):
        full = registros_del_mes(self.month)
        expected = {r.sucursal_id: (r.nomina_directa if
            {"nomina", "cargas_patronales"} & set(r.gastos_mensuales["totales"]) else None) for r in full}
        with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()):
            with CaptureQueriesContext(connection) as queries:
                result = _rentabilidad_por_mes(self.month, self.month, None)
        self.assertEqual(result[self.month][self.branch.pk]["nomina"], expected[self.branch.pk])
        self.assertFalse([q for q in queries if q["sql"].lstrip().split()[0] in {"INSERT", "UPDATE", "DELETE"}])

    def test_one_output_month_only_reads_that_month_and_its_yoy(self):
        months = [date(2025, 10, 1)] + [date(2026, m, 1) for m in range(1, 11)]
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=months):
            with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=None):
                with patch("rentabilidad.services_resultado.registros_del_mes", return_value=[]) as read:
                    report = build_reporte_mensual_consolidado(hasta=date(2026, 10, 1), meses=1)
        self.assertEqual({c.args[0] for c in read.call_args_list}, {date(2025, 10, 1), date(2026, 10, 1)})
        self.assertEqual(report["meses"], 1)
        self.assertEqual(report["filas"][0]["alcance"], "SIN_DATOS")

    def test_payroll_perimeter_zero_missing_inactive_and_actual_sua_match_full_reader(self):
        branches = [self.branch]
        period = NominaPeriodo.objects.first()
        cases = [("SALE-NONE", True, False, "sale", None),
                 ("SALE-ZERO", True, False, "sale", 0),
                 ("WASTE", False, False, "waste", 7),
                 ("PAY-ONLY", True, True, None, 7),
                 ("NO-ROW", True, False, None, 7),
                 ("INACTIVE", False, True, None, 7),
                 ("OLD", True, True, None, None)]
        for code, active, stored, source, payroll in cases:
            branch = Sucursal.objects.create(codigo=code, nombre=code, activa=active)
            branches.append(branch)
            if stored:
                SucursalRentabilidad.objects.create(sucursal=branch, periodo=self.month, nomina_directa=999)
            if source == "sale":
                point = PointBranch.objects.create(external_id=code, name=code, erp_branch=branch)
                PointDailySale.objects.create(branch=point, product=self.product, sale_date=self.month,
                    quantity=0, source_endpoint="/Report/PrintReportes?idreporte=3")
            if source == "waste":
                MermaMensualSucursal.objects.create(sucursal=branch, periodo=self.month,
                    nombre_producto="Lectura incluso cero", unidades_merma=0, costo_merma=0)
            if payroll is not None:
                employee = Empleado.objects.create(codigo=code, nombre="Fixture", departamento="VENTAS", sucursal_ref=branch)
                NominaLinea.objects.create(periodo=period, empleado=employee, salario_base=payroll)
        area = AreaPresupuesto.objects.create(codigo="gastos-venta", nombre="Personal")
        rubro = RubroPresupuesto.objects.create(area=area, sucursal=self.branch, concepto="Infonavit-RCV", tipo="EGRESO")
        LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=self.month,
            monto_real=D("50.01"), fuente_real="AUTO:SIPARE",
            metadata={"cedula_imss": {"tipo": "BIMESTRAL", "registro_patronal": "E52-40157-10-0"}})
        expected = {r.sucursal_id: (r.nomina_directa if
            {"nomina", "cargas_patronales"} & set(r.gastos_mensuales["totales"]) else None)
            for r in registros_del_mes(self.month)}
        with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure(branches)):
            result = _rentabilidad_por_mes(self.month, self.month, None)[self.month]
        self.assertEqual({sid: row["nomina"] for sid, row in result.items()},
                         {b.pk: expected.get(b.pk) for b in branches})
        self.assertEqual(result[self.branch.pk]["nomina"], D("62.01"))
        self.assertEqual(result[branches[2].pk]["nomina"], D("0"))
        self.assertIsNone(result[branches[1].pk]["nomina"])

    def test_invalid_closure_still_rejects_before_any_costing(self):
        close = self.closure()
        close["errores"] = ["fuente desactualizada"]
        with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=close):
            with self.assertRaisesMessage(ValueError, "fuente desactualizada"):
                build_reporte_mensual_consolidado(hasta=date(2026, 10, 1), meses=1)

    def test_absent_close_keeps_original_costing_and_default_twenty_four_month_scope(self):
        months = [date(year, month, 1) for year in (2025, 2026) for month in range(1, 13)]
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=months):
            with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=None):
                with patch("rentabilidad.services_resultado.registros_del_mes", return_value=[]) as read:
                    report = build_reporte_mensual_consolidado(hasta=date(2026, 12, 1))
        self.assertEqual({c.args[0] for c in read.call_args_list}, set(months))
        self.assertEqual(len(report["filas"]), 12)

    def test_csv_serializes_the_report_once_without_rebuilding(self):
        from .management.commands.reporte_mensual_consolidado import Command
        with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=None):
            report = build_reporte_mensual_consolidado(hasta=self.month)
        expected = Command()._render_csv(report, detalle=True)
        user = get_user_model().objects.create_superuser("read-report", password="test-only")
        self.client.force_login(user)
        builder = Mock(return_value=report)
        with patch("rentabilidad.services_reporte_mensual.build_reporte_mensual_consolidado", builder):
            with patch("rentabilidad.management.commands.reporte_mensual_consolidado.build_reporte_mensual_consolidado", builder):
                response = self.client.get("/rentabilidad/reporte-mensual/", {"hasta": "2026-09", "formato": "csv"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content.decode(), expected)
        self.assertEqual(builder.call_count, 1)

    def test_only_incompatible_prior_year_outside_output_can_skip_costing(self):
        months = [date(2025, m, 1) for m in (9, 10, 11, 12)]
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=months):
            with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()):
                with patch("rentabilidad.services_resultado.registros_del_mes", return_value=[]) as read:
                    report = build_reporte_mensual_consolidado(hasta=date(2026, 10, 1))
        called = {c.args[0] for c in read.call_args_list}
        self.assertNotIn(date(2025, 9, 1), called)
        self.assertTrue(set(months[1:]) <= called)
        september = next(f for f in report["filas"] if f["periodo"] == self.month)
        self.assertIsNone(september["yoy"]["ingresos"])
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=months):
            with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()):
                with patch("rentabilidad.services_resultado.registros_del_mes", return_value=[]) as read:
                    build_reporte_mensual_consolidado(hasta=self.month, meses=13)
        self.assertIn(date(2025, 9, 1), {c.args[0] for c in read.call_args_list})

    def test_closure_without_selected_branch_rows_cannot_skip_yoy_costing(self):
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=[date(2025, 9, 1)]):
            with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()):
                with patch("rentabilidad.services_resultado.registros_del_mes", return_value=[]) as read:
                    build_reporte_mensual_consolidado(hasta=self.month, meses=1, sucursal_id=999999)
        self.assertIn(date(2025, 9, 1), {c.args[0] for c in read.call_args_list})
