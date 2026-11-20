"""Exact report semantics without repeating discarded monthly costing."""
from datetime import date
from decimal import Decimal as D
from unittest.mock import patch, Mock
from types import SimpleNamespace

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
from .services_reporte_mensual import build_reporte_mensual_consolidado, _rentabilidad_por_mes, reporte_a_json


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

    def test_unknown_branch_keeps_missing_values_without_inventing_zero(self):
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=[date(2025, 9, 1)]):
            with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()):
                with patch("rentabilidad.services_resultado.registros_del_mes", return_value=[]) as read:
                    report = build_reporte_mensual_consolidado(hasta=self.month, meses=1, sucursal_id=999999)
        row = report["filas"][0]
        self.assertEqual(row["alcance"], "SIN_DATOS")
        self.assertTrue(all(row[key] is None for key in ("ingresos", "nomina_total", "utilidad_neta")))
        self.assertTrue(all(value is None for value in row["yoy"].values()))

    def _filtered_cost_row(self, branch, month):
        zero = month == date(2026, 10, 1)
        factor = 2 if month.year == 2026 else 1
        return SimpleNamespace(
            sucursal_id=branch.pk, sucursal=branch,
            ventas_netas=D("0") if zero else D("100") * factor,
            nomina_directa=D("0") if zero else D("25") * factor,
            utilidad_operativa=D("-1") if zero else D("20") * factor,
            gasto_fijo_total=D("3"), en_perimetro_fuente=True,
            gastos_mensuales={"totales": {"nomina": D("25")}}, central_registrado=D("14"),
        )

    def test_empty_selected_close_does_not_cost_a_base_that_only_produces_none_yoy(self):
        branch = Sucursal.objects.create(codigo="FILTER-EMPTY", nombre="Selección sin reparto")
        previous = date(2025, 9, 1)
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=[previous]):
            with patch("rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()):
                with patch("rentabilidad.services_reporte_mensual._horas_extra_por_mes", return_value={self.month: {branch.pk: D("7.03")}, previous: {branch.pk: D("2.01")}}):
                    with patch("rentabilidad.services_resultado.registros_del_mes", return_value=[self._filtered_cost_row(branch, previous)]) as read:
                        report = build_reporte_mensual_consolidado(hasta=self.month, meses=1, sucursal_id=branch.pk)
        row = report["filas"][0]
        self.assertTrue(all(row[key] is None for key in ("ingresos", "nomina_total", "utilidad_neta")))
        self.assertEqual(row["horas_extra"], D("7.03"))
        self.assertEqual(row["sucursales"][0]["alcance"], "SIN_DATOS")
        self.assertTrue(all(value is None for value in row["yoy"].values()))
        read.assert_not_called()

    def test_filter_pruning_preserves_full_json_csv_with_rich_base_extras_and_real_zero(self):
        import rentabilidad.services_reporte_mensual as service
        from .management.commands.reporte_mensual_consolidado import Command
        branch = Sucursal.objects.create(codigo="FILTER-PARITY", nombre="Selección sin reparto")
        months = [date(year, month, 1) for year in (2025, 2026) for month in range(1, 13)]
        hours = {self.month: {branch.pk: D("7.03")}, date(2025, 9, 1): {branch.pk: D("2.01")}}
        original = service._rentabilidad_por_mes

        def without_cost_pruning(*args, **kwargs):
            # Existing internal option: retain every base, including rich ones.
            kwargs["periodos_salida"] = None
            return original(*args, **kwargs)

        for size in (1, 12):
            for scope in (None, branch.pk):
                with self.subTest(months=size, scope=scope):
                    with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=months), patch(
                        "rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()
                    ), patch("rentabilidad.services_reporte_mensual._horas_extra_por_mes", return_value=hours), patch(
                        "rentabilidad.services_resultado.registros_del_mes", side_effect=lambda month: [self._filtered_cost_row(branch, month)]
                    ) as reader:
                        with patch.object(service, "_rentabilidad_por_mes", side_effect=without_cost_pruning):
                            baseline = service.build_reporte_mensual_consolidado(hasta=date(2026, 10, 1), meses=size, sucursal_id=scope)
                        reader.reset_mock()
                        candidate = service.build_reporte_mensual_consolidado(hasta=date(2026, 10, 1), meses=size, sucursal_id=scope)
                    self.assertEqual(reporte_a_json(candidate), reporte_a_json(baseline))
                    self.assertEqual(Command()._render_csv(candidate, detalle=True), Command()._render_csv(baseline, detalle=True))
                    october = candidate["filas"][-1]
                    self.assertEqual(october["ingresos"], D("0"))
                    self.assertEqual(october["nomina_total"], D("0"))
                    self.assertEqual(october["yoy"]["ingresos"], D("-100"))
                    self.assertTrue({date(2025, 10, 1), date(2026, 10, 1)} <= {c.args[0] for c in reader.call_args_list})

    def test_empty_selected_close_keeps_a_rich_prior_year_when_it_is_an_output_month(self):
        branch = Sucursal.objects.create(codigo="FILTER-OUTPUT", nombre="Selección sin reparto")
        previous = date(2025, 9, 1)
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=[previous]), patch(
            "rentabilidad.services_resultado.leer_cierre_conciliado", return_value=self.closure()
        ), patch("rentabilidad.services_resultado.registros_del_mes", side_effect=lambda month: [self._filtered_cost_row(branch, month)]) as reader:
            report = build_reporte_mensual_consolidado(hasta=self.month, meses=13, sucursal_id=branch.pk)
        self.assertEqual(report["filas"][0]["periodo"], previous)
        self.assertEqual(report["filas"][0]["ingresos"], D("100"))
        self.assertIn(previous, {c.args[0] for c in reader.call_args_list})
        self.assertTrue(all(value is None for value in report["filas"][-1]["yoy"].values()))

    def test_absent_close_keeps_the_selected_valid_base_and_comparable_yoy(self):
        branch = Sucursal.objects.create(codigo="FILTER-OPEN", nombre="Selección con datos")
        previous = date(2025, 9, 1)
        with patch("ventas.services.sales_read_service.point_sales_evidence_months", return_value=[previous, self.month]), patch(
            "rentabilidad.services_resultado.leer_cierre_conciliado", return_value=None
        ), patch("rentabilidad.services_resultado.registros_del_mes", side_effect=lambda month: [self._filtered_cost_row(branch, month)]) as reader:
            report = build_reporte_mensual_consolidado(hasta=self.month, meses=1, sucursal_id=branch.pk)
        self.assertEqual(report["filas"][0]["yoy"]["ingresos"], D("100"))
        self.assertEqual(report["filas"][0]["yoy"]["nomina_total"], D("100"))
        self.assertIn(previous, {c.args[0] for c in reader.call_args_list})
