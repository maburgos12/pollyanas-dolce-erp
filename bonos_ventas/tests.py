from datetime import date
from decimal import Decimal

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from rrhh.models import Empleado, NominaLinea, NominaPeriodo

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, VentaCategoriaSucursal
from .services import sync_ventas_categorias


class BonosVentasTests(TestCase):
    def test_recalcular_presentacion_usa_dias_trabajados_como_base(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", sucursal="Payán", area="Ventas")
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            sucursal=sucursal,
            dias_trabajados=15,
            dias_uniforme=15,
            dias_puntualidad=15,
        )

        bono.recalcular()

        self.assertTrue(bono.pasa_asistencia)
        self.assertEqual(bono.sub1, Decimal("225.00"))

    def test_sync_pos_bridge_agrupa_por_branch_erp_y_categoria_producto(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        branch = PointBranch.objects.create(external_id="1", name="Payán", erp_branch=sucursal)
        product = PointProduct.objects.create(external_id="G1", name="Pastel Grande", category="Grande")
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026, umbral_crecimiento_pct=Decimal("5.00"))
        PointDailySale.objects.create(branch=branch, product=product, sale_date=date(2026, 5, 2), quantity=Decimal("21.000"))
        PointDailySale.objects.create(branch=branch, product=product, sale_date=date(2025, 5, 2), quantity=Decimal("10.000"))

        created = sync_ventas_categorias(periodo)

        self.assertEqual(created, 1)
        venta = VentaCategoriaSucursal.objects.get(periodo=periodo, sucursal=sucursal, categoria="GRANDE")
        self.assertEqual(venta.cantidad_actual, Decimal("21.000"))
        self.assertEqual(venta.cantidad_anterior, Decimal("10.000"))
        self.assertTrue(venta.activo_bono)

    def test_aplicar_a_nomina_escribe_total_en_linea_bonos(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", sucursal="Payán", area="Ventas")
        periodo_bono = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)
        nomina = NominaPeriodo.objects.create(fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 31))
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo_bono,
            empleado=empleado,
            sucursal=sucursal,
            dias_trabajados=23,
            dias_uniforme=23,
            dias_puntualidad=23,
            bono_extra=Decimal("50.00"),
        )
        bono.recalcular()
        bono.save()

        updated = periodo_bono.aplicar_a_nomina(nomina)

        self.assertEqual(updated, 1)
        linea = NominaLinea.objects.get(periodo=nomina, empleado=empleado)
        self.assertEqual(linea.bonos, Decimal("275.00"))
