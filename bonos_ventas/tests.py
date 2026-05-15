from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from rrhh.models import Empleado, NominaLinea, NominaPeriodo

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, VentaCategoriaSucursal
from .services import sync_ventas_categorias


class BonosVentasTests(TestCase):
    def test_pwa_usa_sesion_django_y_expone_csrf(self):
        user = get_user_model().objects.create_user(username="pwa-ventas")
        self.client.force_login(user)

        response = self.client.get("/bonos-ventas/app/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("csrftoken", response.cookies)
        content = response.content.decode()
        self.assertIn("credentials:'same-origin'", content)
        self.assertIn("/bonos-ventas/manifest.json", content)
        self.assertIn("/bonos-ventas/sw.js", content)
        self.assertNotIn("pd_logistica_access", content)

    def test_manifest_y_service_worker_de_ventas_sirven_con_content_type_correcto(self):
        manifest = self.client.get("/bonos-ventas/manifest.json")
        sw = self.client.get("/bonos-ventas/sw.js")

        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest["Content-Type"], "application/manifest+json")
        self.assertEqual(manifest.json()["start_url"], "/bonos-ventas/app/")
        self.assertEqual(sw.status_code, 200)
        self.assertIn("application/javascript", sw["Content-Type"])
        self.assertIn("pollyanas-bonos-ventas-pwa", sw.content.decode())

    def test_api_ventas_acepta_post_con_sesion_y_csrf(self):
        client = Client(enforce_csrf_checks=True)
        user = get_user_model().objects.create_user(username="csrf-ventas")
        client.force_login(user)
        client.get("/bonos-ventas/app/")
        csrf_token = client.cookies["csrftoken"].value

        response = client.post(
            "/api/bonos-ventas/periodos/",
            {"mes": 2, "anio": 2098, "dias_laborables": 23},
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf_token,
        )

        self.assertEqual(response.status_code, 201)

    def test_recalcular_presentacion_usa_dias_trabajados_como_base(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
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
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
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

    def test_inicializar_bonos_reporta_empleados_ventas_sin_sucursal(self):
        user = get_user_model().objects.create_user(username="bonos")
        self.client.force_login(user)
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)
        con_sucursal = Empleado.objects.create(nombre="Empleado Con Sucursal", area="VENTAS", sucursal="Payán")
        sin_sucursal = Empleado.objects.create(nombre="Empleado Sin Sucursal", area="VENTAS", sucursal="")

        response = self.client.post(f"/api/bonos-ventas/periodos/{periodo.id}/inicializar-bonos/")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["creados"], 1)
        self.assertEqual(payload["total_ventas"], 2)
        self.assertEqual(payload["sin_sucursal"], [sin_sucursal.nombre])
        self.assertTrue(BonoVentasEmpleado.objects.filter(periodo=periodo, empleado=con_sucursal, sucursal=sucursal).exists())
