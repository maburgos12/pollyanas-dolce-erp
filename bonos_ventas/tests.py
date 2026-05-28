import json
from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import Client, TestCase, override_settings

from core.access import ROLE_RRHH, ROLE_VENTAS
from core.navigation import NAV_GROUPS
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from rrhh.models import Empleado, NominaLinea, NominaPeriodo, PermisoSalida

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, VentaCategoriaSucursal
from .services import sync_ventas_categorias


@override_settings(SECURE_SSL_REDIRECT=False)
class BonosVentasTests(TestCase):
    def test_sidebar_de_ventas_apunta_al_dashboard_erp(self):
        comercial = next(group for group in NAV_GROUPS if group["key"] == "comercial")
        bonos_item = next(item for item in comercial["items"] if item[0] == "ventas" and item[1] == "bonos")

        self.assertEqual(bonos_item[3], "/bonos-ventas/dashboard/")

    def test_dashboard_erp_muestra_configuracion_y_app_de_captura(self):
        user = get_user_model().objects.create_superuser(username="admin-bonos-ventas", password="x")
        self.client.force_login(user)
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleada Dashboard Ventas", area="VENTAS", sucursal="Payán")
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)
        BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)

        response = self.client.get("/bonos-ventas/dashboard/?mes=5&anio=2026")

        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        self.assertIn("Control de configuración y ajustes", content)
        self.assertIn("Abrir app de captura", content)
        self.assertIn("Bono ventas adicional", content)
        self.assertIn("Buscar empleada por nombre", content)
        self.assertIn("Empleada Dashboard Ventas", content)
        self.assertEqual(response["Cache-Control"], "max-age=0, no-cache, no-store, must-revalidate, private")

    def test_raiz_web_de_bonos_ventas_redirige_al_dashboard(self):
        response = self.client.get("/bonos-ventas/")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/bonos-ventas/dashboard/")

    def test_app_desktop_redirige_al_dashboard_y_captura_forzada_funciona(self):
        user = get_user_model().objects.create_superuser(username="desktop-ventas")
        self.client.force_login(user)

        redirected = self.client.get("/bonos-ventas/app/", HTTP_USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X)")
        forced = self.client.get("/bonos-ventas/app/?captura=1", HTTP_USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X)")

        self.assertEqual(redirected.status_code, 302)
        self.assertEqual(redirected["Location"], "/bonos-ventas/dashboard/")
        self.assertEqual(forced.status_code, 200)

    def test_dashboard_erp_actualiza_configuracion_del_periodo(self):
        user = get_user_model().objects.create_superuser(username="admin-config-ventas", password="x")
        self.client.force_login(user)
        ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)

        response = self.client.post(
            "/bonos-ventas/dashboard/",
            {
                "action": "config",
                "mes": "5",
                "anio": "2026",
                "dias_laborables": "24",
                "bono_base": "350.00",
                "pct_uniforme": "15.00",
                "pct_asistencia": "40.00",
                "pct_puntualidad": "20.00",
                "limite_uniforme": "1",
                "limite_asistencia": "2",
                "limite_puntualidad": "1",
                "bono_ventas_adicional": "500.00",
                "umbral_crecimiento_pct": "6.00",
                "peso_grande": "20.00",
                "peso_mediano": "30.00",
                "peso_chico": "20.00",
                "peso_mini": "15.00",
                "peso_velas_accesorios": "5.00",
                "peso_vasos": "10.00",
            },
        )

        self.assertEqual(response.status_code, 302)
        periodo = ConfigBonoVentasPeriodo.objects.get(mes=5, anio=2026)
        self.assertEqual(periodo.dias_laborables, 24)
        self.assertEqual(periodo.bono_base, Decimal("350.00"))
        self.assertEqual(periodo.pct_asistencia, Decimal("40.00"))
        self.assertEqual(periodo.bono_ventas_adicional, Decimal("500.00"))
        self.assertEqual(periodo.umbral_crecimiento_pct, Decimal("6.00"))

    def test_pwa_usa_sesion_django_y_expone_csrf(self):
        user = get_user_model().objects.create_superuser(username="pwa-ventas")
        self.client.force_login(user)

        response = self.client.get("/bonos-ventas/app/?captura=1")

        self.assertEqual(response.status_code, 200)
        self.assertIn("csrftoken", response.cookies)
        content = response.content.decode()
        self.assertIn("credentials:'same-origin'", content)
        self.assertIn("/bonos-ventas/manifest.json", content)
        self.assertIn("/static/bonos_ventas/icons/apple-touch-icon-180.png?v=20260521", content)
        self.assertIn("/bonos-ventas/sw.js", content)
        self.assertIn("href:'/logout/'", content)
        self.assertIn("Cerrar sesión", content)
        self.assertIn("employee-search", content)
        self.assertIn("Teclea nombre o apellido", content)
        self.assertIn("Vista previa tamaño carta", content)
        self.assertIn("Imprimir / PDF", content)
        self.assertIn("Permiso ${lastPermiso.folio} registrado", content)
        self.assertIn("Imprimir / guardar PDF", content)
        self.assertIn("ReactDOM.createPortal", content)
        self.assertIn("body > :not(.print-modal)", content)
        self.assertIn("transform:none!important", content)
        self.assertIn("Firma empleado", content)
        self.assertNotIn("pointer-events:none", content)
        self.assertNotIn("if(!dom)", content)
        self.assertIn(".day-cell.dom.worked", content)
        self.assertIn(".day-cell.saving", content)
        self.assertIn("savingDia", content)
        self.assertIn("const nextWorked=!(r&&r.tiene_asistencia);", content)
        self.assertIn("{tiene_asistencia:nextWorked,tiene_uniforme:nextWorked,tiene_puntualidad:nextWorked}", content)
        self.assertIn("onClick:()=>togDia(d)", content)
        self.assertNotIn("togDia(d); setSelDia(d);", content)
        self.assertNotIn("pd_logistica_access", content)

    def test_manifest_y_service_worker_de_ventas_sirven_con_content_type_correcto(self):
        manifest = self.client.get("/bonos-ventas/manifest.json")
        sw = self.client.get("/bonos-ventas/sw.js")

        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest["Content-Type"], "application/manifest+json")
        self.assertEqual(manifest.json()["start_url"], "/bonos-ventas/app/?captura=1")
        self.assertIn(
            "/static/bonos_ventas/icons/apple-touch-icon-180.png",
            [icon["src"] for icon in manifest.json()["icons"]],
        )
        self.assertEqual(sw.status_code, 200)
        self.assertIn("application/javascript", sw["Content-Type"])
        sw_content = sw.content.decode()
        self.assertIn("pollyanas-bonos-ventas-pwa-v3", sw_content)
        self.assertIn('url.pathname.startsWith("/bonos-ventas/dashboard/")', sw_content)

    def test_api_ventas_acepta_post_con_sesion_y_csrf(self):
        client = Client(enforce_csrf_checks=True)
        user = get_user_model().objects.create_superuser(username="csrf-ventas")
        client.force_login(user)
        client.get("/bonos-ventas/app/?captura=1")
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
        Empleado.objects.all().delete()
        user = get_user_model().objects.create_user(username="bonos")
        user.groups.add(Group.objects.create(name=ROLE_VENTAS))
        user.groups.add(Group.objects.create(name=ROLE_RRHH))
        self.client.force_login(user)
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)
        con_sucursal = Empleado.objects.create(nombre="Empleado Con Sucursal", area="VENTAS", sucursal="Payán")
        repartidor = Empleado.objects.create(nombre="Repartidor Con Sucursal", area="REPARTIDOR", sucursal="Payán")
        sin_sucursal = Empleado.objects.create(nombre="Empleado Sin Sucursal", area="VENTAS", sucursal="")
        Empleado.objects.create(nombre="Empleado Hornos", area="HORNOS", sucursal="Payán")
        Empleado.objects.create(nombre="Empleado Inactivo", area="VENTAS", sucursal="Payán", activo=False)

        response = self.client.post(f"/api/bonos-ventas/periodos/{periodo.id}/inicializar-bonos/")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["creados"], 2)
        self.assertEqual(payload["total_ventas"], 3)
        self.assertEqual(payload["sin_sucursal"], [sin_sucursal.nombre])
        self.assertTrue(BonoVentasEmpleado.objects.filter(periodo=periodo, empleado=con_sucursal, sucursal=sucursal).exists())
        self.assertTrue(BonoVentasEmpleado.objects.filter(periodo=periodo, empleado=repartidor, sucursal=sucursal).exists())

    def test_permisos_equipo_ventas_crea_y_preautoriza(self):
        Empleado.objects.all().delete()
        user = get_user_model().objects.create_user(username="jefe-ventas")
        user.groups.add(Group.objects.create(name=ROLE_VENTAS))
        user.groups.add(Group.objects.create(name=ROLE_RRHH))
        self.client.force_login(user)
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)
        jefe = Empleado.objects.create(nombre="Jefe Ventas", departamento=Empleado.DEP_VENTAS, usuario_erp=user)
        empleado = Empleado.objects.create(
            nombre="Empleado Ventas Permiso",
            area="VENTAS",
            sucursal="Payán",
            jefe_directo=jefe,
        )
        repartidor = Empleado.objects.create(
            nombre="Empleado Repartidor Permiso",
            area="REPARTIDOR",
            sucursal="Payán",
            jefe_directo=jefe,
        )
        Empleado.objects.create(nombre="Empleado Ventas Fuera Periodo", area="VENTAS", sucursal="Payán")
        Empleado.objects.create(nombre="Empleado Produccion", area="HORNOS")
        BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)
        BonoVentasEmpleado.objects.create(periodo=periodo, empleado=repartidor, sucursal=sucursal)

        listado = self.client.get(f"/api/bonos-ventas/permisos/?mes=5&anio=2026&sucursal={sucursal.id}")

        self.assertEqual(listado.status_code, 200)
        empleados_payload = listado.json()["empleados"]
        self.assertEqual([row["id"] for row in empleados_payload], [repartidor.id, empleado.id])
        self.assertEqual({row["sucursal_nombre"] for row in empleados_payload}, {"Payán"})

        creado = self.client.post(
            "/api/bonos-ventas/permisos/",
            json.dumps(
                {
                    "empleado": empleado.id,
                    "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                    "fecha_inicio": "2026-05-20T13:00:00",
                    "fecha_fin": "2026-05-20T15:00:00",
                    "goce_sueldo": "false",
                    "motivo": "Cita medica",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(creado.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=creado.json()["id"])
        self.assertEqual(permiso.origen_solicitud, PermisoSalida.ORIGEN_BONOS_VENTAS)
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PENDIENTE)
        self.assertFalse(permiso.goce_sueldo)
        self.assertTrue(creado.json()["puede_preautorizar"])

        preautorizado = self.client.post(f"/api/bonos-ventas/permisos/{permiso.id}/preautorizar/")

        self.assertEqual(preautorizado.status_code, 200)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PREAUTORIZADO)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertEqual(permiso.autorizado_jefe_por, user)
