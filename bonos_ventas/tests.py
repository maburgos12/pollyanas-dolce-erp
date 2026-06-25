import json
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import Client, TestCase, override_settings

from core.access import ROLE_RRHH, ROLE_VENTAS
from core.navigation import NAV_GROUPS
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from rrhh.models import Empleado, HoraExtra, NominaLinea, NominaPeriodo, PermisoSalida

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, RegistroDiarioVentas, VentaCategoriaSucursal
from .services import sync_ventas_categorias
from .views import _recalcular_desde_registros


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
        self.assertIn("label:'Sucursal',value:contextLabel", content)
        self.assertNotIn("Monto calculado", content)
        self.assertIn("grid-template-columns:repeat(2,minmax(0,1fr))", content)
        self.assertIn("margin:28px auto 0", content)
        self.assertIn("Permiso ${lastPermiso.folio} registrado", content)
        self.assertIn("Imprimir / guardar PDF", content)
        self.assertIn("Sincronizar repartidores", content)
        self.assertIn("/api/bonos-ventas/ventas-categoria/sync-repartidores/", content)
        self.assertIn("const REPARTIDORES_KEY = 'REPARTIDORES';", content)
        self.assertIn("grupoVentasKey", content)
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
        self.assertIn("onClick:()=>togDia(dia)", content)
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
        self.assertIn("pollyanas-bonos-ventas-pwa-v16", sw_content)
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

    def test_recalcular_periodo_parcial_no_castiga_dias_futuros(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=6,
            anio=2026,
            dias_laborables=31,
            fecha_inicio=date(2026, 5, 28),
            fecha_fin=date(2026, 6, 27),
        )
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            sucursal=sucursal,
            dias_trabajados=10,
            dias_asistencia=10,
            dias_uniforme=10,
            dias_puntualidad=10,
        )

        with patch("bonos_ventas.models.timezone.localdate", return_value=date(2026, 6, 11)):
            bono.recalcular()

        self.assertTrue(bono.pasa_asistencia)
        self.assertEqual(bono.sub1, Decimal("225.00"))

    def test_una_falta_cancela_bono_ventas_con_limite_cero(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=5,
            anio=2026,
            dias_laborables=23,
            limite_asistencia=0,
            pct_uniforme=Decimal("15.00"),
            pct_asistencia=Decimal("45.00"),
            pct_puntualidad=Decimal("40.00"),
        )
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            sucursal=sucursal,
            dias_trabajados=22,
            dias_asistencia=22,
            dias_uniforme=22,
            dias_puntualidad=22,
        )

        bono.recalcular()

        self.assertFalse(bono.pasa_asistencia)
        self.assertEqual(bono.sub1, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_falta_superior_al_limite_cancela_bono_ventas_completo(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=5,
            anio=2026,
            dias_laborables=23,
            limite_asistencia=0,
            pct_uniforme=Decimal("15.00"),
            pct_asistencia=Decimal("45.00"),
            pct_puntualidad=Decimal("40.00"),
        )
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            sucursal=sucursal,
            dias_trabajados=23,
            dias_asistencia=22,
            dias_uniforme=23,
            dias_puntualidad=23,
        )

        bono.recalcular()

        self.assertFalse(bono.pasa_asistencia)
        self.assertEqual(bono.sub1, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_tres_retardos_cancelan_bono_ventas_completo(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=5,
            anio=2026,
            dias_laborables=23,
            limite_puntualidad=2,
            pct_uniforme=Decimal("15.00"),
            pct_asistencia=Decimal("45.00"),
            pct_puntualidad=Decimal("40.00"),
        )
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            sucursal=sucursal,
            dias_trabajados=23,
            dias_asistencia=23,
            dias_uniforme=23,
            dias_puntualidad=20,
        )

        bono.recalcular()

        self.assertFalse(bono.pasa_puntualidad)
        self.assertEqual(bono.sub1, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_regla_cancelacion_personalizada_cancela_bono_con_una_falta(self):
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=5,
            anio=2026,
            dias_laborables=23,
            limite_asistencia=2,
            limite_puntualidad=2,
            pct_uniforme=Decimal("15.00"),
            pct_asistencia=Decimal("45.00"),
            pct_puntualidad=Decimal("40.00"),
        )
        periodo.cancela_por_asistencia = True
        periodo.limite_asistencia_cancelacion = 0
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            sucursal=sucursal,
            dias_trabajados=22,
            dias_asistencia=22,
            dias_uniforme=23,
            dias_puntualidad=23,
        )

        bono.recalcular()

        self.assertTrue(bono.pasa_asistencia)
        self.assertTrue(bono.pasa_puntualidad)
        self.assertTrue(bono.cancela_bono)
        self.assertEqual(bono.sub1, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_recalcular_desde_registros_cuenta_solo_asistencias_reales(self):
        user = get_user_model().objects.create_superuser(username="captura-ventas")
        self.client.force_login(user)
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS", sucursal="Payán")
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=5,
            anio=2026,
            dias_laborables=23,
            pct_uniforme=Decimal("15.00"),
            pct_asistencia=Decimal("45.00"),
            pct_puntualidad=Decimal("40.00"),
        )
        bono = BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)
        for dia in range(1, 24):
            RegistroDiarioVentas.objects.create(
                bono=bono,
                dia=dia,
                tiene_asistencia=True,
                tiene_uniforme=True,
                tiene_puntualidad=True,
            )
        for dia in range(24, 32):
            RegistroDiarioVentas.objects.create(
                bono=bono,
                dia=dia,
                tiene_asistencia=False,
                tiene_uniforme=True,
                tiene_puntualidad=True,
            )

        _recalcular_desde_registros(bono)

        bono.refresh_from_db()
        self.assertEqual(bono.dias_trabajados, 23)
        self.assertEqual(bono.dias_uniforme, 23)
        self.assertEqual(bono.dias_puntualidad, 23)
        self.assertTrue(bono.pasa_asistencia)
        self.assertEqual(bono.total_a_pagar, Decimal("300.00"))

    def test_recalcular_desde_registros_permite_dias_extra_trabajados(self):
        user = get_user_model().objects.create_superuser(username="captura-ventas-extra")
        self.client.force_login(user)
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Ventas Extra", area="VENTAS", sucursal="Payán")
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        bono = BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)
        for dia in range(1, 25):
            RegistroDiarioVentas.objects.create(bono=bono, dia=dia, tiene_asistencia=True)

        _recalcular_desde_registros(bono)

        bono.refresh_from_db()
        self.assertEqual(bono.dias_trabajados, 24)

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
        repartidor = Empleado.objects.create(nombre="Repartidor Con Sucursal", area="REPARTIDORES", puesto_operativo="REPARTIDOR", participa_bonos_ventas=True, sucursal="Payán")
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
        resumen = self.client.get("/api/bonos-ventas/bonos/resumen/?mes=5&anio=2026")
        self.assertEqual(resumen.status_code, 200)
        repartidor_row = next(row for row in resumen.json()["bonos"] if row["empleado"] == repartidor.id)
        self.assertTrue(repartidor_row["es_repartidor"])

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
            area="REPARTIDORES",
            puesto_operativo="REPARTIDOR",
            participa_bonos_ventas=True,
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
        repartidores = self.client.get("/api/bonos-ventas/permisos/?mes=5&anio=2026&sucursal=REPARTIDORES")
        self.assertEqual(repartidores.status_code, 200)
        self.assertEqual([row["id"] for row in repartidores.json()["empleados"]], [repartidor.id])

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

    def test_horas_extra_ventas_crea_y_autoriza_en_rrhh(self):
        Empleado.objects.all().delete()
        user = get_user_model().objects.create_user(username="jefe-ventas-he")
        user.groups.add(Group.objects.get_or_create(name=ROLE_VENTAS)[0])
        user.groups.add(Group.objects.get_or_create(name=ROLE_RRHH)[0])
        self.client.force_login(user)
        sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)
        jefe = Empleado.objects.create(nombre="Jefe Ventas HE", departamento=Empleado.DEP_VENTAS, usuario_erp=user)
        empleado = Empleado.objects.create(
            nombre="Empleado Ventas HE",
            area="VENTAS",
            sucursal="Payán",
            jefe_directo=jefe,
            salario_diario=Decimal("400.00"),
        )
        BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)

        creado = self.client.post(
            "/api/bonos-ventas/horas-extra/",
            json.dumps(
                {
                    "empleado": empleado.id,
                    "mes": 5,
                    "anio": 2026,
                    "sucursal": sucursal.id,
                    "fecha": "2026-05-20",
                    "horas": "1.50",
                    "notas": "Cierre de sucursal",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(creado.status_code, 201)
        hora_extra = HoraExtra.objects.get(pk=creado.json()["id"])
        self.assertEqual(hora_extra.empleado, empleado)
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(hora_extra.jefe_directo, user)
        self.assertTrue(creado.json()["puede_autorizar"])
        self.assertTrue(creado.json()["puede_editar"])
        self.assertTrue(creado.json()["puede_eliminar"])

        corregido = self.client.post(
            f"/api/bonos-ventas/horas-extra/{hora_extra.id}/editar/",
            json.dumps(
                {
                    "fecha": "2026-05-21",
                    "horas": "2.00",
                    "notas": "Cierre corregido",
                    "motivo_cambio": "Faltaba media hora",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(corregido.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.fecha.isoformat(), "2026-05-21")
        self.assertEqual(hora_extra.horas, Decimal("2.00"))
        self.assertIn("Faltaba media hora", hora_extra.notas)

        hora_cancelada = HoraExtra.objects.create(
            empleado=empleado,
            fecha="2026-05-22",
            horas=Decimal("1.00"),
            jefe_directo=user,
            notas="Duplicada",
        )
        eliminado = self.client.post(
            f"/api/bonos-ventas/horas-extra/{hora_cancelada.id}/eliminar/",
            json.dumps({"motivo_cambio": "Solicitud duplicada"}),
            content_type="application/json",
        )

        self.assertEqual(eliminado.status_code, 200)
        hora_cancelada.refresh_from_db()
        self.assertEqual(hora_cancelada.estado, HoraExtra.ESTADO_CANCELADO)

        autorizado = self.client.post(f"/api/bonos-ventas/horas-extra/{hora_extra.id}/autorizar/")

        self.assertEqual(autorizado.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.autorizado_por, user)
        self.assertEqual(hora_extra.monto_calculado, Decimal("200.00"))

        editar_autorizada = self.client.post(
            f"/api/bonos-ventas/horas-extra/{hora_extra.id}/editar/",
            json.dumps(
                {
                    "fecha": "2026-05-21",
                    "horas": "2.50",
                    "notas": "Captura corregida despues de autorizar",
                    "motivo_cambio": "Se autorizo con horas incorrectas",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(editar_autorizada.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.horas, Decimal("2.50"))
        self.assertEqual(hora_extra.monto_calculado, Decimal("250.00"))
        self.assertIn("Se autorizo con horas incorrectas", hora_extra.notas)
