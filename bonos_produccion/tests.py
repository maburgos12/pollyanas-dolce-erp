import json
from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings

from core.navigation import NAV_GROUPS
from rrhh.models import Empleado, NominaLinea, NominaPeriodo, PermisoSalida

from .models import AREA_HORNOS, AREA_LOGISTICA, AREA_PRODUCCION, BonoProduccionEmpleado, ConfigBonoPeriodo


@override_settings(SECURE_SSL_REDIRECT=False)
class BonosProduccionTests(TestCase):
    def test_sidebar_de_produccion_apunta_al_dashboard_erp(self):
        produccion = next(group for group in NAV_GROUPS if group["key"] == "produccion")
        bonos_item = next(item for item in produccion["items"] if item[0] == "produccion" and item[1] == "bonos")

        self.assertEqual(bonos_item[3], "/bonos-produccion/dashboard/")

    def test_dashboard_erp_muestra_configuracion_y_app_de_captura(self):
        user = get_user_model().objects.create_superuser(username="admin-bonos", password="x")
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(nombre="Empleado Dashboard", area="PRODUCCION")
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_PRODUCCION)

        response = self.client.get("/bonos-produccion/dashboard/?mes=5&anio=2026")

        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        self.assertIn("Control de configuración y ajustes", content)
        self.assertIn("Abrir app de captura", content)
        self.assertIn("Monto logística", content)
        self.assertIn("Buscar empleado por nombre", content)
        self.assertIn("Usa concepto producción", content)
        self.assertIn("Empleado Dashboard", content)
        self.assertEqual(response["Cache-Control"], "max-age=0, no-cache, no-store, must-revalidate, private")

    def test_raiz_web_de_bonos_produccion_redirige_al_dashboard(self):
        response = self.client.get("/bonos-produccion/")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/bonos-produccion/dashboard/")

    def test_dashboard_erp_actualiza_configuracion_del_periodo(self):
        user = get_user_model().objects.create_superuser(username="admin-config-bonos", password="x")
        self.client.force_login(user)
        ConfigBonoPeriodo.objects.create(mes=5, anio=2026)

        response = self.client.post(
            "/bonos-produccion/dashboard/",
            {
                "action": "config",
                "mes": "5",
                "anio": "2026",
                "dias_laborables": "24",
                "monto_hornos": "1100.00",
                "monto_area_produccion": "900.00",
                "monto_armado": "875.00",
                "monto_logistica": "800.00",
                "monto_crucero": "950.00",
                "premio_embetunado": "400.00",
                "regla_hornos_usa_produccion": "on",
                "regla_hornos_pct_produccion": "60.00",
                "regla_hornos_pct_asistencia": "20.00",
                "regla_hornos_pct_puntualidad": "15.00",
                "regla_hornos_pct_uniforme": "5.00",
                "regla_hornos_limite_produccion": "2",
                "regla_hornos_limite_asistencia": "2",
                "regla_hornos_limite_puntualidad": "2",
                "regla_hornos_limite_uniforme": "1",
                "regla_produccion_usa_produccion": "on",
                "regla_produccion_pct_produccion": "65.00",
                "regla_produccion_pct_asistencia": "15.00",
                "regla_produccion_pct_puntualidad": "15.00",
                "regla_produccion_pct_uniforme": "5.00",
                "regla_produccion_limite_produccion": "2",
                "regla_produccion_limite_asistencia": "2",
                "regla_produccion_limite_puntualidad": "2",
                "regla_produccion_limite_uniforme": "1",
                "regla_armado_usa_produccion": "on",
                "regla_armado_pct_produccion": "65.00",
                "regla_armado_pct_asistencia": "15.00",
                "regla_armado_pct_puntualidad": "15.00",
                "regla_armado_pct_uniforme": "5.00",
                "regla_armado_limite_produccion": "2",
                "regla_armado_limite_asistencia": "2",
                "regla_armado_limite_puntualidad": "2",
                "regla_armado_limite_uniforme": "1",
                "regla_logistica_pct_produccion": "0.00",
                "regla_logistica_pct_asistencia": "50.00",
                "regla_logistica_pct_puntualidad": "30.00",
                "regla_logistica_pct_uniforme": "20.00",
                "regla_logistica_limite_produccion": "0",
                "regla_logistica_limite_asistencia": "3",
                "regla_logistica_limite_puntualidad": "2",
                "regla_logistica_limite_uniforme": "1",
                "regla_crucero_usa_produccion": "on",
                "regla_crucero_pct_produccion": "65.00",
                "regla_crucero_pct_asistencia": "15.00",
                "regla_crucero_pct_puntualidad": "15.00",
                "regla_crucero_pct_uniforme": "5.00",
                "regla_crucero_limite_produccion": "2",
                "regla_crucero_limite_asistencia": "2",
                "regla_crucero_limite_puntualidad": "2",
                "regla_crucero_limite_uniforme": "1",
            },
        )

        self.assertEqual(response.status_code, 302)
        periodo = ConfigBonoPeriodo.objects.get(mes=5, anio=2026)
        self.assertEqual(periodo.dias_laborables, 24)
        self.assertEqual(periodo.monto_logistica, Decimal("800.00"))
        regla_hornos = periodo.reglas_area.get(area=AREA_HORNOS)
        regla_logistica = periodo.reglas_area.get(area=AREA_LOGISTICA)
        self.assertEqual(regla_hornos.pct_produccion, Decimal("60.00"))
        self.assertFalse(regla_logistica.usa_produccion)
        self.assertEqual(regla_logistica.pct_asistencia, Decimal("50.00"))

    def test_dashboard_erp_actualiza_ajuste_de_empleado(self):
        user = get_user_model().objects.create_superuser(username="admin-ajuste-bonos", password="x")
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(nombre="Empleado Ajuste", area="PRODUCCION")
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_PRODUCCION)

        response = self.client.post(
            "/bonos-produccion/dashboard/",
            {
                "action": "ajuste_bono",
                "mes": "5",
                "anio": "2026",
                "bono_id": str(bono.id),
                "dias_trabajados": "20",
                "dias_uniforme": "20",
                "dias_asistencia": "20",
                "dias_puntualidad": "20",
                "dias_produccion": "20",
                "total_embetunados": "14",
                "ajuste_positivo": "50.00",
                "ajuste_negativo": "10.00",
                "bono_extra": "25.00",
                "observaciones": "Ajuste operativo",
            },
        )

        self.assertEqual(response.status_code, 302)
        bono.refresh_from_db()
        self.assertEqual(bono.dias_trabajados, 20)
        self.assertEqual(bono.total_embetunados, 14)
        self.assertEqual(bono.ajuste_positivo, Decimal("50.00"))
        self.assertEqual(bono.observaciones, "Ajuste operativo")

    def test_reglas_por_area_permiten_logistica_sin_concepto_produccion(self):
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, monto_logistica=Decimal("1000.00"))
        periodo.asegurar_reglas_area()
        regla = periodo.reglas_area.get(area=AREA_LOGISTICA)
        self.assertFalse(regla.usa_produccion)

        empleado = Empleado.objects.create(nombre="Empleado Logistica", area="LOGISTICA")
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_LOGISTICA,
            dias_trabajados=20,
            dias_uniforme=20,
            dias_asistencia=20,
            dias_puntualidad=20,
            dias_produccion=0,
        )

        bono.recalcular()

        self.assertTrue(bono.pasa_produccion)
        self.assertEqual(bono.monto_produccion, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("1000.00"))

    def test_premio_embetunado_se_asigna_en_area_produccion(self):
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, premio_embetunado=Decimal("400.00"))
        empleado_1 = Empleado.objects.create(nombre="Empleado Produccion 1", area="PRODUCCION")
        empleado_2 = Empleado.objects.create(nombre="Empleado Produccion 2", area="PRODUCCION")
        bono_1 = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado_1,
            area=AREA_PRODUCCION,
            dias_trabajados=20,
            dias_uniforme=20,
            dias_asistencia=20,
            dias_puntualidad=20,
            dias_produccion=20,
            total_embetunados=12,
        )
        bono_2 = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado_2,
            area=AREA_PRODUCCION,
            dias_trabajados=20,
            dias_uniforme=20,
            dias_asistencia=20,
            dias_puntualidad=20,
            dias_produccion=20,
            total_embetunados=18,
        )

        periodo.recalcular_todos()

        bono_1.refresh_from_db()
        bono_2.refresh_from_db()
        self.assertFalse(bono_1.gano_premio_embetunado)
        self.assertTrue(bono_2.gano_premio_embetunado)
        self.assertEqual(bono_2.monto_premio_embetunado, Decimal("400.00"))

    def test_pwa_usa_sesion_django_y_expone_csrf(self):
        user = get_user_model().objects.create_user(username="pwa-produccion")
        self.client.force_login(user)

        response = self.client.get("/bonos-produccion/app/?captura=1")

        self.assertEqual(response.status_code, 200)
        self.assertIn("csrftoken", response.cookies)
        content = response.content.decode()
        self.assertIn("credentials:'same-origin'", content)
        self.assertIn("/bonos-produccion/manifest.json", content)
        self.assertIn("/bonos-produccion/sw.js", content)
        self.assertIn("href:'/logout/'", content)
        self.assertIn("Cerrar sesión", content)
        self.assertIn("employee-search", content)
        self.assertIn("Teclea nombre o apellido", content)
        self.assertIn("r.redirected", content)
        self.assertNotIn("pd_logistica_access", content)

    def test_app_de_produccion_redirige_a_dashboard_en_escritorio(self):
        user = get_user_model().objects.create_user(username="desktop-produccion")
        self.client.force_login(user)

        response = self.client.get(
            "/bonos-produccion/app/",
            HTTP_USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/bonos-produccion/dashboard/")

    def test_app_de_produccion_conserva_pwa_en_movil_o_captura_forzada(self):
        user = get_user_model().objects.create_user(username="mobile-produccion")
        self.client.force_login(user)

        mobile = self.client.get(
            "/bonos-produccion/app/",
            HTTP_USER_AGENT="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile/15E148",
        )
        forced = self.client.get(
            "/bonos-produccion/app/?captura=1",
            HTTP_USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        )

        self.assertEqual(mobile.status_code, 200)
        self.assertEqual(forced.status_code, 200)
        self.assertIn("Bonos de Produccion", mobile.content.decode())
        self.assertIn("Bonos de Produccion", forced.content.decode())

    def test_manifest_y_service_worker_de_produccion_sirven_con_content_type_correcto(self):
        manifest = self.client.get("/bonos-produccion/manifest.json")
        sw = self.client.get("/bonos-produccion/sw.js")

        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest["Content-Type"], "application/manifest+json")
        self.assertEqual(manifest.json()["start_url"], "/bonos-produccion/app/?captura=1")
        self.assertEqual(sw.status_code, 200)
        self.assertIn("application/javascript", sw["Content-Type"])
        sw_content = sw.content.decode()
        self.assertIn("pollyanas-bonos-produccion-pwa-v4", sw_content)
        self.assertIn('cache: "no-store"', sw_content)
        self.assertIn('url.pathname.startsWith("/bonos-produccion/dashboard/")', sw_content)

    def test_api_produccion_acepta_post_con_sesion_y_csrf(self):
        client = Client(enforce_csrf_checks=True)
        user = get_user_model().objects.create_user(username="csrf-produccion")
        client.force_login(user)
        client.get("/bonos-produccion/app/?captura=1")
        csrf_token = client.cookies["csrftoken"].value

        response = client.post(
            "/api/bonos-produccion/periodos/",
            {"mes": 1, "anio": 2098, "dias_laborables": 23},
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf_token,
        )

        self.assertEqual(response.status_code, 201)

    def test_recalcular_usa_dias_trabajados_como_base_de_asistencia(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION")
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area="HORNOS",
            dias_trabajados=15,
            dias_uniforme=15,
            dias_asistencia=15,
            dias_puntualidad=15,
            dias_produccion=15,
        )

        bono.recalcular()

        self.assertTrue(bono.pasa_asistencia)
        self.assertEqual(bono.total_a_pagar, Decimal("1000.00"))

    def test_aplicar_a_nomina_escribe_total_en_linea_bonos(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION")
        periodo_bono = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        nomina = NominaPeriodo.objects.create(fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 31))
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo_bono,
            empleado=empleado,
            area="HORNOS",
            dias_trabajados=15,
            dias_uniforme=15,
            dias_asistencia=15,
            dias_puntualidad=15,
            dias_produccion=15,
        )
        bono.recalcular()
        bono.save()

        updated = periodo_bono.aplicar_a_nomina(nomina)

        self.assertEqual(updated, 1)
        linea = NominaLinea.objects.get(periodo=nomina, empleado=empleado)
        self.assertEqual(linea.dias_trabajados, Decimal("15"))
        self.assertEqual(linea.bonos, Decimal("1000.00"))

    def test_inicializar_bonos_usa_area_produccion_sin_sucursal(self):
        user = get_user_model().objects.create_user(username="bonos")
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION", sucursal="")

        response = self.client.post(f"/api/bonos-produccion/periodos/{periodo.id}/inicializar-bonos/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["creados"], 1)
        bono = BonoProduccionEmpleado.objects.get(periodo=periodo, empleado=empleado)
        self.assertEqual(bono.area, AREA_PRODUCCION)

    def test_logistica_es_area_valida_de_bonos_produccion(self):
        periodo = ConfigBonoPeriodo.objects.create(mes=6, anio=2026)
        empleado = Empleado.objects.create(nombre="Empleado Logistica", area="LOGISTICA")

        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_LOGISTICA,
            dias_trabajados=10,
            dias_uniforme=10,
            dias_asistencia=10,
            dias_puntualidad=10,
            dias_produccion=10,
        )
        bono.recalcular()

        self.assertEqual(bono.area, AREA_LOGISTICA)
        self.assertEqual(bono.total_a_pagar, Decimal("850.00"))

    def test_permisos_equipo_produccion_crea_y_rechaza(self):
        user = get_user_model().objects.create_user(username="jefe-produccion")
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(nombre="Empleado Hornos Permiso", area="PRODUCCION")
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")

        listado = self.client.get("/api/bonos-produccion/permisos/?mes=5&anio=2026&area=HORNOS")

        self.assertEqual(listado.status_code, 200)
        self.assertEqual([row["id"] for row in listado.json()["empleados"]], [empleado.id])

        creado = self.client.post(
            "/api/bonos-produccion/permisos/",
            json.dumps(
                {
                    "empleado": empleado.id,
                    "tipo": PermisoSalida.TIPO_PERMISO_DIA,
                    "fecha_inicio": "2026-05-21T08:00:00",
                    "fecha_fin": "2026-05-21T16:00:00",
                    "goce_sueldo": True,
                    "motivo": "Tramite familiar",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(creado.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=creado.json()["id"])
        self.assertEqual(permiso.origen_solicitud, PermisoSalida.ORIGEN_BONOS_PRODUCCION)
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PENDIENTE)

        rechazado = self.client.post(f"/api/bonos-produccion/permisos/{permiso.id}/rechazar/")

        self.assertEqual(rechazado.status_code, 200)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_RECHAZADO)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_RECHAZADO)
        self.assertEqual(permiso.autorizado_jefe_por, user)
