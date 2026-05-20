import json
from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from rrhh.models import Empleado, NominaLinea, NominaPeriodo, PermisoSalida

from .models import AREA_HORNOS, AREA_LOGISTICA, AREA_PRODUCCION, BonoProduccionEmpleado, ConfigBonoPeriodo


class BonosProduccionTests(TestCase):
    def test_pwa_usa_sesion_django_y_expone_csrf(self):
        user = get_user_model().objects.create_user(username="pwa-produccion")
        self.client.force_login(user)

        response = self.client.get("/bonos-produccion/app/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("csrftoken", response.cookies)
        content = response.content.decode()
        self.assertIn("credentials:'same-origin'", content)
        self.assertIn("/bonos-produccion/manifest.json", content)
        self.assertIn("/bonos-produccion/sw.js", content)
        self.assertIn("employee-search", content)
        self.assertIn("Teclea nombre o apellido", content)
        self.assertNotIn("pd_logistica_access", content)

    def test_manifest_y_service_worker_de_produccion_sirven_con_content_type_correcto(self):
        manifest = self.client.get("/bonos-produccion/manifest.json")
        sw = self.client.get("/bonos-produccion/sw.js")

        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest["Content-Type"], "application/manifest+json")
        self.assertEqual(manifest.json()["start_url"], "/bonos-produccion/app/")
        self.assertEqual(sw.status_code, 200)
        self.assertIn("application/javascript", sw["Content-Type"])
        self.assertIn("pollyanas-bonos-produccion-pwa", sw.content.decode())

    def test_api_produccion_acepta_post_con_sesion_y_csrf(self):
        client = Client(enforce_csrf_checks=True)
        user = get_user_model().objects.create_user(username="csrf-produccion")
        client.force_login(user)
        client.get("/bonos-produccion/app/")
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
