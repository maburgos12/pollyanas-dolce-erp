from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from rrhh.models import BonoEsquema, Empleado
from rrhh.services_bonos import sincronizar_bonos_operativos_periodo_actual

from .models import BonoProduccionEmpleado, ConfigBonoPeriodo


class DashboardFuenteRRHHTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_superuser("bonos-rrhh", "bonos@example.com", "test")
        self.client.force_login(user)

    def test_periodo_ausente_no_se_presenta_como_cero_empleados(self):
        ConfigBonoPeriodo.objects.create(mes=8, anio=2026)

        response = self.client.get("/bonos-produccion/dashboard/?mes=9&anio=2026")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "El periodo seleccionado aún no existe")
        self.assertContains(response, "?mes=8&amp;anio=2026")
        self.assertNotContains(response, "0 empleados · $0.00")

    def test_periodo_sin_filas_muestra_elegibles_y_permite_inicializar(self):
        hoy = timezone.localdate()
        periodo = ConfigBonoPeriodo.objects.create(mes=hoy.month, anio=hoy.year)
        elegible = Empleado.objects.create(
            nombre="Colaboradora hornos", area="HORNOS",
            departamento=Empleado.DEP_PRODUCCION, puesto_operativo="HORNOS",
            participa_bonos_produccion=True,
        )
        Empleado.objects.create(
            nombre="Colaboradora no elegible", area="ARMADO",
            departamento=Empleado.DEP_PRODUCCION, puesto_operativo="ARMADO",
            participa_bonos_produccion=False,
        )

        response = self.client.get(f"/bonos-produccion/dashboard/?mes={hoy.month}&anio={hoy.year}")
        self.assertContains(response, "1 colaborador elegible en RRHH")
        self.assertContains(response, "Sincronizar personal desde RRHH")

        response = self.client.post("/bonos-produccion/dashboard/", {
            "action": "inicializar", "mes": hoy.month, "anio": hoy.year,
        })

        self.assertEqual(response.status_code, 302)
        self.assertTrue(BonoProduccionEmpleado.objects.filter(periodo=periodo, empleado=elegible).exists())
        self.assertEqual(BonoProduccionEmpleado.objects.filter(periodo=periodo).count(), 1)

    def test_inicializar_async_devuelve_toast_y_destino_del_mismo_periodo(self):
        hoy = timezone.localdate()
        ConfigBonoPeriodo.objects.create(mes=hoy.month, anio=hoy.year)

        response = self.client.post(
            "/bonos-produccion/dashboard/",
            {"action": "inicializar", "mes": hoy.month, "anio": hoy.year},
            HTTP_ACCEPT="application/json", HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["toast"]["type"], "success")
        self.assertIn(f"?mes={hoy.month}&anio={hoy.year}#personal-rrhh", response.json()["redirect"])

    def test_periodo_historico_no_inicializa_elegibles_actuales(self):
        hoy = timezone.localdate()
        mes_historico = 12 if hoy.month == 1 else hoy.month - 1
        anio_historico = hoy.year - 1 if hoy.month == 1 else hoy.year
        periodo = ConfigBonoPeriodo.objects.create(mes=mes_historico, anio=anio_historico)
        Empleado.objects.create(
            nombre="Alta actual", departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo="HORNOS", participa_bonos_produccion=True,
        )

        response = self.client.get(
            f"/bonos-produccion/dashboard/?mes={mes_historico}&anio={anio_historico}"
        )
        self.assertNotContains(response, "Sincronizar personal desde RRHH")
        response = self.client.post("/bonos-produccion/dashboard/", {
            "action": "inicializar", "mes": mes_historico, "anio": anio_historico,
        })
        self.assertEqual(BonoProduccionEmpleado.objects.filter(periodo=periodo).count(), 0)

    def test_sincronizacion_conserva_captura_preexistente(self):
        hoy = timezone.localdate()
        periodo = ConfigBonoPeriodo.objects.create(mes=hoy.month, anio=hoy.year)
        empleado = Empleado.objects.create(
            nombre="Colaboradora con ajuste", departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo="HORNOS", participa_bonos_produccion=True,
        )
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo, empleado=empleado, area="HORNOS", bono_extra=125,
        )

        self.client.post("/bonos-produccion/dashboard/", {
            "action": "inicializar", "mes": hoy.month, "anio": hoy.year,
        })

        bono.refresh_from_db()
        self.assertEqual(bono.bono_extra, 125)
        self.assertEqual(BonoProduccionEmpleado.objects.filter(periodo=periodo, empleado=empleado).count(), 1)

    def test_sync_actual_acepta_esquema_activo_como_elegibilidad(self):
        hoy = timezone.localdate()
        periodo = ConfigBonoPeriodo.objects.create(mes=hoy.month, anio=hoy.year)
        empleado = Empleado.objects.create(
            nombre="Colaboradora con esquema", departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo="HORNOS", participa_bonos_produccion=False,
        )
        esquema, _ = BonoEsquema.objects.update_or_create(
            codigo="PRODUCCION", defaults={"nombre": "Producción", "activo": True},
        )
        empleado.bonos_esquemas.add(esquema)

        sincronizar_bonos_operativos_periodo_actual(empleado)

        self.assertTrue(BonoProduccionEmpleado.objects.filter(periodo=periodo, empleado=empleado).exists())
