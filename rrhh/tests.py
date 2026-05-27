from django.contrib.auth.models import Group, User
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from decimal import Decimal
from pathlib import Path
from rest_framework.test import APIClient
from unittest import SkipTest

from core.models import Notificacion
from rrhh.models import (
    AsistenciaEmpleado,
    BonoEsquema,
    Empleado,
    EmpleadoBaja,
    HoraExtra,
    NominaConceptoLinea,
    NominaImportacion,
    NominaLinea,
    NominaPeriodo,
    PlantillaAutorizada,
    PermisoSalida,
    Prestamo,
    PrestamoCuota,
    Turno,
    VacanteRRHH,
)
from rrhh.services.lista_raya import parse_lista_raya_xls


LISTA_RAYA_SAMPLE = Path("/Users/mauricioburgos/Downloads/Lista de raya del 16 al 31 de abril 2026.xls")


class CapitalHumanoServiceTests(TestCase):
    def test_permiso_de_jefatura_requiere_direccion_antes_de_rrhh(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        rrhh_user = User.objects.create_user(username="paula")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        dg_user = User.objects.create_user(username="mauricio")
        dg_user.groups.add(Group.objects.create(name="DG"))
        jefa_ventas = Empleado.objects.create(
            nombre="Johana Lopez",
            departamento=Empleado.DEP_VENTAS,
            puesto="Jefe de Ventas",
        )
        permiso = PermisoSalida.objects.create(
            empleado=jefa_ventas,
            tipo=PermisoSalida.TIPO_PERMISO_DIA,
            fecha_inicio=datetime(2026, 5, 26, 8, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            motivo="Permiso de jefatura",
            estado_jefe=PermisoSalida.ESTADO_JEFE_PREAUTORIZADO,
            autorizado_jefe_por=dg_user,
        )

        self.assertTrue(permiso.requiere_direccion)
        self.assertEqual(permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_PENDIENTE)

        self.client.force_login(rrhh_user)
        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "aprobar"},
        )
        self.assertEqual(response.status_code, 302)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)

        self.client.force_login(dg_user)
        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "autorizar_direccion"},
        )
        self.assertEqual(response.status_code, 302)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_AUTORIZADO)
        self.assertEqual(permiso.autorizado_direccion_por, dg_user)

        self.client.force_login(rrhh_user)
        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "aprobar"},
        )
        self.assertEqual(response.status_code, 302)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertEqual(permiso.autorizado_por, rrhh_user)

    def test_permiso_operativo_no_requiere_direccion(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        rrhh_user = User.objects.create_user(username="paula")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        jefa_ventas = Empleado.objects.create(
            nombre="Johana Lopez",
            departamento=Empleado.DEP_VENTAS,
            puesto="Jefe de Ventas",
        )
        cajera = Empleado.objects.create(
            nombre="Cajera Operativa",
            departamento=Empleado.DEP_VENTAS,
            area="VENTAS",
            puesto="Cajera",
            jefe_directo=jefa_ventas,
        )
        permiso = PermisoSalida.objects.create(
            empleado=cajera,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=datetime(2026, 5, 26, 13, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            fecha_fin=datetime(2026, 5, 26, 15, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            motivo="Cita",
            estado_jefe=PermisoSalida.ESTADO_JEFE_PREAUTORIZADO,
        )

        self.assertFalse(permiso.requiere_direccion)
        self.assertEqual(permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_NO_REQUIERE)

        self.client.force_login(rrhh_user)
        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "aprobar"},
        )
        self.assertEqual(response.status_code, 302)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)

    def test_rrhh_no_aprueba_permiso_sin_preautorizacion_de_jefe(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        rrhh_user = User.objects.create_user(username="paula")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        empleado = Empleado.objects.create(nombre="Empleado Pendiente Jefe", departamento=Empleado.DEP_VENTAS)
        permiso = PermisoSalida.objects.create(
            empleado=empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=datetime(2026, 5, 26, 13, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            fecha_fin=datetime(2026, 5, 26, 15, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            motivo="Cita",
            estado_jefe=PermisoSalida.ESTADO_JEFE_PENDIENTE,
        )

        self.client.force_login(rrhh_user)
        response = self.client.get(reverse("rrhh:rrhh_permisos_list"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Aprobar RRHH")

        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "aprobar"},
        )

        self.assertEqual(response.status_code, 302)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
        self.assertIsNone(permiso.autorizado_por)

    def test_generar_horas_extra_desde_asistencia(self):
        from datetime import date, datetime, time
        from zoneinfo import ZoneInfo

        from rrhh.models import AsistenciaEmpleado, HoraExtra, Turno
        from rrhh.services import calcular_horas_extra, generar_horas_extra_automatico

        empleado = Empleado.objects.create(nombre="Empleado HE", salario_diario="400.00")
        turno = Turno.objects.create(
            nombre="Matutino",
            hora_entrada=time(8, 0),
            hora_salida=time(16, 0),
            tolerancia_minutos=10,
        )
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=empleado,
            fecha=date(2026, 5, 13),
            entrada=datetime(2026, 5, 13, 8, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            salida=datetime(2026, 5, 13, 17, 15, tzinfo=ZoneInfo("America/Mazatlan")),
            minutos_trabajados=555,
            turno=turno,
            fuente="manual",
        )

        self.assertEqual(calcular_horas_extra(asistencia), Decimal("1.25"))
        he = generar_horas_extra_automatico(asistencia)
        self.assertIsNotNone(he)
        self.assertEqual(HoraExtra.objects.count(), 1)
        self.assertEqual(he.horas, Decimal("1.25"))

        he.estado = "autorizado"
        he.horas = Decimal("1.00")
        he.save(update_fields=["estado", "horas"])
        asistencia.minutos_trabajados = 600
        asistencia.save(update_fields=["minutos_trabajados"])

        actualizado = generar_horas_extra_automatico(asistencia)
        actualizado.refresh_from_db()
        self.assertEqual(actualizado.estado, "autorizado")
        self.assertEqual(actualizado.horas, Decimal("1.00"))

    def test_prestamo_genera_cuotas_y_recalcula_saldo(self):
        from datetime import date

        from rrhh.models import Prestamo, PrestamoCuota
        from rrhh.services_prestamos import aplicar_cobro_manual, generar_cuotas

        user = User.objects.create_user(username="paula", password="pass123")
        empleado = Empleado.objects.create(nombre="Empleado Préstamo", salario_diario="400.00")
        prestamo = Prestamo.objects.create(
            empleado=empleado,
            concepto="Apoyo personal",
            fecha_solicitud=date(2026, 5, 10),
            fecha_deposito=date(2026, 5, 14),
            importe=Decimal("1000.00"),
            num_quincenas=2,
            descuento_quincenal=Decimal("500.00"),
            creado_por=user,
        )

        cuotas = generar_cuotas(prestamo)
        self.assertEqual(len(cuotas), 2)
        self.assertEqual(PrestamoCuota.objects.filter(prestamo=prestamo).count(), 2)

        cuota = prestamo.cuotas.first()
        aplicar_cobro_manual(cuota, Decimal("500.00"), user, "Primer cobro")
        prestamo.refresh_from_db()
        cuota.refresh_from_db()
        self.assertEqual(cuota.estado, PrestamoCuota.ESTADO_COBRADO)
        self.assertEqual(prestamo.saldo_actual, Decimal("500.00"))

    def test_indicadores_capital_humano_renderiza_datos_reales_y_capturas(self):
        from datetime import date

        user = User.objects.create_user(username="paula", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        user.groups.add(rrhh_group)
        empleado = Empleado.objects.create(
            codigo="100",
            nombre="Empleado Indicadores",
            area="VENTAS",
            fecha_ingreso=date(2026, 5, 1),
            salario_diario=Decimal("400.00"),
        )
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1),
            fecha_fin=date(2026, 5, 15),
            total_bruto=Decimal("1000.00"),
            total_descuentos=Decimal("100.00"),
            total_neto=Decimal("900.00"),
        )
        linea = NominaLinea.objects.create(
            periodo=periodo,
            empleado=empleado,
            dias_trabajados=Decimal("15.00"),
            salario_base=Decimal("900.00"),
            total_percepciones=Decimal("1000.00"),
            descuentos=Decimal("100.00"),
            neto_calculado=Decimal("900.00"),
        )
        NominaConceptoLinea.objects.create(
            linea=linea,
            tipo=NominaConceptoLinea.TIPO_PERCEPCION,
            codigo_concepto="4",
            nombre="Horas extras",
            valor=Decimal("2.00"),
            importe=Decimal("200.00"),
        )
        EmpleadoBaja.objects.create(
            empleado=empleado,
            nombre=empleado.nombre,
            area=empleado.area,
            fecha_ingreso=date(2026, 5, 1),
            fecha_baja=date(2026, 5, 20),
            motivo=EmpleadoBaja.MOTIVO_NO_APTO,
            creado_por=user,
        )
        PlantillaAutorizada.objects.create(anio=2026, mes=5, area="VENTAS", cantidad=2, actualizado_por=user)
        VacanteRRHH.objects.create(
            area="VENTAS",
            puesto="CAJERA",
            fecha_solicitada=date(2026, 5, 2),
            estado=VacanteRRHH.ESTADO_RECLUTAMIENTO,
            creado_por=user,
        )

        self.client.force_login(user)
        response = self.client.get(reverse("rrhh:rrhh_indicadores"), {"mes": "2026-05"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Capital Humano con fuente única ERP")
        self.assertContains(response, "Empleado Indicadores")
        self.assertContains(response, "CAJERA")
        self.assertContains(response, "2.00")


class CapitalHumanoAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username="empleado.app",
            email="empleado.app@example.com",
            password="pass123",
        )
        self.empleado = Empleado.objects.create(
            nombre="Empleado App",
            email="empleado.app@example.com",
            salario_diario="350.00",
        )
        self.client = APIClient()
        self.client.force_authenticate(user=self.user)

    def test_permiso_api_crea_folio_para_empleado_actual(self):
        resp = self.client.post(
            reverse("rrhh:permiso-list"),
            {
                "tipo": "cita_medica",
                "fecha_inicio": timezone.datetime(2026, 5, 14, 10, 0, tzinfo=timezone.get_current_timezone()).isoformat(),
                "fecha_fin": timezone.datetime(2026, 5, 14, 12, 0, tzinfo=timezone.get_current_timezone()).isoformat(),
                "motivo": "Consulta programada",
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["empleado"], self.empleado.id)
        self.assertTrue(resp.data["folio"].startswith("PS-"))
        self.assertEqual(resp.data["estado"], "solicitado")

    def test_hora_extra_api_crea_para_empleado_actual_por_nombre_reordenado(self):
        user = User.objects.create_user(
            username="paula.lugo",
            first_name="Paula Elizabeth",
            last_name="Lugo Espinoza",
            email="capitalhumano@pollyanasdolce.com",
            password="pass123",
        )
        empleado = Empleado.objects.create(
            nombre="LUGO ESPINOZA PAULA ELIZABETH",
            salario_diario="500.00",
        )
        self.client.force_authenticate(user=user)

        me_resp = self.client.get(reverse("rrhh:capital_humano_me"))
        self.assertEqual(me_resp.status_code, 200)
        self.assertEqual(me_resp.data["empleado"], empleado.id)

        resp = self.client.post(
            reverse("rrhh:hora-extra-list"),
            {
                "fecha": "2026-05-14",
                "horas": "2.50",
                "notas": "Cierre de sucursal",
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["empleado"], empleado.id)
        self.assertEqual(resp.data["estado"], "pendiente")
        self.assertEqual(resp.data["notas"], "Cierre de sucursal")

    def test_hora_extra_api_notifica_y_solo_jefe_directo_autoriza(self):
        jefe_user = User.objects.create_user(username="johana.lopez", password="pass123")
        jefe_empleado = Empleado.objects.create(nombre="Johana Lopez", usuario_erp=jefe_user)
        empleado_user = User.objects.create_user(
            username="empleado.he",
            email="empleado.he@example.com",
            password="pass123",
        )
        empleado = Empleado.objects.create(
            nombre="Empleado Horas Extra",
            email="empleado.he@example.com",
            salario_diario="400.00",
            jefe_directo=jefe_empleado,
        )
        rrhh_user = User.objects.create_user(username="rrhh.he", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        self.client.force_authenticate(user=empleado_user)

        resp = self.client.post(
            reverse("rrhh:hora-extra-list"),
            {"fecha": "2026-05-20", "horas": "2.00", "notas": "Cierre operativo"},
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        hora_extra = HoraExtra.objects.get(pk=resp.data["id"])
        self.assertEqual(hora_extra.empleado, empleado)
        self.assertEqual(hora_extra.jefe_directo, jefe_user)
        self.assertEqual(Notificacion.objects.filter(usuario=jefe_user, tipo=Notificacion.TIPO_HORA_EXTRA).count(), 1)

        self.client.force_authenticate(user=rrhh_user)
        resp_rrhh = self.client.post(reverse("rrhh:hora-extra-autorizar", args=[hora_extra.id]))
        self.assertEqual(resp_rrhh.status_code, 403)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)

        self.client.force_authenticate(user=jefe_user)
        resp_jefe = self.client.post(reverse("rrhh:hora-extra-autorizar", args=[hora_extra.id]))
        self.assertEqual(resp_jefe.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.autorizado_por, jefe_user)
        self.assertIsNotNone(hora_extra.fecha_autorizacion_jefe)
        self.assertEqual(hora_extra.monto_calculado, Decimal("200.00"))

    def test_usuario_sin_empleado_no_se_vincula_a_otro_empleado(self):
        Empleado.objects.create(nombre="XANTECO MENA MARISOL", salario_diario="500.00")
        user = User.objects.create_user(
            username="yesenia.soto",
            first_name="Yesenia",
            last_name="Soto",
            email="admon.yesenia@pollyanasdolce.com",
            password="pass123",
        )
        self.client.force_authenticate(user=user)

        me_resp = self.client.get(reverse("rrhh:capital_humano_me"))
        self.assertEqual(me_resp.status_code, 200)
        self.assertIsNone(me_resp.data["empleado"])

        resp = self.client.post(
            reverse("rrhh:hora-extra-list"),
            {
                "fecha": "2026-05-14",
                "horas": "1.00",
                "notas": "No debe crear",
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 400)

    def test_rutas_capital_humano_cargan(self):
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        self.user.groups.add(rrhh_group)
        self.client.force_login(self.user)

        for url_name in ["rrhh_dashboard", "rrhh_prestamos_lista", "rrhh_monitor_sync"]:
            resp = self.client.get(reverse(f"rrhh:{url_name}"))
            self.assertEqual(resp.status_code, 200)
        resp = self.client.get(reverse("rrhh:rrhh_pwa"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Registrar horas extra")

    @override_settings(ERP_PUBLIC_API_KEY="hik-test-key")
    def test_receptor_hik_crea_y_actualiza_asistencia(self):
        from datetime import time

        empleado = Empleado.objects.create(nombre="Empleado Hik", codigo="2", salario_diario="400.00")
        Turno.objects.create(nombre="Matutino", hora_entrada=time(8, 0), hora_salida=time(16, 0), tolerancia_minutos=10)

        entrada = self.client.post(
            reverse("rrhh:rrhh_receptor_hik"),
            {
                "eventos": [
                    {
                        "employee_no": "2",
                        "name": "Empleado Hik",
                        "attendance_status": "checkIn",
                        "time": "2026-05-14T08:05:00-07:00",
                        "serial_no": 9991,
                    }
                ]
            },
            format="json",
            HTTP_X_API_KEY="hik-test-key",
        )
        self.assertEqual(entrada.status_code, 200)
        self.assertEqual(entrada.json()["procesados"], 1)

        salida = self.client.post(
            reverse("rrhh:rrhh_receptor_hik"),
            {
                "eventos": [
                    {
                        "employee_no": "2",
                        "name": "Empleado Hik",
                        "attendance_status": "checkOut",
                        "time": "2026-05-14T17:15:00-07:00",
                        "serial_no": 9992,
                    }
                ]
            },
            format="json",
            HTTP_X_API_KEY="hik-test-key",
        )
        self.assertEqual(salida.status_code, 200)
        self.assertEqual(salida.json()["procesados"], 1)

        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha="2026-05-14")
        self.assertEqual(asistencia.fuente, AsistenciaEmpleado.FUENTE_HIKCONNECT_API)
        self.assertEqual(asistencia.minutos_trabajados, 550)
        self.assertIsNotNone(asistencia.turno)

    def test_receptor_hik_rechaza_sin_api_key(self):
        resp = self.client.post(reverse("rrhh:rrhh_receptor_hik"), {"eventos": []}, format="json")
        self.assertEqual(resp.status_code, 401)


class RRHHViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="rrhh", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        self.user.groups.add(rrhh_group)
        self.client.login(username="rrhh", password="pass123")

    def test_empleados_view_and_create(self):
        resp = self.client.get(reverse("rrhh:empleados"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "RRHH · Empleados")
        self.assertContains(resp, "Alta de empleado")
        self.assertContains(resp, "Vista rápida")
        self.assertContains(resp, "Catálogo de empleados")
        self.assertContains(resp, "Modificar")
        self.assertTrue(resp.context["focus_cards"])
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp.context)
        self.assertIn("critical_path_rows", resp.context)
        self.assertIn("handoff_map", resp.context)
        self.assertIn("owner", resp.context["handoff_map"][0])
        self.assertIn("depends_on", resp.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp.context["handoff_map"][0])
        self.assertIn("next_step", resp.context["handoff_map"][0])
        self.assertIn("completion", resp.context["handoff_map"][0])
        self.assertTrue(resp.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("executive_radar_rows", resp.context)
        self.assertIn("erp_command_center", resp.context)
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
        self.assertTrue(resp.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp.context)
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Demo",
                "area": "Producción",
                "puesto": "Pastelero",
                "salario_diario": "450.00",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Empleado Demo")

        empleado = Empleado.objects.get(nombre="Empleado Demo")
        resp_update = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "update",
                "empleado_id": str(empleado.id),
                "nombre": "Empleado Demo Editado",
                "rfc": "DEMO-010101-AA1",
                "curp": "DEMO010101HSLAAA01",
                "nss": "11-22-33-4444-5",
                "area": "Administración",
                "puesto": "Coordinador",
                "tipo_contrato": "FIJO",
                "fecha_ingreso": "2026-04-30",
                "salario_diario": "500.00",
                "telefono": "6870000000",
                "email": "demo@example.com",
                "sucursal": "Matriz",
                "activo": "on",
            },
            follow=True,
        )
        self.assertEqual(resp_update.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.nombre, "Empleado Demo Editado")
        self.assertEqual(empleado.rfc, "DEMO-010101-AA1")
        self.assertEqual(empleado.area, "Administración")

    def test_rrhh_root_requires_rrhh_access(self):
        user = User.objects.create_user(username="solo.mantenimiento", password="pass123")
        self.client.force_login(user)

        resp = self.client.get(reverse("rrhh:home"))

        self.assertEqual(resp.status_code, 403)

    def test_empleados_can_focus_operational_subset(self):
        self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Sin Area",
                "puesto": "Auxiliar",
                "salario_diario": "250.00",
            },
            follow=True,
        )
        resp = self.client.get(reverse("rrhh:empleados"), {"enterprise_focus": "SIN_AREA"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "SIN_AREA")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_estructura_organizacional_inicial_actualiza_sin_duplicar_empleados(self):
        from rrhh.services_organizacion import aplicar_estructura_organizacional_inicial

        yesenia = Empleado.objects.create(nombre="SOTO INZUNZA YESENIA", area="ADMINISTRACION")
        johana = Empleado.objects.create(nombre="LOPEZ PALOS JOHANA ADELIN", area="ADMINISTRACION")
        carolina = Empleado.objects.create(nombre="CAYETANO VALENZUELA CAROLINA", area="ADMINISTRACION")
        Empleado.objects.create(nombre="LUGO ESPINOZA PAULA ELIZABETH", area="ADMINISTRACION")
        roxana = Empleado.objects.create(nombre="RIVAS SOLIS ROXANA", area="ADMINISTRACION")
        julissa = Empleado.objects.create(nombre="ANGULO PARRA JULISSA", area="PRODUCCION")
        jorge = Empleado.objects.create(nombre="PEREZ VALENZUELA JORGE ISAAC", area="ALMACEN")
        limpieza = Empleado.objects.create(nombre="GARCIA HIGUERA BEATRIZ", area="AFANADORA")
        repartidor = Empleado.objects.create(nombre="LOPEZ VILLALOBOS JORGE ALFONSO", area="REPARTIDOR")
        hornos = Empleado.objects.create(nombre="COTA MEDINA MINERVA CECILIA", area="HORNOS", puesto="Hornos")
        call_center = Empleado.objects.create(nombre="CALL CENTER DEMO", area="VENTAS", puesto="Call Center")
        marketing = Empleado.objects.create(nombre="MARKETING EXTERNO DEMO", area="MARKETING")

        total_antes = Empleado.objects.count()
        resultado = aplicar_estructura_organizacional_inicial()

        self.assertEqual(Empleado.objects.count(), total_antes)
        self.assertGreaterEqual(resultado["actualizados"], 1)

        yesenia.refresh_from_db()
        johana.refresh_from_db()
        carolina.refresh_from_db()
        roxana.refresh_from_db()
        julissa.refresh_from_db()
        jorge.refresh_from_db()
        limpieza.refresh_from_db()
        repartidor.refresh_from_db()
        hornos.refresh_from_db()
        call_center.refresh_from_db()
        marketing.refresh_from_db()

        self.assertEqual(yesenia.departamento, "ADMINISTRACION")
        self.assertEqual(yesenia.puesto, "Jefe de Administración")
        self.assertEqual(johana.departamento, "VENTAS")
        self.assertEqual(carolina.departamento, "PRODUCCION")
        self.assertEqual(roxana.jefe_directo, carolina)
        self.assertEqual(julissa.jefe_directo, carolina)
        self.assertEqual(jorge.departamento, "MANTENIMIENTO")
        self.assertEqual(jorge.jefe_directo, yesenia)
        self.assertEqual(limpieza.departamento, "ADMINISTRACION")
        self.assertEqual(limpieza.puesto_operativo, "LIMPIEZA")
        self.assertEqual(limpieza.jefe_directo, yesenia)
        self.assertEqual(repartidor.departamento, "VENTAS")
        self.assertTrue(repartidor.participa_bonos_ventas)
        self.assertEqual(repartidor.jefe_directo, johana)
        self.assertEqual(hornos.departamento, "PRODUCCION")
        self.assertEqual(hornos.puesto_operativo, "HORNOS")
        self.assertTrue(hornos.participa_bonos_produccion)
        self.assertEqual(call_center.tipo_personal, "POLLYANA")
        self.assertEqual(marketing.tipo_personal, "EXTERNO")

    def test_logistica_con_adscripcion_temporal_conserva_origen_y_bonos(self):
        from rrhh.services_organizacion import aplicar_estructura_organizacional_inicial

        johana = Empleado.objects.create(nombre="LOPEZ PALOS JOHANA ADELIN", area="VENTAS")
        carolina = Empleado.objects.create(nombre="CAYETANO VALENZUELA CAROLINA", area="PRODUCCION")
        repartidor = Empleado.objects.create(nombre="REPARTIDOR TEMPORAL", area="REPARTIDOR")
        envio = Empleado.objects.create(nombre="ENVIO SUCURSAL TEMPORAL", area="LOGISTICA", puesto="Envío a sucursal")

        aplicar_estructura_organizacional_inicial()

        repartidor.refresh_from_db()
        envio.refresh_from_db()
        self.assertEqual(repartidor.departamento_origen, Empleado.DEP_LOGISTICA)
        self.assertEqual(repartidor.departamento, Empleado.DEP_VENTAS)
        self.assertEqual(repartidor.jefe_directo, johana)
        self.assertTrue(repartidor.participa_bonos_ventas)
        self.assertEqual(envio.departamento_origen, Empleado.DEP_LOGISTICA)
        self.assertEqual(envio.departamento, Empleado.DEP_PRODUCCION)
        self.assertEqual(envio.jefe_directo, carolina)
        self.assertTrue(envio.participa_bonos_produccion)

    def test_organizacion_capital_humano_renderiza_jerarquia(self):
        jefe = Empleado.objects.create(
            nombre="LOPEZ PALOS JOHANA ADELIN",
            area="VENTAS",
            departamento="VENTAS",
            puesto="Jefe de Ventas",
        )
        Empleado.objects.create(
            nombre="LOPEZ VILLALOBOS JORGE ALFONSO",
            area="REPARTIDOR",
            departamento="VENTAS",
            puesto="Repartidor",
            puesto_operativo="REPARTIDOR",
            jefe_directo=jefe,
            participa_bonos_ventas=True,
        )

        resp = self.client.get(reverse("rrhh:rrhh_organizacion"))

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Mapa organizacional")
        self.assertContains(resp, "LOPEZ PALOS JOHANA ADELIN")
        self.assertContains(resp, "LOPEZ VILLALOBOS JORGE ALFONSO")
        self.assertContains(resp, "Jefe directo")

    def test_empleados_expone_campos_de_organizacion(self):
        Empleado.objects.create(nombre="LOPEZ PALOS JOHANA ADELIN", departamento="VENTAS", puesto="Jefe de Ventas")
        BonoEsquema.objects.get_or_create(codigo="VENTAS", defaults={"nombre": "Ventas"})

        resp = self.client.get(reverse("rrhh:empleados"))

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Departamento")
        self.assertContains(resp, "Puesto operativo")
        self.assertContains(resp, "Jefe directo")
        self.assertContains(resp, "Esquemas de bono")
        self.assertContains(resp, "Agregar esquema de bono")
        self.assertContains(resp, "Puesto descriptivo")
        self.assertContains(resp, "Regla para bonos y permisos")
        self.assertContains(resp, "Quién lo supervisa hoy")
        self.assertContains(resp, "Guardar cambios arriba")
        self.assertContains(resp, "Ventas")
        self.assertContains(resp, "Producción")
        self.assertContains(resp, "Área / división")
        self.assertContains(resp, "Hornos")
        self.assertContains(resp, "Armado")
        self.assertContains(resp, "Otro...")
        self.assertContains(resp, "modal-catalogo-otro")
        self.assertContains(resp, "modal-bono-esquema-otro")

    def test_empleados_crea_esquema_bono_otro_y_lo_asigna(self):
        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Bono Logistica",
                "area": "REPARTIDORES",
                "puesto": "Apoyo",
                "departamento": Empleado.DEP_LOGISTICA,
                "bono_esquema_otro_nombre": "Bono logística",
                "bono_esquema_otro_departamento": Empleado.DEP_LOGISTICA,
                "bono_esquema_otro_area": "Repartidores",
                "bono_esquema_otro_descripcion": "Bono para logística temporal",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="Empleado Bono Logistica")
        esquema = BonoEsquema.objects.get(codigo="BONO_LOGISTICA")
        self.assertEqual(esquema.departamento, Empleado.DEP_LOGISTICA)
        self.assertEqual(esquema.area, "REPARTIDORES")
        self.assertIn(esquema, empleado.bonos_esquemas.all())
        self.assertTrue(empleado.participa_bonos_ventas)
        self.assertFalse(empleado.participa_bonos_produccion)

    def test_empleados_sincroniza_esquemas_base_con_banderas_legacy(self):
        ventas, _ = BonoEsquema.objects.get_or_create(
            codigo="VENTAS",
            defaults={"nombre": "Ventas", "departamento": Empleado.DEP_VENTAS},
        )
        produccion, _ = BonoEsquema.objects.get_or_create(
            codigo="PRODUCCION",
            defaults={"nombre": "Producción", "departamento": Empleado.DEP_PRODUCCION},
        )

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Bono Base",
                "area": "CAJAS",
                "puesto": "Cajas",
                "bono_esquemas": [str(ventas.id), str(produccion.id)],
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="Empleado Bono Base")
        self.assertTrue(empleado.participa_bonos_ventas)
        self.assertTrue(empleado.participa_bonos_produccion)
        self.assertEqual(set(empleado.bonos_esquemas.values_list("codigo", flat=True)), {"VENTAS", "PRODUCCION"})

    def test_empleados_guarda_area_y_puesto_operativo_otro_desde_modal(self):
        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Catalogo Otro",
                "departamento": Empleado.DEP_PRODUCCION,
                "area": "__otro__",
                "area_otro": "Decorado especial",
                "puesto": "Auxiliar",
                "puesto_operativo": "__otro__",
                "puesto_operativo_otro": "DECORADO",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="Empleado Catalogo Otro")
        self.assertEqual(empleado.area, "DECORADO ESPECIAL")
        self.assertEqual(empleado.puesto_operativo, "DECORADO")

    def test_bonos_usan_campos_de_rrhh_sin_repetir_area_macro(self):
        from bonos_produccion.models import AREA_LOGISTICA, area_bono_produccion_empleado
        from bonos_ventas.empleados import empleados_elegibles_bonos_ventas

        repartidor = Empleado.objects.create(
            nombre="REPARTIDOR BONO RRHH",
            area="REPARTIDORES",
            departamento_origen=Empleado.DEP_LOGISTICA,
            departamento=Empleado.DEP_VENTAS,
            puesto_operativo="REPARTIDOR",
            participa_bonos_ventas=True,
        )
        envio = Empleado.objects.create(
            nombre="ENVIO BONO RRHH",
            area="ENVIO A SUCURSAL",
            departamento_origen=Empleado.DEP_LOGISTICA,
            departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo="ENVIO_SUCURSAL",
            participa_bonos_produccion=True,
        )

        self.assertIn(repartidor, empleados_elegibles_bonos_ventas())
        self.assertEqual(area_bono_produccion_empleado(envio), AREA_LOGISTICA)

    def test_rrhh_crea_prestamo_y_solo_direccion_aprueba(self):
        from datetime import date

        empleado = Empleado.objects.create(nombre="Empleado Préstamo Vista", salario_diario="400.00")
        resp_nuevo = self.client.get(reverse("rrhh:rrhh_prestamo_nuevo"))
        self.assertEqual(resp_nuevo.status_code, 200)
        self.assertContains(resp_nuevo, "Nuevo préstamo")

        prestamo = Prestamo.objects.create(
            empleado=empleado,
            concepto="Apoyo autorizado",
            fecha_solicitud=date(2026, 5, 10),
            fecha_deposito=date(2026, 5, 14),
            importe=Decimal("1000.00"),
            num_quincenas=2,
            descuento_quincenal=Decimal("500.00"),
            saldo_actual=Decimal("1000.00"),
            estado=Prestamo.ESTADO_AUTORIZADO,
            firma_jefe=True,
            autorizado_jefe=self.user,
            fecha_auth_jefe=timezone.now(),
            creado_por=self.user,
        )

        resp_rrhh = self.client.post(reverse("rrhh:rrhh_prestamo_auth_dg", args=[prestamo.pk]))
        self.assertEqual(resp_rrhh.status_code, 403)
        self.assertEqual(PrestamoCuota.objects.filter(prestamo=prestamo).count(), 0)

        director = User.objects.create_user(username="director", password="pass123")
        dg_group, _ = Group.objects.get_or_create(name="DG")
        director.groups.add(dg_group)
        self.client.force_login(director)

        resp_dg = self.client.post(reverse("rrhh:rrhh_prestamo_auth_dg", args=[prestamo.pk]), follow=True)
        self.assertEqual(resp_dg.status_code, 200)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_ACTIVO)
        self.assertEqual(PrestamoCuota.objects.filter(prestamo=prestamo).count(), 2)

    def test_prestamo_bloquea_nueva_solicitud_si_empleado_tiene_saldo_pendiente(self):
        from datetime import date

        empleado = Empleado.objects.create(nombre="Empleado con Deuda", salario_diario="400.00")
        Prestamo.objects.create(
            empleado=empleado,
            concepto="Préstamo vigente",
            fecha_solicitud=date(2026, 5, 1),
            importe=Decimal("1200.00"),
            num_quincenas=4,
            descuento_quincenal=Decimal("300.00"),
            saldo_actual=Decimal("600.00"),
            estado=Prestamo.ESTADO_ACTIVO,
            creado_por=self.user,
        )

        resp = self.client.post(
            reverse("rrhh:rrhh_prestamo_nuevo"),
            {
                "empleado": str(empleado.id),
                "concepto": "Segundo préstamo",
                "metodo_pago": Prestamo.METODO_TRANSFERENCIA,
                "fecha_solicitud": "2026-05-20",
                "importe": "500.00",
                "num_quincenas": "2",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "no puede solicitar un nuevo préstamo")
        self.assertEqual(Prestamo.objects.filter(empleado=empleado).count(), 1)

    def test_jefe_asignado_ve_y_autoriza_prestamo_en_su_bandeja(self):
        from datetime import date

        jefe = User.objects.create_user(username="johana", password="pass123")
        jefe.groups.add(Group.objects.create(name="VENTAS"))
        empleado = Empleado.objects.create(nombre="Empleado con Jefe", area="VENTAS", salario_diario="400.00")
        prestamo = Prestamo.objects.create(
            empleado=empleado,
            concepto="Solicitud con jefe",
            fecha_solicitud=date(2026, 5, 15),
            importe=Decimal("800.00"),
            num_quincenas=2,
            descuento_quincenal=Decimal("400.00"),
            saldo_actual=Decimal("800.00"),
            estado=Prestamo.ESTADO_SOLICITADO,
            jefe_directo=jefe,
            creado_por=self.user,
        )

        self.client.force_login(jefe)
        resp_lista = self.client.get(reverse("rrhh:rrhh_prestamos_lista"))
        self.assertEqual(resp_lista.status_code, 200)
        self.assertContains(resp_lista, "Préstamos por autorizar")
        self.assertContains(resp_lista, "Por autorizar por mí")
        self.assertContains(resp_lista, prestamo.folio)

        resp_auth = self.client.post(reverse("rrhh:rrhh_prestamo_auth_jefe", args=[prestamo.pk]), follow=True)
        self.assertEqual(resp_auth.status_code, 200)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_AUTORIZADO)
        self.assertEqual(prestamo.autorizado_jefe, jefe)

    def test_jefe_asignado_ve_y_autoriza_horas_extra_en_su_bandeja(self):
        from datetime import date

        jefe_user = User.objects.create_user(username="carolina", password="pass123")
        jefe_user.groups.add(Group.objects.create(name="PRODUCCION"))
        jefe_empleado = Empleado.objects.create(nombre="Carolina Cayetano", usuario_erp=jefe_user)
        empleado = Empleado.objects.create(
            nombre="Empleado Produccion HE",
            area="HORNOS",
            salario_diario="400.00",
            jefe_directo=jefe_empleado,
        )
        hora_extra = HoraExtra.objects.create(
            empleado=empleado,
            jefe_directo=jefe_user,
            fecha=date(2026, 5, 20),
            horas=Decimal("1.50"),
            notas="Carga de producción",
        )

        self.client.force_login(jefe_user)
        resp_lista = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertEqual(resp_lista.status_code, 200)
        self.assertContains(resp_lista, "Empleado Produccion HE")
        self.assertContains(resp_lista, "Autorizar")

        resp_auth = self.client.post(
            reverse("rrhh:rrhh_he_list"),
            {"hora_extra_id": str(hora_extra.id), "action": "autorizar"},
            follow=True,
        )
        self.assertEqual(resp_auth.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.autorizado_por, jefe_user)
        self.assertIsNotNone(hora_extra.fecha_autorizacion_jefe)

    def test_prestamo_tiene_formato_imprimible_y_regresos_a_dashboard(self):
        from datetime import date

        empleado = Empleado.objects.create(nombre="Empleado Formato", salario_diario="400.00")
        prestamo = Prestamo.objects.create(
            empleado=empleado,
            concepto="Formato papel",
            fecha_solicitud=date(2026, 5, 18),
            importe=Decimal("900.00"),
            num_quincenas=3,
            descuento_quincenal=Decimal("300.00"),
            saldo_actual=Decimal("900.00"),
            estado=Prestamo.ESTADO_SOLICITADO,
            creado_por=self.user,
        )

        resp_print = self.client.get(reverse("rrhh:rrhh_prestamo_imprimir", args=[prestamo.pk]))
        self.assertEqual(resp_print.status_code, 200)
        self.assertContains(resp_print, "SOLICITUD Y AUTORIZACIÓN DE PRÉSTAMO")
        self.assertContains(resp_print, "Firma del empleado")

        resp_quincena = self.client.get(reverse("rrhh:rrhh_quincena_cobros"))
        self.assertContains(resp_quincena, "Volver a préstamos")

        resp_importar = self.client.get(reverse("rrhh:rrhh_importar_contpaq"))
        self.assertContains(resp_importar, "Volver a préstamos")

    def test_nomina_create_and_line(self):
        self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Nómina",
                "area": "Ventas",
                "puesto": "Vendedor",
                "salario_diario": "350.00",
            },
            follow=True,
        )

        resp_nomina_index = self.client.get(reverse("rrhh:nomina"))
        self.assertEqual(resp_nomina_index.status_code, 200)
        self.assertContains(resp_nomina_index, "Cadena documental ERP")
        self.assertContains(resp_nomina_index, "Cadena troncal de RRHH")
        self.assertContains(resp_nomina_index, "Ruta crítica ERP")
        self.assertContains(resp_nomina_index, "Radar ejecutivo ERP")
        self.assertContains(resp_nomina_index, "Depende de")
        self.assertContains(resp_nomina_index, "Dependencia")
        self.assertContains(resp_nomina_index, "Madurez ERP de RRHH")
        self.assertContains(resp_nomina_index, "Criterios de cierre ERP")
        self.assertContains(resp_nomina_index, "Cierre global")
        self.assertContains(resp_nomina_index, "Cadena de control de RRHH")
        self.assertContains(resp_nomina_index, "Entrega de RRHH a downstream")
        self.assertContains(resp_nomina_index, "Cierre por etapa documental")
        self.assertContains(resp_nomina_index, "Mesa de gobierno ERP")
        self.assertContains(resp_nomina_index, "Centro de mando ERP")
        self.assertContains(resp_nomina_index, "Responsable")
        self.assertContains(resp_nomina_index, "Cierre")
        self.assertContains(resp_nomina_index, "Salud operativa ERP")
        self.assertTrue(resp_nomina_index.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_nomina_index.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp_nomina_index.context)
        self.assertIn("critical_path_rows", resp_nomina_index.context)
        self.assertIn("handoff_map", resp_nomina_index.context)
        self.assertIn("owner", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("depends_on", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("next_step", resp_nomina_index.context["handoff_map"][0])
        self.assertIn("completion", resp_nomina_index.context["handoff_map"][0])
        self.assertTrue(resp_nomina_index.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp_nomina_index.context)
        self.assertIn("executive_radar_rows", resp_nomina_index.context)
        self.assertIn("erp_command_center", resp_nomina_index.context)
        self.assertIn("owner", resp_nomina_index.context["document_stage_rows"][0])
        self.assertIn("completion", resp_nomina_index.context["document_stage_rows"][0])
        self.assertTrue(resp_nomina_index.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_nomina_index.context)
        self.assertTrue(resp_nomina_index.context["operational_health_cards"])

        resp_nomina = self.client.post(
            reverse("rrhh:nomina"),
            {
                "tipo_periodo": "QUINCENAL",
                "fecha_inicio": "2026-02-01",
                "fecha_fin": "2026-02-15",
                "estatus": "BORRADOR",
            },
            follow=True,
        )
        self.assertEqual(resp_nomina.status_code, 200)
        self.assertContains(resp_nomina, "Capturar línea de nómina")
        self.assertContains(resp_nomina, "Cadena documental ERP")
        self.assertContains(resp_nomina, "Cadena troncal de RRHH")
        self.assertContains(resp_nomina, "Ruta crítica ERP")
        self.assertContains(resp_nomina, "Radar ejecutivo ERP")
        self.assertContains(resp_nomina, "Depende de")
        self.assertContains(resp_nomina, "Dependencia")
        self.assertContains(resp_nomina, "Madurez ERP de RRHH")
        self.assertContains(resp_nomina, "Criterios de cierre ERP")
        self.assertContains(resp_nomina, "Cierre global")
        self.assertContains(resp_nomina, "Cadena de control de RRHH")
        self.assertContains(resp_nomina, "Entrega de RRHH a downstream")
        self.assertContains(resp_nomina, "Cierre por etapa documental")
        self.assertContains(resp_nomina, "Mesa de gobierno ERP")
        self.assertContains(resp_nomina, "Centro de mando ERP")
        self.assertContains(resp_nomina, "Responsable")
        self.assertContains(resp_nomina, "Cierre")
        self.assertContains(resp_nomina, "Salud operativa ERP")
        self.assertTrue(resp_nomina.context["enterprise_chain"])
        self.assertIn("dependency_status", resp_nomina.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp_nomina.context)
        self.assertIn("critical_path_rows", resp_nomina.context)
        self.assertIn("handoff_map", resp_nomina.context)
        self.assertIn("owner", resp_nomina.context["handoff_map"][0])
        self.assertIn("depends_on", resp_nomina.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp_nomina.context["handoff_map"][0])
        self.assertIn("next_step", resp_nomina.context["handoff_map"][0])
        self.assertIn("completion", resp_nomina.context["handoff_map"][0])
        self.assertTrue(resp_nomina.context["document_stage_rows"])
        self.assertIn("erp_governance_rows", resp_nomina.context)
        self.assertIn("executive_radar_rows", resp_nomina.context)
        self.assertIn("erp_command_center", resp_nomina.context)
        self.assertIn("owner", resp_nomina.context["document_stage_rows"][0])
        self.assertIn("completion", resp_nomina.context["document_stage_rows"][0])
        self.assertTrue(resp_nomina.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_nomina.context)
        self.assertTrue(resp_nomina.context["operational_health_cards"])

        from rrhh.models import Empleado, NominaPeriodo

        empleado = Empleado.objects.get(nombre="Empleado Nómina")
        periodo = NominaPeriodo.objects.first()

        resp_line = self.client.post(
            reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            {
                "action": "add_line",
                "empleado_id": empleado.id,
                "dias_trabajados": "15",
                "bonos": "500",
                "descuentos": "120",
            },
            follow=True,
        )
        self.assertEqual(resp_line.status_code, 200)
        self.assertContains(resp_line, "Cadena documental ERP")
        self.assertContains(resp_line, "Cadena troncal de RRHH")
        self.assertContains(resp_line, "Ruta crítica ERP")
        self.assertContains(resp_line, "Radar ejecutivo ERP")
        self.assertContains(resp_line, "Depende de")
        self.assertContains(resp_line, "Dependencia")
        self.assertContains(resp_line, "Madurez ERP de RRHH")
        self.assertContains(resp_line, "Criterios de cierre ERP")
        self.assertContains(resp_line, "Cierre global")
        self.assertContains(resp_line, "Cadena de control de RRHH")
        self.assertContains(resp_line, "Entrega de RRHH a downstream")
        self.assertContains(resp_line, "Cierre por etapa documental")
        self.assertContains(resp_line, "Mesa de gobierno ERP")
        self.assertContains(resp_line, "Centro de mando ERP")
        self.assertContains(resp_line, "Responsable")
        self.assertContains(resp_line, "Cierre")
        self.assertContains(resp_line, "Salud operativa ERP")
        self.assertIn("dependency_status", resp_line.context["enterprise_chain"][0])
        self.assertIn("maturity_summary", resp_line.context)
        self.assertIn("critical_path_rows", resp_line.context)
        self.assertIn("handoff_map", resp_line.context)
        self.assertIn("owner", resp_line.context["handoff_map"][0])
        self.assertIn("depends_on", resp_line.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp_line.context["handoff_map"][0])
        self.assertIn("next_step", resp_line.context["handoff_map"][0])
        self.assertIn("completion", resp_line.context["handoff_map"][0])
        self.assertIn("erp_governance_rows", resp_line.context)
        self.assertIn("executive_radar_rows", resp_line.context)
        self.assertIn("erp_command_center", resp_line.context)
        self.assertIn("owner", resp_line.context["document_stage_rows"][0])
        self.assertIn("completion", resp_line.context["document_stage_rows"][0])
        self.assertTrue(resp_line.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp_line.context)
        periodo.refresh_from_db()
        self.assertGreater(periodo.total_neto, 0)

    def test_redirect_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("rrhh:empleados"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)


class ListaRayaImportTests(TestCase):
    @classmethod
    def setUpClass(cls):
        if not LISTA_RAYA_SAMPLE.exists():
            raise SkipTest("No está disponible el archivo real de lista de raya.")
        super().setUpClass()

    def test_parser_cuadra_con_totales_generales(self):
        result = parse_lista_raya_xls(LISTA_RAYA_SAMPLE)

        self.assertEqual(result.fecha_inicio.isoformat(), "2026-04-16")
        self.assertEqual(result.fecha_fin.isoformat(), "2026-04-30")
        self.assertEqual(len(result.empleados), 67)
        self.assertEqual(result.total_percepciones_calculado, result.total_percepciones_reportado)
        self.assertEqual(result.total_deducciones_calculado, result.total_deducciones_reportado)
        self.assertEqual(result.total_neto_calculado, result.total_neto_reportado)
        self.assertEqual(result.empleados[0].codigo, "2")
        self.assertEqual(result.empleados[0].rfc, "LOMM-750126-DB4")
        self.assertEqual(result.empleados[0].curp, "LOMM-750126-MSLPDR06")

    def test_command_dry_run_no_toca_base(self):
        call_command("importar_lista_raya", str(LISTA_RAYA_SAMPLE))

        self.assertEqual(Empleado.objects.count(), 0)
        self.assertEqual(NominaPeriodo.objects.count(), 0)
        self.assertEqual(NominaImportacion.objects.count(), 0)

    def test_command_commit_importa_periodo_y_conceptos(self):
        call_command("importar_lista_raya", str(LISTA_RAYA_SAMPLE), "--commit")

        periodo = NominaPeriodo.objects.get(fecha_inicio="2026-04-16", fecha_fin="2026-04-30")
        self.assertEqual(Empleado.objects.count(), 67)
        self.assertEqual(periodo.lineas.count(), 67)
        self.assertEqual(NominaConceptoLinea.objects.count(), 336)
        self.assertEqual(periodo.total_bruto, periodo.importaciones.first().total_percepciones)
        self.assertEqual(periodo.total_descuentos, periodo.importaciones.first().total_deducciones)
        self.assertEqual(periodo.total_neto, periodo.importaciones.first().total_neto)
