from django.contrib.auth.models import Group, User
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from decimal import Decimal
from pathlib import Path
from rest_framework.test import APIClient
from unittest import SkipTest
from unittest.mock import patch

from core.models import Notificacion, Sucursal, UserModuleAccess, UserProfile
from rrhh.models import (
    AsistenciaEmpleado,
    BonoEsquema,
    CatalogoFuncionOperativa,
    Empleado,
    EmpleadoBaja,
    EmpleadoIdentidadPendiente,
    HoraExtra,
    NominaConceptoLinea,
    NominaImportacion,
    NominaLinea,
    NominaPeriodo,
    PlantillaAutorizada,
    PermisoSalida,
    PoliticaVacaciones,
    Prestamo,
    PrestamoCuota,
    ReglamentoLaboral,
    ReglaLaboral,
    SolicitudVacaciones,
    Turno,
    VacanteRRHH,
)
from rrhh.services.lista_raya import parse_lista_raya_xls


LISTA_RAYA_SAMPLE = Path("/Users/mauricioburgos/Downloads/Lista de raya del 16 al 31 de abril 2026.xls")


class CapitalHumanoServiceTests(TestCase):
    def test_vacaciones_calcula_dias_laborables_y_reserva_saldo(self):
        from datetime import date

        from rrhh.services_vacaciones import (
            aprobar_solicitud_vacaciones_rrhh,
            crear_solicitud_vacaciones,
            saldo_vacaciones_empleado,
        )

        rrhh_user = User.objects.create_user(username="paula")
        rrhh_user.groups.add(Group.objects.create(name="RRHH"))
        empleado = Empleado.objects.create(
            nombre="Colaborador Vacaciones",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )

        solicitud = crear_solicitud_vacaciones(
            empleado=empleado,
            fecha_inicio=date(2026, 6, 8),
            fecha_fin=date(2026, 6, 12),
            motivo="Descanso programado",
            actor=rrhh_user,
        )

        self.assertEqual(solicitud.estado, SolicitudVacaciones.ESTADO_SOLICITADA)
        self.assertEqual(solicitud.dias_laborables, Decimal("5"))
        saldo = saldo_vacaciones_empleado(empleado, periodo_anio=2026)
        self.assertEqual(saldo["generado"], Decimal("12.00"))
        self.assertEqual(saldo["reservado"], Decimal("5"))
        self.assertEqual(saldo["disponible"], Decimal("7.00"))

        aprobar_solicitud_vacaciones_rrhh(solicitud, rrhh_user)
        saldo = saldo_vacaciones_empleado(empleado, periodo_anio=2026)
        self.assertEqual(saldo["consumido"], Decimal("5"))
        self.assertEqual(saldo["reservado"], Decimal("0"))
        self.assertEqual(saldo["disponible"], Decimal("7.00"))

    def test_jefe_crea_y_preautoriza_vacaciones_de_equipo(self):
        from datetime import date

        jefe_user = User.objects.create_user(username="johana")
        jefa = Empleado.objects.create(
            nombre="Johana Lopez",
            fecha_ingreso=date(2023, 1, 1),
            usuario_erp=jefe_user,
            activo=True,
        )
        colaborador = Empleado.objects.create(
            nombre="Cajera Operativa",
            fecha_ingreso=date(2025, 1, 1),
            jefe_directo=jefa,
            activo=True,
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )

        self.client.force_login(jefe_user)
        response = self.client.post(
            reverse("rrhh:rrhh_vacaciones_list"),
            {
                "action": "crear",
                "empleado_id": colaborador.id,
                "fecha_inicio": "2026-06-15",
                "fecha_fin": "2026-06-19",
                "motivo": "Solicitud capturada por jefa directa",
            },
        )

        self.assertEqual(response.status_code, 302)
        solicitud = SolicitudVacaciones.objects.get(empleado=colaborador)
        self.assertEqual(solicitud.jefe_directo, jefe_user)

        response = self.client.post(
            reverse("rrhh:rrhh_vacaciones_list"),
            {"action": "preautorizar_jefe", "solicitud_id": solicitud.id},
        )
        self.assertEqual(response.status_code, 302)
        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estado, SolicitudVacaciones.ESTADO_PREAUTORIZADA)
        self.assertEqual(solicitud.preautorizado_por, jefe_user)

    def test_reglamento_interno_renderiza_reglas_vacaciones(self):
        reglamento = ReglamentoLaboral.objects.create(
            nombre="Reglamento interno FONSMA",
            version="2026-04-09",
            estado=ReglamentoLaboral.ESTADO_VIGENTE,
        )
        ReglaLaboral.objects.create(
            reglamento=reglamento,
            clave="art-30",
            articulo="ARTICULO 30",
            tipo=ReglaLaboral.TIPO_VACACIONES,
            titulo="Cómputo por días laborables",
            texto="Solo se incluyen días laborables.",
        )
        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)
        rrhh_user.groups.add(Group.objects.create(name="RRHH"))

        self.client.force_login(rrhh_user)
        response = self.client.get(reverse("rrhh:rrhh_reglamento_interno"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Reglamento interno FONSMA")
        self.assertContains(response, "Cómputo por días laborables")

    def test_organizacion_renderiza_boton_reglamento(self):
        from datetime import date

        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)
        ReglamentoLaboral.objects.create(
            nombre="Reglamento interno FONSMA",
            version="2026-04-09",
            estado=ReglamentoLaboral.ESTADO_VIGENTE,
        )
        Empleado.objects.create(
            nombre="Colaborador Vacaciones",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )

        self.client.force_login(rrhh_user)
        response = self.client.get(reverse("rrhh:rrhh_organizacion"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mapa organizacional")
        self.assertContains(response, "Ver reglamento interno")

    def test_vacaciones_list_no_muestra_boton_reglamento(self):
        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)

        self.client.force_login(rrhh_user)
        response = self.client.get(reverse("rrhh:rrhh_vacaciones_list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Solicitud de vacaciones")
        self.assertNotContains(response, "Ver reglamento interno")

    def test_vacaciones_list_expone_puesto_del_empleado_en_selector(self):
        from datetime import date

        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)
        Empleado.objects.create(
            nombre="Johana Lopez",
            fecha_ingreso=date(2024, 1, 1),
            activo=True,
            puesto="Jefa de Ventas",
            puesto_operativo="CAJAS",
            departamento=Empleado.DEP_VENTAS,
            area="VENTAS",
            sucursal="Matriz",
        )

        self.client.force_login(rrhh_user)
        response = self.client.get(reverse("rrhh:rrhh_vacaciones_list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-puesto="Jefa de Ventas"')
        self.assertContains(response, 'data-puesto-operativo="CAJAS"')
        self.assertContains(response, 'data-departamento="Ventas"')
        self.assertContains(response, "El puesto y área aparecerán automáticamente.")

    def test_permiso_de_jefatura_lo_resuelve_direccion_no_rrhh(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from rrhh.services_permisos import resolver_permiso_direccion

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
        self.assertEqual(response.status_code, 403)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
        self.assertIsNone(permiso.autorizado_por)

        resolver_permiso_direccion(permiso, dg_user, aprobar=True)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_AUTORIZADO)
        self.assertEqual(permiso.autorizado_direccion_por, dg_user)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertEqual(permiso.autorizado_por, dg_user)

    def test_permiso_operativo_lo_resuelve_jefe_directo_no_rrhh(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from rrhh.services_permisos import resolver_permiso_jefe

        rrhh_user = User.objects.create_user(username="paula")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        jefe_user = User.objects.create_user(username="johana")
        jefa_ventas = Empleado.objects.create(
            nombre="Johana Lopez",
            departamento=Empleado.DEP_VENTAS,
            puesto="Jefe de Ventas",
            usuario_erp=jefe_user,
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
            estado_jefe=PermisoSalida.ESTADO_JEFE_PENDIENTE,
        )

        self.assertFalse(permiso.requiere_direccion)
        self.assertEqual(permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_NO_REQUIERE)

        self.client.force_login(rrhh_user)
        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "aprobar"},
        )
        self.assertEqual(response.status_code, 403)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
        self.assertIsNone(permiso.autorizado_por)

        resolver_permiso_jefe(permiso, jefe_user, aprobar=True)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PREAUTORIZADO)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertEqual(permiso.autorizado_por, jefe_user)

    def test_supervisora_y_encargada_produccion_las_resuelve_jefe_directo(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        carolina_user = User.objects.create_user(username="carolina.cayetano")
        carolina = Empleado.objects.create(
            nombre="CAYETANO VALENZUELA CAROLINA",
            departamento=Empleado.DEP_PRODUCCION,
            puesto="Jefe de Produccion",
            usuario_erp=carolina_user,
        )
        roxana = Empleado.objects.create(
            nombre="RIVAS SOLIS ROXANA",
            departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo="Supervisora de Produccion",
            jefe_directo=carolina,
        )
        julissa = Empleado.objects.create(
            nombre="ANGULO PARRA JULISSA",
            departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo="Encargada de Produccion",
            jefe_directo=carolina,
        )

        for empleado in (roxana, julissa):
            permiso = PermisoSalida.objects.create(
                empleado=empleado,
                tipo=PermisoSalida.TIPO_PERMISO_HORA,
                fecha_inicio=datetime(2026, 5, 27, 15, 0, tzinfo=ZoneInfo("America/Mazatlan")),
                fecha_fin=datetime(2026, 5, 27, 16, 0, tzinfo=ZoneInfo("America/Mazatlan")),
                motivo="Salida temprano",
            )
            self.assertFalse(permiso.requiere_direccion, empleado.nombre)
            self.assertEqual(permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_NO_REQUIERE)

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

        self.assertEqual(response.status_code, 403)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
        self.assertIsNone(permiso.autorizado_por)

    def test_permiso_de_capital_humano_no_lo_autoriza_la_misma_persona_rrhh(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        rrhh_user = User.objects.create_user(username="paula", first_name="Paula", last_name="Lugo")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        paula = Empleado.objects.create(
            nombre="LUGO ESPINOZA PAULA ELIZABETH",
            departamento=Empleado.DEP_RRHH,
            puesto="RRHH",
            usuario_erp=rrhh_user,
        )
        permiso = PermisoSalida.objects.create(
            empleado=paula,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=datetime(2026, 5, 26, 13, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            fecha_fin=datetime(2026, 5, 26, 15, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            motivo="Permiso Capital Humano",
        )

        self.assertTrue(permiso.requiere_direccion)
        self.client.force_login(rrhh_user)
        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "aprobar"},
        )

        self.assertEqual(response.status_code, 403)
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

    def assertHoraLocal(self, dt, hora, minuto=0):
        local_dt = timezone.localtime(dt)
        self.assertEqual((local_dt.hour, local_dt.minute), (hora, minuto))

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

    def test_permiso_api_no_permite_aprobacion_rrhh(self):
        rrhh_user = User.objects.create_user(username="rrhh.api", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        permiso = PermisoSalida.objects.create(
            empleado=self.empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=timezone.datetime(2026, 5, 14, 10, 0, tzinfo=timezone.get_current_timezone()),
            motivo="Permiso operativo",
            estado_jefe=PermisoSalida.ESTADO_JEFE_PREAUTORIZADO,
        )
        self.client.force_authenticate(user=rrhh_user)

        resp = self.client.post(reverse("rrhh:permiso-aprobar", args=[permiso.id]))

        self.assertEqual(resp.status_code, 403)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
        self.assertIsNone(permiso.autorizado_por)

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

        for url_name in ["rrhh_dashboard", "rrhh_prestamos_lista", "rrhh_monitor_sync", "rrhh_importar"]:
            resp = self.client.get(reverse(f"rrhh:{url_name}"))
            self.assertEqual(resp.status_code, 200)
        resp_importar = self.client.get(reverse("rrhh:rrhh_importar"))
        self.assertContains(resp_importar, "Carga y sincronización de asistencia")
        self.assertContains(resp_importar, "Cargar archivo al ERP")
        self.assertContains(resp_importar, "Últimas lecturas automáticas")
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

    def test_isapi_normaliza_eventos_validos_del_checador(self):
        from rrhh.services_hikvision import normalizar_eventos_isapi

        eventos = normalizar_eventos_isapi(
            {
                "AcsEvent": {
                    "InfoList": [
                        {
                            "name": "EVERARDO RODRIGUEZ LIZARRAGA",
                            "employeeNoString": "340",
                            "time": "2026-05-01T11:27:06-07:00",
                            "attendanceStatus": "checkIn",
                            "label": "ENTRADA",
                            "serialNo": 235726,
                        },
                        {
                            "time": "2026-05-01T11:27:06-07:00",
                            "label": "ENTRADA",
                            "serialNo": 235727,
                        },
                    ]
                }
            }
        )

        self.assertEqual(len(eventos), 1)
        self.assertEqual(eventos[0]["employee_no"], "340")
        self.assertEqual(eventos[0]["attendance_status"], "checkIn")
        self.assertEqual(eventos[0]["serial_no"], 235726)

    def test_procesar_eventos_hik_conserva_flujo_dos_marcajes(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        empleado = Empleado.objects.create(nombre="Empleado ISAPI", codigo="340", salario_diario="400.00")

        resultado = procesar_eventos_hik(
            [
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T08:01:00-07:00",
                    "serial_no": 1,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkOut",
                    "time": "2026-05-01T16:11:00-07:00",
                    "serial_no": 2,
                },
            ]
        )

        self.assertEqual(resultado["procesados"], 2)
        self.assertEqual(resultado["duplicados"], 0)
        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha="2026-05-01")
        self.assertHoraLocal(asistencia.entrada, 8, 1)
        self.assertHoraLocal(asistencia.salida, 16, 11)
        self.assertEqual(asistencia.minutos_trabajados, 490)
        self.assertEqual(asistencia.fuente, AsistenciaEmpleado.FUENTE_HIKCONNECT_API)

    def test_procesar_eventos_hik_asigna_cuatro_marcajes_y_descuenta_comida(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        empleado = Empleado.objects.create(nombre="Empleado ISAPI", codigo="340", salario_diario="400.00")

        resultado = procesar_eventos_hik(
            [
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T08:00:00-07:00",
                    "serial_no": 1,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:00:00-07:00",
                    "serial_no": 2,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:35:00-07:00",
                    "serial_no": 3,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkOut",
                    "time": "2026-05-01T17:00:00-07:00",
                    "serial_no": 4,
                },
            ]
        )

        self.assertEqual(resultado["procesados"], 4)
        self.assertEqual(resultado["duplicados"], 0)
        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha="2026-05-01")
        self.assertHoraLocal(asistencia.entrada, 8)
        self.assertHoraLocal(asistencia.salida_comida, 13)
        self.assertHoraLocal(asistencia.regreso_comida, 13, 35)
        self.assertHoraLocal(asistencia.salida, 17)
        self.assertEqual(asistencia.minutos_comida, 35)
        self.assertEqual(asistencia.minutos_trabajados, 505)
        self.assertEqual(asistencia.fuente, AsistenciaEmpleado.FUENTE_HIKCONNECT_API)

    def test_procesar_eventos_hik_reclasifica_cuatro_marcajes_en_corridas_incrementales(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        empleado = Empleado.objects.create(nombre="Empleado ISAPI", codigo="340", salario_diario="400.00")

        procesar_eventos_hik(
            [
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T08:00:00-07:00",
                    "serial_no": 1,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:00:00-07:00",
                    "serial_no": 2,
                },
            ]
        )

        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha="2026-05-01")
        self.assertHoraLocal(asistencia.entrada, 8)
        self.assertHoraLocal(asistencia.salida_comida, 13)
        self.assertIsNone(asistencia.salida)

        resultado = procesar_eventos_hik(
            [
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:35:00-07:00",
                    "serial_no": 3,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkOut",
                    "time": "2026-05-01T17:15:00-07:00",
                    "serial_no": 4,
                },
            ]
        )

        self.assertEqual(resultado["procesados"], 2)
        asistencia.refresh_from_db()
        self.assertHoraLocal(asistencia.regreso_comida, 13, 35)
        self.assertHoraLocal(asistencia.salida, 17, 15)
        self.assertEqual(asistencia.minutos_comida, 35)
        self.assertEqual(asistencia.minutos_trabajados, 520)

    def test_procesar_eventos_hik_tres_marcajes_quedan_en_revision(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        empleado = Empleado.objects.create(nombre="Empleado ISAPI", codigo="340", salario_diario="400.00")

        procesar_eventos_hik(
            [
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T08:00:00-07:00",
                    "serial_no": 1,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:00:00-07:00",
                    "serial_no": 2,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:35:00-07:00",
                    "serial_no": 3,
                },
            ]
        )

        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha="2026-05-01")
        self.assertHoraLocal(asistencia.entrada, 8)
        self.assertHoraLocal(asistencia.salida_comida, 13)
        self.assertHoraLocal(asistencia.regreso_comida, 13, 35)
        self.assertIsNone(asistencia.salida)
        self.assertEqual(asistencia.minutos_comida, 35)
        self.assertEqual(asistencia.minutos_trabajados, 0)
        self.assertIn("REVISIÓN: 3 marcajes", asistencia.observacion)

    def test_procesar_eventos_hik_cinco_marcajes_usa_ultima_como_salida(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        empleado = Empleado.objects.create(nombre="Empleado ISAPI", codigo="340", salario_diario="400.00")

        procesar_eventos_hik(
            [
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T08:00:00-07:00",
                    "serial_no": 1,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:00:00-07:00",
                    "serial_no": 2,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T13:35:00-07:00",
                    "serial_no": 3,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T16:55:00-07:00",
                    "serial_no": 4,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkOut",
                    "time": "2026-05-01T17:10:00-07:00",
                    "serial_no": 5,
                },
            ]
        )

        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha="2026-05-01")
        self.assertHoraLocal(asistencia.salida, 17, 10)
        self.assertEqual(asistencia.minutos_comida, 35)
        self.assertEqual(asistencia.minutos_trabajados, 515)
        self.assertIn("Marcajes extra", asistencia.observacion)

    def test_procesar_eventos_hik_limpia_observaciones_tecnicas_anteriores(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        empleado = Empleado.objects.create(nombre="Empleado ISAPI", codigo="340", salario_diario="400.00")
        AsistenciaEmpleado.objects.create(
            empleado=empleado,
            fecha="2026-05-01",
            observacion="breakOut@10:02 | breakIn@10:37 | Nota manual RRHH",
        )

        procesar_eventos_hik(
            [
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkIn",
                    "time": "2026-05-01T08:00:00-07:00",
                    "serial_no": 1,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "breakOut",
                    "time": "2026-05-01T13:00:00-07:00",
                    "serial_no": 2,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "breakIn",
                    "time": "2026-05-01T13:35:00-07:00",
                    "serial_no": 3,
                },
                {
                    "employee_no": "340",
                    "name": "Empleado ISAPI",
                    "attendance_status": "checkOut",
                    "time": "2026-05-01T17:00:00-07:00",
                    "serial_no": 4,
                },
            ]
        )

        asistencia = AsistenciaEmpleado.objects.get(empleado=empleado, fecha="2026-05-01")
        self.assertNotIn("breakOut@", asistencia.observacion)
        self.assertNotIn("breakIn@", asistencia.observacion)
        self.assertIn("Nota manual RRHH", asistencia.observacion)

    def test_importar_asistencia_isapi_registra_importacion_api(self):
        from datetime import date

        from rrhh.models import ImportacionChecador
        from rrhh.services_hikvision import importar_asistencia_isapi

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "AcsEvent": {
                        "responseStatusStrg": "OK",
                        "numOfMatches": 1,
                        "InfoList": [
                            {
                                "name": "Empleado ISAPI",
                                "employeeNoString": "341",
                                "time": "2026-05-02T08:01:00-07:00",
                                "attendanceStatus": "checkIn",
                                "serialNo": 10,
                            }
                        ],
                    }
                }

        class FakeSession:
            def post(self, *args, **kwargs):
                return FakeResponse()

        Empleado.objects.create(nombre="Empleado ISAPI", codigo="341", salario_diario="400.00")

        resultado = importar_asistencia_isapi(
            fecha_inicio=date(2026, 5, 2),
            fecha_fin=date(2026, 5, 2),
            base_url="http://127.0.0.1:28073",
            username="admin",
            password="secret",
            session=FakeSession(),
        )

        self.assertEqual(resultado["procesados"], 1)
        self.assertTrue(AsistenciaEmpleado.objects.filter(empleado__codigo="341", fecha="2026-05-02").exists())
        importacion = ImportacionChecador.objects.get()
        self.assertEqual(importacion.metodo, ImportacionChecador.METODO_API)
        self.assertIn("ISAPI", importacion.log)


class RRHHViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="rrhh", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        self.user.groups.add(rrhh_group)
        self.client.login(username="rrhh", password="pass123")

    def test_empleados_view_and_create(self):
        resp = self.client.get(reverse("rrhh:empleados"), secure=True)
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "RRHH · Empleados")
        self.assertContains(resp, "Alta de empleado")
        self.assertContains(resp, "Código de empleado / ID checador")
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

    def test_empleados_crea_y_edita_codigo_operativo(self):
        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "codigo": " 346 ",
                "nombre": "REY IVAN VALDEZ FELIX",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="REY IVAN VALDEZ FELIX")
        self.assertEqual(empleado.codigo, "346")

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "update",
                "empleado_id": str(empleado.id),
                "codigo": " 00346 ",
                "nombre": "REY IVAN VALDEZ FELIX",
                "salario_diario": "300.00",
                "activo": "on",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.codigo, "00346")

    def test_empleados_liga_usuario_repartidor_y_crea_identidad_logistica(self):
        from logistica.models import Repartidor

        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        usuario = User.objects.create_user(username="ivan.felix", password="pass123")

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "nombre": "VALDEZ FÉLIX REY IVÁN",
                "area": "REPARTIDORES",
                "usuario_erp": str(usuario.id),
                "sucursal_app_id": str(sucursal.id),
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="VALDEZ FÉLIX REY IVÁN")
        usuario.refresh_from_db()
        self.assertEqual(empleado.usuario_erp, usuario)
        self.assertEqual(usuario.get_full_name(), "VALDEZ FÉLIX REY IVÁN")
        self.assertTrue(usuario.groups.filter(name="repartidor").exists())
        self.assertEqual(UserProfile.objects.get(user=usuario).sucursal, sucursal)
        repartidor = Repartidor.objects.get(user=usuario)
        self.assertEqual(repartidor.sucursal, sucursal)
        self.assertEqual(str(repartidor), "VALDEZ FÉLIX REY IVÁN")

    def test_empleados_crea_usuario_repartidor_con_password_y_licencia(self):
        from logistica.models import Repartidor

        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        archivo = SimpleUploadedFile("licencia.pdf", b"PDF demo", content_type="application/pdf")

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "nombre": "REPARTIDOR NUEVO",
                "area": "REPARTIDORES",
                "crear_usuario_erp": "on",
                "nuevo_usuario_username": "rep.nuevo",
                "nuevo_usuario_password": "Temporal123",
                "sucursal_app_id": str(sucursal.id),
                "numero_licencia": "LIC-123",
                "licencia_expedicion": "2026-01-01",
                "licencia_expiracion": "2028-01-01",
                "archivo_licencia": archivo,
                "salario_diario": "300.00",
            },
            follow=True,
            secure=True,
        )

        self.assertEqual(resp.status_code, 200)
        usuario = User.objects.get(username="rep.nuevo")
        empleado = Empleado.objects.get(nombre="REPARTIDOR NUEVO")
        repartidor = Repartidor.objects.get(user=usuario)
        self.assertEqual(empleado.usuario_erp, usuario)
        self.assertTrue(usuario.check_password("Temporal123"))
        self.assertEqual(usuario.get_full_name(), "REPARTIDOR NUEVO")
        self.assertTrue(usuario.groups.filter(name="repartidor").exists())
        self.assertEqual(UserProfile.objects.get(user=usuario).sucursal, sucursal)
        self.assertEqual(repartidor.numero_licencia, "LIC-123")
        self.assertEqual(str(repartidor.licencia_expedicion), "2026-01-01")
        self.assertEqual(str(repartidor.licencia_expiracion), "2028-01-01")
        self.assertIn("licencia", repartidor.archivo_licencia.name)

    def test_empleados_autoriza_conductor_occasional_sin_grupo_repartidor(self):
        from api.logistica_views import _can_operate_pwa
        from logistica.models import Repartidor

        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        usuario = User.objects.create_user(username="carolina.cayetano", password="pass123")
        usuario.groups.add(Group.objects.create(name="repartidor"))

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "nombre": "CAYETANO VALENZUELA CAROLINA",
                "area": "EMBETUNADO",
                "puesto_operativo": "EMBETUNADO",
                "usuario_erp": str(usuario.id),
                "sucursal_app_id": str(sucursal.id),
                "logistica_tipo_identidad": "empleado_conductor_ocasional",
                "motivo_autorizacion": "Vueltas de la empresa",
                "autorizado_por": "Dirección",
                "notas_identidad": "No es repartidora operativa.",
                "numero_licencia": "LIC-OCASIONAL",
                "licencia_expedicion": "2026-01-01",
                "licencia_expiracion": "2028-01-01",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="CAYETANO VALENZUELA CAROLINA")
        usuario.refresh_from_db()
        repartidor = Repartidor.objects.get(user=usuario)
        self.assertEqual(empleado.usuario_erp, usuario)
        self.assertEqual(UserProfile.objects.get(user=usuario).sucursal, sucursal)
        self.assertEqual(repartidor.tipo_identidad, Repartidor.TIPO_EMPLEADO_CONDUCTOR_OCASIONAL)
        self.assertEqual(repartidor.motivo_autorizacion, "Vueltas de la empresa")
        self.assertEqual(repartidor.autorizado_por, "Dirección")
        self.assertEqual(repartidor.notas_identidad, "No es repartidora operativa.")
        self.assertEqual(repartidor.numero_licencia, "LIC-OCASIONAL")
        self.assertFalse(usuario.groups.filter(name="repartidor").exists())
        self.assertTrue(_can_operate_pwa(usuario))

    def test_empleados_bloquea_crear_usuario_duplicado_desde_rrhh(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        User.objects.create_user(username="rep.duplicado", password="pass12345")

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "nombre": "REPARTIDOR DUPLICADO",
                "area": "REPARTIDORES",
                "crear_usuario_erp": "on",
                "nuevo_usuario_username": "rep.duplicado",
                "nuevo_usuario_password": "Temporal123",
                "sucursal_app_id": str(sucursal.id),
                "salario_diario": "300.00",
            },
            follow=True,
            secure=True,
        )

        self.assertEqual(resp.status_code, 200)
        self.assertFalse(Empleado.objects.filter(nombre="REPARTIDOR DUPLICADO").exists())
        self.assertEqual(User.objects.filter(username="rep.duplicado").count(), 1)
        self.assertContains(resp, "Ese usuario ya existe")

    def test_identidad_operativa_limpia_sucursal_app(self):
        from rrhh.services_identidad import asegurar_identidad_operativa_empleado

        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        usuario = User.objects.create_user(username="usuario.sucursal", password="pass123")
        empleado = Empleado.objects.create(nombre="Empleado Sucursal", usuario_erp=usuario)
        UserProfile.objects.create(user=usuario, sucursal=sucursal)

        asegurar_identidad_operativa_empleado(empleado, sucursal_app_id=None)

        self.assertIsNone(UserProfile.objects.get(user=usuario).sucursal)

    def test_empleados_bloquea_codigo_duplicado(self):
        existente = Empleado.objects.create(nombre="Empleado Existente", codigo="346")
        otro = Empleado.objects.create(nombre="Empleado Otro", codigo="999")

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "update",
                "empleado_id": str(otro.id),
                "codigo": "346",
                "nombre": otro.nombre,
                "salario_diario": "300.00",
                "activo": "on",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        otro.refresh_from_db()
        self.assertEqual(otro.codigo, "999")
        self.assertContains(resp, f"El código 346 ya pertenece a {existente.nombre}")

    def test_checador_desconocido_crea_pendiente_con_sugerencia_por_nombre(self):
        from rrhh.services_hikvision import procesar_eventos_hik

        empleado = Empleado.objects.create(nombre="REY IVAN VALDEZ FELIX", codigo="EMP-2606-001")

        resultado = procesar_eventos_hik(
            [
                {
                    "employee_no": "346",
                    "name": "REY IVAN VALDEZ FELIX",
                    "attendance_status": "checkIn",
                    "time": "2026-06-04T08:05:00-07:00",
                    "serial_no": 34601,
                }
            ]
        )

        self.assertEqual(resultado["errores"], 1)
        pendiente = EmpleadoIdentidadPendiente.objects.get(fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION, codigo_externo="346")
        self.assertEqual(pendiente.nombre_externo, "REY IVAN VALDEZ FELIX")
        self.assertEqual(pendiente.empleado_sugerido, empleado)
        self.assertEqual(Empleado.objects.filter(nombre="REY IVAN VALDEZ FELIX").count(), 1)
        resp = self.client.get(reverse("rrhh:empleados"), secure=True)
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Código recibido")
        self.assertContains(resp, "Nombre recibido")
        self.assertContains(resp, "Empleado sugerido")
        self.assertContains(resp, "No crea otro empleado")
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
        self.assertTrue(resp.context["release_gate_rows"])
        self.assertIn("release_gate_completion", resp.context)
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Demo",
                "area": "HORNOS",
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
                "area": "ALMACEN",
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
        self.assertEqual(empleado.area, "ALMACEN")

    def test_identidad_pendiente_codigo_existente_se_cierra_desde_conciliacion(self):
        empleado = Empleado.objects.create(nombre="ANAYA BERNAL CARLOS EZEQUIEL", codigo="347")
        pendiente = EmpleadoIdentidadPendiente.objects.create(
            fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION,
            codigo_externo="347",
            nombre_externo="CARLOS EZEQUIEL ANAYA BERNAL",
        )

        resp = self.client.get(reverse("rrhh:empleados"), secure=True)

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Código ya está en RRHH")
        self.assertContains(resp, "Cerrar conciliado")

        resp_post = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "cerrar_identidad_codigo",
                "pendiente_id": str(pendiente.id),
            },
            follow=True,
            secure=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        pendiente.refresh_from_db()
        self.assertEqual(pendiente.estado, EmpleadoIdentidadPendiente.ESTADO_VINCULADO)
        self.assertEqual(pendiente.empleado_sugerido, empleado)
        self.assertEqual(pendiente.resuelto_por, self.user)
        self.assertIsNotNone(pendiente.resuelto_en)
        self.assertContains(resp_post, "ya conciliado")

    def test_identidad_pendiente_permite_vinculo_manual_sin_sugerencia(self):
        empleado = Empleado.objects.create(nombre="Empleado Revisión Manual", codigo="EMP-2606-001")
        pendiente = EmpleadoIdentidadPendiente.objects.create(
            fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION,
            codigo_externo="348",
            nombre_externo="NOMBRE EXTERNO SIN MATCH",
        )

        resp = self.client.get(reverse("rrhh:empleados"), secure=True)

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Revisión manual")
        self.assertContains(resp, "Aceptar vínculo")

        resp_post = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "vincular_identidad",
                "pendiente_id": str(pendiente.id),
                "empleado_id": str(empleado.id),
            },
            follow=True,
            secure=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        empleado.refresh_from_db()
        pendiente.refresh_from_db()
        self.assertEqual(empleado.codigo, "348")
        self.assertEqual(pendiente.estado, EmpleadoIdentidadPendiente.ESTADO_VINCULADO)
        self.assertEqual(pendiente.empleado_sugerido, empleado)
        self.assertContains(resp_post, "Conciliación cerrada")

    def test_identidad_pendiente_permite_descartar_con_nota(self):
        pendiente = EmpleadoIdentidadPendiente.objects.create(
            fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION,
            codigo_externo="999",
            nombre_externo="DATO ENVIADO POR ERROR",
        )

        resp_post = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "descartar_identidad",
                "pendiente_id": str(pendiente.id),
                "notas_resolucion": "No corresponde a personal activo.",
            },
            follow=True,
            secure=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        pendiente.refresh_from_db()
        self.assertEqual(pendiente.estado, EmpleadoIdentidadPendiente.ESTADO_DESCARTADO)
        self.assertEqual(pendiente.resuelto_por, self.user)
        self.assertIn("No corresponde a personal activo.", pendiente.notas)
        self.assertContains(resp_post, "descartado")

    def test_empleados_baja_expone_datos_para_autollenado(self):
        empleado = Empleado.objects.create(
            nombre="Empleado Baja Autollenado",
            codigo="BAJA-001",
            area="Producción",
            puesto="Pastelero",
            fecha_ingreso="2026-01-15",
            tipo_contrato=Empleado.CONTRATO_FIJO,
            salario_diario="450.00",
        )

        resp = self.client.get(reverse("rrhh:empleados"), secure=True)

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, f'value="{empleado.id}"')
        self.assertContains(resp, 'data-nombre="Empleado Baja Autollenado"')
        self.assertContains(resp, 'data-area="Producción"')
        self.assertContains(resp, 'data-puesto="Pastelero"')
        self.assertContains(resp, 'data-fecha-ingreso="2026-01-15"')
        self.assertContains(resp, f'data-tipo-contrato="{Empleado.CONTRATO_FIJO}"')

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
        embetunado = Empleado.objects.create(nombre="EMBETUNADO DEMO", area="PRODUCCION")
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
        embetunado.refresh_from_db()
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
        self.assertEqual(embetunado.departamento, "PRODUCCION")
        self.assertEqual(embetunado.puesto_operativo, "EMBETUNADO")
        self.assertTrue(embetunado.participa_bonos_produccion)
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
        self.assertContains(resp, "Mapa de identidad y accesos")
        self.assertContains(resp, "Lectura dry-run, sin escrituras.")
        self.assertContains(resp, "Ver equipo")
        self.assertContains(resp, "Puesto operativo")
        self.assertNotContains(resp, "org-chip")
        self.assertEqual(resp.context["jefes"][0].equipo_activo, 1)
        self.assertContains(resp, 'data-native-select="true"')

    def test_catalogos_solo_gestion_rrhh_y_guarda_esquema_normalizado(self):
        viewer = User.objects.create_user(username="rrhh.viewer", password="pass123")
        UserModuleAccess.objects.create(user=viewer, module="rrhh", access=UserModuleAccess.ACCESS_VIEW, updated_by=self.user)
        self.client.force_login(viewer)

        resp_forbidden = self.client.get(reverse("rrhh:rrhh_catalogos"))
        self.assertEqual(resp_forbidden.status_code, 403)

        self.client.force_login(self.user)
        resp = self.client.get(reverse("rrhh:rrhh_catalogos"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Catálogos oficiales")
        self.assertContains(resp, "Área / división y puesto operativo")
        self.assertContains(resp, "Nueva función operativa")
        self.assertContains(resp, "Esquemas de bono")
        self.assertContains(resp, "Catálogos")

        resp_post = self.client.post(
            reverse("rrhh:rrhh_catalogos"),
            {
                "action": "bono_esquema",
                "nombre": "Almacén",
                "departamento": Empleado.DEP_ADMINISTRACION,
                "area": "ALMACEN",
                "descripcion": "Bono para almacén",
                "activo": "on",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        esquema = BonoEsquema.objects.get(codigo="ALMACEN")
        self.assertEqual(esquema.nombre, "Almacén")
        self.assertEqual(esquema.departamento, Empleado.DEP_ADMINISTRACION)
        self.assertEqual(esquema.area, "ALMACEN")

        self.client.post(
            reverse("rrhh:rrhh_catalogos"),
            {
                "action": "bono_esquema",
                "nombre": "almacen",
                "departamento": Empleado.DEP_ADMINISTRACION,
                "area": "ALMACEN",
                "activo": "on",
            },
            follow=True,
        )
        self.assertEqual(BonoEsquema.objects.filter(codigo="ALMACEN").count(), 1)

    def test_catalogo_operativo_alimenta_alta_de_empleado(self):
        self.client.post(
            reverse("rrhh:rrhh_catalogos"),
            {
                "action": "funcion_operativa",
                "codigo": "DECORADO",
                "etiqueta": "Decorado",
                "departamento_origen": Empleado.DEP_PRODUCCION,
                "departamento_actual": Empleado.DEP_PRODUCCION,
                "puesto_operativo": "DECORADO",
                "nivel_organizacional": Empleado.NIVEL_COLABORADOR,
                "activo": "on",
            },
            follow=True,
        )

        self.assertTrue(CatalogoFuncionOperativa.objects.filter(codigo="DECORADO", activo=True).exists())

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Decorado",
                "area": "DECORADO",
                "puesto": "Decoradora",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="Empleado Decorado")
        self.assertEqual(empleado.area, "DECORADO")
        self.assertEqual(empleado.departamento, Empleado.DEP_PRODUCCION)
        self.assertEqual(empleado.puesto_operativo, "DECORADO")

    def test_empleados_expone_campos_de_organizacion(self):
        jefe = Empleado.objects.create(nombre="LOPEZ PALOS JOHANA ADELIN", departamento="VENTAS", puesto="Jefe de Ventas")
        colaborador = Empleado.objects.create(nombre="COLABORADOR SIN JERARQUIA", departamento="VENTAS", puesto="Cajera")
        BonoEsquema.objects.get_or_create(codigo="VENTAS", defaults={"nombre": "Ventas"})

        resp = self.client.get(reverse("rrhh:empleados"))

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Departamento")
        self.assertContains(resp, "Puesto operativo")
        self.assertContains(resp, "Jefe directo")
        self.assertContains(resp, "LOPEZ PALOS JOHANA ADELIN")
        self.assertIn(jefe, list(resp.context["empleados_jefes"]))
        self.assertNotIn(colaborador, list(resp.context["empleados_jefes"]))
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
        self.assertContains(resp, "Nivel organizacional")
        self.assertContains(resp, "Colaborador")
        self.assertContains(resp, "Encargada / encargado")
        self.assertContains(resp, "Supervisión")
        self.assertContains(resp, 'data-searchable-select="true"')
        self.assertContains(resp, "Crear usuario y contraseña para este empleado")
        self.assertContains(resp, 'name="jefe_directo" data-jefe-directo-select data-native-select="true"')
        self.assertNotContains(resp, '<select id="usuario_erp"')
        self.assertNotContains(resp, "modal-catalogo-otro")
        self.assertNotContains(resp, "Agregar otro valor")
        self.assertNotContains(resp, "__otro__")
        self.assertContains(resp, "modal-bono-esquema-otro")

    def test_empleados_rechaza_jefe_directo_fuera_de_jerarquia(self):
        jefe_ventas = Empleado.objects.create(
            nombre="JEFA VENTAS",
            departamento=Empleado.DEP_VENTAS,
            nivel_organizacional=Empleado.NIVEL_JEFATURA,
        )

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Produccion",
                "area": "HORNOS",
                "departamento": Empleado.DEP_PRODUCCION,
                "jefe_directo": str(jefe_ventas.id),
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        self.assertFalse(Empleado.objects.filter(nombre="Empleado Produccion").exists())
        self.assertContains(resp, "El jefe directo debe corresponder a la jerarquia del departamento.")

    def test_empleados_permite_direccion_como_jefe_transversal(self):
        direccion = Empleado.objects.create(
            nombre="DIRECCION GENERAL",
            departamento=Empleado.DEP_ADMINISTRACION,
            nivel_organizacional=Empleado.NIVEL_DIRECCION,
        )

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Produccion Direccion",
                "area": "HORNOS",
                "departamento": Empleado.DEP_PRODUCCION,
                "jefe_directo": str(direccion.id),
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="Empleado Produccion Direccion")
        self.assertEqual(empleado.jefe_directo, direccion)

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
        self.assertFalse(empleado.participa_bonos_ventas)
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

    def test_empleados_update_respeta_checkbox_manual_de_bono(self):
        produccion, _ = BonoEsquema.objects.get_or_create(
            codigo="PRODUCCION",
            defaults={"nombre": "Producción", "departamento": Empleado.DEP_PRODUCCION},
        )
        empleado = Empleado.objects.create(
            nombre="Empleado Checkbox Produccion",
            departamento_origen=Empleado.DEP_LOGISTICA,
            departamento=Empleado.DEP_PRODUCCION,
            salario_diario="300.00",
        )

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "update",
                "empleado_id": str(empleado.id),
                "nombre": empleado.nombre,
                "area": "",
                "departamento_origen": Empleado.DEP_LOGISTICA,
                "departamento": Empleado.DEP_PRODUCCION,
                "puesto_operativo": "",
                "bono_esquemas": [str(produccion.id)],
                "salario_diario": "300.00",
                "activo": "on",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertTrue(empleado.participa_bonos_produccion)
        self.assertEqual(set(empleado.bonos_esquemas.values_list("codigo", flat=True)), {"PRODUCCION"})

    def test_empleados_rechaza_area_y_puesto_operativo_fuera_de_catalogo(self):
        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Catalogo Cerrado",
                "departamento": Empleado.DEP_PRODUCCION,
                "area": "DECORADO ESPECIAL",
                "puesto": "Auxiliar",
                "puesto_operativo": "DECORADO",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Organización inválida")
        self.assertFalse(Empleado.objects.filter(nombre="Empleado Catalogo Cerrado").exists())

    def test_empleados_update_conserva_valores_legacy_actuales_sin_crear_nuevos(self):
        empleado = Empleado.objects.create(
            nombre="Empleado Legacy Catalogo",
            area="AFANADORA",
            puesto_operativo="ENCARGADA_PRODUCCION",
            salario_diario="300.00",
        )

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "update",
                "empleado_id": str(empleado.id),
                "nombre": empleado.nombre,
                "area": "AFANADORA",
                "departamento": Empleado.DEP_PRODUCCION,
                "puesto_operativo": "ENCARGADA_PRODUCCION",
                "salario_diario": "300.00",
                "activo": "on",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.area, "AFANADORA")
        self.assertEqual(empleado.puesto_operativo, "ENCARGADA_PRODUCCION")

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

    def test_empleados_update_cajas_activa_bono_ventas_actual(self):
        from datetime import date
        from bonos_ventas.models import BonoVentasEmpleado, ConfigBonoVentasPeriodo

        ventas, _ = BonoEsquema.objects.get_or_create(
            codigo="VENTAS",
            defaults={"nombre": "Ventas", "departamento": Empleado.DEP_VENTAS},
        )
        sucursal = Sucursal.objects.create(codigo="TUN", nombre="El Túnel")
        periodo = ConfigBonoVentasPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(
            nombre="Empleado Cajas Ventas",
            area="ADMINISTRACION",
            puesto_operativo="",
            sucursal=sucursal.nombre,
            salario_diario="300.00",
        )

        with patch("rrhh.services_bonos.timezone.localdate", return_value=date(2026, 5, 27)):
            resp = self.client.post(
                reverse("rrhh:empleados"),
                {
                    "action": "update",
                    "empleado_id": str(empleado.id),
                    "nombre": empleado.nombre,
                    "area": "CAJAS",
                    "departamento_origen": Empleado.DEP_VENTAS,
                    "departamento": Empleado.DEP_VENTAS,
                    "puesto_operativo": "CAJAS",
                    "bono_esquemas": [str(ventas.id)],
                    "sucursal": sucursal.nombre,
                    "salario_diario": "300.00",
                    "activo": "on",
                },
                secure=True,
                follow=True,
            )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertTrue(empleado.participa_bonos_ventas)
        bono = BonoVentasEmpleado.objects.get(periodo=periodo, empleado=empleado)
        self.assertEqual(bono.sucursal, sucursal)

    def test_empleados_update_retira_bono_produccion_actual_vacio(self):
        from datetime import date
        from bonos_produccion.models import AREA_PRODUCCION, BonoProduccionEmpleado, ConfigBonoPeriodo

        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(
            nombre="Empleado Sale Produccion",
            area="PRODUCCION",
            puesto_operativo="PRODUCCION",
            participa_bonos_produccion=True,
            salario_diario="300.00",
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_PRODUCCION)

        with patch("rrhh.services_bonos.timezone.localdate", return_value=date(2026, 5, 27)):
            resp = self.client.post(
                reverse("rrhh:empleados"),
                {
                    "action": "update",
                    "empleado_id": str(empleado.id),
                    "nombre": empleado.nombre,
                    "area": "CAJAS",
                    "departamento_origen": Empleado.DEP_VENTAS,
                    "departamento": Empleado.DEP_VENTAS,
                    "puesto_operativo": "CAJAS",
                    "salario_diario": "300.00",
                    "activo": "on",
                },
                secure=True,
                follow=True,
            )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertFalse(empleado.participa_bonos_produccion)
        self.assertFalse(BonoProduccionEmpleado.objects.filter(periodo=periodo, empleado=empleado).exists())

    def test_empleados_update_retira_bono_produccion_actual_en_borrador_con_registros(self):
        from datetime import date
        from bonos_produccion.models import (
            AREA_PRODUCCION,
            BonoProduccionEmpleado,
            ConfigBonoPeriodo,
            RegistroDiarioProduccion,
        )

        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(
            nombre="Encargada No Elegible Bono",
            area="PRODUCCION",
            puesto_operativo="ENCARGADA_PRODUCCION",
            participa_bonos_produccion=True,
            salario_diario="300.00",
        )
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_PRODUCCION)
        RegistroDiarioProduccion.objects.create(
            bono=bono,
            dia=1,
            tiene_uniforme=True,
            tiene_puntualidad=True,
            tiene_asistencia=True,
            tiene_produccion=True,
        )

        with patch("rrhh.services_bonos.timezone.localdate", return_value=date(2026, 5, 27)):
            resp = self.client.post(
                reverse("rrhh:empleados"),
                {
                    "action": "update",
                    "empleado_id": str(empleado.id),
                    "nombre": empleado.nombre,
                    "area": "PRODUCCION",
                    "departamento_origen": Empleado.DEP_PRODUCCION,
                    "departamento": Empleado.DEP_PRODUCCION,
                    "puesto_operativo": "ENCARGADA_PRODUCCION",
                    "salario_diario": "300.00",
                    "activo": "on",
                },
                secure=True,
                follow=True,
            )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertFalse(empleado.participa_bonos_produccion)
        self.assertFalse(BonoProduccionEmpleado.objects.filter(periodo=periodo, empleado=empleado).exists())

    def test_empleados_update_crea_bono_produccion_actual_si_se_activa(self):
        from datetime import date
        from bonos_produccion.models import AREA_HORNOS, BonoProduccionEmpleado, ConfigBonoPeriodo

        produccion, _ = BonoEsquema.objects.get_or_create(
            codigo="PRODUCCION",
            defaults={"nombre": "Producción", "departamento": Empleado.DEP_PRODUCCION},
        )
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(
            nombre="Empleado Entra Produccion",
            area="CAJAS",
            puesto_operativo="CAJAS",
            salario_diario="300.00",
        )

        with patch("rrhh.services_bonos.timezone.localdate", return_value=date(2026, 5, 27)):
            resp = self.client.post(
                reverse("rrhh:empleados"),
                {
                    "action": "update",
                    "empleado_id": str(empleado.id),
                    "nombre": empleado.nombre,
                    "area": "HORNOS",
                    "departamento_origen": Empleado.DEP_PRODUCCION,
                    "departamento": Empleado.DEP_PRODUCCION,
                    "puesto_operativo": "HORNOS",
                    "bono_esquemas": [str(produccion.id)],
                    "salario_diario": "300.00",
                    "activo": "on",
                },
                secure=True,
                follow=True,
            )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertTrue(empleado.participa_bonos_produccion)
        bono = BonoProduccionEmpleado.objects.get(periodo=periodo, empleado=empleado)
        self.assertEqual(bono.area, AREA_HORNOS)

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

        resp_rrhh = self.client.post(reverse("rrhh:rrhh_prestamo_auth_jefe", args=[prestamo.pk]))
        self.assertEqual(resp_rrhh.status_code, 403)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_SOLICITADO)
        self.assertIsNone(prestamo.autorizado_jefe)

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

    def test_capital_humano_no_autoriza_su_propio_prestamo(self):
        from datetime import date

        paula_user = User.objects.create_user(username="paula.rrhh", password="pass123")
        paula_user.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        director = User.objects.create_user(username="director.rrhh", password="pass123")
        director.groups.add(Group.objects.get_or_create(name="DG")[0])
        paula = Empleado.objects.create(
            nombre="LUGO ESPINOZA PAULA ELIZABETH",
            departamento=Empleado.DEP_RRHH,
            usuario_erp=paula_user,
        )
        prestamo = Prestamo.objects.create(
            empleado=paula,
            concepto="Solicitud Capital Humano",
            fecha_solicitud=date(2026, 5, 15),
            importe=Decimal("800.00"),
            num_quincenas=2,
            descuento_quincenal=Decimal("400.00"),
            saldo_actual=Decimal("800.00"),
            estado=Prestamo.ESTADO_SOLICITADO,
            jefe_directo=director,
            creado_por=paula_user,
        )

        self.client.force_login(paula_user)
        resp_paula = self.client.post(reverse("rrhh:rrhh_prestamo_auth_jefe", args=[prestamo.pk]))
        self.assertEqual(resp_paula.status_code, 403)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_SOLICITADO)
        self.assertIsNone(prestamo.autorizado_jefe)

        self.client.force_login(director)
        resp_jefe = self.client.post(reverse("rrhh:rrhh_prestamo_auth_jefe", args=[prestamo.pk]), follow=True)
        self.assertEqual(resp_jefe.status_code, 200)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_AUTORIZADO)
        self.assertEqual(prestamo.autorizado_jefe, director)

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
                "area": "CAJAS",
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

    def test_nomina_importa_lista_raya_desde_web(self):
        if not LISTA_RAYA_SAMPLE.exists():
            raise SkipTest("No está disponible el archivo real de lista de raya.")

        upload = SimpleUploadedFile(
            "lista_raya_abril_2026.xls",
            LISTA_RAYA_SAMPLE.read_bytes(),
            content_type="application/vnd.ms-excel",
        )

        response = self.client.post(
            reverse("rrhh:nomina"),
            {
                "action": "import_lista_raya",
                "archivo": upload,
            },
        )

        self.assertEqual(response.status_code, 302)
        periodo = NominaPeriodo.objects.get(fecha_inicio="2026-04-16", fecha_fin="2026-04-30")
        self.assertEqual(Empleado.objects.count(), 67)
        self.assertEqual(periodo.lineas.count(), 67)
        self.assertEqual(NominaConceptoLinea.objects.count(), 336)
        self.assertEqual(NominaImportacion.objects.count(), 1)
        self.assertEqual(response.url, reverse("rrhh:nomina_detail", kwargs={"pk": periodo.pk}))

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


class ListaRayaIdentidadTests(TestCase):
    def test_importar_lista_raya_usa_empleado_sugerido_y_no_duplica(self):
        from datetime import date
        from tempfile import NamedTemporaryFile

        from rrhh.services.lista_raya import (
            EmpleadoListaRaya,
            ListaRayaParseResult,
            importar_lista_raya_nomina,
        )

        empleado = Empleado.objects.create(nombre="REY IVAN VALDEZ FELIX", codigo="EMP-2606-001")
        row = EmpleadoListaRaya(
            codigo="346",
            nombre="REY IVAN VALDEZ FELIX",
            area="PRODUCCION",
            rfc="",
            nss="",
            curp="",
            fecha_ingreso=date(2026, 6, 4),
            salario_diario=Decimal("300.00"),
            sdi=Decimal("300.00"),
            sbc=Decimal("300.00"),
            dias_pagados=Decimal("15"),
            horas_trabajadas=Decimal("120"),
            horas_dia=Decimal("8"),
            horas_extra=Decimal("0"),
            ausencias=Decimal("0"),
            incapacidades=Decimal("0"),
            total_percepciones=Decimal("4500.00"),
            total_deducciones=Decimal("0.00"),
            neto=Decimal("4500.00"),
            conceptos=[],
        )
        parse_result = ListaRayaParseResult(
            source_path="fake.xls",
            source_hash="hash-identidad-test",
            empresa="Pollyana's Dolce",
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            periodo_numero="11",
            empleados=[row],
            total_empleados_reportado=1,
            total_percepciones_reportado=Decimal("4500.00"),
            total_deducciones_reportado=Decimal("0.00"),
            total_neto_reportado=Decimal("4500.00"),
        )

        with NamedTemporaryFile(suffix=".xls") as tmp, patch("rrhh.services.lista_raya.parse_lista_raya_xls", return_value=parse_result):
            importar_lista_raya_nomina(tmp.name, commit=True)

        self.assertEqual(Empleado.objects.filter(nombre="REY IVAN VALDEZ FELIX").count(), 1)
        self.assertEqual(NominaLinea.objects.get().empleado, empleado)
        pendiente = EmpleadoIdentidadPendiente.objects.get(fuente=EmpleadoIdentidadPendiente.FUENTE_NOMINA, codigo_externo="346")
        self.assertEqual(pendiente.empleado_sugerido, empleado)
        empleado.refresh_from_db()
        self.assertEqual(empleado.codigo, "EMP-2606-001")

    def test_command_dry_run_no_toca_base(self):
        if not LISTA_RAYA_SAMPLE.exists():
            raise SkipTest("No está disponible el archivo real de lista de raya.")
        call_command("importar_lista_raya", str(LISTA_RAYA_SAMPLE))

        self.assertEqual(Empleado.objects.count(), 0)
        self.assertEqual(NominaPeriodo.objects.count(), 0)
        self.assertEqual(NominaImportacion.objects.count(), 0)

    def test_command_commit_importa_periodo_y_conceptos(self):
        if not LISTA_RAYA_SAMPLE.exists():
            raise SkipTest("No está disponible el archivo real de lista de raya.")
        call_command("importar_lista_raya", str(LISTA_RAYA_SAMPLE), "--commit")

        periodo = NominaPeriodo.objects.get(fecha_inicio="2026-04-16", fecha_fin="2026-04-30")
        self.assertEqual(Empleado.objects.count(), 67)
        self.assertEqual(periodo.lineas.count(), 67)
        self.assertEqual(NominaConceptoLinea.objects.count(), 336)
        self.assertEqual(periodo.total_bruto, periodo.importaciones.first().total_percepciones)
        self.assertEqual(periodo.total_descuentos, periodo.importaciones.first().total_deducciones)
        self.assertEqual(periodo.total_neto, periodo.importaciones.first().total_neto)
