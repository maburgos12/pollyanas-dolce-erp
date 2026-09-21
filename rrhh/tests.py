from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.test import TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from decimal import Decimal
from pathlib import Path
from rest_framework.test import APIClient
from unittest import SkipTest
from unittest.mock import patch

from core.models import Notificacion, Sucursal, UserModuleAccess, UserProfile
from rrhh.models import (
    AplicacionGoceVacaciones,
    AsistenciaEmpleado,
    BonoEsquema,
    CatalogoFuncionOperativa,
    Empleado,
    EmpleadoBaja,
    EmpleadoIdentidadPendiente,
    HoraExtra,
    IncidenciaAsistencia,
    IncapacidadEmpleado,
    NominaConceptoLinea,
    NominaImportacion,
    NominaLinea,
    NominaPeriodo,
    MovimientoVacaciones,
    PlantillaAutorizada,
    PermisoSalida,
    PeriodoVacacional,
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
        PeriodoVacacional.objects.create(
            empleado=empleado,
            aniversario=date(2026, 1, 1),
            fecha_limite=date(2026, 7, 1),
            antiguedad_anios=1,
            dias_generados=Decimal("12.00"),
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

    def test_incapacidad_bloquea_vacaciones_traslapadas(self):
        from datetime import date

        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)
        empleado = Empleado.objects.create(
            nombre="Colaborador Incapacidad",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )
        incidencia = IncidenciaAsistencia.objects.create(
            empleado=empleado,
            fecha=date(2026, 6, 16),
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            detalle="Falta antes de capturar incapacidad.",
        )

        self.client.force_login(rrhh_user)
        response = self.client.post(
            reverse("rrhh:rrhh_incapacidad_crear"),
            {
                "empleado": empleado.id,
                "fecha_inicio": "2026-06-15",
                "fecha_fin": "2026-06-20",
                "tipo": IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
                "folio": "IMSS-1",
                "estado": IncapacidadEmpleado.ESTADO_ACTIVA,
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(IncapacidadEmpleado.objects.count(), 1)
        incidencia.refresh_from_db()
        self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_RESUELTO)

        response = self.client.post(
            reverse("rrhh:rrhh_vacaciones_list"),
            {
                "action": "crear",
                "empleado_id": empleado.id,
                "fecha_inicio": "2026-06-16",
                "fecha_fin": "2026-06-18",
                "motivo": "Cruza incapacidad",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(SolicitudVacaciones.objects.filter(empleado=empleado).exists())
        response = self.client.get(reverse("rrhh:rrhh_incapacidades"))
        self.assertContains(response, "IMSS-1")

    def test_incapacidades_exigen_permiso_de_nomina(self):
        vacaciones_user = User.objects.create_user(username="vacaciones-viewer")
        nomina_user = User.objects.create_user(username="nomina-viewer")
        UserModuleAccess.objects.create(
            user=vacaciones_user,
            module="rrhh.vacaciones",
            access=UserModuleAccess.ACCESS_MANAGE,
        )
        UserModuleAccess.objects.create(
            user=nomina_user,
            module="rrhh.nomina",
            access=UserModuleAccess.ACCESS_VIEW,
        )

        self.client.force_login(vacaciones_user)
        self.assertEqual(self.client.get(reverse("rrhh:rrhh_incapacidades")).status_code, 403)

        self.client.force_login(nomina_user)
        response = self.client.get(reverse("rrhh:rrhh_incapacidades"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Alta de incapacidad")

    def test_cancelar_incapacidad_futura_no_crea_faltas_futuras(self):
        from datetime import date

        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)
        empleado = Empleado.objects.create(
            nombre="Colaborador Incapacidad Futura",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        incapacidad = IncapacidadEmpleado.objects.create(
            empleado=empleado,
            fecha_inicio=date(2026, 7, 1),
            fecha_fin=date(2026, 7, 5),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            folio="IMSS-FUTURA",
        )

        self.client.force_login(rrhh_user)
        with patch("rrhh.views_incapacidades.timezone.localdate", return_value=date(2026, 6, 16)):
            response = self.client.post(
                reverse("rrhh:rrhh_incapacidad_cancelar", args=[incapacidad.id]),
                {"comentario_cancelacion": "Captura futura cancelada."},
            )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(IncidenciaAsistencia.objects.filter(empleado=empleado).exists())

    def test_incapacidad_rechaza_traslape_y_cancelada_no_bloquea(self):
        from datetime import date

        empleado = Empleado.objects.create(
            nombre="Colaborador Duplicado Incapacidad",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        incapacidad = IncapacidadEmpleado.objects.create(
            empleado=empleado,
            fecha_inicio=date(2026, 6, 15),
            fecha_fin=date(2026, 6, 20),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            folio="IMSS-2",
        )
        duplicada = IncapacidadEmpleado(
            empleado=empleado,
            fecha_inicio=date(2026, 6, 18),
            fecha_fin=date(2026, 6, 22),
            tipo=IncapacidadEmpleado.TIPO_RIESGO_TRABAJO,
            folio="IMSS-3",
        )
        with self.assertRaises(ValidationError):
            duplicada.full_clean()
    def test_vacaciones_respeta_descansos_oficiales_moviles_lft(self):
        from datetime import date

        from rrhh.services_vacaciones import contar_dias_laborables, es_dia_laborable

        self.assertFalse(es_dia_laborable(date(2026, 2, 2)))
        self.assertTrue(es_dia_laborable(date(2026, 2, 5)))
        self.assertFalse(es_dia_laborable(date(2026, 3, 16)))
        self.assertFalse(es_dia_laborable(date(2026, 11, 16)))
        self.assertEqual(contar_dias_laborables(date(2026, 2, 2), date(2026, 2, 6)), Decimal("4"))

    def test_vacaciones_usa_antiguedad_a_fecha_inicio(self):
        from datetime import date

        from rrhh.services_vacaciones import crear_solicitud_vacaciones, saldo_vacaciones_empleado

        rrhh_user = User.objects.create_user(username="paula")
        empleado = Empleado.objects.create(
            nombre="Colaborador Aniversario Futuro",
            fecha_ingreso=date(2025, 12, 1),
            activo=True,
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )
        PeriodoVacacional.objects.create(
            empleado=empleado,
            aniversario=date(2026, 12, 1),
            fecha_limite=date(2027, 6, 1),
            antiguedad_anios=1,
            dias_generados=Decimal("12.00"),
        )

        with patch("rrhh.services_vacaciones.timezone.localdate", return_value=date(2026, 6, 16)):
            solicitud = crear_solicitud_vacaciones(
                empleado=empleado,
                fecha_inicio=date(2026, 12, 2),
                fecha_fin=date(2026, 12, 4),
                motivo="Descanso al cumplir aniversario",
                actor=rrhh_user,
            )

        self.assertEqual(solicitud.dias_laborables, Decimal("3"))
        saldo = saldo_vacaciones_empleado(empleado, periodo_anio=2026, al=date(2026, 12, 2))
        self.assertEqual(saldo["generado"], Decimal("12.00"))
        self.assertEqual(saldo["reservado"], Decimal("3"))

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
        PeriodoVacacional.objects.create(
            empleado=colaborador,
            aniversario=date(2026, 1, 1),
            fecha_limite=date(2026, 7, 1),
            antiguedad_anios=1,
            dias_generados=Decimal("12.00"),
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

    def test_vacaciones_administrativas_las_preautoriza_dg(self):
        from datetime import date
        from rrhh.services_vacaciones import crear_solicitud_vacaciones, preautorizar_solicitud_vacaciones_jefe

        dg_user = User.objects.create_user(username="mauricio")
        dg_user.groups.add(Group.objects.create(name="DG"))
        rrhh_user = User.objects.create_user(username="paula")
        rrhh_user.groups.add(Group.objects.create(name="RRHH"))
        empleada = Empleado.objects.create(
            nombre="YESENIA SOTO INZUNZA",
            departamento=Empleado.DEP_ADMINISTRACION,
            puesto="Responsable Administracion",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )
        PeriodoVacacional.objects.create(
            empleado=empleada,
            aniversario=date(2026, 1, 1),
            fecha_limite=date(2026, 7, 1),
            antiguedad_anios=1,
            dias_generados=Decimal("12.00"),
        )

        solicitud = crear_solicitud_vacaciones(
            empleado=empleada,
            fecha_inicio=date(2026, 6, 15),
            fecha_fin=date(2026, 6, 19),
            motivo="Descanso administrativo",
            actor=rrhh_user,
        )

        self.assertEqual(solicitud.jefe_directo, dg_user)

        preautorizar_solicitud_vacaciones_jefe(solicitud, dg_user, aprobar=True)
        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estado, SolicitudVacaciones.ESTADO_PREAUTORIZADA)
        self.assertEqual(solicitud.preautorizado_por, dg_user)

    def test_superuser_ve_y_preautoriza_vacaciones_asignadas_a_otro_usuario(self):
        from datetime import date

        jefe_user = User.objects.create_user(username="jefe.vacaciones")
        super_user = User.objects.create_user(username="mauricio.admin", is_superuser=True, is_staff=True)
        jefe = Empleado.objects.create(
            nombre="Jefe Vacaciones",
            fecha_ingreso=date(2024, 1, 1),
            usuario_erp=jefe_user,
            activo=True,
        )
        colaborador = Empleado.objects.create(
            nombre="Colaborador Solicitud Ocho Dias",
            fecha_ingreso=date(2025, 1, 1),
            jefe_directo=jefe,
            activo=True,
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )
        solicitud = SolicitudVacaciones.objects.create(
            empleado=colaborador,
            fecha_inicio=date(2026, 7, 6),
            fecha_fin=date(2026, 7, 15),
            dias_laborables=Decimal("8.00"),
            motivo="Vacaciones ocho dias",
            jefe_directo=jefe_user,
            creado_por=jefe_user,
        )

        self.client.force_login(super_user)
        response = self.client.get(reverse("rrhh:rrhh_vacaciones_list"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Colaborador Solicitud Ocho Dias")
        self.assertContains(response, "Preautorizar")

        response = self.client.post(
            reverse("rrhh:rrhh_vacaciones_list"),
            {"action": "preautorizar_jefe", "solicitud_id": solicitud.id},
        )
        self.assertEqual(response.status_code, 302)
        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estado, SolicitudVacaciones.ESTADO_PREAUTORIZADA)
        self.assertEqual(solicitud.preautorizado_por, super_user)

    def test_api_vacaciones_equipo_superuser_no_filtra_por_jefe_directo(self):
        from datetime import date

        jefe_user = User.objects.create_user(username="jefe.api")
        super_user = User.objects.create_user(username="admin.api", is_superuser=True, is_staff=True)
        jefe = Empleado.objects.create(
            nombre="Jefe API",
            fecha_ingreso=date(2024, 1, 1),
            usuario_erp=jefe_user,
            activo=True,
        )
        colaborador = Empleado.objects.create(
            nombre="Colaborador API Vacaciones",
            fecha_ingreso=date(2025, 1, 1),
            jefe_directo=jefe,
            activo=True,
        )
        SolicitudVacaciones.objects.create(
            empleado=colaborador,
            fecha_inicio=date(2026, 7, 6),
            fecha_fin=date(2026, 7, 15),
            dias_laborables=Decimal("8.00"),
            motivo="Vacaciones ocho dias",
            jefe_directo=jefe_user,
            creado_por=jefe_user,
        )

        client = APIClient()
        client.force_authenticate(user=super_user)
        response = client.get(reverse("rrhh:vacaciones-list"), {"equipo": "true"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["empleado_nombre"], "Colaborador API Vacaciones")

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
        self.assertContains(response, "Vacaciones")
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

    def test_vacaciones_list_muestra_saldos_e_historial(self):
        from datetime import date

        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)
        empleado = Empleado.objects.create(
            nombre="Carolina Cayetano",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
            puesto="Jefa de Produccion",
            sucursal="CEDIS",
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )
        MovimientoVacaciones.objects.create(
            empleado=empleado,
            tipo=MovimientoVacaciones.TIPO_AJUSTE,
            dias=Decimal("7.00"),
            periodo_anio=2025,
            descripcion="[saldo-inicial-vacaciones-20260616] pendiente de goce 2025",
        )

        self.client.force_login(rrhh_user)
        with patch("rrhh.views.timezone.localdate", return_value=date(2026, 6, 16)), patch(
            "rrhh.services_vacaciones.timezone.localdate", return_value=date(2026, 6, 16)
        ):
            response = self.client.get(reverse("rrhh:rrhh_vacaciones_list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Saldos e historial")
        self.assertContains(response, "Fecha límite legal")
        self.assertContains(response, "Goce anterior")
        self.assertContains(response, "Ajuste manual")
        self.assertContains(response, "Carolina Cayetano")

    @override_settings(VACACIONES_GOCE_FIFO_ACTIVO=True)
    def test_vacaciones_list_muestra_desglose_fifo_en_saldos_y_solicitudes(self):
        from datetime import date

        rrhh_user = User.objects.create_user(
            username="paula.fifo", is_superuser=True, is_staff=True
        )
        empleado = Empleado.objects.create(
            nombre="Carolina FIFO",
            fecha_ingreso=date(2022, 3, 7),
            activo=True,
        )
        periodo_2025 = PeriodoVacacional.objects.create(
            empleado=empleado,
            aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7),
            antiguedad_anios=3,
            dias_generados=Decimal("7.00"),
        )
        periodo_2026 = PeriodoVacacional.objects.create(
            empleado=empleado,
            aniversario=date(2026, 3, 7),
            fecha_limite=date(2026, 9, 7),
            antiguedad_anios=4,
            dias_generados=Decimal("18.00"),
        )
        solicitud = SolicitudVacaciones.objects.create(
            empleado=empleado,
            fecha_inicio=date(2026, 7, 20),
            fecha_fin=date(2026, 7, 31),
            dias_laborables=Decimal("10.00"),
        )
        AplicacionGoceVacaciones.objects.create(
            solicitud=solicitud,
            periodo=periodo_2025,
            dias=Decimal("7.00"),
            estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )
        AplicacionGoceVacaciones.objects.create(
            solicitud=solicitud,
            periodo=periodo_2026,
            dias=Decimal("3.00"),
            estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )

        self.client.force_login(rrhh_user)
        response = self.client.get(reverse("rrhh:rrhh_vacaciones_list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Desglose por periodo")
        self.assertContains(response, "2025 · 7.00 días")
        self.assertContains(response, "2026 · 3.00 días")
        self.assertContains(response, "Cómo se aplicarán los días")
        self.assertContains(response, "data-async-action")
        self.assertNotContains(response, "salario_diario")

    @override_settings(VACACIONES_GOCE_FIFO_ACTIVO=True)
    def test_vacaciones_list_responde_json_y_conserva_filtro_y_ancla(self):
        from datetime import date

        rrhh_user = User.objects.create_user(
            username="paula.async", is_superuser=True, is_staff=True
        )
        empleado = Empleado.objects.create(
            nombre="Carolina Async",
            fecha_ingreso=date(2022, 3, 7),
            activo=True,
        )
        PeriodoVacacional.objects.create(
            empleado=empleado,
            aniversario=date(2026, 3, 7),
            fecha_limite=date(2026, 9, 7),
            antiguedad_anios=4,
            dias_generados=Decimal("18.00"),
        )
        self.client.force_login(rrhh_user)

        response = self.client.post(
            f"{reverse('rrhh:rrhh_vacaciones_list')}?q=Carolina",
            {
                "action": "crear",
                "empleado_id": empleado.id,
                "fecha_inicio": "2026-07-20",
                "fecha_fin": "2026-07-24",
                "motivo": "Cobertura validada",
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["toast"]["type"], "success")
        self.assertTrue(payload["reload"])
        self.assertIn("?q=Carolina", payload["redirect"])
        self.assertIn("#vac-solicitud-", payload["redirect"])

    def test_vacaciones_historial_filtra_y_concilia_saldo(self):
        from datetime import date

        rrhh_user = User.objects.create_user(username="paula", is_superuser=True, is_staff=True)
        empleado = Empleado.objects.create(
            nombre="Carolina Cayetano",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
            puesto="Jefa de Produccion",
            sucursal="CEDIS",
        )
        PoliticaVacaciones.objects.create(
            antiguedad_desde=1,
            antiguedad_hasta=5,
            dias_laborables=Decimal("12.00"),
            vigente_desde=date(2026, 1, 1),
        )

        self.client.force_login(rrhh_user)
        response = self.client.get(reverse("rrhh:rrhh_vacaciones_list"), {"q": "nadie"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Sin empleados para esos filtros.")

        response = self.client.post(
            reverse("rrhh:rrhh_vacaciones_list"),
            {
                "action": "ajustar_saldo",
                "empleado_id": empleado.id,
                "periodo_anio": "2026",
                "dias_ajuste": "-2",
                "descripcion": "Corrección por pago timbrado",
            },
        )

        self.assertEqual(response.status_code, 302)
        movimiento = MovimientoVacaciones.objects.get(empleado=empleado)
        self.assertEqual(movimiento.tipo, MovimientoVacaciones.TIPO_AJUSTE)
        self.assertEqual(movimiento.dias, Decimal("-2.00"))
        self.assertEqual(movimiento.periodo_anio, 2026)
        self.assertEqual(movimiento.actor, rrhh_user)
        self.assertIn("[conciliacion-manual] Corrección por pago timbrado", movimiento.descripcion)

    def test_admin_borra_solicitud_al_borrar_movimiento_vacaciones_ligado(self):
        from datetime import date

        from django.contrib import admin

        empleado = Empleado.objects.create(nombre="Paula Lugo", fecha_ingreso=date(2025, 1, 1), activo=True)
        solicitud = SolicitudVacaciones.objects.create(
            empleado=empleado,
            fecha_inicio=date(2026, 7, 15),
            fecha_fin=date(2026, 7, 24),
            dias_laborables=Decimal("9.00"),
            estado=SolicitudVacaciones.ESTADO_APROBADA,
        )
        reservado = MovimientoVacaciones.objects.create(
            empleado=empleado,
            solicitud=solicitud,
            tipo=MovimientoVacaciones.TIPO_RESERVADO,
            dias=Decimal("9.00"),
            periodo_anio=2026,
        )
        MovimientoVacaciones.objects.create(
            empleado=empleado,
            solicitud=solicitud,
            tipo=MovimientoVacaciones.TIPO_CONSUMIDO,
            dias=Decimal("9.00"),
            periodo_anio=2026,
        )
        ajuste = MovimientoVacaciones.objects.create(
            empleado=empleado,
            tipo=MovimientoVacaciones.TIPO_AJUSTE,
            dias=Decimal("2.00"),
            periodo_anio=2026,
        )

        model_admin = admin.site._registry[MovimientoVacaciones]
        model_admin.delete_queryset(None, MovimientoVacaciones.objects.filter(pk=reservado.pk))

        self.assertFalse(SolicitudVacaciones.objects.filter(pk=solicitud.pk).exists())
        self.assertFalse(MovimientoVacaciones.objects.filter(solicitud_id=solicitud.pk).exists())
        self.assertTrue(MovimientoVacaciones.objects.filter(pk=ajuste.pk).exists())

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

    def test_jefe_directo_prevalece_para_personal_administrativo_nominal(self):
        mauricio_user = User.objects.create_user(username="maburgos12", is_superuser=True, is_staff=True)
        mauricio = Empleado.objects.create(
            nombre="MAURICIO BURGOS",
            usuario_erp=mauricio_user,
            nivel_organizacional=Empleado.NIVEL_DIRECCION,
        )
        paula = Empleado.objects.create(
            nombre="LUGO ESPINOZA PAULA ELIZABETH",
            departamento=Empleado.DEP_RRHH,
            puesto="Jefe de Recursos Humanos",
            jefe_directo=mauricio,
        )
        permiso = PermisoSalida.objects.create(
            empleado=paula,
            tipo=PermisoSalida.TIPO_PERMISO_DIA,
            fecha_inicio=timezone.datetime(2026, 5, 26, 8, 0, tzinfo=timezone.get_current_timezone()),
            motivo="Permiso Capital Humano",
        )

        self.assertFalse(permiso.requiere_direccion)
        self.assertEqual(permiso.estado_direccion, PermisoSalida.ESTADO_DIRECCION_NO_REQUIERE)

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

    def test_superuser_resuelve_permiso_asignado_a_otro_jefe(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from rrhh.services_permisos import resolver_permiso_jefe

        jefe_user = User.objects.create_user(username="jefe.permiso")
        super_user = User.objects.create_user(username="mauricio.super", is_superuser=True, is_staff=True)
        jefe = Empleado.objects.create(nombre="Jefe Permiso", usuario_erp=jefe_user)
        empleado = Empleado.objects.create(nombre="Empleado Permiso", jefe_directo=jefe)
        permiso = PermisoSalida.objects.create(
            empleado=empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=datetime(2026, 5, 26, 13, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            fecha_fin=datetime(2026, 5, 26, 15, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            motivo="Cita",
            estado_jefe=PermisoSalida.ESTADO_JEFE_PENDIENTE,
        )

        resolver_permiso_jefe(permiso, super_user, aprobar=True)

        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PREAUTORIZADO)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertEqual(permiso.autorizado_por, super_user)

    def test_superuser_autoriza_permiso_de_jefe_desde_panel_rrhh(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        super_user = User.objects.create_user(username="mauricio.panel", is_superuser=True, is_staff=True)
        mauricio = Empleado.objects.create(
            nombre="MAURICIO ANTONIO BURGOS FONSECA",
            usuario_erp=super_user,
            nivel_organizacional=Empleado.NIVEL_DIRECCION,
        )
        paula = Empleado.objects.create(
            nombre="LUGO ESPINOZA PAULA ELIZABETH",
            departamento=Empleado.DEP_RRHH,
            puesto="Jefe de Recursos Humanos",
            jefe_directo=mauricio,
        )
        permiso = PermisoSalida.objects.create(
            empleado=paula,
            tipo=PermisoSalida.TIPO_PERMISO_DIA,
            fecha_inicio=datetime(2026, 7, 3, 8, 0, tzinfo=ZoneInfo("America/Mazatlan")),
            motivo="Tramites personales",
            estado_jefe=PermisoSalida.ESTADO_JEFE_PENDIENTE,
            requiere_direccion=False,
            estado_direccion=PermisoSalida.ESTADO_DIRECCION_NO_REQUIERE,
        )

        self.client.force_login(super_user)
        response = self.client.get(reverse("rrhh:rrhh_permisos_list"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, permiso.folio)
        self.assertContains(response, "Autorizar jefe")

        response = self.client.post(
            reverse("rrhh:rrhh_permisos_list"),
            {"permiso_id": permiso.id, "action": "preautorizar_jefe"},
        )

        self.assertEqual(response.status_code, 302)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PREAUTORIZADO)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertEqual(permiso.autorizado_por, super_user)

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
            usuario_erp=self.user,
        )
        self.client = APIClient()
        self.client.force_authenticate(user=self.user)

    def assertHoraLocal(self, dt, hora, minuto=0):
        local_dt = timezone.localtime(dt)
        self.assertEqual((local_dt.hour, local_dt.minute), (hora, minuto))

    @override_settings(VACACIONES_GOCE_FIFO_ACTIVO=True)
    def test_vacaciones_saldo_agrega_periodos_y_propuesta_fifo_sin_importes(self):
        from datetime import date

        PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2025, 3, 7),
            fecha_limite=date(2025, 9, 7),
            antiguedad_anios=3,
            dias_generados=Decimal("7.00"),
        )
        PeriodoVacacional.objects.create(
            empleado=self.empleado,
            aniversario=date(2026, 3, 7),
            fecha_limite=date(2026, 9, 7),
            antiguedad_anios=4,
            dias_generados=Decimal("18.00"),
        )

        response = self.client.get(
            reverse("rrhh:vacaciones-saldo"),
            {
                "empleado": self.empleado.id,
                "fecha_inicio": "2026-07-20",
                "fecha_fin": "2026-07-30",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("saldo", response.data)
        self.assertIn("disponible", response.data)
        self.assertEqual([fila["anio"] for fila in response.data["periodos"]], [2025, 2026])
        self.assertEqual(
            [(fila["anio"], fila["dias"]) for fila in response.data["propuesta_fifo"]],
            [(2025, Decimal("7.00")), (2026, Decimal("3.00"))],
        )
        self.assertEqual(response.data["dias_laborables"], Decimal("10"))
        self.assertTrue(response.data["fifo_activo"])
        self.assertNotIn("importe", str(response.data).lower())

    @override_settings(VACACIONES_GOCE_FIFO_ACTIVO=False)
    def test_vacaciones_saldo_no_promete_fifo_mientras_esta_inactivo(self):
        PoliticaVacaciones.objects.create(
            antiguedad_desde=0,
            antiguedad_hasta=None,
            dias_laborables=Decimal("18.00"),
        )

        response = self.client.get(
            reverse("rrhh:vacaciones-saldo"),
            {
                "empleado": self.empleado.id,
                "fecha_inicio": "2026-07-20",
                "fecha_fin": "2026-07-24",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.data["fifo_activo"])
        self.assertEqual(response.data["propuesta_fifo"], [])
        self.assertTrue(response.data["saldo_suficiente"])
        self.assertEqual(response.data["faltante"], Decimal("0"))

    def test_vacaciones_saldo_sin_periodos_conserva_totales_legacy(self):
        PoliticaVacaciones.objects.create(
            antiguedad_desde=0,
            antiguedad_hasta=0,
            dias_laborables=Decimal("12.00"),
        )
        response = self.client.get(reverse("rrhh:vacaciones-saldo"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["generado"], response.data["saldo"]["generado"])
        self.assertEqual(response.data["reservado"], response.data["saldo"]["reservado"])
        self.assertEqual(response.data["consumido"], response.data["saldo"]["consumido"])
        self.assertEqual(response.data["disponible"], response.data["saldo"]["disponible"])

    def test_vacaciones_saldo_no_expone_otro_empleado_sin_permiso(self):
        otro = Empleado.objects.create(nombre="Otro empleado privado")

        response = self.client.get(
            reverse("rrhh:vacaciones-saldo"), {"empleado": otro.id}
        )

        self.assertEqual(response.status_code, 403)

    def test_vacaciones_saldo_rrhh_puede_consultar_empleado_autorizado(self):
        rrhh_user = User.objects.create_user(username="rrhh.saldos")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        rrhh_user.groups.add(rrhh_group)
        otro = Empleado.objects.create(nombre="Empleado consultado RRHH")
        self.client.force_authenticate(user=rrhh_user)

        response = self.client.get(
            reverse("rrhh:vacaciones-saldo"), {"empleado": otro.id}
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["empleado"], otro.id)

    def test_vacaciones_saldo_permiso_explicito_de_vacaciones_puede_consultar(self):
        user = User.objects.create_user(username="vacaciones.consulta")
        UserModuleAccess.objects.create(
            user=user,
            module="rrhh.vacaciones",
            access=UserModuleAccess.ACCESS_VIEW,
        )
        otro = Empleado.objects.create(nombre="Empleado visible vacaciones")
        self.client.force_authenticate(user=user)

        response = self.client.get(
            reverse("rrhh:vacaciones-saldo"), {"empleado": otro.id}
        )

        self.assertEqual(response.status_code, 200)

    def test_vacaciones_saldo_otro_submodulo_rrhh_no_otorga_acceso(self):
        user = User.objects.create_user(username="vacantes.sin.vacaciones")
        UserModuleAccess.objects.create(
            user=user,
            module="rrhh.vacantes",
            access=UserModuleAccess.ACCESS_VIEW,
        )
        otro = Empleado.objects.create(nombre="Empleado privado vacaciones")
        self.client.force_authenticate(user=user)

        response = self.client.get(
            reverse("rrhh:vacaciones-saldo"), {"empleado": otro.id}
        )

        self.assertEqual(response.status_code, 403)

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

    def test_hora_extra_api_superuser_autoriza_asignada_a_otro_jefe(self):
        from datetime import date

        jefe_user = User.objects.create_user(username="jefe.he", password="pass123")
        super_user = User.objects.create_user(username="mauricio.he", is_superuser=True, is_staff=True)
        jefe = Empleado.objects.create(nombre="Jefe HE", usuario_erp=jefe_user)
        empleado = Empleado.objects.create(
            nombre="Empleado HE",
            salario_diario="350.00",
            jefe_directo=jefe,
        )
        hora_extra = HoraExtra.objects.create(
            empleado=empleado,
            fecha=date(2026, 5, 20),
            horas=Decimal("2.00"),
            estado=HoraExtra.ESTADO_PENDIENTE,
            jefe_directo=jefe_user,
        )

        self.client.force_authenticate(user=super_user)
        response = self.client.post(reverse("rrhh:hora-extra-autorizar", args=[hora_extra.id]))

        self.assertEqual(response.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.autorizado_por, super_user)

    def test_hora_extra_administrativa_la_autoriza_dg(self):
        dg_user = User.objects.create_user(username="mauricio", password="pass123")
        dg_user.groups.add(Group.objects.create(name="DG"))
        empleado_user = User.objects.create_user(
            username="yesenia.soto",
            email="yesenia@example.com",
            password="pass123",
        )
        empleada = Empleado.objects.create(
            nombre="YESENIA SOTO INZUNZA",
            email="yesenia@example.com",
            departamento=Empleado.DEP_ADMINISTRACION,
            puesto="Responsable Administracion",
            salario_diario="800.00",
        )
        self.client.force_authenticate(user=empleado_user)

        resp = self.client.post(
            reverse("rrhh:hora-extra-list"),
            {"fecha": "2026-05-20", "horas": "2.00", "notas": "Cierre administrativo"},
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        hora_extra = HoraExtra.objects.get(pk=resp.data["id"])
        self.assertEqual(hora_extra.empleado, empleada)
        self.assertEqual(hora_extra.jefe_directo, dg_user)
        self.assertEqual(Notificacion.objects.filter(usuario=dg_user, tipo=Notificacion.TIPO_HORA_EXTRA).count(), 1)

        self.client.force_authenticate(user=dg_user)
        resp_dg = self.client.post(reverse("rrhh:hora-extra-autorizar", args=[hora_extra.id]))
        self.assertEqual(resp_dg.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.autorizado_por, dg_user)

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

    def test_prestamo_api_crea_solicitud_para_empleado_actual(self):
        resp = self.client.post(
            reverse("rrhh:prestamo-list"),
            {
                "concepto": "Apoyo personal",
                "metodo_pago": Prestamo.METODO_TRANSFERENCIA,
                "fecha_deposito": "2026-06-30",
                "importe": "1200.00",
                "num_quincenas": 4,
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["empleado"], self.empleado.id)
        self.assertTrue(resp.data["folio"].startswith("PR-"))
        self.assertEqual(resp.data["estado"], Prestamo.ESTADO_SOLICITADO)
        self.assertEqual(Decimal(resp.data["descuento_quincenal"]), Decimal("300.00"))
        self.assertEqual(Decimal(resp.data["saldo_actual"]), Decimal("1200.00"))

    def test_prestamo_api_jefe_directo_autoriza_desde_pwa(self):
        jefe_user = User.objects.create_user(username="jefe.prestamo", password="pass123")
        jefe_empleado = Empleado.objects.create(nombre="Jefe Prestamo", usuario_erp=jefe_user)
        self.empleado.jefe_directo = jefe_empleado
        self.empleado.save(update_fields=["jefe_directo"])

        resp = self.client.post(
            reverse("rrhh:prestamo-list"),
            {
                "concepto": "Apoyo personal",
                "metodo_pago": Prestamo.METODO_TRANSFERENCIA,
                "importe": "1200.00",
                "num_quincenas": 4,
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 201)
        prestamo = Prestamo.objects.get(pk=resp.data["id"])
        self.assertEqual(prestamo.jefe_directo, jefe_user)
        self.assertFalse(resp.data["puede_autorizar_jefe"])
        self.assertEqual(Notificacion.objects.filter(usuario=jefe_user, tipo=Notificacion.TIPO_PRESTAMO).count(), 1)

        self.client.force_authenticate(user=jefe_user)
        lista = self.client.get(reverse("rrhh:prestamo-list"), {"mis": "true"})
        self.assertEqual(lista.status_code, 200)
        self.assertTrue(lista.data[0]["puede_autorizar_jefe"])

        auth = self.client.post(reverse("rrhh:prestamo-autorizar-jefe", args=[prestamo.id]))
        self.assertEqual(auth.status_code, 200)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_AUTORIZADO)
        self.assertEqual(prestamo.autorizado_jefe, jefe_user)

    def test_prestamo_api_direccion_aprueba_desde_pwa(self):
        director = User.objects.create_user(username="director.prestamo", password="pass123")
        director.groups.add(Group.objects.get_or_create(name="DG")[0])
        prestamo = Prestamo.objects.create(
            empleado=self.empleado,
            concepto="Apoyo autorizado por jefe",
            fecha_solicitud=timezone.localdate(),
            importe=Decimal("1200.00"),
            num_quincenas=4,
            descuento_quincenal=Decimal("300.00"),
            saldo_actual=Decimal("1200.00"),
            estado=Prestamo.ESTADO_AUTORIZADO,
            firma_jefe=True,
            autorizado_jefe=self.user,
            fecha_auth_jefe=timezone.now(),
            creado_por=self.user,
        )

        self.client.force_authenticate(user=director)
        lista = self.client.get(reverse("rrhh:prestamo-list"), {"mis": "true"})

        self.assertEqual(lista.status_code, 200)
        self.assertTrue(lista.data[0]["puede_aprobar_direccion"])

        auth = self.client.post(reverse("rrhh:prestamo-aprobar-direccion", args=[prestamo.id]))

        self.assertEqual(auth.status_code, 200)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_ACTIVO)
        self.assertEqual(prestamo.autorizado_dg, director)
        self.assertEqual(PrestamoCuota.objects.filter(prestamo=prestamo).count(), 4)

    def test_prestamo_api_bloquea_solicitud_si_hay_saldo_vigente(self):
        Prestamo.objects.create(
            empleado=self.empleado,
            concepto="Préstamo vigente",
            fecha_solicitud=timezone.localdate(),
            importe=Decimal("900.00"),
            num_quincenas=3,
            descuento_quincenal=Decimal("300.00"),
            saldo_actual=Decimal("300.00"),
            estado=Prestamo.ESTADO_ACTIVO,
            creado_por=self.user,
        )

        resp = self.client.post(
            reverse("rrhh:prestamo-list"),
            {
                "concepto": "Segundo préstamo",
                "metodo_pago": Prestamo.METODO_TRANSFERENCIA,
                "importe": "500.00",
                "num_quincenas": 2,
            },
            format="json",
        )

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(Prestamo.objects.filter(empleado=self.empleado).count(), 1)

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


class HoraExtraAutorizacionAPIsTests(TestCase):
    def test_borrado_individual_sin_vinculo_y_extra_inexistente_conservan_respuesta(self):
        hora, _ = self._extra_y_url("generica", manual=True)
        asistencia = AsistenciaEmpleado.objects.create(empleado=hora.empleado, fecha=hora.fecha)
        response = self.client.delete(reverse("rrhh:asistencia-detail", args=[asistencia.pk]))
        self.assertEqual(response.status_code, 204)
        self.assertFalse(AsistenciaEmpleado.objects.filter(pk=asistencia.pk).exists())
        self.assertTrue(HoraExtra.objects.filter(pk=hora.pk).exists())
        response = self.client.patch(reverse("rrhh:hora-extra-detail", args=[hora.pk + 999999]),
            {"notas": "No existe"}, format="json")
        self.assertEqual(response.status_code, 404)

    def test_cascada_empleado_bloquea_todas_las_jornadas_antes_de_filas(self):
        from datetime import date
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        for queryset in (False, True):
            with self.subTest(queryset=queryset):
                empleado = Empleado.objects.create(nombre="Cascada jornadas")
                empleado_id = empleado.pk
                for dia in (18, 19):
                    AsistenciaEmpleado.objects.create(empleado=empleado, fecha=date(2026, 9, dia))
                HoraExtra.objects.create(empleado=empleado, fecha=date(2026, 9, 20), horas=Decimal("0.50"))
                with CaptureQueriesContext(connection) as consultas:
                    if queryset:
                        Empleado.objects.filter(pk=empleado.pk).delete()
                    else:
                        empleado.delete()
                sqls = [consulta["sql"] for consulta in consultas]
                primera_fila = next(i for i, sql in enumerate(sqls) if "FOR UPDATE" in sql)
                self.assertEqual(sum("pg_advisory_xact_lock" in sql for sql in sqls[:primera_fila]), 3)
                self.assertFalse(AsistenciaEmpleado.objects.filter(empleado_id=empleado_id).exists())
                self.assertFalse(HoraExtra.objects.filter(empleado_id=empleado_id).exists())

    def test_overflow_real_no_autoriza_con_monto_nulo(self):
        from django.db import DataError, connection

        hora, url = self._extra_y_url("generica", manual=True)
        hora.empleado.salario_diario = Decimal("9999999999.99")
        hora.empleado.save(update_fields=["salario_diario"])
        hora.horas = Decimal("99.99")
        hora.save(update_fields=["horas"])
        antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
        errores_pg = []
        def observar(execute, sql, params, many, context):
            try:
                return execute(sql, params, many, context)
            except DataError as exc:
                errores_pg.append(str(exc))
                raise
        self.client.raise_request_exception = False
        with connection.execute_wrapper(observar):
            response = self.client.post(url)
        self.assertTrue(any("overflow" in error for error in errores_pg), errores_pg)
        self.assertGreaterEqual(response.status_code, 400)
        self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_bonos_aborta_destino_compuesto_con_empleado_obsoleto(self):
        from rrhh.services_extra_bloqueos import bloquear_hora_extra

        for consumidor in ("produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor)
                otra, _ = self._extra_y_url(consumidor)
                snapshot = {}
                def mover_y_bloquear(*args, **kwargs):
                    HoraExtra.objects.filter(pk=hora.pk).update(empleado_id=otra.empleado_id)
                    snapshot.update(HoraExtra.objects.filter(pk=hora.pk).values().get())
                    return bloquear_hora_extra(*args, **kwargs)
                with patch("rrhh.bonos_horas_extra.bloquear_hora_extra", side_effect=mover_y_bloquear):
                    response = self.client.post(url.replace("/autorizar/", "/editar/"), {
                        "fecha": "2026-09-19", "horas": "0.50", "notas": "Corrección",
                        "motivo_cambio": "Jornada destino",
                    }, format="json")
                self.assertEqual(response.status_code, 409)
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), snapshot)

    def test_jornada_se_bloquea_antes_de_filas_e_incidencias(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        from rrhh.services import generar_horas_extra_automatico
        from rrhh.services_asistencia_reglas import evaluar_dia_empleado
        from rrhh.services_ajustes_asistencia import crear_ajuste_asistencia, aprobar_ajuste_asistencia
        from rrhh.signals_extra import conciliar_dia_extra

        hora, _ = self._extra_y_url("generica")
        hora.empleado.fecha_ingreso = hora.fecha
        hora.empleado.save(update_fields=["fecha_ingreso"])
        asistencia = hora.asistencia
        acciones = [
            lambda: generar_horas_extra_automatico(asistencia),
            lambda: evaluar_dia_empleado(hora.empleado, hora.fecha),
            lambda: conciliar_dia_extra(hora.empleado_id, hora.fecha),
            lambda: crear_ajuste_asistencia(
                hora.empleado, hora.fecha, "entrada", {"entrada": asistencia.entrada.isoformat()},
                "Prueba orden", self.jefe_user,
            ),
        ]
        for indice, accion in enumerate(acciones):
            with self.subTest(accion=indice), CaptureQueriesContext(connection) as consultas:
                resultado = accion()
            sqls = [consulta["sql"] for consulta in consultas]
            lock = next(i for i, sql in enumerate(sqls) if "pg_advisory_xact_lock" in sql)
            escrituras = [i for i, sql in enumerate(sqls) if "FOR UPDATE" in sql or sql.startswith(("INSERT", "UPDATE", "DELETE"))]
            self.assertTrue(escrituras)
            self.assertLess(lock, min(escrituras))
        with CaptureQueriesContext(connection) as consultas:
            aprobar_ajuste_asistencia(resultado, self.jefe_user)
        sqls = [consulta["sql"] for consulta in consultas]
        lock = next(i for i, sql in enumerate(sqls) if "pg_advisory_xact_lock" in sql)
        self.assertLess(lock, next(i for i, sql in enumerate(sqls) if "FOR UPDATE" in sql))

    def setUp(self):
        self.jefe_user = User.objects.create_superuser(username="jefe.extra.apis", password="pruebas")
        self.jefe = Empleado.objects.create(nombre="Jefe extras APIs", usuario_erp=self.jefe_user)
        self.client = APIClient()
        self.client.force_authenticate(self.jefe_user)

    def _extra_y_url(self, consumidor, *, manual=False):
        from datetime import date, datetime
        from bonos_produccion.models import BonoProduccionEmpleado, ConfigBonoPeriodo, AREA_HORNOS
        from bonos_ventas.models import BonoVentasEmpleado, ConfigBonoVentasPeriodo

        empleado = Empleado.objects.create(
            nombre=f"Empleado {consumidor}", jefe_directo=self.jefe,
            salario_diario=Decimal("400"), participa_bonos_produccion=True,
            area="PRODUCCION" if consumidor == "produccion" else "VENTAS",
        )
        asistencia = None if manual else AsistenciaEmpleado.objects.create(
            empleado=empleado, fecha=date(2026, 9, 18),
            entrada=timezone.make_aware(datetime(2026, 9, 18, 8)),
            salida=timezone.make_aware(datetime(2026, 9, 18, 16, 30)),
        )
        hora = HoraExtra.objects.create(
            empleado=empleado, fecha=date(2026, 9, 18), asistencia=asistencia,
            jefe_directo=self.jefe_user, horas=Decimal("0.50"),
            notas="Captura manual" if manual else "[Detección automática] Sin turno",
        )
        if consumidor == "generica":
            return hora, reverse("rrhh:hora-extra-autorizar", args=[hora.pk])
        if consumidor == "produccion":
            periodo, _ = ConfigBonoPeriodo.objects.get_or_create(mes=9, anio=2026)
            BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        else:
            periodo, _ = ConfigBonoVentasPeriodo.objects.get_or_create(mes=9, anio=2026)
            sucursal, _ = Sucursal.objects.get_or_create(codigo="EXTRA-API", defaults={"nombre": "Sucursal API"})
            BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)
        return hora, f"/api/bonos-{consumidor}/horas-extra/{hora.pk}/autorizar/?mes=9&anio=2026"

    def test_todas_las_apis_bloquean_automatica_sin_turno_sin_mutar(self):
        for consumidor in ("generica", "produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor)
                antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                response = self.client.post(url)
                self.assertEqual(response.status_code, 400)
                self.assertIn("Asigna el turno", response.json()["detail"])
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_todas_las_apis_bloquean_estados_no_pendientes(self):
        for consumidor in ("generica", "produccion", "ventas"):
            hora, url = self._extra_y_url(consumidor, manual=True)
            for estado in (HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_RECHAZADO, HoraExtra.ESTADO_CANCELADO, HoraExtra.ESTADO_PAGADO):
                with self.subTest(consumidor=consumidor, estado=estado):
                    HoraExtra.objects.filter(pk=hora.pk).update(estado=estado)
                    antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                    response = self.client.post(url)
                    self.assertEqual(response.status_code, 400)
                    self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_todas_las_apis_autorizan_manual_y_conservan_respuesta(self):
        for consumidor in ("generica", "produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor, manual=True)
                response = self.client.post(url)
                self.assertEqual(response.status_code, 200)
                hora.refresh_from_db()
                self.assertEqual(hora.estado, HoraExtra.ESTADO_AUTORIZADO)
                self.assertEqual(hora.monto_calculado, Decimal("50"))
                self.assertEqual(hora.autorizado_por_id, self.jefe_user.pk)
                if consumidor == "generica":
                    self.assertEqual(response.json(), {"ok": True, "monto": "50.00"})
                else:
                    self.assertEqual(response.json()["id"], hora.pk)
                    self.assertEqual(response.json()["estado"], HoraExtra.ESTADO_AUTORIZADO)

    def test_todas_las_apis_bloquean_saldo_automatico_obsoleto(self):
        from datetime import time
        from rrhh.bonos_horas_extra import _hora_extra_payload

        turno = Turno.objects.create(nombre="Turno APIs", hora_entrada=time(8), hora_salida=time(16))
        for consumidor in ("generica", "produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor)
                AsistenciaEmpleado.objects.filter(pk=hora.asistencia_id).update(turno=turno)
                HoraExtra.objects.filter(pk=hora.pk).update(horas=Decimal("2.00"))
                hora.refresh_from_db()
                payload = _hora_extra_payload(hora, self.jefe_user, puede_gestionar=True)
                self.assertFalse(payload["puede_autorizar"])
                self.assertTrue(payload["puede_rechazar"])
                self.assertIn("saldo automático vigente", payload["revision_extra"])
                antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                response = self.client.post(url)
                self.assertEqual(response.status_code, 400)
                self.assertIn("saldo automático vigente", response.json()["detail"])
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_ajuste_justificado_permite_autorizar_y_conserva_el_calculo_original(self):
        from datetime import datetime, time
        from rrhh.services_extra_conciliacion import conciliar_extra_diario, contexto_hora_extra

        hora, url = self._extra_y_url("produccion")
        turno = Turno.objects.create(nombre="Turno ajuste", hora_entrada=time(8), hora_salida=time(16))
        AsistenciaEmpleado.objects.filter(pk=hora.asistencia_id).update(
            turno=turno, salida=timezone.make_aware(datetime(2026, 9, 18, 16, 57)),
        )
        editada = self.client.post(url.replace("/autorizar/", "/editar/"), {
            "fecha": hora.fecha.isoformat(), "horas": "1",
            "notas": hora.notas, "motivo_cambio": "Cerrar la hora completa",
        }, format="json")
        self.assertEqual(editada.status_code, 200)
        hora.refresh_from_db()
        self.assertEqual(hora.ajuste_autorizacion["saldo"], "0.95")
        self.assertEqual(hora.ajuste_autorizacion["motivo"], "Cerrar la hora completa")
        self.assertTrue(contexto_hora_extra(hora)["ajuste_justificado"])
        notas = self.client.patch(reverse("rrhh:hora-extra-detail", args=[hora.pk]), {
            "empleado": hora.empleado_id, "fecha": hora.fecha.isoformat(),
            "horas": "1.00", "notas": hora.notas + "\nRevisado por Capital Humano.",
        }, format="json")
        self.assertEqual(notas.status_code, 200)
        hora.refresh_from_db()
        self.assertEqual(hora.ajuste_autorizacion["saldo"], "0.95")
        autorizada = self.client.post(url)
        self.assertEqual(autorizada.status_code, 200)
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora.horas, Decimal("1.00"))
        conciliacion = conciliar_extra_diario(hora.asistencia, [hora])
        self.assertEqual(conciliacion["detectado_minutos"], 57)
        self.assertEqual(conciliacion["autorizado_minutos"], 60)
        self.assertEqual(conciliacion["estado"], "Ajuste justificado autorizado")

    def test_ajuste_justificado_caduca_si_cambian_las_checadas(self):
        from datetime import datetime, time
        from rrhh.bonos_horas_extra import _hora_extra_payload
        from rrhh.services import generar_horas_extra_automatico

        hora, url = self._extra_y_url("produccion")
        turno = Turno.objects.create(nombre="Turno ajuste caducable", hora_entrada=time(8), hora_salida=time(16))
        AsistenciaEmpleado.objects.filter(pk=hora.asistencia_id).update(
            turno=turno, salida=timezone.make_aware(datetime(2026, 9, 18, 16, 57)),
        )
        editada = self.client.post(url.replace("/autorizar/", "/editar/"), {
            "fecha": hora.fecha.isoformat(), "horas": "1.00",
            "notas": hora.notas, "motivo_cambio": "Cerrar la hora completa",
        }, format="json")
        self.assertEqual(editada.status_code, 200)
        generar_horas_extra_automatico(AsistenciaEmpleado.objects.get(pk=hora.asistencia_id))
        hora.refresh_from_db()
        self.assertEqual(hora.horas, Decimal("1.00"))
        AsistenciaEmpleado.objects.filter(pk=hora.asistencia_id).update(
            salida=timezone.make_aware(datetime(2026, 9, 18, 17, 3)),
        )
        hora.refresh_from_db()
        payload = _hora_extra_payload(hora, self.jefe_user, puede_gestionar=True)
        self.assertFalse(payload["puede_autorizar"])
        self.assertTrue(payload["puede_rechazar"])
        self.assertTrue(payload["ajuste_requiere_revision"])
        bloqueada = self.client.post(url)
        self.assertEqual(bloqueada.status_code, 400)
        self.assertIn("saldo automático vigente", bloqueada.json()["detail"])
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_PENDIENTE)

    def test_reconocer_correccion_historica_no_cambia_horas_y_es_idempotente(self):
        from datetime import datetime, time
        from django.core.management import call_command
        from io import StringIO

        hora, url = self._extra_y_url("produccion")
        turno = Turno.objects.create(nombre="Turno histórico", hora_entrada=time(8), hora_salida=time(16))
        AsistenciaEmpleado.objects.filter(pk=hora.asistencia_id).update(
            turno=turno, salida=timezone.make_aware(datetime(2026, 9, 18, 16, 57)),
        )
        HoraExtra.objects.filter(pk=hora.pk).update(
            horas=Decimal("1.00"),
            notas="[Detección automática] Saldo anterior.\n\n"
                  "Correccion registrada por jefe.extra.apis el 2026-09-21 11:47: Cerrar la hora completa.",
        )
        preview = StringIO()
        call_command("registrar_ajustes_extra_pendientes", "--ids", str(hora.pk), stdout=preview)
        self.assertIn("por_registrar", preview.getvalue())
        hora.refresh_from_db()
        self.assertEqual(hora.ajuste_autorizacion, {})
        salida = StringIO()
        call_command("registrar_ajustes_extra_pendientes", "--ids", str(hora.pk), "--apply", stdout=salida)
        self.assertIn("registrado", salida.getvalue())
        hora.refresh_from_db()
        self.assertEqual(hora.horas, Decimal("1.00"))
        self.assertEqual(hora.ajuste_autorizacion["saldo"], "0.95")
        repetida = StringIO()
        call_command("registrar_ajustes_extra_pendientes", "--ids", str(hora.pk), "--apply", stdout=repetida)
        self.assertIn("ya_registrado", repetida.getvalue())
        self.assertEqual(self.client.post(url).status_code, 200)

    def test_editar_notas_en_cada_api_no_elude_origen_automatico(self):
        for consumidor in ("generica", "produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor)
                if consumidor == "generica":
                    editada = self.client.patch(
                        reverse("rrhh:hora-extra-detail", args=[hora.pk]),
                        {"notas": "Motivo corregido sin prefijo"}, format="json",
                    )
                else:
                    editada = self.client.post(url.replace("/autorizar/", "/editar/"), {
                        "notas": "Motivo corregido sin prefijo", "fecha": hora.fecha.isoformat(),
                        "horas": str(hora.horas), "motivo_cambio": "Aclarar el motivo operativo",
                    }, format="json")
                self.assertEqual(editada.status_code, 200)
                hora.refresh_from_db()
                self.assertIsNotNone(hora.asistencia_id)
                self.assertFalse(hora.notas.startswith("[Detección automática]"))
                antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                response = self.client.post(url)
                self.assertEqual(response.status_code, 400)
                self.assertIn("Asigna el turno", response.json()["detail"])
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_origen_automatico_depende_del_enlace_y_no_de_notas(self):
        from rrhh.services_extra_conciliacion import es_hora_extra_automatica

        for notas in ("", "Motivo manual", "[Detección automática] Extra"):
            with self.subTest(notas=notas):
                self.assertTrue(es_hora_extra_automatica(HoraExtra(asistencia_id=1, notas=notas)))
                self.assertFalse(es_hora_extra_automatica(HoraExtra(asistencia_id=None, notas=notas)))

    def test_api_no_elimina_asistencia_vinculada_ni_degrada_origen(self):
        hora, url = self._extra_y_url("generica")
        antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
        response = self.client.delete(reverse("rrhh:asistencia-detail", args=[hora.asistencia_id]))
        self.assertEqual(response.status_code, 409)
        self.assertTrue(AsistenciaEmpleado.objects.filter(pk=hora.asistencia_id).exists())
        self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)
        self.assertEqual(self.client.post(url).status_code, 400)

    def test_dominio_protege_borrado_de_asistencia_vinculada(self):
        from django.db import transaction
        from django.db.models.deletion import ProtectedError

        hora, _url = self._extra_y_url("generica")
        with self.assertRaises(ProtectedError), transaction.atomic():
            AsistenciaEmpleado.objects.filter(pk=hora.asistencia_id).delete()
        hora.refresh_from_db()
        self.assertIsNotNone(hora.asistencia_id)

    def test_bonos_cambio_fecha_permite_rechazar_vinculo_inconsistente(self):
        for consumidor in ("produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor)
                response = self.client.post(url.replace("/autorizar/", "/editar/"), {
                    "fecha": "2026-09-19", "horas": "0.50", "notas": "Corregir fecha",
                    "motivo_cambio": "Corregir jornada capturada",
                }, format="json")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(self.client.post(url).status_code, 400)
                response = self.client.post(url.replace("/autorizar/", "/rechazar/"))
                self.assertEqual(response.status_code, 200)
                hora.refresh_from_db()
                self.assertEqual(hora.estado, HoraExtra.ESTADO_RECHAZADO)

    def test_todas_las_apis_rechazan_pendiente_y_conservan_respuesta(self):
        for consumidor in ("generica", "produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor)
                url = url.replace("/autorizar/", "/rechazar/")
                response = self.client.post(url)
                self.assertEqual(response.status_code, 200)
                hora.refresh_from_db()
                self.assertEqual(hora.estado, HoraExtra.ESTADO_RECHAZADO)
                self.assertIsNone(hora.monto_calculado)
                if consumidor == "generica":
                    self.assertEqual(response.json(), {"ok": True})
                else:
                    self.assertEqual(response.json()["id"], hora.pk)
                    self.assertEqual(response.json()["estado"], HoraExtra.ESTADO_RECHAZADO)
                antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                self.assertEqual(self.client.post(url).status_code, 400)
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_rechazo_superusuario_conserva_permisos_distintos_de_bonos(self):
        otro_jefe = User.objects.create_user(username="otro.jefe.extra.api")
        for consumidor in ("generica", "produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = self._extra_y_url(consumidor, manual=True)
                HoraExtra.objects.filter(pk=hora.pk).update(jefe_directo=otro_jefe)
                antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                response = self.client.post(url.replace("/autorizar/", "/rechazar/"))
                self.assertEqual(response.status_code, 200 if consumidor == "generica" else 403)
                if consumidor != "generica":
                    self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)


class HoraExtraAutorizacionConcurrenteTests(TransactionTestCase):
    def test_borrado_lote_asistencias_y_traslado_admin_no_se_bloquean(self):
        from datetime import date
        from hashlib import blake2b
        from queue import Queue
        from threading import Event, Thread
        from time import monotonic
        from django.contrib import admin
        from django.db import close_old_connections, connection, connections
        from django.test import RequestFactory
        from rrhh.admin import AsistenciaAdmin, HoraExtraAdmin

        empleado = Empleado.objects.create(nombre="Lote asistencia")
        def clave(fecha):
            return int.from_bytes(blake2b(f"rrhh:extra:{empleado.pk}:{fecha}".encode(), digest_size=8).digest(), "big", signed=True)
        menor, mayor = sorted([date(2026, 9, 18), date(2026, 9, 19)], key=clave)
        # PK ascendente deliberadamente opuesto al orden de advisory.
        primera = AsistenciaEmpleado.objects.create(empleado=empleado, fecha=mayor)
        segunda = AsistenciaEmpleado.objects.create(empleado=empleado, fecha=menor)
        hora = HoraExtra.objects.create(empleado=empleado, fecha=menor, horas=Decimal("0.50"))
        primer_lock, continuar, fin_traslado = Event(), Event(), Event()
        resultados, backend_pid = Queue(), Queue()
        request = RequestFactory().post("/admin/rrhh/")

        def eliminar():
            close_old_connections()
            try:
                def observar(execute, sql, params, many, context):
                    result = execute(sql, params, many, context)
                    if "pg_advisory_xact_lock" in sql and not primer_lock.is_set():
                        primer_lock.set()
                        if not continuar.wait(10):
                            raise TimeoutError("Borrado no liberado")
                    return result
                with connection.execute_wrapper(observar):
                    AsistenciaAdmin(AsistenciaEmpleado, admin.site).delete_queryset(request,
                        AsistenciaEmpleado.objects.filter(pk__in=[primera.pk, segunda.pk]))
                resultados.put("")
            except Exception as exc:
                resultados.put(exc)
            finally:
                connections.close_all()

        def trasladar():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '8s'")
                    cursor.execute("SELECT pg_backend_pid()")
                    backend_pid.put(cursor.fetchone()[0])
                actual = HoraExtra.objects.get(pk=hora.pk)
                actual.fecha = mayor
                HoraExtraAdmin(HoraExtra, admin.site).save_model(request, actual, None, True)
                resultados.put("")
            except Exception as exc:
                resultados.put(exc)
            finally:
                fin_traslado.set()
                connections.close_all()

        borrador, traslado = Thread(target=eliminar, daemon=True), Thread(target=trasladar, daemon=True)
        bloqueado = False
        borrador.start()
        try:
            self.assertTrue(primer_lock.wait(5))
            traslado.start()
            pid = backend_pid.get(timeout=5)
            limite = monotonic() + 5
            while monotonic() < limite and not fin_traslado.is_set():
                with connection.cursor() as cursor:
                    cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                    bloqueado = cursor.fetchone()[0]
                if bloqueado:
                    break
                fin_traslado.wait(0.01)
        finally:
            continuar.set()
            borrador.join(12)
            if traslado.ident:
                traslado.join(12)
        self.assertFalse(borrador.is_alive())
        self.assertFalse(traslado.is_alive())
        for _ in range(2):
            resultado = resultados.get(timeout=1)
            self.assertNotIsInstance(resultado, Exception)
            self.assertEqual(resultado, "")
        self.assertTrue(bloqueado)
        self.assertFalse(AsistenciaEmpleado.objects.filter(pk__in=[primera.pk, segunda.pk]).exists())
        hora.refresh_from_db()
        self.assertEqual(hora.fecha, mayor)
        self.assertEqual(hora.estado, HoraExtra.ESTADO_PENDIENTE)

    def test_movimiento_durante_espera_del_helper_devuelve_conflicto(self):
        from datetime import date
        from queue import Queue
        from threading import Event, Thread
        from time import monotonic
        from django.db import close_old_connections, connection, connections, transaction
        from rrhh.services_extra_bloqueos import bloquear_jornadas_extra

        self.jefe_user = User.objects.create_superuser(username="conflicto.helper", password="pruebas")
        self.jefe = Empleado.objects.create(nombre="Jefe conflicto", usuario_erp=self.jefe_user)
        for consumidor in ("generica", "produccion", "ventas"):
            with self.subTest(consumidor=consumidor):
                hora, url = HoraExtraAutorizacionAPIsTests._extra_y_url(self, consumidor, manual=True)
                resultados, backend_pid = Queue(), Queue()
                fin = Event()
                def editar():
                    close_old_connections()
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute("SET lock_timeout = '8s'")
                            cursor.execute("SELECT pg_backend_pid()")
                            backend_pid.put(cursor.fetchone()[0])
                        client = APIClient()
                        client.force_authenticate(self.jefe_user)
                        if consumidor == "generica":
                            response = client.patch(reverse("rrhh:hora-extra-detail", args=[hora.pk]),
                                {"notas": "Edición esperando"}, format="json")
                        else:
                            response = client.post(url.replace("/autorizar/", "/editar/"), {
                                "fecha": hora.fecha.isoformat(), "horas": "0.50",
                                "notas": "Edición esperando", "motivo_cambio": "Conflicto",
                            }, format="json")
                        resultados.put(response)
                    except Exception as exc:
                        resultados.put(exc)
                    finally:
                        fin.set()
                        connections.close_all()
                worker = Thread(target=editar, daemon=True)
                bloqueado = False
                try:
                    with transaction.atomic():
                        bloquear_jornadas_extra([(hora.empleado_id, hora.fecha), (hora.empleado_id, date(2026, 9, 19))])
                        worker.start()
                        pid = backend_pid.get(timeout=5)
                        limite = monotonic() + 5
                        while monotonic() < limite and not fin.is_set():
                            with connection.cursor() as cursor:
                                cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                                bloqueado = cursor.fetchone()[0]
                            if bloqueado:
                                break
                            fin.wait(0.01)
                        actual = HoraExtra.objects.get(pk=hora.pk)
                        actual.fecha = date(2026, 9, 19)
                        actual.save(update_fields=["fecha"])
                        antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                finally:
                    worker.join(12)
                self.assertFalse(worker.is_alive())
                resultado = resultados.get(timeout=1)
                if isinstance(resultado, Exception):
                    raise resultado
                self.assertTrue(bloqueado)
                self.assertEqual(resultado.status_code, 409)
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_patch_parcial_con_jornada_movida_aborta_sin_escribir(self):
        from datetime import date
        from queue import Queue
        from threading import Event, Thread, current_thread
        from django.db import close_old_connections, connections
        from rrhh.api_views import HoraExtraViewSet

        jefe = User.objects.create_superuser(username="patch.jornada", password="pruebas")
        empleado_a = Empleado.objects.create(nombre="Jornada A")
        empleado_b = Empleado.objects.create(nombre="Jornada B")
        hora = HoraExtra.objects.create(empleado=empleado_a, fecha=date(2026, 9, 18),
            jefe_directo=jefe, horas=Decimal("0.50"), notas="Original")
        leida, continuar = Event(), Event()
        resultados = Queue()
        original = HoraExtraViewSet.get_object

        def observar(view):
            actual = original(view)
            if current_thread() is worker and not leida.is_set():
                leida.set()
                if not continuar.wait(10):
                    raise TimeoutError("PATCH no liberado")
            return actual

        def editar():
            close_old_connections()
            try:
                client = APIClient()
                client.force_authenticate(jefe)
                resultados.put(client.patch(reverse("rrhh:hora-extra-detail", args=[hora.pk]),
                    {"empleado": empleado_b.pk}, format="json"))
            except Exception as exc:
                resultados.put(exc)
            finally:
                connections.close_all()

        worker = Thread(target=editar, daemon=True)
        with patch.object(HoraExtraViewSet, "get_object", observar):
            worker.start()
            try:
                self.assertTrue(leida.wait(5))
                client = APIClient()
                client.force_authenticate(jefe)
                response = client.patch(reverse("rrhh:hora-extra-detail", args=[hora.pk]),
                    {"fecha": "2026-09-19"}, format="json")
                self.assertEqual(response.status_code, 200)
                antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
            finally:
                continuar.set()
                worker.join(12)
        self.assertFalse(worker.is_alive())
        resultado = resultados.get(timeout=1)
        if isinstance(resultado, Exception):
            raise resultado
        self.assertEqual(resultado.status_code, 409)
        self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_admin_guardado_y_resolver_no_invierten_bloqueos(self):
        from datetime import date
        from queue import Queue
        from threading import Event, Thread
        from time import monotonic
        from django.contrib import admin
        from django.db import close_old_connections, connection, connections, transaction
        from django.test import RequestFactory
        from rrhh.admin import HoraExtraAdmin
        from rrhh.services_horas_extra_autorizacion import resolver_hora_extra

        jefe = User.objects.create_superuser(username="admin.extra.concurrente", password="pruebas")
        empleado = Empleado.objects.create(nombre="Admin extra", salario_diario=Decimal("400"))
        hora = HoraExtra.objects.create(empleado=empleado, fecha=date(2026, 9, 18),
            jefe_directo=jefe, horas=Decimal("0.50"), notas="Antes admin")
        guardada, continuar, fin_resolver = Event(), Event(), Event()
        resultados, backend_pid = Queue(), Queue()

        def guardar_admin():
            close_old_connections()
            try:
                request = RequestFactory().post("/admin/rrhh/horaextra/")
                request.user = jefe
                def observar(execute, sql, params, many, context):
                    result = execute(sql, params, many, context)
                    if sql.startswith('UPDATE "rrhh_horaextra"') and not guardada.is_set():
                        guardada.set()
                        if not continuar.wait(10):
                            raise TimeoutError("Admin no liberado")
                    return result
                with transaction.atomic(), connection.execute_wrapper(observar):
                    actual = HoraExtra.objects.get(pk=hora.pk)
                    actual.horas = Decimal("0.75")
                    HoraExtraAdmin(HoraExtra, admin.site).save_model(request, actual, None, True)
                resultados.put("")
            except Exception as exc:
                resultados.put(exc)
            finally:
                connections.close_all()

        def autorizar():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '8s'")
                    cursor.execute("SELECT pg_backend_pid()")
                    backend_pid.put(cursor.fetchone()[0])
                resultados.put(resolver_hora_extra(hora.pk, "autorizar", jefe)[2])
            except Exception as exc:
                resultados.put(exc)
            finally:
                fin_resolver.set()
                connections.close_all()

        escritor, autorizador = Thread(target=guardar_admin, daemon=True), Thread(target=autorizar, daemon=True)
        bloqueado = False
        escritor.start()
        try:
            self.assertTrue(guardada.wait(5))
            autorizador.start()
            pid = backend_pid.get(timeout=5)
            limite = monotonic() + 5
            while monotonic() < limite and not fin_resolver.is_set():
                with connection.cursor() as cursor:
                    cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                    bloqueado = cursor.fetchone()[0]
                if bloqueado:
                    break
                fin_resolver.wait(0.01)
        finally:
            continuar.set()
            escritor.join(12)
            if autorizador.ident:
                autorizador.join(12)
        self.assertFalse(escritor.is_alive())
        self.assertFalse(autorizador.is_alive())
        for _ in range(2):
            resultado = resultados.get(timeout=1)
            self.assertNotIsInstance(resultado, Exception)
            self.assertEqual(resultado, "")
        self.assertTrue(bloqueado)
        hora.refresh_from_db()
        self.assertEqual(hora.horas, Decimal("0.75"))
        self.assertEqual(hora.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora.monto_calculado, Decimal("75.00"))

    def test_orm_autocommit_y_admin_bloquean_antes_del_sql(self):
        from datetime import date
        from django.contrib import admin
        from django.db import connection
        from django.test import RequestFactory
        from django.test.utils import CaptureQueriesContext
        from rrhh.admin import HoraExtraAdmin

        self.assertTrue(connection.get_autocommit())
        empleado = Empleado.objects.create(nombre="Orden ORM")
        request = RequestFactory().post("/admin/rrhh/horaextra/")
        model_admin = HoraExtraAdmin(HoraExtra, admin.site)
        hora = HoraExtra(empleado=empleado, fecha=date(2026, 9, 18), horas=Decimal("0.50"))
        operaciones = [
            (lambda: hora.save(force_insert=True, using="default"), 'INSERT INTO "rrhh_horaextra"'),
            (lambda: model_admin.save_model(request, hora, None, True), 'UPDATE "rrhh_horaextra"'),
            (lambda: hora.save(force_update=True, using="default", update_fields=["notas"]), 'UPDATE "rrhh_horaextra"'),
            (lambda: model_admin.delete_model(request, hora), 'DELETE FROM "rrhh_horaextra"'),
        ]
        for operacion, sql_objetivo in operaciones:
            with self.subTest(sql=sql_objetivo), CaptureQueriesContext(connection) as consultas:
                operacion()
            sqls = [consulta["sql"] for consulta in consultas]
            bloqueo = next(i for i, sql in enumerate(sqls) if "pg_advisory_xact_lock" in sql)
            escritura = next(i for i, sql in enumerate(sqls) if sql.startswith(sql_objetivo))
            self.assertLess(bloqueo, escritura)
            self.assertIn("BEGIN", sqls[:escritura])
            self.assertIn("COMMIT", sqls[escritura:])

        segunda = HoraExtra(empleado=empleado, fecha=date(2026, 9, 19), horas=Decimal("0.50"))
        with CaptureQueriesContext(connection) as consultas:
            model_admin.save_model(request, segunda, None, False)
        sqls = [consulta["sql"] for consulta in consultas]
        self.assertLess(next(i for i, sql in enumerate(sqls) if "pg_advisory_xact_lock" in sql),
            next(i for i, sql in enumerate(sqls) if sql.startswith('INSERT INTO "rrhh_horaextra"')))
        tercera = HoraExtra.objects.create(empleado=empleado, fecha=date(2026, 9, 20), horas=Decimal("0.50"))
        with CaptureQueriesContext(connection) as consultas:
            model_admin.delete_queryset(request, HoraExtra.objects.filter(pk__in=[segunda.pk, tercera.pk]))
        sqls = [consulta["sql"] for consulta in consultas]
        primera_fila = next(i for i, sql in enumerate(sqls) if "FOR UPDATE" in sql)
        self.assertEqual(sum("pg_advisory_xact_lock" in sql for sql in sqls[:primera_fila]), 2)
        self.assertFalse(HoraExtra.objects.filter(pk__in=[segunda.pk, tercera.pk]).exists())

    def test_raw_fixture_no_concilia_y_update_fields_conserva_jornada_real(self):
        from datetime import date
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        empleado = Empleado.objects.create(nombre="Fixture extra")
        hora = HoraExtra(empleado=empleado, fecha=date(2026, 9, 18), horas=Decimal("0.50"), creado_en=timezone.now())
        with CaptureQueriesContext(connection) as consultas:
            hora.save_base(raw=True, force_insert=True, using="default")
        self.assertFalse(any("pg_advisory_xact_lock" in consulta["sql"] for consulta in consultas))
        otra = HoraExtra.objects.get(pk=hora.pk)
        otra.fecha = date(2026, 9, 19)
        otra.save(update_fields=["fecha"])
        hora.estado = HoraExtra.ESTADO_RECHAZADO
        with patch("rrhh.signals_extra.conciliar_dia_extra") as conciliar:
            hora.save(update_fields=["estado"])
        conciliar.assert_called_once_with(empleado.pk, date(2026, 9, 19))
        hora.refresh_from_db()
        self.assertEqual(hora.fecha, date(2026, 9, 19))
        self.assertEqual(hora.estado, HoraExtra.ESTADO_RECHAZADO)

    def test_primera_asistencia_serializa_con_resolucion_manual(self):
        from datetime import date, datetime, time
        from queue import Queue
        from threading import Event, Thread
        from time import monotonic
        from django.db import close_old_connections, connection, connections
        from rrhh.services import generar_horas_extra_automatico
        from rrhh.services_horas_extra_autorizacion import resolver_hora_extra

        jefe = User.objects.create_user(username="extra.primera.concurrente")
        empleado = Empleado.objects.create(nombre="Primera asistencia", salario_diario=Decimal("400"))
        fecha = date(2026, 9, 18)
        turno = Turno.objects.create(nombre="Primera", hora_entrada=time(8), hora_salida=time(16))
        manual = HoraExtra.objects.create(
            empleado=empleado, fecha=fecha, jefe_directo=jefe, horas=Decimal("0.50"), notas="Manual",
        )
        sin_asistencia, continuar, generador_terminado = Event(), Event(), Event()
        resultados, pid_generador = Queue(), Queue()

        def autorizar():
            close_old_connections()
            try:
                def observar(execute, sql, params, many, context):
                    result = execute(sql, params, many, context)
                    if 'FROM "rrhh_asistenciaempleado"' in sql and "FOR UPDATE" in sql and not sin_asistencia.is_set():
                        sin_asistencia.set()
                        if not continuar.wait(10):
                            raise TimeoutError("No se liberó resolución sin asistencia")
                    return result
                with connection.execute_wrapper(observar):
                    resultados.put(resolver_hora_extra(manual.pk, "autorizar", jefe)[2])
            except Exception as exc:
                resultados.put(exc)
            finally:
                connections.close_all()

        def generar(asistencia):
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '8s'")
                    cursor.execute("SELECT pg_backend_pid()")
                    pid_generador.put(cursor.fetchone()[0])
                generar_horas_extra_automatico(asistencia)
                resultados.put("")
            except Exception as exc:
                resultados.put(exc)
            finally:
                generador_terminado.set()
                connections.close_all()

        autorizador = Thread(target=autorizar, daemon=True)
        generador = None
        bloqueado = False
        autorizador.start()
        try:
            self.assertTrue(sin_asistencia.wait(5))
            # INSERT confirmado después del SELECT vacío; reproduce la ventana real.
            asistencia = AsistenciaEmpleado.objects.create(
                empleado=empleado, fecha=fecha, turno=turno,
                entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
                salida=timezone.make_aware(datetime.combine(fecha, time(17))),
            )
            generador = Thread(target=generar, args=(asistencia,), daemon=True)
            generador.start()
            pid = pid_generador.get(timeout=5)
            limite = monotonic() + 5
            while monotonic() < limite and not generador_terminado.is_set():
                with connection.cursor() as cursor:
                    cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                    bloqueado = cursor.fetchone()[0]
                if bloqueado:
                    break
                generador_terminado.wait(0.01)
        finally:
            continuar.set()
            autorizador.join(12)
            if generador:
                generador.join(12)
        self.assertFalse(autorizador.is_alive())
        self.assertIsNotNone(generador)
        self.assertFalse(generador.is_alive())
        for _ in range(2):
            resultado = resultados.get(timeout=1)
            if isinstance(resultado, Exception):
                raise resultado
            self.assertEqual(resultado, "")
        self.assertTrue(bloqueado, "El generador debe esperar la jornada aunque el SELECT inicial fuese vacío")
        manual.refresh_from_db()
        self.assertEqual(manual.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(manual.monto_calculado, Decimal("50.00"))
        automatica = HoraExtra.objects.get(asistencia=asistencia)
        self.assertEqual(automatica.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(automatica.horas, Decimal("0.50"))

    def test_patch_obsoleto_no_revierte_autorizacion_concurrente(self):
        from datetime import date
        from queue import Queue
        from threading import Event, Thread, current_thread
        from django.db import close_old_connections, connections
        from rrhh.api_views import HoraExtraViewSet

        jefe = User.objects.create_superuser(username="extra.patch.concurrente", password="pruebas")
        empleado = Empleado.objects.create(nombre="Extra PATCH", salario_diario=Decimal("400"))
        hora = HoraExtra.objects.create(
            empleado=empleado, fecha=date(2026, 9, 18), jefe_directo=jefe,
            horas=Decimal("0.50"), notas="Antes del PATCH",
        )
        leida, continuar = Event(), Event()
        resultado = Queue()
        original = HoraExtraViewSet.get_object

        def observar(view):
            instance = original(view)
            if current_thread() is worker and not leida.is_set():
                leida.set()
                if not continuar.wait(timeout=10):
                    raise TimeoutError("No se liberó el PATCH")
            return instance

        def editar():
            close_old_connections()
            try:
                client = APIClient()
                client.force_authenticate(jefe)
                resultado.put(client.patch(reverse("rrhh:hora-extra-detail", args=[hora.pk]), {
                    "notas": "Después del PATCH",
                }, format="json"))
            except Exception as exc:
                resultado.put(exc)
            finally:
                connections.close_all()

        worker = Thread(target=editar, daemon=True)
        with patch.object(HoraExtraViewSet, "get_object", observar):
            worker.start()
            try:
                self.assertTrue(leida.wait(timeout=5))
                client = APIClient()
                client.force_authenticate(jefe)
                response = client.post(reverse("rrhh:hora-extra-autorizar", args=[hora.pk]))
                self.assertEqual(response.status_code, 200)
                autorizada = HoraExtra.objects.filter(pk=hora.pk).values().get()
            finally:
                continuar.set()
                worker.join(timeout=12)
        self.assertFalse(worker.is_alive())
        response = resultado.get(timeout=1)
        if isinstance(response, Exception):
            raise response
        self.assertEqual(response.status_code, 200)
        actual = HoraExtra.objects.filter(pk=hora.pk).values().get()
        for campo in ("estado", "monto_calculado", "autorizado_por_id", "fecha_autorizacion_jefe"):
            self.assertEqual(actual[campo], autorizada[campo], campo)
        self.assertEqual(actual["notas"], "Después del PATCH")

    def test_resolucion_manual_y_generador_serializan_sin_deadlock(self):
        from datetime import date, datetime, time
        from queue import Queue
        from threading import Event, Thread
        from time import monotonic

        from django.db import close_old_connections, connection, connections, transaction
        from django.test import Client
        from rrhh.services import generar_horas_extra_automatico

        self.assertEqual(connection.vendor, "postgresql")
        jefe = User.objects.create_user(username="jefe.extra.manual.concurrente")
        empleado = Empleado.objects.create(nombre="Manual concurrencia", salario_diario=Decimal("400"))
        turno = Turno.objects.create(nombre="Turno manual", hora_entrada=time(8), hora_salida=time(16))
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=empleado, fecha=date(2026, 9, 18), turno=turno,
            entrada=timezone.make_aware(datetime(2026, 9, 18, 8)),
            salida=timezone.make_aware(datetime(2026, 9, 18, 16, 30)),
        )
        manual = HoraExtra.objects.create(
            empleado=empleado, fecha=asistencia.fecha, jefe_directo=jefe,
            horas=Decimal("0.25"), notas="Apoyo manual",
        )
        client = Client()
        client.force_login(jefe)
        esperando_asistencia, terminado = Event(), Event()
        resultado, backend_pid = Queue(), Queue()

        def autorizar():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '8s'")
                    cursor.execute("SELECT pg_backend_pid()")
                    backend_pid.put(cursor.fetchone()[0])

                def observar(execute, sql, params, many, context):
                    if "pg_advisory_xact_lock" in sql:
                        esperando_asistencia.set()
                    return execute(sql, params, many, context)

                with connection.execute_wrapper(observar):
                    resultado.put(client.post(reverse("rrhh:rrhh_he_list"), {
                        "hora_extra_id": manual.pk, "action": "autorizar",
                    }, HTTP_ACCEPT="application/json"))
            except Exception as exc:
                resultado.put(exc)
            finally:
                connections.close_all()
                terminado.set()

        bloqueado = False
        worker = Thread(target=autorizar, daemon=True)
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL lock_timeout = '8s'")
                from rrhh.services_extra_bloqueos import bloquear_jornadas_extra
                bloquear_jornadas_extra([(asistencia.empleado_id, asistencia.fecha)])
                asistencia = AsistenciaEmpleado.objects.select_for_update().get(pk=asistencia.pk)
                worker.start()
                pid = backend_pid.get(timeout=5)
                if esperando_asistencia.wait(timeout=5):
                    limite = monotonic() + 5
                    while monotonic() < limite and not terminado.is_set():
                        with connection.cursor() as cursor:
                            cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                            bloqueado = cursor.fetchone()[0]
                        if bloqueado:
                            break
                        terminado.wait(timeout=0.01)
                asistencia.salida = timezone.make_aware(datetime(2026, 9, 18, 17))
                asistencia.save(update_fields=["salida"])
                generar_horas_extra_automatico(asistencia)
        finally:
            worker.join(timeout=12)
        self.assertFalse(worker.is_alive())
        response = resultado.get(timeout=1)
        if isinstance(response, Exception):
            raise response
        self.assertTrue(bloqueado, "La manual debe esperar primero la asistencia del día")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        manual.refresh_from_db()
        self.assertEqual(manual.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(manual.monto_calculado, Decimal("25"))
        automatica = HoraExtra.objects.get(asistencia=asistencia)
        self.assertEqual(automatica.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(automatica.horas, Decimal("0.75"))

    def test_autorizacion_espera_asistencia_y_rechaza_propuesta_cancelada(self):
        from datetime import date, datetime, time
        from queue import Queue
        from threading import Event, Thread
        from time import monotonic

        from django.db import close_old_connections, connection, connections, transaction
        from django.test import Client
        from rrhh.services import generar_horas_extra_automatico

        self.assertEqual(connection.vendor, "postgresql")
        jefe = User.objects.create_user(username="jefe.extra.concurrente")
        empleado = Empleado.objects.create(nombre="Repartidor concurrencia", salario_diario=Decimal("400"))
        turno = Turno.objects.create(nombre="Turno concurrencia", hora_entrada=time(8), hora_salida=time(16))
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=empleado, fecha=date(2026, 9, 18), turno=turno,
            entrada=timezone.make_aware(datetime(2026, 9, 18, 8)),
            salida=timezone.make_aware(datetime(2026, 9, 18, 16, 30)),
        )
        hora = HoraExtra.objects.create(
            empleado=empleado, fecha=asistencia.fecha, asistencia=asistencia,
            jefe_directo=jefe, horas=Decimal("0.50"), notas="[Detección automática] Concurrencia",
        )
        client = Client()
        client.force_login(jefe)
        intentando_bloquear = Event()
        terminado = Event()
        resultado = Queue()
        backend_pid = Queue()

        def autorizar():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    backend_pid.put(cursor.fetchone()[0])

                def observar_bloqueo(execute, sql, params, many, context):
                    if "pg_advisory_xact_lock" in sql:
                        intentando_bloquear.set()
                    return execute(sql, params, many, context)

                with connection.execute_wrapper(observar_bloqueo):
                    response = client.post(reverse("rrhh:rrhh_he_list"), {
                        "hora_extra_id": hora.pk, "action": "autorizar",
                    }, HTTP_ACCEPT="application/json")
                resultado.put(response)
            except Exception as exc:
                resultado.put(exc)
            finally:
                connections.close_all()
                terminado.set()

        bloqueado_en_postgres = False
        worker = Thread(target=autorizar, daemon=True)
        try:
            with transaction.atomic():
                from rrhh.services_extra_bloqueos import bloquear_jornadas_extra
                bloquear_jornadas_extra([(asistencia.empleado_id, asistencia.fecha)])
                asistencia = AsistenciaEmpleado.objects.select_for_update().get(pk=asistencia.pk)
                worker.start()
                pid = backend_pid.get(timeout=5)
                if intentando_bloquear.wait(timeout=5):
                    limite = monotonic() + 5
                    while monotonic() < limite and not terminado.is_set():
                        with connection.cursor() as cursor:
                            cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                            bloqueado_en_postgres = cursor.fetchone()[0]
                        if bloqueado_en_postgres:
                            break
                        terminado.wait(timeout=0.01)
                asistencia.salida = timezone.make_aware(datetime(2026, 9, 18, 16))
                asistencia.save(update_fields=["salida"])
                generar_horas_extra_automatico(asistencia)
        finally:
            worker.join(timeout=10)
        self.assertFalse(worker.is_alive(), "La autorización no liberó su conexión")
        response = resultado.get(timeout=1)
        if isinstance(response, Exception):
            raise response
        self.assertTrue(bloqueado_en_postgres, "La petición debe esperar el bloqueo real de asistencia")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_CANCELADO)
        self.assertEqual(hora.horas, Decimal("0.50"))
        self.assertIsNone(hora.monto_calculado)
        self.assertIsNone(hora.autorizado_por_id)
        self.assertIsNone(hora.fecha_autorizacion_jefe)


class RRHHViewsTests(TestCase):
    def test_conflicto_de_jornada_conserva_toast_y_ancla(self):
        from datetime import date
        from rrhh.services_extra_bloqueos import JornadaExtraConflict

        empleado = Empleado.objects.create(nombre="Conflicto bandeja")
        hora = HoraExtra.objects.create(empleado=empleado, fecha=date(2026, 9, 18),
            jefe_directo=self.user, horas=Decimal("0.50"))
        antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
        datos = {"hora_extra_id": hora.pk, "action": "autorizar"}
        with patch("rrhh.views.resolver_hora_extra", side_effect=JornadaExtraConflict("La jornada cambió. Recarga.")):
            response = self.client.post(reverse("rrhh:rrhh_he_list"), datos, HTTP_ACCEPT="application/json")
            self.assertEqual(response.status_code, 409)
            self.assertFalse(response.json()["ok"])
            self.assertEqual(response.json()["toast"]["type"], "error")
            self.assertTrue(response.json()["toast"]["persistent"])
            response = self.client.post(reverse("rrhh:rrhh_he_list"), datos)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response["Location"], f'{reverse("rrhh:rrhh_he_list")}#hora-extra-{hora.pk}')
        self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

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

    def test_empleados_muestra_sucursal_como_select_de_catalogo(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Sucursal", sucursal="Matriz", sucursal_ref=sucursal)

        resp = self.client.get(reverse("rrhh:empleados"), secure=True)

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, '<select id="sucursal" class="input-field" name="sucursal_id">', html=False)
        self.assertContains(resp, f'<option value="{sucursal.id}">{sucursal.nombre}</option>', html=False)
        self.assertContains(resp, f'<select id="edit_sucursal_{empleado.id}" class="input-field" name="sucursal_id">', html=False)
        self.assertContains(resp, f'<option value="{sucursal.id}" selected>{sucursal.nombre}</option>', html=False)

    def test_empleados_campos_de_usuario_se_pueden_revelar_desde_el_checkbox(self):
        resp = self.client.get(reverse("rrhh:empleados"), secure=True)

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'class="rrhh-field" data-new-user-field hidden', count=2)
        self.assertNotContains(resp, 'class="rrhh-field pd-u-c8be1ccba6" data-new-user-field')
        self.assertContains(resp, "field.hidden = !show;")
        self.assertContains(resp, "input.required = show;")

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

    def test_empleados_crea_sucursal_desde_catalogo_y_guarda_nombre_canonico(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "nombre": "Empleado Catalogo Sucursal",
                "sucursal_id": str(sucursal.id),
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado = Empleado.objects.get(nombre="Empleado Catalogo Sucursal")
        self.assertEqual(empleado.sucursal, "Sucursal Matriz")
        self.assertEqual(empleado.sucursal_ref_id, sucursal.id)

    def test_empleados_update_sucursal_desde_catalogo_canoniza_texto_legacy(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz", activa=True)
        empleado = Empleado.objects.create(nombre="Empleado Legacy Matriz", sucursal="Matriz", salario_diario="300.00")

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "update",
                "empleado_id": str(empleado.id),
                "nombre": empleado.nombre,
                "sucursal_id": str(sucursal.id),
                "salario_diario": "300.00",
                "activo": "on",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.sucursal, "Sucursal Matriz")
        self.assertEqual(empleado.sucursal_ref_id, sucursal.id)

    def test_empleados_rechaza_sucursal_fuera_de_catalogo(self):
        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "nombre": "Empleado Sucursal Invalida",
                "sucursal": "Sucursal Inventada",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Selecciona una sucursal válida del catálogo.")
        self.assertFalse(Empleado.objects.filter(nombre="Empleado Sucursal Invalida").exists())

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

    def test_baja_desactiva_empleado_usuario_y_repartidor_operativo(self):
        from api.logistica_views import _can_operate_pwa
        from logistica.models import Repartidor

        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        usuario = User.objects.create_user(username="rep.baja", password="pass123")
        usuario.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        empleado = Empleado.objects.create(
            nombre="REPARTIDOR BAJA",
            fecha_ingreso=timezone.localdate(),
            area="REPARTIDORES",
            puesto_operativo="REPARTIDOR",
            usuario_erp=usuario,
            activo=True,
        )
        Repartidor.objects.create(user=usuario, sucursal=sucursal)

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "baja",
                "empleado": str(empleado.id),
                "fecha_baja": "2026-06-15",
                "motivo": EmpleadoBaja.MOTIVO_OTRO,
            },
            follow=True,
        )

        self.assertEqual(resp.status_code, 200)
        self.assertTrue(EmpleadoBaja.objects.filter(empleado=empleado).exists())
        empleado.refresh_from_db()
        usuario.refresh_from_db()
        self.assertFalse(empleado.activo)
        self.assertFalse(usuario.is_active)
        self.assertFalse(usuario.groups.filter(name__iexact="repartidor").exists())
        self.assertFalse(_can_operate_pwa(usuario))

        activos = self.client.get(reverse("rrhh:empleados"))
        self.assertNotIn(empleado.id, [e.id for e in activos.context["empleados"]])
        inactivos = self.client.get(reverse("rrhh:empleados"), {"estado": "inactivos"})
        self.assertIn(empleado.id, [e.id for e in inactivos.context["empleados"]])

    def test_crear_empleadobaja_directo_desactiva_empleado(self):
        # Cubre rutas fuera de las vistas (admin de Django, shell, imports).
        empleado = Empleado.objects.create(
            nombre="BAJA DIRECTA MODELO",
            fecha_ingreso=timezone.localdate(),
            activo=True,
        )
        EmpleadoBaja.objects.create(
            empleado=empleado,
            fecha_ingreso=empleado.fecha_ingreso,
            fecha_baja=timezone.localdate(),
            motivo=EmpleadoBaja.MOTIVO_OTRO,
        )
        empleado.refresh_from_db()
        self.assertFalse(empleado.activo)

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

    def test_empleados_revierte_alta_si_faltan_credenciales_de_usuario(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)

        resp = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "nombre": "REPARTIDOR SIN CREDENCIALES",
                "area": "REPARTIDORES",
                "crear_usuario_erp": "on",
                "sucursal_app_id": str(sucursal.id),
                "salario_diario": "300.00",
            },
            follow=True,
            secure=True,
        )

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Captura el usuario para el acceso ERP/app.")
        self.assertFalse(Empleado.objects.filter(nombre="REPARTIDOR SIN CREDENCIALES").exists())

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
        # El view ya solo acepta sucursales del catálogo (sucursal_id o texto que
        # resuelva a una Sucursal real); el texto libre "Matriz" dejó de ser válido.
        sucursal_matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
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
                "sucursal_id": str(sucursal_matriz.id),
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
        resp_detalle = self.client.get(reverse("rrhh:rrhh_prestamo_detalle", args=[prestamo.pk]))
        self.assertContains(resp_detalle, "Autorizar jefe")
        self.assertIn("no-store", resp_detalle["Cache-Control"])

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

    def test_direccion_ve_y_aprueba_prestamo_autorizado_en_su_bandeja(self):
        from datetime import date

        director = User.objects.create_user(username="director.bandeja", password="pass123")
        director.groups.add(Group.objects.get_or_create(name="DG")[0])
        empleado = Empleado.objects.create(nombre="Empleado pendiente Dirección", salario_diario="400.00")
        prestamo = Prestamo.objects.create(
            empleado=empleado,
            concepto="Solicitud lista para Dirección",
            fecha_solicitud=date(2026, 7, 30),
            importe=Decimal("1600.00"),
            num_quincenas=4,
            descuento_quincenal=Decimal("400.00"),
            saldo_actual=Decimal("1600.00"),
            estado=Prestamo.ESTADO_AUTORIZADO,
            firma_jefe=True,
            autorizado_jefe=self.user,
            fecha_auth_jefe=timezone.now(),
            creado_por=self.user,
        )

        self.client.force_login(director)
        response = self.client.get(reverse("rrhh:rrhh_prestamos_lista"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["por_autorizar"].count(), 1)
        self.assertContains(response, prestamo.folio)
        self.assertContains(response, "Aprobar Dirección")
        self.assertContains(response, reverse("rrhh:rrhh_prestamo_auth_dg", args=[prestamo.pk]))
        self.assertContains(response, "data-async-action")

        response = self.client.post(
            reverse("rrhh:rrhh_prestamo_auth_dg", args=[prestamo.pk]),
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_ACTIVO)
        self.assertEqual(prestamo.autorizado_dg, director)
        self.assertEqual(PrestamoCuota.objects.filter(prestamo=prestamo).count(), 4)

    def test_superuser_autoriza_prestamo_asignado_a_otro_jefe(self):
        from datetime import date

        jefe = User.objects.create_user(username="jefe.prestamo", password="pass123")
        super_user = User.objects.create_user(username="mauricio.prestamo", is_superuser=True, is_staff=True)
        empleado = Empleado.objects.create(nombre="Empleado Prestamo Superuser", salario_diario="400.00")
        prestamo = Prestamo.objects.create(
            empleado=empleado,
            concepto="Solicitud superuser",
            fecha_solicitud=date(2026, 5, 15),
            importe=Decimal("800.00"),
            num_quincenas=2,
            descuento_quincenal=Decimal("400.00"),
            saldo_actual=Decimal("800.00"),
            estado=Prestamo.ESTADO_SOLICITADO,
            jefe_directo=jefe,
            creado_por=self.user,
        )

        self.client.force_login(super_user)
        response = self.client.post(reverse("rrhh:rrhh_prestamo_auth_jefe", args=[prestamo.pk]), follow=True)

        self.assertEqual(response.status_code, 200)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_AUTORIZADO)
        self.assertEqual(prestamo.autorizado_jefe, super_user)

    def test_jefe_duplicado_por_email_ve_y_autoriza_prestamo(self):
        from datetime import date

        jefe_asignado = User.objects.create_user(
            username="maburgos12",
            email="mauricioburgos12@gmail.com",
            password="pass123",
        )
        jefe_login = User.objects.create_user(
            username="maburgos12@pollyanasdolce.com",
            email="mauricioburgos12@gmail.com",
            password="pass123",
        )
        empleado = Empleado.objects.create(nombre="Empleado con Jefe Duplicado", area="VENTAS", salario_diario="400.00")
        prestamo = Prestamo.objects.create(
            empleado=empleado,
            concepto="Solicitud con jefe duplicado",
            fecha_solicitud=date(2026, 5, 15),
            importe=Decimal("800.00"),
            num_quincenas=2,
            descuento_quincenal=Decimal("400.00"),
            saldo_actual=Decimal("800.00"),
            estado=Prestamo.ESTADO_SOLICITADO,
            jefe_directo=jefe_asignado,
            creado_por=self.user,
        )

        self.client.force_login(jefe_login)
        resp_detalle = self.client.get(reverse("rrhh:rrhh_prestamo_detalle", args=[prestamo.pk]))
        self.assertContains(resp_detalle, "Autorizar jefe")

        resp_lista = self.client.get(reverse("rrhh:rrhh_prestamos_lista"))
        self.assertContains(resp_lista, prestamo.folio)

        resp_auth = self.client.post(reverse("rrhh:rrhh_prestamo_auth_jefe", args=[prestamo.pk]), follow=True)
        self.assertEqual(resp_auth.status_code, 200)
        prestamo.refresh_from_db()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_AUTORIZADO)
        self.assertEqual(prestamo.autorizado_jefe, jefe_login)

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

    def _hora_extra_para_autorizacion(self, *, automatica=True, turno=None):
        from datetime import date, datetime

        jefe = User.objects.create_user(username="jefe.contexto.extra")
        jefe_empleado = Empleado.objects.create(nombre="Jefe de reparto", usuario_erp=jefe)
        empleado = Empleado.objects.create(
            nombre="Repartidor contexto extra", puesto_operativo="REPARTIDOR",
            jefe_directo=jefe_empleado, salario_diario=Decimal("400.00"),
        )
        asistencia = None
        if automatica:
            asistencia = AsistenciaEmpleado.objects.create(
                empleado=empleado, fecha=date(2026, 9, 18), turno=turno,
                entrada=timezone.make_aware(datetime(2026, 9, 18, 8)),
                salida=timezone.make_aware(datetime(2026, 9, 18, 16, 30)),
            )
        hora = HoraExtra.objects.create(
            empleado=empleado, jefe_directo=jefe, asistencia=asistencia,
            fecha=date(2026, 9, 18), horas=Decimal("0.50"),
            notas="[Detección automática] Extra propuesta" if automatica else "Apoyo manual",
        )
        self.client.force_login(jefe)
        return hora

    def test_hora_automatica_sin_turno_no_se_puede_autorizar(self):
        hora = self._hora_extra_para_autorizacion()
        antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "autorizar",
        }, follow=True)
        self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)
        self.assertContains(response, "Asigna el turno")
        self.assertEqual(response.redirect_chain[0][0], f'{reverse("rrhh:rrhh_he_list")}#hora-extra-{hora.pk}')

    def test_hora_manual_sin_turno_conserva_autorizacion(self):
        hora = self._hora_extra_para_autorizacion(automatica=False)
        response = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertContains(response, "No evaluada en captura manual")
        self.assertNotContains(response, "Confirmada por captura manual")
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "autorizar",
        })
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(response.url, f'{reverse("rrhh:rrhh_he_list")}#hora-extra-{hora.pk}')

    def test_hora_automatica_con_saldo_obsoleto_no_se_autoriza_ni_muta(self):
        from datetime import time

        turno = Turno.objects.create(nombre="Turno saldo actual", hora_entrada=time(8), hora_salida=time(16))
        hora = self._hora_extra_para_autorizacion(turno=turno)
        HoraExtra.objects.filter(pk=hora.pk).update(horas=Decimal("2.00"), monto_calculado=Decimal("200.00"))
        antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "autorizar",
        }, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertIn("reevalúa", response.json()["toast"]["message"])
        self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)
        self.assertContains(self.client.get(reverse("rrhh:rrhh_he_list")), 'disabled aria-disabled="true"')

    def test_hora_automatica_saldo_parcial_considera_cobertura_de_otro_jefe(self):
        from datetime import time

        turno = Turno.objects.create(nombre="Turno saldo parcial", hora_entrada=time(8), hora_salida=time(15, 30))
        hora = self._hora_extra_para_autorizacion(turno=turno)
        cobertura = HoraExtra.objects.create(
            empleado=hora.empleado, fecha=hora.fecha, horas=Decimal("0.50"),
            notas="Cobertura independiente", estado=HoraExtra.ESTADO_AUTORIZADO,
        )
        HoraExtra.objects.create(
            empleado=hora.empleado, fecha=hora.fecha, horas=Decimal("3.00"),
            notas="Cancelada no cubre", estado=HoraExtra.ESTADO_CANCELADO,
        )
        response = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertNotContains(response, 'disabled aria-disabled="true"')
        self.assertNotContains(response, f'id="hora-extra-{cobertura.pk}"')
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "autorizar",
        }, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora.horas, Decimal("0.50"))

    def test_hora_extra_no_pendiente_no_admite_acciones(self):
        hora = self._hora_extra_para_autorizacion(automatica=False)
        for estado in (HoraExtra.ESTADO_CANCELADO, HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO, HoraExtra.ESTADO_RECHAZADO):
            HoraExtra.objects.filter(pk=hora.pk).update(estado=estado)
            antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
            for action in ("autorizar", "rechazar"):
                with self.subTest(estado=estado, action=action):
                    response = self.client.post(reverse("rrhh:rrhh_he_list"), {
                        "hora_extra_id": hora.pk, "action": action,
                    }, HTTP_ACCEPT="application/json")
                    self.assertEqual(response.status_code, 400)
                    self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_horas_extra_contexto_no_agrega_consultas_por_registro(self):
        from datetime import timedelta
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        hora = self._hora_extra_para_autorizacion()

        def consultas_extra():
            with CaptureQueriesContext(connection) as consultas:
                response = self.client.get(reverse("rrhh:rrhh_he_list"))
            self.assertEqual(response.status_code, 200)
            return [q["sql"] for q in consultas if 'FROM "rrhh_horaextra"' in q["sql"]]

        una = consultas_extra()
        for dia in range(1, 5):
            asistencia = AsistenciaEmpleado.objects.create(
                empleado=hora.empleado, fecha=hora.fecha + timedelta(days=dia),
                entrada=hora.asistencia.entrada + timedelta(days=dia),
                salida=hora.asistencia.salida + timedelta(days=dia),
            )
            HoraExtra.objects.create(
                empleado=hora.empleado, fecha=asistencia.fecha, asistencia=asistencia,
                jefe_directo=hora.jefe_directo, horas=Decimal("0.50"), notas=hora.notas,
            )
        cinco = consultas_extra()
        self.assertEqual(len(cinco), len(una))

    def test_hora_automatica_bloqueada_responde_json_sin_mutar(self):
        hora = self._hora_extra_para_autorizacion()
        antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
        for headers in ({"HTTP_ACCEPT": "application/json"}, {"HTTP_X_REQUESTED_WITH": "XMLHttpRequest"}):
            with self.subTest(headers=headers):
                response = self.client.post(reverse("rrhh:rrhh_he_list"), {
                    "hora_extra_id": hora.pk, "action": "autorizar",
                }, **headers)
                self.assertEqual(response.status_code, 400)
                self.assertFalse(response.json()["ok"])
                self.assertEqual(response.json()["toast"]["type"], "error")
                self.assertTrue(response.json()["toast"]["persistent"])
                self.assertIn("Asigna el turno", response.json()["toast"]["message"])
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_hora_automatica_historica_sin_turno_recomienda_revision_sin_mutar(self):
        hora = self._hora_extra_para_autorizacion()
        for estado in (HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO):
            with self.subTest(estado=estado):
                HoraExtra.objects.filter(pk=hora.pk).update(estado=estado)
                antes = HoraExtra.objects.filter(pk=hora.pk).values().get()
                response = self.client.get(reverse("rrhh:rrhh_he_list"))
                self.assertContains(response, "Revisión recomendada")
                self.assertEqual(HoraExtra.objects.filter(pk=hora.pk).values().get(), antes)

    def test_hora_automatica_sin_extra_detectado_no_se_puede_autorizar(self):
        from datetime import time

        turno = Turno.objects.create(nombre="Turno completo", hora_entrada=time(8), hora_salida=time(16, 30))
        hora = self._hora_extra_para_autorizacion(turno=turno)
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "autorizar",
        }, follow=True)
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertContains(response, "No se detectan horas extra")

    def test_horas_extra_contexto_semantico_y_acciones_progresivas(self):
        hora = self._hora_extra_para_autorizacion()
        response = self.client.get(reverse("rrhh:rrhh_he_list"))
        for contenido in (
            f'id="hora-extra-{hora.pk}"', 'aria-label="Contexto del cálculo"',
            "<dt>Modalidad</dt>", "<dt>Comida</dt>", "<dt>Turno</dt>", "<dt>Resultado</dt>",
            "Ruta", "Comida no observable", "Sin turno asignado", "No calculable",
            'disabled aria-disabled="true"', 'data-async-action data-reset-on-success="false"',
            'data-pending-label="Autorizando…"', 'data-pending-label="Rechazando…"',
            'class="ch-calculation-warning"', 'role="status"', "Asigna el turno",
            "?v=20260920-contexto-extra-v1",
        ):
            self.assertContains(response, contenido)

    def test_hora_automatica_positiva_sin_comida_permite_autorizacion_json(self):
        from datetime import time

        turno = Turno.objects.create(nombre="Turno reparto", hora_entrada=time(8), hora_salida=time(16))
        hora = self._hora_extra_para_autorizacion(turno=turno)
        response = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertContains(response, "Requiere revisión")
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "autorizar",
        }, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["toast"]["type"], "success")
        self.assertTrue(response.json()["reload"])
        self.assertEqual(response.json()["redirect"], f'{reverse("rrhh:rrhh_he_list")}#hora-extra-{hora.pk}')
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_AUTORIZADO)

    def test_hora_automatica_bloqueada_permite_rechazar_json(self):
        hora = self._hora_extra_para_autorizacion()
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "rechazar",
        }, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["toast"]["type"], "success")
        self.assertTrue(response.json()["reload"])
        self.assertEqual(response.json()["redirect"], f'{reverse("rrhh:rrhh_he_list")}#hora-extra-{hora.pk}')
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_RECHAZADO)

    def test_hora_extra_accion_invalida_no_muta_y_responde_error(self):
        hora = self._hora_extra_para_autorizacion(automatica=False)
        response = self.client.post(reverse("rrhh:rrhh_he_list"), {
            "hora_extra_id": hora.pk, "action": "otra",
        }, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        hora.refresh_from_db()
        self.assertEqual(hora.estado, HoraExtra.ESTADO_PENDIENTE)

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

    def test_superuser_ve_y_autoriza_horas_extra_asignadas_a_otro_jefe(self):
        from datetime import date

        jefe_user = User.objects.create_user(username="jefe.he.web", password="pass123")
        super_user = User.objects.create_user(username="mauricio.he.web", is_superuser=True, is_staff=True)
        empleado = Empleado.objects.create(
            nombre="Empleado HE Superuser",
            area="PRODUCCION",
            salario_diario="400.00",
        )
        hora_extra = HoraExtra.objects.create(
            empleado=empleado,
            jefe_directo=jefe_user,
            fecha=date(2026, 5, 20),
            horas=Decimal("1.50"),
            notas="Carga extraordinaria",
        )

        self.client.force_login(super_user)
        resp_lista = self.client.get(reverse("rrhh:rrhh_he_list"))
        self.assertEqual(resp_lista.status_code, 200)
        self.assertContains(resp_lista, "Empleado HE Superuser")
        self.assertContains(resp_lista, "Autorizar")

        resp_auth = self.client.post(
            reverse("rrhh:rrhh_he_list"),
            {"hora_extra_id": str(hora_extra.id), "action": "autorizar"},
            follow=True,
        )
        self.assertEqual(resp_auth.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.autorizado_por, super_user)

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


class ReporteAsistenciaFechaIngresoTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula-fecha-ingreso", is_superuser=True, is_staff=True)
        self.client.force_login(self.user)

    def test_reporte_no_castiga_faltas_previas_a_fecha_ingreso(self):
        from datetime import date

        empleado = Empleado.objects.create(
            nombre="ANAYA BERNAL CARLOS EZEQUIEL",
            codigo="347",
            fecha_ingreso=date(2026, 6, 10),
            activo=True,
            sucursal="Produccion",
        )
        IncidenciaAsistencia.objects.create(
            empleado=empleado,
            fecha=date(2026, 6, 9),
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_RESUELTO,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            detalle="Incidencia resuelta por reevaluacion automatica.",
        )
        IncidenciaAsistencia.objects.create(
            empleado=empleado,
            fecha=date(2026, 6, 11),
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            detalle="Sin registro posterior al ingreso.",
        )

        response = self.client.get(
            reverse("rrhh:rrhh_reporte_asistencia"),
            {
                "fecha_inicio": "2026-06-01",
                "fecha_fin": "2026-06-11",
                "empleado": str(empleado.id),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        reporte = response.context["reportes"][0]
        self.assertEqual(reporte["resumen"]["faltas"], 1)
        filas_pre_ingreso = [fila for fila in reporte["filas"] if fila["fecha"] < empleado.fecha_ingreso]
        self.assertEqual(len(filas_pre_ingreso), 9)
        self.assertTrue(all(fila["estado_laboral"] == "pre_ingreso" for fila in filas_pre_ingreso))
        self.assertContains(response, "No laborado (previo ingreso)")
        self.assertContains(response, "Sin registro posterior al ingreso.")
        self.assertNotContains(response, "Incidencia resuelta por reevaluacion automatica.")

    def test_export_reporte_marca_pre_ingreso_como_no_laborado(self):
        from datetime import date

        empleado = Empleado.objects.create(
            nombre="ANAYA BERNAL CARLOS EZEQUIEL",
            codigo="347",
            fecha_ingreso=date(2026, 6, 10),
            activo=True,
            sucursal="Produccion",
        )
        IncidenciaAsistencia.objects.create(
            empleado=empleado,
            fecha=date(2026, 6, 9),
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_RESUELTO,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            detalle="No debe exportar como falta.",
        )

        response = self.client.get(
            reverse("rrhh:rrhh_reporte_asistencia"),
            {
                "fecha_inicio": "2026-06-09",
                "fecha_fin": "2026-06-10",
                "empleado": str(empleado.id),
                "export": "csv",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn("No laborado (previo ingreso)", content)
        self.assertIn("No aplica", content)
        self.assertNotIn("No debe exportar como falta.", content)


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
