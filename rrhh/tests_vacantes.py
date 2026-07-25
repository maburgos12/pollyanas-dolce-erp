from datetime import date

from django.contrib.auth.models import Group, User
from django.core.exceptions import PermissionDenied, ValidationError
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Notificacion, Sucursal
from rrhh.models import (
    AltaPendienteEmpleado,
    CandidatoVacante,
    Empleado,
    VacanteCobertura,
    VacanteMovimiento,
    VacanteRRHH,
    VacanteSeguimiento,
)
from rrhh.services_vacantes import (
    agregar_candidato,
    agregar_seguimiento_vacante,
    aprobar_vacante_autorizacion,
    avanzar_etapa_candidato,
    can_autorizar_vacante,
    can_solicitar_vacantes,
    can_ver_vacante,
    consumir_alta_pendiente,
    cubrir_vacante,
    crear_alta_pendiente_desde_candidato,
    crear_solicitud_vacante,
    devolver_vacante_correccion,
    enviar_vacante_autorizacion,
    iniciar_reclutamiento_vacante,
    reenviar_vacante_revision,
)
from rrhh.views import _module_tabs


class VacantesSolicitudServiceTests(TestCase):
    def setUp(self):
        self.rrhh_user = User.objects.create_user(username="paula.rrhh", password="pass123")
        self.rrhh_user.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        self.dg_user = User.objects.create_user(username="mauricio.dg", password="pass123")
        self.dg_user.groups.add(Group.objects.get_or_create(name="DG")[0])
        self.solicitante = User.objects.create_user(username="johana.ventas", password="pass123")
        self.solicitante.groups.add(Group.objects.get_or_create(name="VENTAS")[0])
        self.jefe_ventas = User.objects.create_user(username="jefa.ventas", password="pass123")
        self.jefe_ventas.groups.add(Group.objects.get_or_create(name="VENTAS")[0])
        Empleado.objects.create(
            nombre="Jefa Ventas",
            departamento=Empleado.DEP_VENTAS,
            puesto="Jefe de Ventas",
            puesto_operativo="JEFATURA",
            usuario_erp=self.jefe_ventas,
        )
        self.sucursal = Sucursal.objects.create(codigo="GSV", nombre="Guasave Centro")

    def test_crear_solicitud_vacante_genera_folio_historial_y_notifica_rrhh(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            sucursal=self.sucursal,
            departamento=Empleado.DEP_VENTAS,
            cantidad_solicitada=2,
            motivo_solicitud="Cubrir dos cajas por apertura de turno.",
        )

        self.assertTrue(vacante.folio.startswith(f"VAC-{timezone.localdate():%y%m}-"))
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_SOLICITADA)
        self.assertEqual(vacante.area, "VENTAS")
        self.assertEqual(vacante.puesto, "CAJERA")
        self.assertEqual(vacante.cantidad_solicitada, 2)
        self.assertEqual(vacante.solicitado_por, self.solicitante)
        self.assertEqual(vacante.movimientos.count(), 1)
        self.assertEqual(vacante.movimientos.first().estado_nuevo, VacanteRRHH.ESTADO_SOLICITADA)
        self.assertTrue(
            Notificacion.objects.filter(
                usuario=self.rrhh_user,
                objeto_tipo="rrhh.VacanteRRHH",
                objeto_id=str(vacante.id),
            ).exists()
        )

    def test_flujo_jefe_directo_reclutamiento_y_cobertura_cierra_solicitud(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            cantidad_solicitada=1,
            departamento=Empleado.DEP_VENTAS,
        )

        enviar_vacante_autorizacion(vacante, self.rrhh_user, "Validada por CH")
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_PENDIENTE_DIRECCION)
        self.assertEqual(vacante.validado_rrhh_por, self.rrhh_user)
        self.assertFalse(vacante.requiere_direccion)
        self.assertEqual(vacante.autorizador_asignado, self.jefe_ventas)
        self.assertTrue(Notificacion.objects.filter(usuario=self.jefe_ventas, objeto_id=str(vacante.id)).exists())
        self.assertFalse(Notificacion.objects.filter(usuario=self.dg_user, objeto_id=str(vacante.id)).exists())
        self.assertTrue(can_ver_vacante(self.dg_user, vacante))
        self.assertFalse(can_autorizar_vacante(self.dg_user, vacante))

        with self.assertRaises(PermissionDenied):
            aprobar_vacante_autorizacion(vacante, self.dg_user, "Dirección no autoriza operativas")

        aprobar_vacante_autorizacion(vacante, self.jefe_ventas, "Aprobada por jefe directo")
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_AUTORIZADA)
        self.assertEqual(vacante.autorizado_por, self.jefe_ventas)

        iniciar_reclutamiento_vacante(vacante, self.rrhh_user)
        empleado = Empleado.objects.create(nombre="Cajera Nueva", departamento=Empleado.DEP_VENTAS)
        cobertura = cubrir_vacante(vacante, empleado, self.rrhh_user, fecha_cobertura=date(2026, 6, 3))
        vacante.refresh_from_db()

        self.assertEqual(cobertura.vacante, vacante)
        self.assertEqual(VacanteCobertura.objects.filter(vacante=vacante).count(), 1)
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_CUBIERTA)
        self.assertEqual(vacante.fecha_cubierta, date(2026, 6, 3))
        self.assertEqual(vacante.empleado_cubrio, empleado)
        self.assertEqual(VacanteMovimiento.objects.filter(vacante=vacante).count(), 5)

    def test_crear_alta_pendiente_desde_candidato_no_crea_empleado_y_reusa_pendiente(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            fecha_necesaria=date(2026, 6, 5),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            sucursal=self.sucursal,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        candidato = agregar_candidato(
            vacante,
            self.rrhh_user,
            nombre="Candidata Nueva",
            telefono="6670000000",
            email="candidata@example.com",
        )
        avanzar_etapa_candidato(candidato, self.rrhh_user, CandidatoVacante.ETAPA_SELECCIONADO, "Aceptada")

        pendiente = crear_alta_pendiente_desde_candidato(candidato, self.rrhh_user, "Preparar alta")
        misma_pendiente = crear_alta_pendiente_desde_candidato(candidato, self.rrhh_user, "Preparar alta otra vez")

        self.assertEqual(pendiente, misma_pendiente)
        self.assertEqual(AltaPendienteEmpleado.objects.count(), 1)
        self.assertEqual(Empleado.objects.filter(nombre="Candidata Nueva").count(), 0)
        self.assertEqual(pendiente.vacante, vacante)
        self.assertEqual(pendiente.candidato, candidato)
        self.assertEqual(pendiente.nombre, "Candidata Nueva")
        self.assertEqual(pendiente.telefono, "6670000000")
        self.assertEqual(pendiente.email, "candidata@example.com")
        self.assertEqual(pendiente.sucursal, self.sucursal.nombre)
        self.assertEqual(pendiente.departamento, Empleado.DEP_VENTAS)
        self.assertEqual(pendiente.area, "VENTAS")
        self.assertEqual(pendiente.puesto, "CAJERA")
        self.assertEqual(pendiente.fecha_ingreso_sugerida, date(2026, 6, 5))

    def test_crear_alta_pendiente_requiere_rrhh(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        candidato = CandidatoVacante.objects.create(
            vacante=vacante,
            nombre="Candidata Nueva",
            etapa_actual=CandidatoVacante.ETAPA_SELECCIONADO,
        )

        with self.assertRaises(PermissionDenied):
            crear_alta_pendiente_desde_candidato(candidato, self.solicitante)

    def test_crear_alta_pendiente_rechaza_vacante_fuera_de_reclutamiento(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_PAUSADA,
        )
        candidato = CandidatoVacante.objects.create(
            vacante=vacante,
            nombre="Candidata Pausada",
            etapa_actual=CandidatoVacante.ETAPA_SELECCIONADO,
        )

        with self.assertRaisesMessage(ValidationError, "vacantes en reclutamiento"):
            crear_alta_pendiente_desde_candidato(candidato, self.rrhh_user)

    def test_consumir_alta_pendiente_liga_candidato_empleado_y_cubre_vacante(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            fecha_necesaria=date(2026, 6, 5),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        candidato = agregar_candidato(vacante, self.rrhh_user, nombre="Candidata Nueva")
        avanzar_etapa_candidato(candidato, self.rrhh_user, CandidatoVacante.ETAPA_SELECCIONADO)
        pendiente = crear_alta_pendiente_desde_candidato(candidato, self.rrhh_user)
        empleado = Empleado.objects.create(nombre="Candidata Nueva", departamento=Empleado.DEP_VENTAS)

        cobertura = consumir_alta_pendiente(pendiente, empleado, self.rrhh_user)
        candidato.refresh_from_db()
        pendiente.refresh_from_db()
        vacante.refresh_from_db()

        self.assertEqual(candidato.empleado, empleado)
        self.assertEqual(candidato.etapa_actual, CandidatoVacante.ETAPA_CONTRATADO)
        self.assertEqual(pendiente.estado, AltaPendienteEmpleado.ESTADO_CONVERTIDA)
        self.assertEqual(pendiente.empleado, empleado)
        self.assertEqual(cobertura.empleado, empleado)
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_CUBIERTA)
        self.assertEqual(vacante.empleado_cubrio, empleado)

    def test_cubrir_vacante_revalida_estado_bajo_lock(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        stale_vacante = VacanteRRHH.objects.get(pk=vacante.pk)
        VacanteRRHH.objects.filter(pk=vacante.pk).update(estado=VacanteRRHH.ESTADO_CUBIERTA)
        empleado = Empleado.objects.create(nombre="Cajera Nueva", departamento=Empleado.DEP_VENTAS)

        with self.assertRaisesMessage(ValidationError, "autorizada"):
            cubrir_vacante(stale_vacante, empleado, self.rrhh_user)

    def test_cubrir_vacante_con_dos_plazas_cierra_hasta_segunda_cobertura(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
            cantidad_solicitada=2,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        primera = Empleado.objects.create(nombre="Cajera Uno", departamento=Empleado.DEP_VENTAS)
        segunda = Empleado.objects.create(nombre="Cajera Dos", departamento=Empleado.DEP_VENTAS)

        cubrir_vacante(vacante, primera, self.rrhh_user, fecha_cobertura=date(2026, 6, 3))
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_RECLUTAMIENTO)
        self.assertEqual(vacante.coberturas.count(), 1)

        cubrir_vacante(vacante, segunda, self.rrhh_user, fecha_cobertura=date(2026, 6, 4))
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_CUBIERTA)
        self.assertEqual(vacante.coberturas.count(), 2)

    def test_autorizador_operativo_puede_usar_nivel_jefatura_sin_puesto_jefatura(self):
        empleado = self.jefe_ventas.empleado_rrhh
        empleado.puesto = "Responsable de ventas"
        empleado.puesto_operativo = "CAJAS"
        empleado.nivel_organizacional = Empleado.NIVEL_JEFATURA
        empleado.save(update_fields=["puesto", "puesto_operativo", "nivel_organizacional"])

        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
        )

        enviar_vacante_autorizacion(vacante, self.rrhh_user, "Validada por CH")
        vacante.refresh_from_db()

        self.assertFalse(vacante.requiere_direccion)
        self.assertEqual(vacante.autorizador_asignado, self.jefe_ventas)

    def test_direccion_autoriza_solo_jefaturas(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="jefe de ventas",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
        )
        enviar_vacante_autorizacion(vacante, self.rrhh_user)
        vacante.refresh_from_db()

        self.assertTrue(vacante.requiere_direccion)
        self.assertEqual(vacante.tipo_autorizacion, VacanteRRHH.AUTORIZACION_DIRECCION)
        self.assertIsNone(vacante.autorizador_asignado)
        self.assertTrue(Notificacion.objects.filter(usuario=self.dg_user, objeto_id=str(vacante.id)).exists())

        aprobar_vacante_autorizacion(vacante, self.dg_user)
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_AUTORIZADA)
        self.assertEqual(vacante.autorizado_por, self.dg_user)

    def test_direccion_detecta_jefatura_primer_nivel_por_nivel_organizacional(self):
        empleado = self.jefe_ventas.empleado_rrhh
        empleado.puesto = "Responsable de ventas"
        empleado.puesto_operativo = "CAJAS"
        empleado.nivel_organizacional = Empleado.NIVEL_JEFATURA
        empleado.save(update_fields=["puesto", "puesto_operativo", "nivel_organizacional"])

        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="jefe de ventas",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
            departamento=Empleado.DEP_VENTAS,
        )
        enviar_vacante_autorizacion(vacante, self.rrhh_user)
        vacante.refresh_from_db()

        self.assertTrue(vacante.requiere_direccion)
        self.assertEqual(vacante.tipo_autorizacion, VacanteRRHH.AUTORIZACION_DIRECCION)

    def test_no_se_inicia_reclutamiento_sin_autorizacion(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.solicitante,
        )

        with self.assertRaises(PermissionDenied):
            aprobar_vacante_autorizacion(vacante, self.dg_user)
        with self.assertRaises(ValidationError):
            iniciar_reclutamiento_vacante(vacante, self.rrhh_user)

    def test_rrhh_captura_a_nombre_de_jefe_y_validacion_autoriza_operativa(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.jefe_ventas,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_VENTAS,
        )

        enviar_vacante_autorizacion(vacante, self.rrhh_user, "Validada por CH")
        vacante.refresh_from_db()

        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_AUTORIZADA)
        self.assertEqual(vacante.creado_por, self.rrhh_user)
        self.assertEqual(vacante.solicitado_por, self.jefe_ventas)
        self.assertEqual(vacante.autorizador_asignado, self.jefe_ventas)
        self.assertEqual(vacante.autorizado_por, self.jefe_ventas)
        self.assertEqual(VacanteMovimiento.objects.filter(vacante=vacante).count(), 2)

    def test_departamento_sin_jefatura_escala_a_direccion_general(self):
        usuario_compras = User.objects.create_user(username="compras.operador", password="pass123")
        usuario_compras.groups.add(Group.objects.get_or_create(name="COMPRAS")[0])
        self.assertFalse(can_solicitar_vacantes(usuario_compras))
        vacante = crear_solicitud_vacante(
            area="compras",
            puesto="auxiliar administrativo",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=usuario_compras,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_COMPRAS,
        )

        enviar_vacante_autorizacion(vacante, self.rrhh_user, "Validada por CH")
        vacante.refresh_from_db()

        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_PENDIENTE_DIRECCION)
        self.assertTrue(vacante.requiere_direccion)
        self.assertEqual(vacante.tipo_autorizacion, VacanteRRHH.AUTORIZACION_DIRECCION)
        self.assertIsNone(vacante.autorizador_asignado)
        self.assertTrue(can_autorizar_vacante(self.dg_user, vacante))
        self.assertTrue(Notificacion.objects.filter(usuario=self.dg_user, objeto_id=str(vacante.id)).exists())

    def test_departamento_sin_jefatura_autoautoriza_a_jefatura_solicitante(self):
        # Caso real: jefa de Ventas pide un repartidor y captura el departamento
        # LOGISTICA, que no tiene empleados en Organización.
        vacante = crear_solicitud_vacante(
            area="logistica",
            puesto="repartidor",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.jefe_ventas,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_LOGISTICA,
        )

        enviar_vacante_autorizacion(vacante, self.rrhh_user, "Validada por CH")
        vacante.refresh_from_db()

        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_AUTORIZADA)
        self.assertFalse(vacante.requiere_direccion)
        self.assertEqual(vacante.autorizador_asignado, self.jefe_ventas)
        self.assertEqual(vacante.autorizado_por, self.jefe_ventas)

    def test_departamento_sin_jefatura_resuelve_por_jefe_directo_del_solicitante(self):
        jefa = Empleado.objects.get(usuario_erp=self.jefe_ventas)
        Empleado.objects.create(
            nombre="Colaboradora Ventas",
            departamento=Empleado.DEP_VENTAS,
            puesto="Vendedora",
            usuario_erp=self.solicitante,
            jefe_directo=jefa,
        )
        vacante = crear_solicitud_vacante(
            area="logistica",
            puesto="repartidor",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.solicitante,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_LOGISTICA,
        )

        enviar_vacante_autorizacion(vacante, self.rrhh_user, "Validada por CH")
        vacante.refresh_from_db()

        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_PENDIENTE_DIRECCION)
        self.assertFalse(vacante.requiere_direccion)
        self.assertEqual(vacante.autorizador_asignado, self.jefe_ventas)

    def test_correccion_y_reenvio_regresan_a_revision_sin_borrar_historial(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.jefe_ventas,
            creado_por=self.jefe_ventas,
            departamento=Empleado.DEP_VENTAS,
        )

        devolver_vacante_correccion(vacante, self.rrhh_user, "Falta fecha necesaria")
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_DEVUELTA_CORRECCION)

        reenviar_vacante_revision(vacante, self.jefe_ventas, "Fecha corregida")
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_REVISION_RRHH)
        self.assertEqual(VacanteMovimiento.objects.filter(vacante=vacante).count(), 3)

    def test_rrhh_agrega_seguimiento_sin_cambiar_estado(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.jefe_ventas,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_VENTAS,
        )
        vacante = enviar_vacante_autorizacion(vacante, self.rrhh_user)
        vacante = iniciar_reclutamiento_vacante(vacante, self.rrhh_user)
        vacante.refresh_from_db()

        seguimiento = agregar_seguimiento_vacante(
            vacante,
            self.rrhh_user,
            etapa=VacanteSeguimiento.ETAPA_ENTREVISTA,
            candidato="Candidata Local",
            comentario="Entrevista agendada",
        )
        vacante.refresh_from_db()

        self.assertEqual(seguimiento.vacante, vacante)
        self.assertEqual(seguimiento.creado_por, self.rrhh_user)
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_RECLUTAMIENTO)


class VacantesSolicitudViewTests(TestCase):
    def setUp(self):
        self.rrhh_user = User.objects.create_user(username="paula.rrhh", password="pass123")
        self.rrhh_user.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        self.dg_user = User.objects.create_user(username="mauricio.dg", password="pass123")
        self.dg_user.groups.add(Group.objects.get_or_create(name="DG")[0])
        self.jefe_produccion = User.objects.create_user(username="jefa.produccion", password="pass123")
        self.jefe_produccion.groups.add(Group.objects.get_or_create(name="PRODUCCION")[0])
        Empleado.objects.create(
            nombre="Jefa Produccion",
            departamento=Empleado.DEP_PRODUCCION,
            puesto="Jefe de Produccion",
            puesto_operativo="JEFATURA",
            usuario_erp=self.jefe_produccion,
        )
        self.jefe_ventas = User.objects.create_user(username="jefa.ventas", password="pass123")
        self.jefe_ventas.groups.add(Group.objects.get_or_create(name="VENTAS")[0])
        Empleado.objects.create(
            nombre="Jefa Ventas",
            departamento=Empleado.DEP_VENTAS,
            puesto="Jefe de Ventas",
            puesto_operativo="JEFATURA",
            usuario_erp=self.jefe_ventas,
        )
        self.sucursal = Sucursal.objects.create(codigo="MTR", nombre="Matriz")

    def test_formulario_crea_solicitud_y_redirige_al_detalle(self):
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:rrhh_vacante_nueva"),
            {
                "area": "produccion",
                "puesto": "hornero",
                "departamento": Empleado.DEP_PRODUCCION,
                "sucursal": self.sucursal.id,
                "fecha_solicitada": "2026-05-28",
                "fecha_necesaria": "2026-06-05",
                "cantidad_solicitada": "1",
                "tipo_solicitud": VacanteRRHH.TIPO_REEMPLAZO,
                "prioridad": VacanteRRHH.PRIORIDAD_ALTA,
                "motivo_solicitud": "Reposición por baja operativa.",
            },
        )

        vacante = VacanteRRHH.objects.get()
        self.assertRedirects(response, reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_SOLICITADA)
        self.assertEqual(vacante.solicitado_por, self.rrhh_user)

    def test_detalle_ejecuta_acciones_de_solicitud(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
        )
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:rrhh_vacante_accion", kwargs={"pk": vacante.pk}),
            {"action": "enviar_autorizacion", "comentario": "Lista para aprobar"},
        )
        self.assertRedirects(response, reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_PENDIENTE_DIRECCION)
        self.assertEqual(vacante.autorizador_asignado, self.jefe_ventas)

        self.client.force_login(self.jefe_ventas)
        response = self.client.post(
            reverse("rrhh:rrhh_vacante_accion", kwargs={"pk": vacante.pk}),
            {"action": "aprobar", "comentario": "Aprobada"},
        )
        self.assertRedirects(response, reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        vacante.refresh_from_db()
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_AUTORIZADA)

    def test_detalle_envia_candidato_a_alta_pendiente(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        candidato = CandidatoVacante.objects.create(
            vacante=vacante,
            nombre="Candidata Alta",
            telefono="6671112233",
            etapa_actual=CandidatoVacante.ETAPA_SELECCIONADO,
        )
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:rrhh_vacante_accion", kwargs={"pk": vacante.pk}),
            {
                "action": "enviar_alta_pendiente",
                "candidato_id": str(candidato.id),
                "comentario": "Lista para alta",
            },
        )

        pendiente = AltaPendienteEmpleado.objects.get(candidato=candidato)
        self.assertRedirects(response, f"{reverse('rrhh:empleados')}?alta_pendiente={pendiente.id}")
        self.assertEqual(Empleado.objects.filter(nombre="Candidata Alta").count(), 0)

    def test_empleados_muestra_y_prerellena_alta_pendiente(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            fecha_necesaria=date(2026, 6, 5),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
            sucursal=self.sucursal,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        candidato = CandidatoVacante.objects.create(
            vacante=vacante,
            nombre="Candidata Alta",
            telefono="6671112233",
            email="alta@example.com",
            etapa_actual=CandidatoVacante.ETAPA_SELECCIONADO,
        )
        pendiente = crear_alta_pendiente_desde_candidato(candidato, self.rrhh_user)
        self.client.force_login(self.rrhh_user)

        response = self.client.get(f"{reverse('rrhh:empleados')}?alta_pendiente={pendiente.id}")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Altas pendientes")
        self.assertContains(response, "Candidata Alta")
        self.assertContains(response, 'name="alta_pendiente_id" value="')
        self.assertContains(response, 'value="Candidata Alta"')
        self.assertContains(response, 'value="6671112233"')
        self.assertContains(response, 'value="alta@example.com"')

    def test_empleados_guarda_alta_pendiente_y_permite_alta_manual(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            fecha_necesaria=date(2026, 6, 5),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        candidato = CandidatoVacante.objects.create(
            vacante=vacante,
            nombre="Candidata Alta",
            etapa_actual=CandidatoVacante.ETAPA_SELECCIONADO,
        )
        pendiente = crear_alta_pendiente_desde_candidato(candidato, self.rrhh_user)
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "alta_pendiente_id": str(pendiente.id),
                "codigo": "9001",
                "nombre": "Candidata Alta",
                "fecha_ingreso": "2026-06-05",
                "salario_diario": "300.00",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        empleado = Empleado.objects.get(codigo="9001")
        candidato.refresh_from_db()
        pendiente.refresh_from_db()
        vacante.refresh_from_db()
        self.assertEqual(candidato.empleado, empleado)
        self.assertEqual(pendiente.estado, AltaPendienteEmpleado.ESTADO_CONVERTIDA)
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_CUBIERTA)

        manual = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "codigo": "9002",
                "nombre": "Alta Manual",
                "salario_diario": "300.00",
            },
            follow=True,
        )
        self.assertEqual(manual.status_code, 200)
        self.assertTrue(Empleado.objects.filter(codigo="9002", nombre="Alta Manual").exists())

    def test_empleados_no_deja_empleado_huerfano_si_alta_pendiente_ya_no_es_valida(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_RECLUTAMIENTO,
        )
        candidato = CandidatoVacante.objects.create(
            vacante=vacante,
            nombre="Candidata Cancelada",
            etapa_actual=CandidatoVacante.ETAPA_SELECCIONADO,
        )
        pendiente = crear_alta_pendiente_desde_candidato(candidato, self.rrhh_user)
        VacanteRRHH.objects.filter(pk=vacante.pk).update(estado=VacanteRRHH.ESTADO_CANCELADA)
        self.client.force_login(self.rrhh_user)

        response = self.client.post(
            reverse("rrhh:empleados"),
            {
                "action": "create",
                "alta_pendiente_id": str(pendiente.id),
                "codigo": "9010",
                "nombre": "Candidata Cancelada",
                "salario_diario": "300.00",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(Empleado.objects.filter(codigo="9010").exists())
        candidato.refresh_from_db()
        pendiente.refresh_from_db()
        self.assertIsNone(candidato.empleado)
        self.assertEqual(candidato.etapa_actual, CandidatoVacante.ETAPA_SELECCIONADO)
        self.assertEqual(pendiente.estado, AltaPendienteEmpleado.ESTADO_PENDIENTE)

    def test_direccion_general_ve_vacante_operativa_sin_autorizarla(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_VENTAS,
        )
        enviar_vacante_autorizacion(vacante, self.rrhh_user)
        vacante.refresh_from_db()

        self.client.force_login(self.dg_user)
        response = self.client.get(reverse("rrhh:rrhh_vacantes"))
        self.assertContains(response, vacante.folio)

        response = self.client.get(reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        self.assertContains(response, vacante.folio)
        self.assertContains(response, "Jefe directo")
        self.assertNotContains(response, 'name="action" value="aprobar"')

    def test_detalle_muestra_proceso_responsable_y_siguiente_paso(self):
        vacante = crear_solicitud_vacante(
            area="ventas",
            puesto="cajera",
            fecha_solicitada=date(2026, 5, 28),
            solicitado_por=self.jefe_ventas,
            creado_por=self.jefe_ventas,
            departamento=Empleado.DEP_VENTAS,
            estado_inicial=VacanteRRHH.ESTADO_REVISION_RRHH,
        )
        self.client.force_login(self.rrhh_user)

        response = self.client.get(reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))

        self.assertContains(response, "Proceso")
        self.assertContains(response, "Responsable")
        self.assertContains(response, "Siguiente paso")
        self.assertContains(response, "Capital Humano")
        self.assertContains(response, "Validar o devolver a corrección.")

    def test_detalle_renderiza_solicitada_sin_validacion_ni_autorizacion(self):
        vacante = crear_solicitud_vacante(
            area="logistica",
            puesto="repartidor",
            fecha_solicitada=date(2026, 5, 15),
            solicitado_por=self.rrhh_user,
            creado_por=self.rrhh_user,
            departamento=Empleado.DEP_LOGISTICA,
            estado_inicial=VacanteRRHH.ESTADO_SOLICITADA,
        )
        self.assertIsNone(vacante.validado_rrhh_por)
        self.assertIsNone(vacante.autorizado_por)
        self.client.force_login(self.rrhh_user)

        response = self.client.get(reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, vacante.folio)
        self.assertContains(response, "Validó RRHH")
        self.assertContains(response, "Autorizó")

    def test_jefatura_puede_crear_solicitud_para_revision_rrhh(self):
        self.assertTrue(can_solicitar_vacantes(self.jefe_ventas))
        self.client.force_login(self.jefe_ventas)
        response = self.client.get(reverse("rrhh:rrhh_vacantes"))
        self.assertContains(response, "Nueva solicitud")

        response = self.client.post(
            reverse("rrhh:rrhh_vacante_nueva"),
            {
                "area": "ventas",
                "puesto": "cajera",
                "departamento": Empleado.DEP_VENTAS,
                "fecha_solicitada": "2026-05-28",
                "fecha_necesaria": "2026-06-05",
                "cantidad_solicitada": "1",
                "tipo_solicitud": VacanteRRHH.TIPO_REEMPLAZO,
                "prioridad": VacanteRRHH.PRIORIDAD_NORMAL,
                "motivo_solicitud": "Reposición de caja.",
            },
        )

        vacante = VacanteRRHH.objects.get(area="VENTAS", puesto="CAJERA")
        self.assertRedirects(response, reverse("rrhh:rrhh_vacante_detalle", kwargs={"pk": vacante.pk}))
        self.assertEqual(vacante.estado, VacanteRRHH.ESTADO_REVISION_RRHH)
        self.assertEqual(vacante.creado_por, self.jefe_ventas)
        self.assertEqual(vacante.solicitado_por, self.jefe_ventas)

    def test_encargada_puede_crear_solicitud_por_nivel_organizacional(self):
        empleado = self.jefe_ventas.empleado_rrhh
        empleado.puesto = "Responsable de cajas"
        empleado.puesto_operativo = "CAJAS"
        empleado.nivel_organizacional = Empleado.NIVEL_ENCARGADA
        empleado.save(update_fields=["puesto", "puesto_operativo", "nivel_organizacional"])

        self.assertTrue(can_solicitar_vacantes(self.jefe_ventas))

    def test_jefatura_solo_ve_tab_de_vacantes_en_tarea_puntual(self):
        labels = [tab["label"] for tab in _module_tabs("vacantes", self.jefe_ventas)]

        self.assertEqual(labels, ["Vacantes"])
