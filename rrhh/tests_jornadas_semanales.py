from contextlib import nullcontext
from datetime import date, time
from queue import Queue
from threading import Event, Thread
from time import monotonic

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, close_old_connections, connection, connections, transaction
from django.test import TestCase, TransactionTestCase
from django.urls import reverse
from core.models import AuditLog

from rrhh.models import (
    AsignacionJornadaEmpleado, AsignacionTurnoEmpleado, AsistenciaEmpleado,
    Empleado, JornadaSemanal, JornadaSemanalDia, Turno,
)
from rrhh.services_turnos import (
    ESTADO_DESCANSO, ESTADO_LABORABLE, ESTADO_SIN_ASIGNACION,
    _turno_legacy_asignado_para_fecha, asignar_jornada_empleado,
    es_jornada_historica_antes_de_asignacion,
    horario_programado_para_fecha, turno_asignado_para_fecha,
)


class JornadaSemanalResolverTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(codigo="JORNADA-RES", nombre="Persona administrativa")
        self.lunes = Turno.objects.create(
            nombre="Administrativo lunes a viernes", hora_entrada=time(8, 30),
            hora_salida=time(16, 30),
        )
        self.sabado = Turno.objects.create(
            nombre="Administrativo sábado", hora_entrada=time(8, 30),
            hora_salida=time(13, 30),
        )

    def asignar_semana(self, *, completa=True, proteger=False):
        jornada = JornadaSemanal.objects.create(nombre="Administrativa")
        for dia in range(7 if completa else 6):
            JornadaSemanalDia.objects.create(
                jornada=jornada, dia_semana=dia,
                turno=self.lunes if dia < 5 else self.sabado if dia == 5 else None,
            )
        asignacion = AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada, fecha_inicio=date(2026, 9, 1),
            motivo="Horario confirmado", proteger_reingesta_historica=proteger,
        )
        return asignacion

    def test_jornada_administrativa_resuelve_lunes_sabado_y_descanso(self):
        asignacion = self.asignar_semana()
        for fecha, turno, estado, salida in (
            (date(2026, 9, 7), self.lunes, ESTADO_LABORABLE, time(16, 30)),
            (date(2026, 9, 12), self.sabado, ESTADO_LABORABLE, time(13, 30)),
            (date(2026, 9, 13), None, ESTADO_DESCANSO, None),
        ):
            with self.subTest(fecha=fecha):
                horario = horario_programado_para_fecha(self.empleado, fecha)
                self.assertEqual(horario.estado, estado)
                self.assertEqual(horario.turno, turno)
                self.assertEqual(horario.asignacion, asignacion)
                self.assertEqual(horario.turno.hora_salida if horario.turno else None, salida)
                self.assertEqual(turno_asignado_para_fecha(self.empleado, fecha), turno)

    def test_fallback_legacy_conserva_turno_y_estado_laborable(self):
        asignacion = AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.lunes, fecha_inicio=date(2026, 9, 1),
        )
        horario = horario_programado_para_fecha(self.empleado, date(2026, 9, 7))
        self.assertEqual(horario.estado, ESTADO_LABORABLE)
        self.assertEqual(horario.turno, self.lunes)
        self.assertEqual(horario.asignacion, asignacion)
        self.assertEqual(turno_asignado_para_fecha(self.empleado, date(2026, 9, 7)), self.lunes)

    def test_fallback_legacy_resuelve_con_dos_consultas(self):
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.lunes, fecha_inicio=date(2026, 9, 1),
        )
        fecha = date(2026, 9, 7)
        with self.assertNumQueries(2):
            horario = horario_programado_para_fecha(self.empleado, fecha)
        self.assertEqual(horario.turno, self.lunes)
        self.assertEqual(_turno_legacy_asignado_para_fecha(self.empleado, fecha), self.lunes)

    def test_sin_asignacion_se_distingue_de_descanso(self):
        horario = horario_programado_para_fecha(self.empleado, date(2026, 9, 7))
        self.assertEqual(horario.estado, ESTADO_SIN_ASIGNACION)
        self.assertIsNone(horario.turno)
        self.assertIsNone(horario.asignacion)

    def test_jornada_semanal_vigente_prevalece_sobre_legacy(self):
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.lunes, fecha_inicio=date(2026, 9, 1),
        )
        asignacion = self.asignar_semana()
        horario = horario_programado_para_fecha(self.empleado, date(2026, 9, 12))
        self.assertEqual(horario.estado, ESTADO_LABORABLE)
        self.assertEqual(horario.turno, self.sabado)
        self.assertEqual(horario.asignacion, asignacion)
        self.assertEqual(turno_asignado_para_fecha(self.empleado, date(2026, 9, 12)), self.sabado)

    def test_dos_asignaciones_semanales_vigentes_se_rechazan(self):
        primera = self.asignar_semana()
        # objects.create no ejecuta full_clean; la restricción SQL solo impide
        # fechas de inicio iguales, lo que permite simular datos ya corruptos.
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=primera.jornada,
            fecha_inicio=date(2026, 9, 2), motivo="Traslape corrupto",
        )
        with self.assertRaises(ValidationError):
            horario_programado_para_fecha(self.empleado, date(2026, 9, 7))

    def test_perfil_semanal_incompleto_se_rechaza(self):
        self.asignar_semana(completa=False)
        with self.assertRaises(ValidationError):
            horario_programado_para_fecha(self.empleado, date(2026, 9, 7))

    def test_reingesta_historica_reconoce_asignacion_semanal_protegida(self):
        self.asignar_semana(proteger=True)
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=date(2026, 9, 7),
        )
        self.assertTrue(es_jornada_historica_antes_de_asignacion(asistencia))

    def test_reingesta_historica_respeta_proteccion_legacy_y_semanal(self):
        legacy = AsignacionTurnoEmpleado.objects.create(
            empleado=self.empleado, turno=self.lunes, fecha_inicio=date(2026, 9, 1),
            proteger_reingesta_historica=True,
        )
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado, fecha=date(2026, 9, 7),
        )
        self.assertTrue(es_jornada_historica_antes_de_asignacion(asistencia))
        legacy.proteger_reingesta_historica = False
        legacy.save(update_fields=["proteger_reingesta_historica"])
        self.assertFalse(es_jornada_historica_antes_de_asignacion(asistencia))
        self.asignar_semana(proteger=False)
        self.assertFalse(es_jornada_historica_antes_de_asignacion(asistencia))


class JornadaSemanalModelTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(codigo="JORNADA-001", nombre="Persona con jornada")

    def jornada(self, nombre="Semana base"):
        return JornadaSemanal.objects.create(nombre=nombre)

    def test_vigencia_de_jornada_rechaza_fin_anterior_al_inicio(self):
        datos = {
            "nombre": "Vigencia inválida",
            "vigencia_desde": date(2026, 9, 10),
            "vigencia_hasta": date(2026, 9, 9),
        }
        with self.assertRaises(ValidationError):
            JornadaSemanal(**datos).full_clean()
        with self.assertRaises(IntegrityError), transaction.atomic():
            JornadaSemanal.objects.create(**datos)

    def test_un_dia_solo_puede_aparecer_una_vez_por_jornada(self):
        jornada = self.jornada()
        JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=0, turno=None)

        with self.assertRaises(ValidationError):
            JornadaSemanalDia(jornada=jornada, dia_semana=0, turno=None).full_clean()
        with self.assertRaises(IntegrityError), transaction.atomic():
            JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=0, turno=None)

        otro = JornadaSemanalDia(jornada=jornada, dia_semana=1, turno=None)
        otro.full_clean()
        otro.save()

    def test_dia_fuera_de_la_semana_se_rechaza(self):
        jornada = self.jornada()

        for dia in (-1, 7):
            with self.subTest(dia=dia), self.assertRaises(ValidationError):
                JornadaSemanalDia(jornada=jornada, dia_semana=dia).full_clean()

    def test_vigencias_traslapadas_se_rechazan(self):
        jornada = self.jornada()
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 14), motivo="Inicial",
        )

        for inicio, fin in (
            (date(2026, 9, 14), date(2026, 9, 20)),
            (date(2026, 8, 25), date(2026, 9, 1)),
            (date(2026, 9, 2), date(2026, 9, 10)),
            (date(2026, 8, 25), None),
        ):
            with self.subTest(inicio=inicio, fin=fin), self.assertRaises(ValidationError):
                AsignacionJornadaEmpleado(
                    empleado=self.empleado, jornada=jornada,
                    fecha_inicio=inicio, fecha_fin=fin, motivo="Cambio",
                ).full_clean()

    def test_vigencias_contiguas_y_empleados_distintos_se_permiten(self):
        jornada = self.jornada()
        AsignacionJornadaEmpleado.objects.create(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 14), motivo="Inicial",
        )

        siguiente = AsignacionJornadaEmpleado(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 15), fecha_fin=None, motivo="Continuidad",
        )
        siguiente.full_clean()
        siguiente.save()

        otra_persona = Empleado.objects.create(codigo="JORNADA-002", nombre="Otra persona")
        paralelo = AsignacionJornadaEmpleado(
            empleado=otra_persona, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=None, motivo="Ingreso",
        )
        paralelo.full_clean()
        paralelo.save()

    def test_motivo_es_obligatorio(self):
        asignacion = AsignacionJornadaEmpleado(
            empleado=self.empleado, jornada=self.jornada(),
            fecha_inicio=date(2026, 9, 1), motivo="",
        )
        with self.assertRaises(ValidationError):
            asignacion.full_clean()

    def test_servicio_asigna_y_rechaza_traslape(self):
        jornada = self.jornada()
        primera = asignar_jornada_empleado(
            empleado=self.empleado, jornada=jornada,
            fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 14),
            motivo="Horario confirmado", actor=None,
        )
        self.assertEqual(primera.motivo, "Horario confirmado")

        with self.assertRaises(ValidationError):
            asignar_jornada_empleado(
                empleado=self.empleado, jornada=jornada,
                fecha_inicio=date(2026, 9, 14), fecha_fin=None,
                motivo="Traslape", actor=None,
            )


class JornadaDesdeFichaEmpleadoTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_user(username="gestor.jornada", password="pass123")
        self.actor.groups.add(Group.objects.get_or_create(name="RRHH")[0])
        self.client.force_login(self.actor)
        self.url = reverse("rrhh:empleados")
        self.jornada = JornadaSemanal.objects.create(nombre="Semana A")
        self.otra = JornadaSemanal.objects.create(nombre="Semana B")

    def datos_alta(self, **extra):
        return {
            "action": "create", "nombre": "Persona nueva", "codigo": "JORNADA-FICHA",
            "fecha_ingreso": "2026-09-01", "jornada_gestion_presente": "1",
            "jornada_id": str(self.jornada.pk), "jornada_fecha_inicio": "2026-09-01",
            "jornada_motivo": "Alta confirmada", **extra,
        }

    def datos_edicion(self, empleado, **extra):
        return {
            "action": "update", "empleado_id": str(empleado.pk),
            "nombre": empleado.nombre, "codigo": empleado.codigo,
            "fecha_ingreso": "2026-09-01", "activo": "on",
            "jornada_gestion_presente": "1", "jornada_id": str(self.otra.pk),
            "jornada_fecha_inicio": "2026-09-15", "jornada_motivo": "Cambio aprobado",
            **extra,
        }

    def empleado_con_jornada(self):
        empleado = Empleado.objects.create(
            nombre="Persona existente", codigo="JORNADA-FICHA",
            fecha_ingreso=date(2026, 9, 1),
        )
        inicial = AsignacionJornadaEmpleado.objects.create(
            empleado=empleado, jornada=self.jornada, fecha_inicio=date(2026, 9, 1),
            motivo="Asignación original",
        )
        return empleado, inicial

    def test_alta_crea_empleado_y_asignacion_y_redirect_con_ancla(self):
        response = self.client.post(self.url, self.datos_alta())
        empleado = Empleado.objects.get(codigo="JORNADA-FICHA")
        asignacion = empleado.jornadas_asignadas.get()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"{self.url}#empleado-{empleado.pk}")
        self.assertEqual(asignacion.jornada, self.jornada)
        self.assertEqual(asignacion.fecha_inicio, date(2026, 9, 1))

    def test_alta_precarga_inicio_desde_fecha_ingreso(self):
        response = self.client.post(self.url, self.datos_alta(jornada_fecha_inicio=""))
        empleado = Empleado.objects.get(codigo="JORNADA-FICHA")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(empleado.jornadas_asignadas.get().fecha_inicio, date(2026, 9, 1))

    def test_cambio_cierra_vigente_y_crea_nueva_sin_reescribir_historial(self):
        empleado, inicial = self.empleado_con_jornada()
        response = self.client.post(self.url, self.datos_edicion(empleado), HTTP_ACCEPT="application/json")
        inicial.refresh_from_db()
        self.assertEqual(inicial.fecha_fin, date(2026, 9, 14))
        self.assertEqual(inicial.motivo, "Asignación original")
        self.assertTrue(empleado.jornadas_asignadas.filter(jornada=self.otra, fecha_inicio=date(2026, 9, 15)).exists())
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["toast"]["type"], "success")
        self.assertEqual(response.json()["redirect"], f"{self.url}#empleado-{empleado.pk}")
        self.assertTrue(response.json()["reload"])

    def test_cliente_antiguo_no_cambia_jornada(self):
        empleado, inicial = self.empleado_con_jornada()
        datos = self.datos_edicion(empleado)
        for campo in ("jornada_gestion_presente", "jornada_id", "jornada_fecha_inicio", "jornada_motivo"):
            datos.pop(campo)
        self.client.post(self.url, datos)
        inicial.refresh_from_db()
        self.assertIsNone(inicial.fecha_fin)
        self.assertEqual(empleado.jornadas_asignadas.count(), 1)

    def test_edicion_sin_jornada_vigente_admite_seccion_vacia(self):
        empleado = Empleado.objects.create(
            nombre="Sin jornada", codigo="JORNADA-FICHA", fecha_ingreso=date(2026, 9, 1),
        )
        response = self.client.post(self.url, self.datos_edicion(
            empleado, jornada_id="", jornada_fecha_inicio="", jornada_motivo="",
        ), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(empleado.jornadas_asignadas.count(), 0)
        con_fecha = self.client.post(self.url, self.datos_edicion(
            empleado, jornada_id="", jornada_motivo="",
        ), HTTP_ACCEPT="application/json")
        self.assertEqual(con_fecha.status_code, 200)
        self.assertEqual(empleado.jornadas_asignadas.count(), 0)

    def test_error_de_jornada_revierte_edicion_y_asignaciones(self):
        empleado, inicial = self.empleado_con_jornada()
        response = self.client.post(self.url, self.datos_edicion(
            empleado, nombre="Nombre que debe revertirse", jornada_motivo="",
        ), HTTP_ACCEPT="application/json")
        empleado.refresh_from_db()
        inicial.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertTrue(response.json()["toast"]["persistent"])
        self.assertEqual(empleado.nombre, "Persona existente")
        self.assertIsNone(inicial.fecha_fin)
        self.assertEqual(empleado.jornadas_asignadas.count(), 1)

    def test_alta_invalida_revierte_empleado(self):
        response = self.client.post(self.url, self.datos_alta(jornada_motivo=""), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("jornada_motivo", response.json()["errors"])
        self.assertFalse(Empleado.objects.filter(codigo="JORNADA-FICHA").exists())

    def test_cierre_explicito_y_reenvio_idempotente(self):
        empleado, inicial = self.empleado_con_jornada()
        datos = self.datos_edicion(empleado, jornada_id="", jornada_motivo="Cierre autorizado")
        primera = self.client.post(self.url, datos)
        segunda = self.client.post(self.url, datos)
        inicial.refresh_from_db()
        self.assertEqual(primera["Location"], f"{self.url}#empleado-{empleado.pk}")
        self.assertEqual(segunda.status_code, 302)
        self.assertEqual(inicial.fecha_fin, date(2026, 9, 14))
        self.assertEqual(empleado.jornadas_asignadas.count(), 1)

    def test_cierre_exige_motivo_y_conserva_historial_ante_error(self):
        empleado, inicial = self.empleado_con_jornada()
        response = self.client.post(self.url, self.datos_edicion(
            empleado, jornada_id="", jornada_motivo="", nombre="No persistir",
        ), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("jornada_motivo", response.json()["errors"])
        empleado.refresh_from_db()
        inicial.refresh_from_db()
        self.assertEqual(empleado.nombre, "Persona existente")
        self.assertIsNone(inicial.fecha_fin)

    def test_cambio_de_asignacion_con_fin_futuro_conserva_fin_original_en_auditoria(self):
        empleado, inicial = self.empleado_con_jornada()
        inicial.fecha_fin = date(2026, 12, 31)
        inicial.save(update_fields=["fecha_fin"])
        response = self.client.post(self.url, self.datos_edicion(empleado))
        self.assertEqual(response.status_code, 302)
        inicial.refresh_from_db()
        self.assertEqual(inicial.fecha_fin, date(2026, 9, 14))
        evento = AuditLog.objects.get(model="rrhh.AsignacionJornadaEmpleado", action="UPDATE", object_id=str(inicial.pk))
        self.assertEqual(evento.user, self.actor)
        self.assertEqual(evento.payload["fecha_fin_anterior"], "2026-12-31")
        self.assertEqual(evento.payload["fecha_fin_nueva"], "2026-09-14")
        self.assertEqual(evento.payload["motivo"], "Cambio aprobado")

    def test_cierre_con_fin_futuro_registra_motivo_sin_cambiar_motivo_original(self):
        empleado, inicial = self.empleado_con_jornada()
        inicial.fecha_fin = date(2026, 12, 31)
        inicial.save(update_fields=["fecha_fin"])
        response = self.client.post(self.url, self.datos_edicion(
            empleado, jornada_id="", jornada_motivo="Cierre documentado",
        ))
        self.assertEqual(response.status_code, 302)
        inicial.refresh_from_db()
        self.assertEqual(inicial.fecha_fin, date(2026, 9, 14))
        self.assertEqual(inicial.motivo, "Asignación original")
        evento = AuditLog.objects.get(model="rrhh.AsignacionJornadaEmpleado", action="UPDATE", object_id=str(inicial.pk))
        self.assertEqual(evento.payload["motivo"], "Cierre documentado")
        self.assertEqual(evento.payload["tipo"], "cierre")

    def test_validaciones_previas_de_alta_y_edicion_responden_json(self):
        alta = self.client.post(self.url, self.datos_alta(jefe_directo="invalido"), HTTP_ACCEPT="application/json")
        self.assertEqual(alta.status_code, 400)
        self.assertFalse(alta.json()["ok"])
        self.assertTrue(alta.json()["toast"]["persistent"])
        empleado, _ = self.empleado_con_jornada()
        edicion = self.client.post(self.url, self.datos_edicion(
            empleado, jefe_directo="invalido",
        ), HTTP_ACCEPT="application/json")
        self.assertEqual(edicion.status_code, 400)
        self.assertFalse(edicion.json()["ok"])
        self.assertTrue(edicion.json()["toast"]["persistent"])

    def test_id_de_edicion_inexistente_responde_json_400(self):
        response = self.client.post(self.url, {
            "action": "update", "empleado_id": "999999999", "nombre": "Desconocido",
        }, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("empleado_id", response.json()["errors"])

    def test_error_html_repopula_draft_una_vez_y_reabre_edicion(self):
        empleado, _ = self.empleado_con_jornada()
        response = self.client.post(self.url, self.datos_edicion(
            empleado, nombre="Nombre en borrador", telefono="6671234567", jornada_motivo="",
        ))
        self.assertEqual(response.status_code, 302)
        self.assertIn(f"#empleado-{empleado.pk}", response["Location"])
        self.assertNotIn("Nombre en borrador", response["Location"])
        pantalla = self.client.get(response["Location"].split("#")[0])
        self.assertContains(pantalla, f'id="empleado-{empleado.pk}"')
        self.assertContains(pantalla, 'value="Nombre en borrador"')
        self.assertContains(pantalla, 'value="6671234567"')
        self.assertContains(pantalla, '<details class="rrhh-edit-panel" open>')
        segunda = self.client.get(response["Location"].split("#")[0])
        self.assertNotContains(segunda, 'value="Nombre en borrador"')

    def test_error_html_de_alta_conserva_campos_y_no_guarda_password(self):
        datos = self.datos_alta(
            nombre="Alta pendiente de corrección", rfc="XAXX010101000",
            telefono="6679876543", crear_usuario_erp="on",
            nuevo_usuario_username="nueva.persona", nuevo_usuario_password="secreto-123",
            jornada_motivo="",
        )
        response = self.client.post(f"{self.url}?estado=activos&q=Buscada", datos)
        self.assertEqual(response.status_code, 302)
        self.assertIn("estado=activos", response["Location"])
        self.assertIn("q=Buscada", response["Location"])
        self.assertIn("#alta-empleado", response["Location"])
        pantalla = self.client.get(response["Location"].split("#")[0])
        self.assertContains(pantalla, 'id="alta-empleado"')
        self.assertContains(pantalla, 'value="Alta pendiente de corrección"')
        self.assertContains(pantalla, 'value="XAXX010101000"')
        self.assertContains(pantalla, 'value="6679876543"')
        self.assertNotContains(pantalla, "secreto-123")
        self.assertNotIn("rrhh_ficha_error_flash", self.client.session)

    def test_draft_de_edicion_visibiliza_empleado_filtrado_sin_cambiar_otros(self):
        empleado, _ = self.empleado_con_jornada()
        otro = Empleado.objects.create(nombre="Otro empleado", codigo="OTRO-001")
        response = self.client.post(f"{self.url}?q=Otro", self.datos_edicion(
            empleado, nombre="Draft visible", salario_diario="123.45", jornada_motivo="",
        ))
        pantalla = self.client.get(response["Location"].split("#")[0])
        self.assertContains(pantalla, 'value="Draft visible"')
        self.assertContains(pantalla, 'value="123.45"')
        self.assertContains(pantalla, f'id="empleado-{empleado.pk}"')
        self.assertContains(pantalla, f'id="empleado-{otro.pk}"')
        empleado.refresh_from_db()
        self.assertEqual(empleado.nombre, "Persona existente")

    def test_nombre_vacio_en_update_conserva_modo_edicion_y_ancla(self):
        empleado, _ = self.empleado_con_jornada()
        response = self.client.post(self.url, self.datos_edicion(
            empleado, nombre="", telefono="6671112222",
        ))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"{self.url}#empleado-{empleado.pk}")
        pantalla = self.client.get(response["Location"].split("#")[0])
        self.assertContains(pantalla, f'id="empleado-{empleado.pk}"')
        self.assertContains(pantalla, '<details class="rrhh-edit-panel" open>')
        self.assertContains(pantalla, f'id="edit_telefono_{empleado.pk}"')
        self.assertContains(pantalla, 'value="6671112222"')
        self.assertIsNone(pantalla.context["alta_form_draft"])

    def test_notas_largas_sobreviven_json_y_flash_html_sin_recorte(self):
        empleado, _ = self.empleado_con_jornada()
        notas = "  Nota de logística " + "x" * 320 + "  "
        datos = self.datos_edicion(empleado, jornada_motivo="", notas_identidad=notas)
        json_response = self.client.post(self.url, datos, HTTP_ACCEPT="application/json")
        self.assertEqual(json_response.status_code, 400)
        self.assertEqual(json_response.json()["values"]["notas_identidad"], notas)
        html_response = self.client.post(self.url, datos)
        pantalla = self.client.get(html_response["Location"].split("#")[0])
        self.assertContains(pantalla, notas)

    def test_limite_de_sesion_reporta_error_sin_recortar_json(self):
        empleado, _ = self.empleado_con_jornada()
        notas = "n" * 10001
        datos = self.datos_edicion(empleado, jornada_motivo="", notas_identidad=notas)
        json_response = self.client.post(self.url, datos, HTTP_ACCEPT="application/json")
        self.assertEqual(json_response.json()["values"]["notas_identidad"], notas)
        html_response = self.client.post(self.url, datos)
        pantalla = self.client.get(html_response["Location"].split("#")[0])
        self.assertContains(pantalla, "supera el límite del borrador")
        self.assertNotContains(pantalla, "n" * 250)

    def test_update_nombre_excesivo_reabre_edicion_sin_500_y_consume_flash(self):
        empleado, _ = self.empleado_con_jornada()
        response = self.client.post(self.url, self.datos_edicion(
            empleado, nombre="N" * 10001, telefono="6671112222",
            fecha_ingreso="f" * 10001, salario_diario="1" * 10001,
            jefe_directo="2" * 10001, sucursal_id="3" * 10001,
            sucursal_app_id="4" * 10001, activo="a" * 10001,
            notas_identidad="n" * 10001,
        ))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"{self.url}#empleado-{empleado.pk}")
        pantalla = self.client.get(self.url)
        self.assertEqual(pantalla.status_code, 200)
        self.assertContains(pantalla, "supera el límite del borrador")
        self.assertContains(pantalla, '<details class="rrhh-edit-panel" open>')
        self.assertContains(pantalla, f'id="empleado-{empleado.pk}"')
        self.assertContains(pantalla, 'value="Persona existente"')
        self.assertContains(pantalla, 'value="6671112222"')
        self.assertIsNone(pantalla.context["alta_form_draft"])
        formulario = next(e.form_values for e in pantalla.context["empleados"] if e.pk == empleado.pk)
        self.assertEqual(formulario.fecha_ingreso, date(2026, 9, 1))
        self.assertEqual(formulario.salario_diario, empleado.salario_diario)
        self.assertTrue(formulario.activo)
        self.assertEqual(formulario.form_draft["notas_identidad"], "")
        siguiente = self.client.get(self.url)
        self.assertNotContains(siguiente, "supera el límite del borrador")
        self.assertNotIn("rrhh_ficha_error_flash", self.client.session)

    def test_alta_sucursal_excesiva_reabre_alta_sin_500(self):
        response = self.client.post(self.url, self.datos_alta(
            nombre="N" * 10001, sucursal_id="9" * 10001,
        ))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"{self.url}#alta-empleado")
        pantalla = self.client.get(self.url)
        self.assertEqual(pantalla.status_code, 200)
        self.assertContains(pantalla, "supera el límite del borrador")
        self.assertIsNotNone(pantalla.context["alta_form_draft"])

    def test_update_con_usuario_sin_perfil_reabre_edicion_sin_500(self):
        empleado, _ = self.empleado_con_jornada()
        usuario = get_user_model().objects.create_user(username="jornada_sin_perfil")
        perfil = getattr(usuario, "userprofile", None)
        if perfil:
            perfil.delete()
        empleado.usuario_erp = usuario
        empleado.save(update_fields=["usuario_erp"])
        response = self.client.post(self.url, self.datos_edicion(empleado, nombre="N" * 10001))
        self.assertEqual(response.status_code, 302)
        pantalla = self.client.get(self.url)
        self.assertEqual(pantalla.status_code, 200)
        self.assertContains(pantalla, "supera el límite del borrador")

    def test_campo_con_max_length_real_se_rechaza_sin_recortar_values(self):
        nombre = "N" * (Empleado._meta.get_field("nombre").max_length + 1)
        response = self.client.post(self.url, self.datos_alta(nombre=nombre), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("nombre", response.json()["errors"])
        self.assertEqual(response.json()["values"]["nombre"], nombre)
        self.assertFalse(Empleado.objects.filter(nombre=nombre).exists())

    def test_id_invalido_de_update_html_regresa_al_catalogo_sin_borrador_de_alta(self):
        response = self.client.post(self.url, {
            "action": "update", "empleado_id": "invalido", "nombre": "Intento de edición",
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"{self.url}#catalogo-empleados")
        pantalla = self.client.get(self.url)
        self.assertContains(pantalla, 'id="catalogo-empleados"')
        self.assertContains(pantalla, "Selecciona un empleado válido para editar.")
        self.assertIsNone(pantalla.context["alta_form_draft"])

    def test_reenvio_de_misma_jornada_no_duplica(self):
        empleado, inicial = self.empleado_con_jornada()
        response = self.client.post(self.url, self.datos_edicion(
            empleado, jornada_id=str(self.jornada.pk), jornada_fecha_inicio="2026-09-01",
        ))
        self.assertEqual(response.status_code, 302)
        inicial.refresh_from_db()
        self.assertIsNone(inicial.fecha_fin)
        self.assertEqual(empleado.jornadas_asignadas.count(), 1)

    def test_mismo_perfil_con_nueva_fecha_genera_periodo_nuevo(self):
        empleado, inicial = self.empleado_con_jornada()
        self.client.post(self.url, self.datos_edicion(empleado, jornada_id=str(self.jornada.pk)))
        inicial.refresh_from_db()
        self.assertEqual(inicial.fecha_fin, date(2026, 9, 14))
        self.assertTrue(empleado.jornadas_asignadas.filter(
            jornada=self.jornada, fecha_inicio=date(2026, 9, 15),
        ).exists())

    def test_fecha_invalida_anterior_e_inactiva_revierten_edicion(self):
        empleado, inicial = self.empleado_con_jornada()
        self.otra.activo = False
        self.otra.save(update_fields=["activo"])
        for cambios, campo in (
            ({"jornada_fecha_inicio": "2026-09-40"}, "jornada_fecha_inicio"),
            ({"jornada_fecha_inicio": "2026-08-31"}, "jornada_fecha_inicio"),
            ({"jornada_id": str(self.otra.pk)}, "jornada_id"),
        ):
            with self.subTest(cambios=cambios):
                response = self.client.post(self.url, self.datos_edicion(
                    empleado, nombre="No persistir", **cambios,
                ), HTTP_ACCEPT="application/json")
                self.assertEqual(response.status_code, 400)
                self.assertIn(campo, response.json()["errors"])
                empleado.refresh_from_db()
                inicial.refresh_from_db()
                self.assertEqual(empleado.nombre, "Persona existente")
                self.assertIsNone(inicial.fecha_fin)

    def test_traslape_no_deja_cierre_parcial(self):
        empleado, inicial = self.empleado_con_jornada()
        AsignacionJornadaEmpleado.objects.create(
            empleado=empleado, jornada=self.otra, fecha_inicio=date(2026, 9, 20),
            fecha_fin=date(2026, 9, 30), motivo="Dato superpuesto previo",
        )
        response = self.client.post(self.url, self.datos_edicion(
            empleado, jornada_fecha_inicio="2026-09-15", nombre="No persistir",
        ), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        empleado.refresh_from_db()
        inicial.refresh_from_db()
        self.assertEqual(empleado.nombre, "Persona existente")
        self.assertIsNone(inicial.fecha_fin)
        self.assertEqual(empleado.jornadas_asignadas.count(), 2)

    def test_dos_vigentes_corruptas_se_rechazan_sin_mutacion(self):
        empleado, inicial = self.empleado_con_jornada()
        segunda = AsignacionJornadaEmpleado.objects.create(
            empleado=empleado, jornada=self.otra, fecha_inicio=date(2026, 9, 10),
            fecha_fin=date(2026, 9, 30), motivo="Dato corrupto",
        )
        response = self.client.post(self.url, self.datos_edicion(
            empleado, jornada_fecha_inicio="2026-09-15", nombre="No persistir",
        ), HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertIn("jornada_fecha_inicio", response.json()["errors"])
        empleado.refresh_from_db()
        inicial.refresh_from_db()
        segunda.refresh_from_db()
        self.assertEqual(empleado.nombre, "Persona existente")
        self.assertIsNone(inicial.fecha_fin)
        self.assertEqual(segunda.fecha_fin, date(2026, 9, 30))

    def test_usuario_sin_gestion_no_puede_crear_editar_ni_aplicar_servicio(self):
        from rrhh.services_jornadas_empleado import aplicar_jornada_desde_post

        empleado, inicial = self.empleado_con_jornada()
        usuario = get_user_model().objects.create_user(username="lector.jornada", password="pass123")
        usuario.groups.add(Group.objects.get_or_create(name="LECTURA")[0])
        self.client.force_login(usuario)
        self.assertEqual(self.client.post(self.url, self.datos_alta()).status_code, 403)
        self.assertEqual(self.client.post(self.url, self.datos_edicion(empleado)).status_code, 403)
        with self.assertRaises(PermissionDenied):
            aplicar_jornada_desde_post(empleado=empleado, post=self.datos_edicion(empleado), actor=usuario)
        self.assertEqual(Empleado.objects.filter(codigo="JORNADA-FICHA").count(), 1)
        inicial.refresh_from_db()
        self.assertIsNone(inicial.fecha_fin)


class AsignacionJornadaConcurrenteTests(TransactionTestCase):
    def test_dos_primeras_asignaciones_simultaneas_se_serializan(self):
        empleado = Empleado.objects.create(codigo="JORNADA-CONC", nombre="Persona concurrente")
        jornada = JornadaSemanal.objects.create(nombre="Semana concurrente")
        primera_bloqueada = Event()
        liberar_primera = Event()
        segunda_terminada = Event()
        resultados = Queue()
        pid_segunda = Queue()

        def asignar(nombre):
            close_old_connections()
            pid = None
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '8s'")
                    cursor.execute("SELECT pg_backend_pid()")
                    pid = cursor.fetchone()[0]
                if nombre == "segunda":
                    pid_segunda.put(pid)

                def observar(execute, sql, params, many, context):
                    resultado = execute(sql, params, many, context)
                    if 'FROM "rrhh_empleado"' in sql and "FOR UPDATE" in sql:
                        primera_bloqueada.set()
                        if not liberar_primera.wait(10):
                            raise TimeoutError("No se liberó la primera asignación")
                    return resultado

                observacion = connection.execute_wrapper(observar) if nombre == "primera" else nullcontext()
                with observacion:
                    asignacion = asignar_jornada_empleado(
                        empleado=empleado, jornada=jornada,
                        fecha_inicio=date(2026, 9, 1), fecha_fin=None,
                        motivo="Asignación concurrente", actor=None,
                    )
                resultados.put((nombre, pid, asignacion.pk))
            except Exception as exc:
                resultados.put((nombre, pid, exc))
            finally:
                if nombre == "segunda":
                    segunda_terminada.set()
                connections.close_all()

        primera = Thread(target=asignar, args=("primera",), daemon=True)
        segunda = Thread(target=asignar, args=("segunda",), daemon=True)
        primera.start()
        try:
            self.assertTrue(primera_bloqueada.wait(5))
            segunda.start()
            pid = pid_segunda.get(timeout=5)
            bloqueada = False
            limite = monotonic() + 5
            while monotonic() < limite and not segunda_terminada.is_set():
                with connection.cursor() as cursor:
                    cursor.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0", [pid])
                    bloqueada = cursor.fetchone()[0]
                if bloqueada:
                    break
                segunda_terminada.wait(0.01)
            self.assertTrue(bloqueada, "La segunda asignación debe esperar la fila del empleado")
        finally:
            liberar_primera.set()
            primera.join(timeout=12)
            if segunda.ident is not None:
                segunda.join(timeout=12)

        self.assertFalse(primera.is_alive() or segunda.is_alive())
        recibidos = [resultados.get(timeout=2) for _ in range(2)]
        resultado = {nombre: (pid, valor) for nombre, pid, valor in recibidos}
        self.assertNotEqual(resultado["primera"][0], resultado["segunda"][0])
        self.assertIsInstance(resultado["primera"][1], int)
        self.assertIsInstance(resultado["segunda"][1], ValidationError)
        self.assertEqual(AsignacionJornadaEmpleado.objects.filter(empleado=empleado).count(), 1)
