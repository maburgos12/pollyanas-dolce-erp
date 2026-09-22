from datetime import date, datetime, time, timedelta
from decimal import Decimal
from io import BytesIO

from openpyxl import load_workbook

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from rrhh.models import AsistenciaEmpleado, Empleado, HoraExtra, IncidenciaAsistencia, Turno
from rrhh.services import calcular_horas_extra, generar_horas_extra_automatico
from rrhh.services_extra_conciliacion import conciliar_extra_diario, diagnosticar_horas_extra, modalidad_marcaje_efectiva
from rrhh.views_asistencia import _build_reporte_asistencia


class ExtraConciliacionTests(TestCase):
    def setUp(self):
        self.fecha = date(2026, 9, 15)
        self.empleado = Empleado.objects.create(codigo='EXTRA-001', nombre='Persona extra', fecha_ingreso=date(2026, 1, 1))
        self.turno = Turno.objects.create(
            nombre='Jornada 8 a 16', hora_entrada=time(8), hora_salida=time(16), tolerancia_minutos=10,
        )

    def asistencia(self, entrada=time(8), salida=time(18), **kwargs):
        def dt(h):
            return timezone.make_aware(datetime.combine(self.fecha, h)) if h else None
        kwargs.setdefault('turno', self.turno)
        return AsistenciaEmpleado.objects.create(empleado=self.empleado, fecha=self.fecha,
            entrada=dt(entrada), salida=dt(salida), minutos_trabajados=565,
            salida_comida=dt(time(12)), regreso_comida=dt(time(12, 35)), minutos_comida=35, **kwargs)

    def test_modalidad_auto_repartidor_es_ruta(self):
        self.empleado.puesto_operativo = "REPARTIDOR"
        self.empleado.save(update_fields=["puesto_operativo"])
        asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API)
        self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_RUTA)

    def test_modalidad_auto_point_es_dos_marcas(self):
        asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_POINT)
        self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_DOS_MARCAS)

    def test_modalidad_auto_repartidor_prevalece_sobre_point(self):
        self.empleado.puesto_operativo = "REPARTIDOR"
        self.empleado.save(update_fields=["puesto_operativo"])
        asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_POINT)
        self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_RUTA)

    def test_modalidad_auto_fuentes_no_point_son_cuatro_marcas(self):
        asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API)
        for fuente in (AsistenciaEmpleado.FUENTE_HIKCONNECT_API, AsistenciaEmpleado.FUENTE_MANUAL):
            asistencia.fuente = fuente
            self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_CUATRO_MARCAS)

    def test_modalidad_explicita_prevalece_sobre_puesto_y_fuente(self):
        self.empleado.puesto_operativo = "REPARTIDOR"
        self.empleado.modalidad_marcaje = Empleado.MARCAJE_CUATRO_MARCAS
        self.empleado.save(update_fields=["puesto_operativo", "modalidad_marcaje"])
        asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_POINT)
        self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_CUATRO_MARCAS)

    def test_sin_turno_no_genera_cantidad_pagable(self):
        asistencia = self.asistencia(turno=None)
        diagnostico = diagnosticar_horas_extra(asistencia)
        self.assertIsNone(diagnostico.minutos)
        self.assertEqual(diagnostico.codigo, 'sin_turno')
        self.assertIn('turno', diagnostico.detalle.lower())
        self.assertTrue(diagnostico.comida_observable)
        self.assertEqual(diagnostico.duracion_minutos, 600)
        self.assertIsNone(generar_horas_extra_automatico(asistencia))
        self.assertFalse(HoraExtra.objects.exists())

    def test_reporte_explica_extra_no_calculable_sin_turno(self):
        self.asistencia(turno=None)
        reportes, _ = _build_reporte_asistencia(
            self.fecha, self.fecha, str(self.empleado.pk), ''
        )
        extra = reportes[0]['filas'][0]['extra']
        self.assertIsNone(extra['detectado_minutos'])
        self.assertEqual(extra['estado'], 'No calculable: falta asignar turno')
        self.assertEqual(extra['codigo'], 'sin_turno')
        self.assertEqual(extra['modalidad'], Empleado.MARCAJE_CUATRO_MARCAS)
        self.assertTrue(extra['comida_observable'])
        self.assertTrue(extra['requiere_revision'])

    def test_repartidor_con_turno_calcula_contra_salida_programada(self):
        self.empleado.puesto_operativo = 'REPARTIDOR'
        self.empleado.save(update_fields=['puesto_operativo'])
        turno = Turno.objects.create(
            nombre='Ruta 8 a 16', hora_entrada=time(8), hora_salida=time(16), tolerancia_minutos=10,
        )
        asistencia = self.asistencia(salida=time(16, 30), turno=turno)
        asistencia.salida_comida = None
        asistencia.regreso_comida = None
        asistencia.save(update_fields=['salida_comida', 'regreso_comida'])
        diagnostico = diagnosticar_horas_extra(asistencia)
        self.assertEqual(diagnostico.minutos, 30)
        self.assertEqual(diagnostico.modalidad, Empleado.MARCAJE_RUTA)
        self.assertTrue(diagnostico.requiere_revision)
        self.assertFalse(diagnostico.comida_observable)

    def test_conciliacion_sin_turno_explica_por_que_no_es_calculable(self):
        asistencia = self.asistencia(turno=None)
        conciliacion = conciliar_extra_diario(asistencia, [])
        self.assertIsNone(conciliacion['detectado_minutos'])
        self.assertEqual(conciliacion['estado'], 'No calculable: falta asignar turno')
        self.assertEqual(conciliacion['base'], 'Falta asignar el turno de esta jornada.')

    def test_una_marca_de_comida_calcula_extra_y_preserva_registro_existente(self):
        asistencia = self.asistencia()
        he = HoraExtra.objects.create(
            empleado=self.empleado, fecha=self.fecha, asistencia=asistencia,
            horas=Decimal('2'), notas='[Detección automática] Registro existente.',
        )
        for campo in ('salida_comida', 'regreso_comida'):
            original = getattr(asistencia, campo)
            setattr(asistencia, campo, None)
            asistencia.save(update_fields=[campo])
            for estado in ('pendiente', 'autorizado', 'rechazado', 'pagado', 'cancelado'):
                with self.subTest(campo=campo, estado=estado):
                    HoraExtra.objects.filter(pk=he.pk).update(estado=estado)
                    before = list(HoraExtra.objects.values())
                    diagnostico = diagnosticar_horas_extra(asistencia)
                    self.assertEqual(diagnostico.codigo, 'calculado_con_revision_comida')
                    self.assertEqual(diagnostico.minutos, 120)
                    self.assertTrue(diagnostico.requiere_revision)
                    self.assertEqual(generar_horas_extra_automatico(asistencia).pk, he.pk)
                    self.assertEqual(list(HoraExtra.objects.values()), before)
            setattr(asistencia, campo, original)
            asistencia.save(update_fields=[campo])

    def test_diagnostico_comida_registrada_no_requiere_revision(self):
        diagnostico = diagnosticar_horas_extra(self.asistencia())
        self.assertEqual(diagnostico.minutos, 120)
        self.assertEqual(diagnostico.codigo, 'calculado')
        self.assertEqual(diagnostico.detalle, 'Comida registrada.')
        self.assertTrue(diagnostico.comida_observable)
        self.assertFalse(diagnostico.requiere_revision)

    def test_diagnostico_marcajes_incompletos(self):
        asistencia = self.asistencia()
        for campo in ('entrada', 'salida'):
            with self.subTest(campo=campo):
                original = getattr(asistencia, campo)
                setattr(asistencia, campo, None)
                diagnostico = diagnosticar_horas_extra(asistencia)
                self.assertIsNone(diagnostico.minutos)
                self.assertEqual(diagnostico.codigo, 'marcaje_incompleto')
                self.assertTrue(diagnostico.requiere_revision)
                setattr(asistencia, campo, original)
        self.assertEqual(diagnosticar_horas_extra(None).codigo, 'marcaje_incompleto')

    def test_una_sola_marca_de_comida_calcula_extra_desde_salida_programada(self):
        asistencia = self.asistencia()
        for campo in ('salida_comida', 'regreso_comida'):
            with self.subTest(campo=campo):
                original = getattr(asistencia, campo)
                setattr(asistencia, campo, None)
                asistencia.save(update_fields=[campo])
                diagnostico = diagnosticar_horas_extra(asistencia)
                self.assertEqual(diagnostico.minutos, 120)
                self.assertEqual(diagnostico.codigo, 'calculado_con_revision_comida')
                self.assertFalse(diagnostico.comida_observable)
                self.assertTrue(diagnostico.requiere_revision)
                self.assertIsNotNone(generar_horas_extra_automatico(asistencia))
                setattr(asistencia, campo, original)
                asistencia.save(update_fields=[campo])
        self.assertEqual(HoraExtra.objects.get().horas, Decimal('2.00'))

    def test_comida_fuera_de_intervalo_no_bloquea_extra_posterior_al_turno(self):
        asistencia = self.asistencia()
        for inicio, fin in (
            (asistencia.entrada - timedelta(minutes=1), asistencia.entrada),
            (asistencia.salida_comida, asistencia.salida_comida),
            (asistencia.salida, asistencia.salida + timedelta(minutes=1)),
        ):
            with self.subTest(inicio=inicio, fin=fin):
                asistencia.salida_comida, asistencia.regreso_comida = inicio, fin
                diagnostico = diagnosticar_horas_extra(asistencia)
                self.assertEqual(diagnostico.minutos, 120)
                self.assertEqual(diagnostico.codigo, 'calculado_con_revision_comida')
                self.assertTrue(diagnostico.requiere_revision)

    def test_entrada_anticipada_no_amplia_extra(self):
        asistencia = self.asistencia(entrada=time(7, 56))
        self.assertEqual(diagnosticar_horas_extra(asistencia).minutos, 120)

    def test_entrada_tardia_no_reduce_extra_posterior_a_salida_programada(self):
        asistencia = self.asistencia(entrada=time(8, 30), salida=time(17))
        self.assertEqual(diagnosticar_horas_extra(asistencia).minutos, 60)

    def test_tolerancia_de_entrada_no_oculta_minutos_posteriores_a_la_salida(self):
        asistencia = self.asistencia(salida=time(16, 5))
        self.assertEqual(diagnosticar_horas_extra(asistencia).minutos, 5)

    def test_turno_nocturno_calcula_desde_salida_programada_del_dia_siguiente(self):
        turno = Turno.objects.create(
            nombre='Nocturno 22 a 6', hora_entrada=time(22), hora_salida=time(6),
        )
        asistencia = self.asistencia(entrada=time(22), salida=time(7), turno=turno)
        asistencia.salida += timedelta(days=1)
        asistencia.salida_comida = None
        asistencia.regreso_comida = None
        self.assertEqual(diagnosticar_horas_extra(asistencia).minutos, 60)

    def test_intervalo_crudo_mayor_a_24_horas_no_se_oculta_por_turno(self):
        asistencia = self.asistencia(entrada=time(7), salida=time(7))
        asistencia.salida += timedelta(days=1, seconds=1)
        diagnostico = diagnosticar_horas_extra(asistencia)
        self.assertIsNone(diagnostico.minutos)
        self.assertEqual(diagnostico.codigo, 'intervalo_invalido')
        self.assertEqual(diagnostico.duracion_minutos, 1440)
        self.assertIn('24 horas', diagnostico.detalle)

    def test_sin_turno_preserva_registro_existente_en_cualquier_estado(self):
        asistencia = self.asistencia(turno=None)
        he = HoraExtra.objects.create(
            empleado=self.empleado, fecha=self.fecha, asistencia=asistencia,
            horas=Decimal('2'), notas='[Detección automática] Registro existente.',
        )
        for estado in ('pendiente', 'autorizado', 'rechazado', 'pagado', 'cancelado'):
            with self.subTest(estado=estado):
                HoraExtra.objects.filter(pk=he.pk).update(estado=estado)
                before = list(HoraExtra.objects.values())
                self.assertEqual(generar_horas_extra_automatico(asistencia).pk, he.pk)
                self.assertEqual(list(HoraExtra.objects.values()), before)

    def test_comida_excedida_no_reduce_extra_posterior_a_salida_programada(self):
        a = self.asistencia()
        a.minutos_comida = 120
        a.minutos_trabajados = 480
        a.regreso_comida = a.salida_comida + timedelta(minutes=120)
        for fuente in ['hikconnect_api', 'point', 'manual']:
            a.fuente = fuente
            self.assertEqual(calcular_horas_extra(a), Decimal('2.00'))

    def test_turno_y_fuentes_comparten_comida_incluida(self):
        turno = Turno.objects.create(nombre='8 horas', hora_entrada=time(8), hora_salida=time(16))
        a = self.asistencia(turno=turno)
        for fuente in ['hikconnect_api', 'point', 'manual']:
            a.fuente = fuente
            self.assertEqual(calcular_horas_extra(a), Decimal('2.00'))

    def test_ocho_horas_no_generan_extra(self):
        a = self.asistencia(salida=time(16))
        self.assertEqual(calcular_horas_extra(a), Decimal('0'))

    def test_salida_faltante_no_se_presenta_como_cero_calculado(self):
        self.asistencia(salida=None)
        reportes, _ = _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), '')
        self.assertIsNone(reportes[0]['filas'][0]['extra']['detectado_minutos'])

    def test_reprocesar_no_duplica(self):
        a = self.asistencia()
        generar_horas_extra_automatico(a)
        generar_horas_extra_automatico(a)
        self.assertEqual(HoraExtra.objects.filter(empleado=self.empleado, fecha=self.fecha).count(), 1)

    def test_autorizacion_independiente_no_se_duplica(self):
        he = HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='autorizado')
        a = self.asistencia()
        generar_horas_extra_automatico(a)
        he.refresh_from_db()
        self.assertEqual(he.horas, Decimal('2'))
        self.assertEqual(HoraExtra.objects.count(), 1)

    def test_autorizacion_tardia_concilia_sin_reevaluar_disciplina(self):
        a = self.asistencia()
        generar_horas_extra_automatico(a)
        he = HoraExtra.objects.get(asistencia=a)
        he.estado = 'autorizado'
        he.save(update_fields=['estado'])
        i = IncidenciaAsistencia.objects.get(empleado=self.empleado, fecha=self.fecha, tipo='hora_extra_pendiente')
        self.assertEqual(i.estado, 'conciliado')
        self.assertFalse(IncidenciaAsistencia.objects.filter(empleado=self.empleado, tipo='falta').exists())

    def test_captura_manual_posterior_reduce_solo_pendiente_automatico(self):
        a = self.asistencia()
        generar_horas_extra_automatico(a)
        he = HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='autorizado')
        auto = HoraExtra.objects.get(asistencia=a)
        self.assertEqual(auto.estado, 'cancelado')
        he.refresh_from_db()
        self.assertEqual(he.horas, Decimal('2'))

    def test_diferencia_de_minutos_visible(self):
        a = self.asistencia(salida=time(18, 4))
        HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='autorizado')
        reportes, _ = _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), '')
        extra = reportes[0]['filas'][0]['extra']
        self.assertEqual(extra['detectado_minutos'], 124)
        self.assertEqual(extra['autorizado_minutos'], 120)
        self.assertEqual(extra['pendiente_minutos'], 4)

    def test_otra_persona_otra_fecha_no_concilian(self):
        other = Empleado.objects.create(codigo='EXTRA-002', nombre='Otra persona')
        HoraExtra.objects.create(empleado=other, fecha=self.fecha, horas=2, estado='autorizado')
        HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha-timedelta(days=1), horas=2, estado='autorizado')
        a = self.asistencia()
        generar_horas_extra_automatico(a)
        self.assertEqual(HoraExtra.objects.get(asistencia=a).estado, 'pendiente')

    def test_rechazo_no_regenera_solicitud(self):
        a = self.asistencia()
        he = generar_horas_extra_automatico(a)
        self.assertIsNotNone(he)
        he.estado = 'rechazado'
        he.save(update_fields=['estado'])
        generar_horas_extra_automatico(a)
        self.assertEqual(HoraExtra.objects.count(), 1)
        he.refresh_from_db()
        self.assertEqual(he.estado, 'rechazado')

    def test_reporte_y_excel_muestran_extra_sin_incidencias(self):
        self.asistencia()
        HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='autorizado')
        user = User.objects.create_superuser('extra-admin', 'test@example.com', 'test')
        self.client.force_login(user)
        params = {'fecha_inicio': str(self.fecha), 'fecha_fin': str(self.fecha), 'empleado': self.empleado.pk}
        response = self.client.get(reverse('rrhh:rrhh_reporte_asistencia'), params)
        self.assertContains(response, 'Extra detectado')
        params['export'] = 'csv'
        response = self.client.get(reverse('rrhh:rrhh_reporte_asistencia'), params)
        self.assertIn('extra_detectado_minutos', response.content.decode())
        self.assertIn('2026-09-15', response.content.decode())
        params['export'] = 'xlsx'
        response = self.client.get(reverse('rrhh:rrhh_reporte_asistencia'), params)
        sheet = load_workbook(BytesIO(response.content), read_only=True).active
        rows = list(sheet.values)
        columns = {name: index for index, name in enumerate(rows[0])}
        self.assertEqual(sum(row[columns['extra_detectado_minutos']] or 0 for row in rows[1:]), 120)
        self.assertEqual(sum(row[columns['extra_autorizado_minutos']] or 0 for row in rows[1:]), 120)

    def test_tolerancia_de_entrada_no_se_aplica_a_la_salida(self):
        self.assertEqual(calcular_horas_extra(self.asistencia(salida=time(16, 10))), Decimal('0.17'))

    def test_autorizacion_superior_se_mantiene_y_advierte(self):
        self.asistencia()
        manual = HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=3, estado='autorizado')
        reportes, _ = _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), '')
        extra = reportes[0]['filas'][0]['extra']
        self.assertEqual(extra['pendiente_minutos'], 0)
        self.assertIn('superior', extra['estado'])
        manual.refresh_from_db()
        self.assertEqual(manual.horas, Decimal('3'))

    def test_cancelacion_explicita_no_se_reactiva(self):
        a = self.asistencia()
        auto = generar_horas_extra_automatico(a)
        auto.estado = 'cancelado'
        auto.save(update_fields=['estado'])
        generar_horas_extra_automatico(a)
        auto.refresh_from_db()
        self.assertEqual(auto.estado, 'cancelado')

    def test_intervalo_mayor_a_un_dia_no_es_extra_calculable(self):
        from rrhh.services_extra_conciliacion import detectar_minutos_extra
        a = self.asistencia()
        a.salida += timedelta(days=1)
        self.assertIsNone(detectar_minutos_extra(a))

    def test_reducir_autorizacion_restaurar_saldo_automatico(self):
        a = self.asistencia()
        generar_horas_extra_automatico(a)
        manual = HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='autorizado')
        manual.horas = Decimal('1')
        manual.save(update_fields=['horas'])
        auto = HoraExtra.objects.get(asistencia=a)
        self.assertEqual(auto.horas, Decimal('1'))
        self.assertEqual(auto.estado, 'pendiente')

    def test_pagado_concilia_y_no_se_sobrescribe(self):
        a = self.asistencia()
        he = HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='pagado')
        generar_horas_extra_automatico(a)
        he.refresh_from_db()
        self.assertEqual(he.estado, 'pagado')
        self.assertEqual(HoraExtra.objects.count(), 1)
        i = IncidenciaAsistencia.objects.get(empleado=self.empleado, fecha=self.fecha, tipo='hora_extra_pendiente')
        self.assertEqual(i.estado, 'conciliado')

    def test_extra_sin_asistencia_se_muestra_con_autorizacion_y_deteccion_desconocida(self):
        HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='autorizado')
        reportes, _ = _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), '')
        extra = reportes[0]['filas'][0]['extra']
        self.assertEqual(extra['autorizado_minutos'], 120)
        self.assertIsNone(extra['detectado_minutos'])

    def test_reporte_no_crea_solicitudes_ni_incidencias_historicas(self):
        self.asistencia()
        before = (HoraExtra.objects.count(), IncidenciaAsistencia.objects.count())
        for _ in range(2):
            _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), '')
        self.assertEqual(before, (HoraExtra.objects.count(), IncidenciaAsistencia.objects.count()))

    def test_turno_nocturno_jornada_con_comida_incluida(self):
        a = self.asistencia(entrada=time(22), salida=time(8))
        a.turno = Turno.objects.create(nombre='Noche', hora_entrada=time(22), hora_salida=time(6))
        a.salida += timedelta(days=1)
        a.salida_comida = a.regreso_comida = None
        self.assertEqual(calcular_horas_extra(a), Decimal('2'))

    def test_marcas_invalidas_no_acreditan_extra(self):
        a = self.asistencia(salida=time(7))
        reportes, _ = _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), '')
        self.assertIsNone(reportes[0]['filas'][0]['extra']['detectado_minutos'])

    def test_cambio_fecha_autorizacion_no_cubre_el_dia_anterior(self):
        a = self.asistencia()
        manual = HoraExtra.objects.create(empleado=self.empleado, fecha=self.fecha, horas=2, estado='autorizado')
        manual.fecha += timedelta(days=1)
        manual.save(update_fields=['fecha'])
        reportes, _ = _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), '')
        self.assertEqual(reportes[0]['filas'][0]['extra']['autorizado_minutos'], 0)
