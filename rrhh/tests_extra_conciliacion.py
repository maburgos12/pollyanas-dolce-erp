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
from rrhh.views_asistencia import _build_reporte_asistencia


class ExtraConciliacionTests(TestCase):
    def setUp(self):
        self.fecha = date(2026, 9, 15)
        self.empleado = Empleado.objects.create(codigo='EXTRA-001', nombre='Persona extra', fecha_ingreso=date(2026, 1, 1))

    def asistencia(self, entrada=time(8), salida=time(18), **kwargs):
        def dt(h):
            return timezone.make_aware(datetime.combine(self.fecha, h)) if h else None
        return AsistenciaEmpleado.objects.create(empleado=self.empleado, fecha=self.fecha,
            entrada=dt(entrada), salida=dt(salida), minutos_trabajados=565,
            salida_comida=dt(time(12)), regreso_comida=dt(time(12, 35)), minutos_comida=35, **kwargs)

    def test_sin_turno_ocho_horas_incluyen_comida(self):
        a = self.asistencia()
        self.assertEqual(calcular_horas_extra(a), Decimal('2.00'))

    def test_comida_excedida_no_se_convierte_en_extra(self):
        a = self.asistencia()
        a.minutos_comida = 120
        a.minutos_trabajados = 480
        a.regreso_comida = a.salida_comida + timedelta(minutes=120)
        self.assertEqual(calcular_horas_extra(a), Decimal('0.58'))

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
        a = self.asistencia(entrada=time(7, 56))
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

    def test_tolerancia_exacta_no_genera_extra(self):
        self.assertEqual(calcular_horas_extra(self.asistencia(salida=time(16, 10))), Decimal('0'))

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
