from datetime import date
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.db import close_old_connections, connection, transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from rest_framework.test import APIClient

from core.models import AuditLog
from rrhh.models import (
    AplicacionGoceVacaciones, Empleado, MovimientoVacaciones,
    PeriodoVacacional, PoliticaVacaciones, SolicitudVacaciones,
)


@override_settings(VACACIONES_GOCE_FIFO_ACTIVO=True)
class AniversarioVacacionesTests(TestCase):
    def setUp(self):
        self.clock = patch('django.utils.timezone.localdate', return_value=date(2026, 9, 14))
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.user = User.objects.create_superuser('rrhh.aniversarios', password='test')
        self.empleado = Empleado.objects.create(
            nombre='Alondra Aniversario', fecha_ingreso=date(2025, 8, 22), activo=True,
        )
        self.politica = PoliticaVacaciones.objects.create(
            antiguedad_desde=1, antiguedad_hasta=5, dias_laborables=12,
            vigente_desde=date(2026, 1, 1),
        )

    def test_get_no_promete_saldo_legacy_ni_escribe_periodos(self):
        client = APIClient()
        client.force_authenticate(self.user)
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET TRANSACTION READ ONLY')
            response = client.get(reverse('rrhh:vacaciones-saldo'), {
                'empleado': self.empleado.pk, 'fecha_inicio': '2026-09-15',
                'fecha_fin': '2026-09-15',
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['disponible'], 0)
        self.assertEqual(response.data['saldo']['disponible'], 0)
        self.assertFalse(response.data['saldo_suficiente'])
        self.assertFalse(PeriodoVacacional.objects.exists())

    def test_pantalla_y_api_comparten_saldo_sin_periodos(self):
        self.client.force_login(self.user)
        response = self.client.get('/rrhh/vacaciones/')
        self.assertEqual(response.status_code, 200)
        fila = response.context['empleados_historial'][0]
        self.assertEqual(fila['disponible_total'], 0)
        self.assertEqual(fila['saldo']['generado'], 0)

    @override_settings(VACACIONES_GOCE_FIFO_ACTIVO=False)
    def test_fifo_desactivado_conserva_legacy_y_no_genera(self):
        from rrhh.tasks import generar_periodos_vacacionales_diario
        generar_periodos_vacacionales_diario()
        client = APIClient()
        client.force_authenticate(self.user)
        response = client.get(reverse('rrhh:vacaciones-saldo'), {'empleado': self.empleado.pk})
        self.assertEqual(response.data['disponible'], 12)
        self.assertFalse(PeriodoVacacional.objects.exists())

    def test_ajuste_manual_requiere_conciliacion_y_baseline_no_se_borra(self):
        from rrhh.services_vacaciones_aniversarios import asegurar_periodo_actual
        ajuste = MovimientoVacaciones.objects.create(
            empleado=self.empleado, tipo='ajuste', dias=-12, periodo_anio=2026,
            descripcion='[conciliacion-manual] Revisión operativa',
        )
        with self.assertRaisesMessage(ValidationError, 'conciliar'):
            asegurar_periodo_actual(self.empleado.pk)
        ajuste.descripcion = '[saldo-inicial-vacaciones-20260616] saldo ciclo ERP 2026'
        ajuste.save()
        asegurar_periodo_actual(self.empleado.pk)
        ajuste.refresh_from_db()
        self.assertEqual(ajuste.dias, -12)
        self.assertEqual(PeriodoVacacional.objects.get().dias_generados, 12)

    def test_no_duplica_periodo_del_mismo_anio_con_fecha_distinta(self):
        from rrhh.services_vacaciones_aniversarios import asegurar_periodo_actual
        PeriodoVacacional.objects.create(
            empleado=self.empleado, aniversario=date(2026, 8, 21),
            fecha_limite=date(2027, 2, 21), antiguedad_anios=1, dias_generados=12,
        )
        with self.assertRaisesMessage(ValidationError, 'conciliar'):
            asegurar_periodo_actual(self.empleado.pk)
        self.assertEqual(PeriodoVacacional.objects.count(), 1)

    def test_aniversario_bisiesto_respeta_antiguedad(self):
        from rrhh.services_vacaciones_aniversarios import ultimo_aniversario_cumplido
        self.empleado.fecha_ingreso = date(2024, 2, 29)
        with patch('django.utils.timezone.localdate', return_value=date(2025, 2, 28)):
            self.assertIsNone(ultimo_aniversario_cumplido(self.empleado))
        with patch('django.utils.timezone.localdate', return_value=date(2025, 3, 1)):
            self.assertEqual(ultimo_aniversario_cumplido(self.empleado), date(2025, 3, 1))

    def test_post_crea_periodo_y_reserva_sin_cargar_ajuste(self):
        from rrhh.services_vacaciones import crear_solicitud_vacaciones
        solicitud = crear_solicitud_vacaciones(
            empleado=self.empleado, fecha_inicio=date(2026, 9, 15),
            fecha_fin=date(2026, 9, 15), motivo='Goce', actor=self.user,
        )
        periodo = PeriodoVacacional.objects.get(empleado=self.empleado)
        self.assertEqual(periodo.aniversario, date(2026, 8, 22))
        self.assertEqual(periodo.dias_generados, 12)
        self.assertEqual(solicitud.aplicaciones_goce.get().dias, 1)
        self.assertFalse(MovimientoVacaciones.objects.filter(tipo='ajuste').exists())

    def test_creacion_idempotente_y_auditable(self):
        from rrhh.services_vacaciones_aniversarios import asegurar_periodo_actual
        for _ in range(2):
            asegurar_periodo_actual(self.empleado.pk, actor=self.user, referencia='prueba')
        periodo = PeriodoVacacional.objects.get(empleado=self.empleado)
        self.assertEqual(periodo.fecha_limite, date(2027, 2, 22))
        self.assertEqual(periodo.dias_generados, 12)
        audit = AuditLog.objects.get(action='VACACIONES_ANIVERSARIO')
        self.assertEqual(audit.user, self.user)
        self.assertEqual(audit.payload['referencia'], 'prueba')
        self.assertFalse(SolicitudVacaciones.objects.exists())
        self.assertFalse(MovimientoVacaciones.objects.exists())

    def test_conserva_periodo_de_saldo_inicial_y_consumos(self):
        from rrhh.services_vacaciones_aniversarios import asegurar_periodo_actual
        periodo = PeriodoVacacional.objects.create(
            empleado=self.empleado, aniversario=date(2026, 8, 22),
            fecha_limite=date(2027, 2, 22), antiguedad_anios=1,
            dias_generados=3, origen='saldo_inicial', notas='Historia original',
        )
        asegurar_periodo_actual(self.empleado.pk)
        periodo.refresh_from_db()
        self.assertEqual(periodo.dias_generados, 3)
        self.assertEqual(periodo.notas, 'Historia original')
        self.assertFalse(AuditLog.objects.filter(action='VACACIONES_ANIVERSARIO').exists())

    def test_no_genera_futuro_inactivo_ni_primer_aniversario_pendiente(self):
        from rrhh.services_vacaciones_aniversarios import asegurar_periodo_actual
        self.empleado.fecha_ingreso = date(2025, 10, 1)
        self.empleado.save()
        asegurar_periodo_actual(self.empleado.pk, al=date(2027, 10, 1))
        self.assertFalse(PeriodoVacacional.objects.exists())
        self.empleado.fecha_ingreso = date(2025, 8, 22)
        self.empleado.activo = False
        self.empleado.save()
        asegurar_periodo_actual(self.empleado.pk)
        self.assertFalse(PeriodoVacacional.objects.exists())

    def test_solicitud_sin_aplicacion_requiere_revision(self):
        from rrhh.services_vacaciones_aniversarios import asegurar_periodo_actual
        SolicitudVacaciones.objects.create(
            empleado=self.empleado, fecha_inicio=date(2026, 8, 24),
            fecha_fin=date(2026, 8, 25), dias_laborables=2, estado='aprobada',
        )
        with self.assertRaisesMessage(ValidationError, 'conciliar'):
            asegurar_periodo_actual(self.empleado.pk)
        self.assertFalse(PeriodoVacacional.objects.exists())

    def test_sin_politica_no_crea_cero_y_no_detiene_otro_empleado(self):
        from rrhh.services_vacaciones_aniversarios import asegurar_periodos_actuales
        otro = Empleado.objects.create(nombre='Sin política', fecha_ingreso=date(2010, 8, 1))
        results = asegurar_periodos_actuales(empleado_ids=[otro.pk, self.empleado.pk])
        self.assertEqual({r['estado'] for r in results}, {'creado', 'revision'})
        self.assertEqual(PeriodoVacacional.objects.get().empleado_id, self.empleado.pk)

    def test_simulacion_no_ejecuta_escrituras_y_comando_es_acotado(self):
        otro = Empleado.objects.create(nombre='Fuera del alcance', fecha_ingreso=date(2025, 8, 1))
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET TRANSACTION READ ONLY')
            call_command('generar_periodos_vacacionales_actuales', empleado_id=[self.empleado.pk], stdout=StringIO())
        self.assertFalse(PeriodoVacacional.objects.exists())
        call_command('generar_periodos_vacacionales_actuales', empleado_id=[self.empleado.pk], ejecutar=True, actor_id=self.user.pk, referencia='regularizacion', stdout=StringIO())
        self.assertTrue(PeriodoVacacional.objects.filter(empleado=self.empleado).exists())
        self.assertFalse(PeriodoVacacional.objects.filter(empleado=otro).exists())

    def test_auditoria_detecta_aniversario_sin_periodo(self):
        from rrhh.tasks import _hallazgos_auditoria_vacaciones
        self.assertTrue(any('Alondra Aniversario' in x and 'aniversario' in x.lower() for x in _hallazgos_auditoria_vacaciones()))

    def test_repetir_tarea_diaria_no_duplica(self):
        from rrhh.tasks import generar_periodos_vacacionales_diario
        generar_periodos_vacacionales_diario()
        generar_periodos_vacacionales_diario()
        self.assertEqual(PeriodoVacacional.objects.filter(empleado=self.empleado).count(), 1)

    def test_post_insuficiente_revierte_periodo_y_solicitud(self):
        from rrhh.services_vacaciones import crear_solicitud_vacaciones
        with self.assertRaises(ValidationError):
            crear_solicitud_vacaciones(empleado=self.empleado, fecha_inicio=date(2026, 10, 1), fecha_fin=date(2026, 10, 31), motivo='Exceso', actor=self.user)
        self.assertFalse(PeriodoVacacional.objects.exists())
        self.assertFalse(SolicitudVacaciones.objects.exists())
        self.assertFalse(AuditLog.objects.filter(action='VACACIONES_ANIVERSARIO').exists())


@override_settings(VACACIONES_GOCE_FIFO_ACTIVO=True)
class AniversarioConcurrenciaTests(TransactionTestCase):
    @patch('django.utils.timezone.localdate', return_value=date(2026, 9, 14))
    def test_dos_generadores_crean_un_solo_periodo_y_auditoria(self, _clock):
        empleado = Empleado.objects.create(nombre='Concurrente', fecha_ingreso=date(2025, 8, 22))
        PoliticaVacaciones.objects.create(antiguedad_desde=1, antiguedad_hasta=None, dias_laborables=12)
        from rrhh.services_vacaciones_aniversarios import asegurar_periodo_actual
        def worker():
            close_old_connections()
            try:
                return asegurar_periodo_actual(empleado.pk, al=date(2026, 9, 14))
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda _: worker(), range(2)))
        self.assertEqual(PeriodoVacacional.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(action='VACACIONES_ANIVERSARIO').count(), 1)
