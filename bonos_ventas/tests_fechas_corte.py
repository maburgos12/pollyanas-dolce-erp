from datetime import date
from importlib import import_module
from pathlib import Path
import shutil
import subprocess
from unittest import skipUnless
from unittest.mock import patch

from django.apps import apps
from django.contrib.auth.models import User
from django.db import connection, IntegrityError, transaction
from django.test import SimpleTestCase, TestCase, override_settings

from core.models import Sucursal
from rrhh.models import Empleado, AsistenciaEmpleado
from .models import ConfigBonoVentasPeriodo, BonoVentasEmpleado, RegistroDiarioVentas
from .serializers import RegistroDiarioVentasSerializer
from .services_checador import sincronizar_asistencia_desde_checador, sincronizar_empleado_dia_desde_checador


@override_settings(SECURE_SSL_REDIRECT=False)
class FechasCorteVentasTests(TestCase):
    def setUp(self):
        self.periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=6, anio=2026, fecha_inicio=date(2026,5,28), fecha_fin=date(2026,6,29),
        )
        sucursal = Sucursal.objects.create(codigo='FECHA', nombre='Prueba fechas')
        empleado = Empleado.objects.create(nombre='Prueba fechas ventas', fecha_ingreso=date(2026,1,1))
        self.bono = BonoVentasEmpleado.objects.create(
            periodo=self.periodo, sucursal=sucursal, empleado=empleado,
            bono_extra=75, ajuste_positivo=125, ajuste_negativo=25,
        )
        self.user = User.objects.create_superuser('prueba-fecha', password='x')
        self.client.force_login(self.user)

    def test_unicidad_por_fecha_y_no_por_numero_de_dia(self):
        for fecha in (date(2026,5,28), date(2026,6,28)):
            RegistroDiarioVentas.objects.create(bono=self.bono, dia=28, fecha=fecha)
        with self.assertRaises(IntegrityError), transaction.atomic():
            RegistroDiarioVentas.objects.create(bono=self.bono, dia=28, fecha=date(2026,6,28))
        self.assertEqual(self.bono.registros.count(), 2)

    @patch('django.utils.timezone.localdate', return_value=date(2026,6,13))
    def test_api_rechaza_ordinal_fecha_incoherente_y_futuro(self, _):
        for payload in ({'dia':1}, {'dia':13,'fecha':'2026-06-09'}, {'dia':28,'fecha':'2026-06-28'},
                        {'dia':27,'fecha':'2026-05-27'}):
            serializer = RegistroDiarioVentasSerializer(data={'bono':self.bono.pk, **payload})
            self.assertFalse(serializer.is_valid(), payload)
        serializer = RegistroDiarioVentasSerializer(data={'bono':self.bono.pk,'dia':1,'fecha':'2026-06-01'})
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.save().fecha, date(2026,6,1))

    def test_migracion_no_reinterpreta_manual_y_conserva_todos_los_valores(self):
        auto = RegistroDiarioVentas.objects.create(bono=self.bono, dia=1, tiene_uniforme=False, puntos_de_vista='Original')
        manual = RegistroDiarioVentas.objects.create(bono=self.bono, dia=2, capturado_por=self.user, puntos_de_vista='Manual')
        antes = list(self.bono.registros.order_by('id').values())
        migration = import_module('bonos_ventas.migrations.0006_registros_fecha_completa')
        with connection.schema_editor(atomic=False) as editor:
            migration.completar_fechas_seguras(apps, editor)
        auto.refresh_from_db(); manual.refresh_from_db()
        self.assertEqual(auto.fecha, date(2026,6,1))
        self.assertIsNone(manual.fecha)
        despues = list(self.bono.registros.order_by('id').values())
        for rows in (antes, despues):
            for row in rows:
                row.pop('fecha', None)
        self.assertEqual(antes, despues)

    @patch('django.utils.timezone.localdate', return_value=date(2026,6,13))
    def test_pendientes_no_se_duplican_ni_recalculan_al_sincronizar(self, _):
        RegistroDiarioVentas.objects.create(bono=self.bono, dia=1, capturado_por=self.user, puntos_de_vista='Sin fecha segura')
        antes = self.bono.__dict__.copy()
        resultado = sincronizar_asistencia_desde_checador(self.periodo)
        self.assertEqual(resultado['bonos_omitidos'], 1)
        self.assertEqual(self.bono.registros.count(), 1)
        self.bono.refresh_from_db()
        for campo in ('dias_trabajados','total_a_pagar','bono_extra','ajuste_positivo','ajuste_negativo'):
            self.assertEqual(getattr(self.bono,campo), antes[campo])
        serializer = RegistroDiarioVentasSerializer(data={'bono':self.bono.pk,'dia':3,'fecha':'2026-06-03'})
        self.assertFalse(serializer.is_valid())

    @patch('django.utils.timezone.localdate', return_value=date(2026,6,13))
    def test_edicion_por_id_no_mueve_fecha_y_captura_manual_no_se_pisa(self, _):
        registro = RegistroDiarioVentas.objects.create(bono=self.bono,dia=1,fecha=date(2026,6,1))
        response = self.client.patch(f'/api/bonos-ventas/registros-diarios/{registro.pk}/',
                                    {'fecha':'2026-06-01','tiene_asistencia':False}, content_type='application/json')
        self.assertEqual(response.status_code, 200)
        registro.refresh_from_db()
        self.assertEqual(registro.capturado_por, self.user)
        anterior = RegistroDiarioVentasSerializer(registro,data={'tiene_asistencia':True},partial=True)
        self.assertFalse(anterior.is_valid(), 'Rechazar edición desde calendario anterior sin fecha')
        AsistenciaEmpleado.objects.create(empleado=self.bono.empleado, fecha=date(2026,6,1))
        sincronizar_asistencia_desde_checador(self.periodo)
        registro.refresh_from_db()
        self.assertFalse(registro.tiene_asistencia)
        serializer = RegistroDiarioVentasSerializer(registro,data={'fecha':'2026-05-28'},partial=True)
        self.assertFalse(serializer.is_valid())
        self.bono.refresh_from_db()
        self.assertEqual((self.bono.bono_extra,self.bono.ajuste_positivo,self.bono.ajuste_negativo),(75,125,25))

    def test_pendiente_permite_nota_por_id_sin_cambiar_asistencia(self):
        registro = RegistroDiarioVentas.objects.create(bono=self.bono, dia=1, capturado_por=self.user)
        serializer = RegistroDiarioVentasSerializer(registro,data={'puntos_de_vista':'Nota corregida'},partial=True)
        self.assertTrue(serializer.is_valid(),serializer.errors)
        self.assertIsNone(serializer.save().fecha)
        serializer = RegistroDiarioVentasSerializer(registro,data={'tiene_asistencia':False},partial=True)
        self.assertFalse(serializer.is_valid())

    @patch('django.utils.timezone.localdate', return_value=date(2026,5,27))
    def test_sync_no_fabrica_asistencias_de_periodo_futuro(self, _):
        resultado = sincronizar_asistencia_desde_checador(self.periodo)
        self.assertEqual(resultado['registros_creados'], 0)
        self.assertFalse(self.bono.registros.exists())
        self.bono.refresh_from_db()
        antes = self.bono.actualizado_en
        resultado = sincronizar_empleado_dia_desde_checador(self.bono.empleado_id, date(2026,6,1))
        self.assertEqual(resultado['bonos_sincronizados'], 0)
        self.bono.refresh_from_db()
        self.assertEqual(self.bono.actualizado_en, antes)
        self.assertFalse(self.bono.registros.exists())


class CalendarioVentasFechasTests(SimpleTestCase):
    @skipUnless(shutil.which('node'), 'Requiere Node para verificar el calendario JavaScript')
    def test_calendario_y_captura_usen_fecha_completa(self):
        subprocess.run([shutil.which('node'),str(Path(__file__).with_name('tests_fechas_calendario.cjs'))],
                       check=True,capture_output=True,text=True)
