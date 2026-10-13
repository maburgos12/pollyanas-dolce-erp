from datetime import date
from importlib import import_module

from django.apps import apps
from django.db import connection
from unittest.mock import patch
from unittest import skipUnless
from pathlib import Path
import shutil
import subprocess

from django.db import IntegrityError, transaction
from django.test import SimpleTestCase, TestCase

from rrhh.models import Empleado, AsistenciaEmpleado
from .models import BonoProduccionEmpleado, ConfigBonoPeriodo, RegistroDiarioProduccion
from .serializers import RegistroDiarioSerializer
from .services_checador import sincronizar_asistencia_desde_checador


class FechasCorteTests(TestCase):
    def setUp(self):
        self.periodo = ConfigBonoPeriodo.objects.create(
            mes=10, anio=2026, fecha_inicio=date(2026, 9, 28), fecha_fin=date(2026, 10, 29),
        )
        self.empleado = Empleado.objects.create(nombre='Prueba fechas corte', area='HORNOS', fecha_ingreso=date(2026, 1, 1))
        self.bono = BonoProduccionEmpleado.objects.create(
            periodo=self.periodo, empleado=self.empleado, area='HORNOS', bono_extra=75,
            ajuste_positivo=125, ajuste_negativo=25,
        )

    def test_dos_meses_mismo_dia_son_registros_distintos(self):
        for fecha in (date(2026, 9, 28), date(2026, 10, 28)):
            RegistroDiarioProduccion.objects.create(bono=self.bono, dia=28, fecha=fecha)
        with self.assertRaises(IntegrityError), transaction.atomic():
            RegistroDiarioProduccion.objects.create(bono=self.bono, dia=28, fecha=date(2026, 10, 28))
        self.assertEqual(self.bono.registros.count(), 2)

    @patch('bonos_produccion.serializers.timezone.localdate', return_value=date(2026, 10, 13))
    def test_api_rechaza_futuro_y_dia_ambiguo(self, _):
        for payload in ({'dia': 28}, {'dia': 28, 'fecha': '2026-10-28'},
                        {'dia': 27, 'fecha': '2026-09-27'}, {'dia': 29, 'fecha': '2026-09-28'}):
            serializer = RegistroDiarioSerializer(data={'bono': self.bono.id, **payload})
            self.assertFalse(serializer.is_valid(), payload)
        serializer = RegistroDiarioSerializer(data={'bono': self.bono.id, 'dia': 28, 'fecha': '2026-09-28'})
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.save().fecha, date(2026, 9, 28))

    @patch('bonos_produccion.serializers.timezone.localdate', return_value=date(2026, 10, 13))
    def test_api_no_reubica_registro_y_conserva_edicion_legacy(self, _):
        registro = RegistroDiarioProduccion.objects.create(bono=self.bono, dia=28, fecha=date(2026, 9, 28))
        serializer = RegistroDiarioSerializer(registro, data={'fecha': '2026-10-28'}, partial=True)
        self.assertFalse(serializer.is_valid())
        legacy = RegistroDiarioProduccion.objects.create(bono=self.bono, dia=29, observacion='Histórico ambiguo')
        serializer = RegistroDiarioSerializer(legacy, data={'observacion': 'Corrección de nota'}, partial=True)
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertIsNone(serializer.save().fecha)

    @patch('bonos_produccion.services_checador.timezone.localdate', return_value=date(2026, 10, 13))
    def test_sync_no_crea_futuros_ni_duplica_septiembre(self, _):
        AsistenciaEmpleado.objects.create(empleado=self.empleado, fecha=date(2026, 9, 28))
        sincronizar_asistencia_desde_checador(self.periodo)
        self.assertTrue(self.bono.registros.get(fecha=date(2026, 9, 28)).tiene_asistencia)
        self.assertFalse(self.bono.registros.filter(fecha__gt=date(2026, 10, 13)).exists())
        with patch('bonos_produccion.services_checador.timezone.localdate', return_value=date(2026, 10, 29)):
            AsistenciaEmpleado.objects.create(empleado=self.empleado, fecha=date(2026, 10, 28))
            sincronizar_asistencia_desde_checador(self.periodo)
            self.assertEqual(self.bono.registros.filter(dia=28, tiene_asistencia=True).count(), 2)
            self.assertEqual(sincronizar_asistencia_desde_checador(self.periodo)['registros_creados'], 0)
        self.bono.refresh_from_db()
        self.assertEqual((self.bono.bono_extra, self.bono.ajuste_positivo, self.bono.ajuste_negativo), (75, 125, 25))

    def test_migracion_completa_solo_fechas_inequivocas_y_conserva_captura(self):
        claro = RegistroDiarioProduccion.objects.create(
            bono=self.bono, dia=2, tiene_asistencia=False, cantidad_embetunados=4, observacion="Captura original",
        )
        ambiguo = RegistroDiarioProduccion.objects.create(bono=self.bono, dia=28, observacion="Sin mes")
        creado = claro.creado_en
        migration = import_module('bonos_produccion.migrations.0010_registros_fecha_completa')
        with connection.schema_editor(atomic=False) as editor:
            migration.completar_fechas_inequivocas(apps, editor)
        claro.refresh_from_db()
        ambiguo.refresh_from_db()
        self.assertEqual(claro.fecha, date(2026, 10, 2))
        self.assertEqual((claro.tiene_asistencia, claro.cantidad_embetunados, claro.observacion, claro.creado_en),
                         (False, 4, "Captura original", creado))
        self.assertIsNone(ambiguo.fecha)


class CalendarioFechasTests(SimpleTestCase):
    @skipUnless(shutil.which("node"), "Requiere Node para verificar el calendario JavaScript")
    def test_calendario_identifica_fechas_y_no_envia_capturas_futuras(self):
        subprocess.run([shutil.which("node"), str(Path(__file__).with_name("tests_fechas_calendario.cjs"))],
                       check=True, capture_output=True, text=True)
