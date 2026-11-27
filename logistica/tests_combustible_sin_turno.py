from datetime import date
from decimal import Decimal
from io import BytesIO
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

from PIL import Image
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import SimpleTestCase, TestCase, TransactionTestCase, override_settings
from django.urls import reverse

from core.models import AuditLog, Sucursal
from logistica.models import BitacoraSalidaLlegada, CargaCombustibleUnidad, Repartidor, Unidad


class RutaCombustibleSinTurnoTests(SimpleTestCase):
    def test_captura_administrativa_tiene_ruta_propia(self):
        from django.urls import resolve
        from django.urls.exceptions import Resolver404
        try:
            found = resolve('/logistica/combustible/nuevo/')
        except Resolver404:
            self.fail('Falta la captura administrativa de combustible sin turno')
        self.assertEqual(found.url_name, 'combustible_nuevo')


class CombustibleSinTurnoTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        media = cls.enterClassContext(TemporaryDirectory(prefix='combustible-sin-turno-'))
        cls.enterClassContext(override_settings(MEDIA_ROOT=media))

    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.dg = User.objects.create_user('dg.sin.turno')
        cls.dg.groups.add(Group.objects.get_or_create(name='DG')[0])
        cls.chofer = User.objects.create_user('carolina.test', first_name='Carolina', last_name='Cayetano')
        sucursal = Sucursal.objects.create(codigo='ST', nombre='Prueba sin turno')
        cls.unidad = Unidad.objects.create(codigo='ST-CH1', descripcion='Cheyenne', sucursal=sucursal)
        cls.repartidor = Repartidor.objects.create(user=cls.chofer, sucursal=sucursal)

    def setUp(self):
        self.client.force_login(self.dg)
        self.url = '/logistica/combustible/nuevo/'

    def datos(self, **changes):
        image = BytesIO()
        Image.new('RGB', (20, 20), 'white').save(image, 'JPEG')
        return dict({
            'unidad': self.unidad.pk, 'repartidor': self.repartidor.pk,
            'fecha_carga': '2026-10-31T13:11', 'litros': '21.277', 'importe_total': '500.00',
            'estacion': 'Servicio Carranza', 'folio_ticket': 'TEST-3003609',
            'observaciones': 'Magna, $23.50 por litro. Captura solicitada por DG.',
            'foto_ticket': SimpleUploadedFile('nota.jpg', image.getvalue(), content_type='image/jpeg'),
        }, **changes)

    def crear(self, **changes):
        with patch('logistica.tasks.auditar_ticket_combustible.delay'):
            return self.client.post(self.url, self.datos(**changes), HTTP_ACCEPT='application/json')

    def test_registra_fecha_ticket_precision_responsable_y_autor_sin_crear_turno(self):
        response = self.crear()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        carga = CargaCombustibleUnidad.objects.get()
        self.assertIsNone(carga.bitacora_id)
        self.assertEqual(carga.repartidor_id, self.repartidor.pk)
        self.assertEqual(carga.registrado_por_id, self.dg.pk)
        self.assertEqual(carga.litros, Decimal('21.277'))
        self.assertEqual(carga.fecha_consumo.astimezone(ZoneInfo('America/Mazatlan')).isoformat(), '2026-10-31T13:11:00-07:00')
        self.assertFalse(BitacoraSalidaLlegada.objects.exists())
        self.assertEqual(AuditLog.objects.get(model='logistica.CargaCombustibleUnidad').user_id, self.dg.pk)

    def test_reintento_misma_foto_no_duplica(self):
        self.assertEqual(self.crear().status_code, 200)
        response = self.crear()
        self.assertEqual(response.status_code, 409)
        self.assertEqual(CargaCombustibleUnidad.objects.count(), 1)

    def test_rollback_no_deja_archivo_ni_carga(self):
        from pathlib import Path
        from django.conf import settings
        antes = set(Path(settings.MEDIA_ROOT).rglob('*.jpg'))
        with patch('logistica.combustible_captura.log_event', side_effect=RuntimeError('Fallo audit log')):
            with self.assertRaises(RuntimeError):
                self.crear()
        self.assertFalse(CargaCombustibleUnidad.objects.exists())
        self.assertEqual(set(Path(settings.MEDIA_ROOT).rglob('*.jpg')), antes)

    def test_carga_historica_sigue_usando_fecha_registro(self):
        from django.utils import timezone
        carga = CargaCombustibleUnidad.objects.create(unidad=self.unidad, repartidor=self.repartidor,
            litros='10.00', importe_total='235.00', foto_ticket='historico.jpg')
        carga.refresh_from_db()
        self.assertEqual(carga.fecha_consumo, carga.fecha_registro)
        from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
        mes = timezone.localtime(carga.fecha_registro).date().replace(day=1)
        self.assertEqual(PresupuestoRealConsolidacionService._build_combustible_index(mes)[self.unidad.codigo], Decimal('235'))

    def test_rechaza_folio_repetido_con_otra_foto(self):
        self.assertEqual(self.crear().status_code, 200)
        image = BytesIO()
        Image.new('RGB', (21, 21), 'black').save(image, 'JPEG')
        response = self.crear(foto_ticket=SimpleUploadedFile('otra.jpg', image.getvalue(), content_type='image/jpeg'))
        self.assertEqual(response.status_code, 409)
        self.assertEqual(CargaCombustibleUnidad.objects.count(), 1)

    def test_repartidor_no_puede_crear_cargas_administrativas(self):
        self.client.force_login(self.chofer)
        self.assertIn(self.crear().status_code, (302, 403))
        self.assertFalse(CargaCombustibleUnidad.objects.exists())

    def test_invalida_no_escribe_y_conserva_datos(self):
        response = self.crear(litros='-1')
        self.assertEqual(response.status_code, 400)
        self.assertFalse(CargaCombustibleUnidad.objects.exists())
        self.assertIn('Litros', response.json()['toast']['message'])

    def test_recibo_invalido_es_404(self):
        self.assertEqual(self.client.get(self.url, {'carga': 'abc'}).status_code, 404)

    def test_reportes_usan_fecha_carga_y_lista_incluye_sin_turno(self):
        self.assertEqual(self.crear().status_code, 200)
        from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
        index = PresupuestoRealConsolidacionService._build_combustible_index(date(2026, 10, 1))
        self.assertEqual(index[self.unidad.codigo], Decimal('500.00'))
        otro = PresupuestoRealConsolidacionService._build_combustible_index(date(2026, 11, 1))
        self.assertNotIn(self.unidad.codigo, otro)
        response = self.client.get(reverse('logistica:bitacoras_lista'), {'fecha_desde': '2026-10-01', 'fecha_hasta': '2026-10-31'})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'TEST-3003609')
        self.assertEqual(response.context['combustible_total'], Decimal('500.00'))
        from reportes.services_conciliacion_combustible import conciliar_combustible
        conciliacion = conciliar_combustible(date(2026, 10, 1))
        self.assertEqual(conciliacion['total_bitacora'], Decimal('500.00'))
        self.assertEqual(conciliacion['cargas'][0]['ticket'], 'TEST-3003609')
        self.assertEqual(conciliacion['cargas'][0]['estado'], 'RESPALDO_PENDIENTE')
        from reportes.services_costeo_etapas import leer_logistica
        costeo = leer_logistica(date(2026, 10, 1), date(2026, 10, 31), {'personas': []})
        self.assertEqual(costeo['unidades'][0]['sin_turno'], Decimal('500.00'))
        self.assertEqual(costeo['unidades'][0]['pendiente'], Decimal('500.00'))

    def test_editar_sin_bitacora_y_api_no_fallan(self):
        self.assertEqual(self.crear().status_code, 200)
        carga = CargaCombustibleUnidad.objects.get()
        url = reverse('logistica:combustible_editar', args=[carga.pk])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        with patch('logistica.tasks.auditar_ticket_combustible.delay'):
            response = self.client.post(url, {'litros': '21.278', 'importe_total': '500', 'motivo': 'Prueba de precisión', 'revision': response.context['form']['revision'].value()})
        self.assertEqual(response.status_code, 302)
        carga.refresh_from_db()
        self.assertEqual(carga.litros, Decimal('21.278'))
        from api.logistica_serializers import LogisticaCargaCombustibleSerializer
        self.assertIsNone(LogisticaCargaCombustibleSerializer(carga).data['bitacora_folio'])


class CombustibleConcurrenteTests(TransactionTestCase):
    def test_dos_capturas_simultaneas_del_mismo_ticket_guardan_una_sola(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections
        from logistica.combustible_captura import CapturaCombustibleForm, CargaDuplicada, registrar_carga_sin_turno
        User = get_user_model()
        actor = User.objects.create_user('dg.concurrente')
        actor.groups.add(Group.objects.get_or_create(name='DG')[0])
        s = Sucursal.objects.create(codigo='CON', nombre='Concurrente')
        unidad = Unidad.objects.create(codigo='CON-1', descripcion='Prueba', sucursal=s)
        repartidor = Repartidor.objects.create(user=actor, sucursal=s)
        barrier = Barrier(2)

        def capturar(_):
            close_old_connections()
            try:
                image = BytesIO()
                Image.new('RGB', (20, 20), 'white').save(image, 'JPEG')
                form = CapturaCombustibleForm(data={'unidad': unidad.pk, 'repartidor': repartidor.pk,
                    'fecha_carga': '2026-10-31T13:11', 'litros': '21.277', 'importe_total': '500',
                    'estacion': 'CONCURRENTE', 'folio_ticket': 'CON-001'},
                    files={'foto_ticket': SimpleUploadedFile('concurrente.jpg', image.getvalue(), content_type='image/jpeg')})
                self.assertTrue(form.is_valid(), form.errors)
                barrier.wait(timeout=10)
                try:
                    return registrar_carga_sin_turno(form=form, actor=User.objects.get(pk=actor.pk)).pk
                except CargaDuplicada:
                    return None
            finally:
                close_old_connections()

        with TemporaryDirectory(prefix='combustible-concurrente-') as media, override_settings(MEDIA_ROOT=media):
            with ThreadPoolExecutor(max_workers=2) as executor:
                resultados = list(executor.map(capturar, range(2)))
            self.assertEqual(sum(r is not None for r in resultados), 1)
            self.assertEqual(CargaCombustibleUnidad.objects.count(), 1)
