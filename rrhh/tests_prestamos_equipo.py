from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APIClient

from core.models import Notificacion
from rrhh.models import Empleado, Prestamo, PrestamoCuota
from rrhh.services_prestamos import aplicar_cobro_manual, generar_cuotas


class PrestamosEquipoTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        users = get_user_model().objects
        cls.jefe = users.create_user('jefa.prestamos')
        cls.jefe.groups.add(Group.objects.get_or_create(name='PRODUCCION')[0])
        cls.director = users.create_user('director.prestamos')
        cls.director.groups.add(Group.objects.get_or_create(name='DG')[0])
        cls.rrhh = users.create_user('rrhh.prestamos')
        cls.rrhh.groups.add(Group.objects.get_or_create(name='RRHH')[0])
        cls.personal = users.create_user('personal.prestamos')
        cls.director_empleado = Empleado.objects.create(nombre='Director', usuario_erp=cls.director)
        cls.jefa_empleado = Empleado.objects.create(nombre='Jefa', usuario_erp=cls.jefe, jefe_directo=cls.director_empleado)
        cls.empleado = Empleado.objects.create(nombre='Colaborador', usuario_erp=cls.personal, jefe_directo=cls.jefa_empleado)
        cls.ajeno = Empleado.objects.create(nombre='Otro equipo')
        cls.inactivo = Empleado.objects.create(nombre='Baja', activo=False, jefe_directo=cls.jefa_empleado)

    def setUp(self):
        self.api = APIClient()
        self.payload = dict(empleado=self.empleado.pk, concepto='Apoyo', metodo_pago='transferencia', importe='1000.00', num_quincenas=3)

    def crear(self, usuario, **changes):
        self.api.force_authenticate(usuario)
        return self.api.post(reverse('rrhh:prestamo-list'), self.payload | changes, format='json')

    def test_rrhh_jefe_direccion_en_orden(self):
        r = self.crear(self.rrhh)
        self.assertEqual(r.status_code, 201, r.data)
        p = Prestamo.objects.get(pk=r.data['id'])
        self.assertEqual(p.jefe_directo, self.jefe)
        self.assertEqual(p.estado, Prestamo.ESTADO_SOLICITADO)
        self.assertFalse(p.cuotas.exists())
        self.api.force_authenticate(self.director)
        url = reverse('rrhh:prestamo-aprobar-direccion', args=[p.pk])
        self.assertEqual(self.api.post(url).status_code, 403)
        self.api.force_authenticate(self.jefe)
        self.assertEqual(self.api.post(reverse('rrhh:prestamo-autorizar-jefe', args=[p.pk])).status_code, 200)
        self.assertEqual(self.api.post(url).status_code, 403)
        self.api.force_authenticate(self.director)
        self.assertEqual(self.api.post(url).status_code, 200)
        p.refresh_from_db()
        self.assertEqual(p.estado, Prestamo.ESTADO_ACTIVO)
        self.assertEqual(sum(p.cuotas.values_list('monto_esperado', flat=True)), p.importe)
        self.assertEqual(self.api.post(url).status_code, 403)
        self.assertEqual(p.cuotas.count(), 3)

    def test_jefa_captura_con_visto_bueno_sin_generar_cuotas(self):
        r = self.crear(self.jefe)
        self.assertEqual(r.status_code, 201, r.data)
        p = Prestamo.objects.get(pk=r.data['id'])
        self.assertEqual(p.estado, Prestamo.ESTADO_AUTORIZADO)
        self.assertEqual(p.autorizado_jefe, self.jefe)
        self.assertTrue(p.fecha_auth_jefe)
        self.assertFalse(p.cuotas.exists())
        self.assertTrue(Notificacion.objects.filter(usuario=self.director, tipo=Notificacion.TIPO_PRESTAMO).exists())
        r = self.api.get(reverse('rrhh:prestamo-list'), {'mis': 'true'})
        self.assertEqual([v['id'] for v in r.data], [p.pk])
        self.assertEqual(self.crear(self.jefe).status_code, 400)
        self.assertEqual(Prestamo.objects.count(), 1)

    def test_solo_equipo_activo_y_personal_sin_acceso_general(self):
        for empleado in (self.ajeno, self.inactivo):
            with self.subTest(empleado=empleado.pk):
                self.assertEqual(self.crear(self.jefe, empleado=empleado.pk).status_code, 403)
        self.api.force_authenticate(self.jefe)
        r = self.api.get(reverse('rrhh:prestamo-empleados'))
        self.assertEqual({e['id'] for e in r.data}, {self.empleado.pk, self.jefa_empleado.pk})
        self.client.force_login(self.jefe)
        self.assertEqual(self.client.get(reverse('rrhh:rrhh_prestamo_nuevo')).status_code, 200)
        self.assertEqual(self.client.get(reverse('rrhh:rrhh_importar_contpaq')).status_code, 403)

    def test_solicitud_propia_de_jefa_no_se_autoriza(self):
        r = self.crear(self.jefe, empleado=self.jefa_empleado.pk)
        self.assertEqual(r.status_code, 201, r.data)
        self.assertEqual(r.data['estado'], Prestamo.ESTADO_SOLICITADO)
        self.assertEqual(self.api.post(reverse('rrhh:prestamo-autorizar-jefe', args=[r.data['id']])).status_code, 403)

    def test_jefa_ve_acceso_sin_prestamos_pendientes(self):
        from core.navigation import build_nav_groups

        self.assertFalse(Prestamo.objects.exists())
        grupos = build_nav_groups(self.jefe, '/dashboard/')
        trabajo = next(g for g in grupos if g['key'] == 'mi_trabajo')
        self.assertIn('/rrhh/prestamos/', [i['url'] for i in trabajo['items']])

    def test_empleado_solicita_para_si_y_no_para_otros(self):
        self.assertEqual(self.crear(self.personal, empleado=self.ajeno.pk).status_code, 403)
        r = self.crear(self.personal)
        self.assertEqual(r.status_code, 201, r.data)
        self.assertEqual(r.data['estado'], Prestamo.ESTADO_SOLICITADO)

    def test_no_se_puede_reescribir_o_borrar_la_solicitud_autorizada(self):
        r = self.crear(self.jefe)
        url = reverse('rrhh:prestamo-detail', args=[r.data['id']])
        self.assertEqual(self.api.patch(url, {'importe': '9999'}, format='json').status_code, 405)
        self.assertEqual(self.api.delete(url).status_code, 405)
        self.assertEqual(Prestamo.objects.get(pk=r.data['id']).importe, Decimal('1000'))

    def test_web_rrhh_ignora_jefe_manipulado_y_valida_importe(self):
        self.client.force_login(self.rrhh)
        url = reverse('rrhh:rrhh_prestamo_nuevo')
        r = self.client.post(url, self.payload | {'importe': '-1', 'jefe_directo': self.director.pk})
        self.assertEqual(r.status_code, 400)
        self.assertFalse(Prestamo.objects.exists())
        r = self.client.post(url, self.payload | {'jefe_directo': self.director.pk, 'fecha_deposito': ''})
        self.assertEqual(r.status_code, 302)
        p = Prestamo.objects.get()
        self.assertEqual(p.jefe_directo, self.jefe)
        self.assertEqual(p.estado, Prestamo.ESTADO_SOLICITADO)

    def test_web_jefa_solicita_para_si_sin_autorizar_y_puede_consultar(self):
        self.client.force_login(self.jefe)
        response = self.client.post(reverse('rrhh:rrhh_prestamo_nuevo'), self.payload | {'empleado': self.jefa_empleado.pk}, follow=True)
        self.assertEqual(response.status_code, 200)
        prestamo = Prestamo.objects.get()
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_SOLICITADO)
        self.assertContains(response, 'Jefe: Pendiente')
        self.assertContains(self.client.get(reverse('rrhh:rrhh_prestamos_lista')), prestamo.folio)

    def test_web_jefa_y_acceso_ajeno(self):
        self.client.force_login(self.jefe)
        url = reverse('rrhh:rrhh_prestamo_nuevo')
        self.assertEqual(self.client.post(url, self.payload | {'empleado': self.ajeno.pk}).status_code, 403)
        self.assertEqual(self.client.post(url, self.payload).status_code, 302)
        self.assertEqual(Prestamo.objects.get().estado, Prestamo.ESTADO_AUTORIZADO)
        self.client.force_login(self.personal)
        self.assertEqual(self.client.get(url).status_code, 403)

    def test_cobro_parcial_y_ultima_cuota_sin_centavos_pendientes(self):
        p = Prestamo.objects.create(empleado=self.empleado, concepto='Prueba', fecha_solicitud=date(2026, 11, 4), importe=Decimal('1000'), num_quincenas=3, descuento_quincenal=Decimal('333.33'), estado=Prestamo.ESTADO_ACTIVO)
        cuotas = generar_cuotas(p)
        self.assertEqual([c.monto_esperado for c in cuotas], [Decimal('333.33'), Decimal('333.33'), Decimal('333.34')])
        cuotas = list(p.cuotas.all())
        aplicar_cobro_manual(cuotas[0], Decimal('100'), self.rrhh)
        p.refresh_from_db()
        self.assertEqual(p.saldo_actual, Decimal('900'))
        self.assertEqual(cuotas[0].estado, PrestamoCuota.ESTADO_PARCIAL)
        for cuota in cuotas:
            aplicar_cobro_manual(cuota, cuota.monto_esperado, self.rrhh)
        p.refresh_from_db()
        self.assertEqual(p.saldo_actual, 0)
        self.assertEqual(p.estado, Prestamo.ESTADO_LIQUIDADO)

    @patch('rrhh.importers_contpaq._parse_xls')
    def test_importacion_contpaq_conserva_fuente_y_aplica_parcial(self, parse):
        from django.core.files.uploadedfile import SimpleUploadedFile
        from django.test import override_settings
        from tempfile import TemporaryDirectory
        from rrhh.importers_contpaq import importar_lista_raya_contpaq
        self.empleado.codigo = 'PRE-TEST'
        self.empleado.save(update_fields=['codigo'])
        p = Prestamo.objects.create(empleado=self.empleado, concepto='Prueba', fecha_solicitud=date(2026, 11, 4), importe=Decimal('1000'), num_quincenas=2, descuento_quincenal=Decimal('500'), estado=Prestamo.ESTADO_ACTIVO)
        generar_cuotas(p)
        parse.return_value = [{'num_empleado': 'PRE-TEST', 'nombre': 'Colaborador', 'prestamo_empresa': Decimal('100')}]
        with TemporaryDirectory() as media, override_settings(MEDIA_ROOT=media):
            result = importar_lista_raya_contpaq(SimpleUploadedFile('test.xls', b'test'), self.rrhh, date(2026, 11, 16), date(2026, 11, 30), 1)
        self.assertEqual(result['aplicados'], 1)
        p.refresh_from_db()
        self.assertEqual(p.saldo_actual, Decimal('900'))
        self.assertEqual(p.cuotas.first().fuente, PrestamoCuota.FUENTE_CONTPAQ)
