from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core import mail
from django.test import TestCase, override_settings

from core.models import Notificacion
from rrhh.models import AvisoCumpleanos, Empleado
from rrhh.services_cumpleanos import (
    empleados_visibles, eventos_entre, fecha_cumpleanos,
    generar_avisos_cumpleanos, puede_ver_cumpleanos,
)


@override_settings(EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend')
class CumpleanosTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.ch = User.objects.create_user('ch', email='ch@pollyanasdolce.com')
        self.ch.groups.add(Group.objects.get_or_create(name='RRHH')[0])
        self.dg = User.objects.create_superuser('dg', email='dg@pollyanasdolce.com', password='test')
        self.jefe_user = User.objects.create_user('jefe', email='jefe@pollyanasdolce.com')
        self.jefe = Empleado.objects.create(codigo='J', nombre='Jefe', departamento='VENTAS',
            nivel_organizacional='JEFATURA', usuario_erp=self.jefe_user)
        self.ventas = Empleado.objects.create(codigo='V', nombre='Ventas sin cuenta',
            departamento='VENTAS', fecha_nacimiento=date(1999,9,17))
        self.prod = Empleado.objects.create(codigo='P', nombre='Producción',
            departamento='PRODUCCION', fecha_nacimiento=date(1998,9,17))
        self.hoy = date(2026,9,17)

    def test_29_febrero_y_cambio_anio(self):
        self.assertEqual(fecha_cumpleanos(date(2000,2,29),2027),date(2027,2,28))
        self.assertEqual(fecha_cumpleanos(date(2000,2,29),2028),date(2028,2,29))
        self.ventas.fecha_nacimiento = date(2001,1,1)
        self.ventas.save()
        rows = eventos_entre(empleados_visibles(self.ch), date(2026,12,29), date(2027,1,4))
        self.assertEqual([(r['empleado'].pk,r['fecha']) for r in rows],[(self.ventas.pk,date(2027,1,1))])

    def test_activo_rrhh_y_alcance_no_dependen_de_cuenta_del_cumpleanero(self):
        self.assertEqual(set(empleados_visibles(self.jefe_user).values_list('pk',flat=True)),
            {self.jefe.pk,self.ventas.pk})
        self.assertIn(self.prod,empleados_visibles(self.ch))
        self.prod.activo = False
        self.prod.save()
        self.assertNotIn(self.prod,empleados_visibles(self.ch))
        outsider = get_user_model().objects.create_user('otro')
        self.assertFalse(puede_ver_cumpleanos(outsider))
        self.assertFalse(empleados_visibles(outsider).exists())
        self.jefe.activo = False
        self.jefe.save()
        self.assertFalse(puede_ver_cumpleanos(self.jefe_user))

    def test_despacho_es_idempotente_privado_y_agrupado(self):
        generar_avisos_cumpleanos(self.hoy)
        generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(Notificacion.objects.count(),3)
        self.assertEqual(len(mail.outbox),3)
        aviso_jefe = next(m for m in mail.outbox if m.to == [self.jefe_user.email])
        self.assertIn('Ventas sin cuenta',aviso_jefe.body)
        self.assertNotIn('Producción',aviso_jefe.body)
        self.assertNotIn('1999',aviso_jefe.body)
        self.assertNotIn('1998',aviso_jefe.body)
        self.assertEqual(AvisoCumpleanos.objects.filter(estado_correo='enviado').count(),3)

    def test_anticipo_semanal_y_sin_historico(self):
        self.ventas.fecha_nacimiento = date(1999,9,20)
        self.ventas.save()
        self.prod.fecha_nacimiento = None
        self.prod.save()
        generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(set(AvisoCumpleanos.objects.values_list('tipo',flat=True)),{'anticipado'})
        generar_avisos_cumpleanos(date(2026,9,21))
        self.assertFalse(AvisoCumpleanos.objects.filter(tipo='semanal').exists())
        self.ventas.fecha_nacimiento = date(1999,9,23)
        self.ventas.save()
        generar_avisos_cumpleanos(date(2026,9,21))
        self.assertEqual(AvisoCumpleanos.objects.filter(tipo='semanal').count(),3)

    def test_correo_fallo_confirmado_recupera_sin_duplicar_erp(self):
        with patch('rrhh.services_cumpleanos.EmailMultiAlternatives.send',return_value=0):
            resultado = generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(resultado['fallidos'],3)
        self.assertEqual(Notificacion.objects.count(),3)
        generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(Notificacion.objects.count(),3)
        self.assertEqual(len(mail.outbox),3)

    def test_error_red_no_reenvia(self):
        with patch('rrhh.services_cumpleanos.EmailMultiAlternatives.send',side_effect=TimeoutError('timeout')):
            generar_avisos_cumpleanos(self.hoy)
        generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(len(mail.outbox),0)
        self.assertEqual(AvisoCumpleanos.objects.filter(estado_correo='incierto').count(),3)

    def test_baja_entre_notificacion_y_correo_se_revalida(self):
        from core.notificaciones import crear_notificacion

        def notificar_y_dar_baja(**kwargs):
            notificacion = crear_notificacion(**kwargs)
            Empleado.objects.filter(pk__in=[self.prod.pk, self.ventas.pk]).update(activo=False)
            return notificacion

        with patch('rrhh.services_cumpleanos.crear_notificacion', side_effect=notificar_y_dar_baja):
            generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(len(mail.outbox), 0)
        self.assertEqual(AvisoCumpleanos.objects.filter(estado_correo='omitido').count(), 3)

    def test_nav_jefatura_visible_sin_abrir_rrhh_y_colaborador_oculto(self):
        from core.navigation import build_nav_groups

        groups = build_nav_groups(self.jefe_user, '/rrhh/cumpleanos/')
        cumple = [item for group in groups for item in group['items'] if item['url'] == '/rrhh/cumpleanos/']
        self.assertEqual(len(cumple), 1)
        outsider = get_user_model().objects.create_user('colaborador')
        groups = build_nav_groups(outsider, '/')
        self.assertFalse(any(item['url'] == '/rrhh/cumpleanos/' for group in groups for item in group['items']))

    def test_baja_antes_de_reintento_no_envia(self):
        with patch('rrhh.services_cumpleanos.EmailMultiAlternatives.send',return_value=0):
            generar_avisos_cumpleanos(self.hoy)
        Empleado.objects.filter(pk__in=[self.prod.pk,self.ventas.pk]).update(activo=False)
        generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(len(mail.outbox),0)
        self.assertEqual(AvisoCumpleanos.objects.filter(estado_correo='omitido').count(),3)

    def test_sin_correo_no_usa_contacto_personal_y_cuentas_compartidas_no_duplican(self):
        self.jefe_user.email = ''
        self.jefe_user.save()
        self.jefe.email = 'personal@example.com'
        self.jefe.save()
        get_user_model().objects.create_superuser('dg2',email=self.dg.email,password='test')
        generar_avisos_cumpleanos(self.hoy)
        self.assertEqual(len(mail.outbox),2)
        self.assertEqual(Notificacion.objects.count(),4)
        self.assertEqual(AvisoCumpleanos.objects.get(usuario=self.jefe_user).estado_correo,'sin_contacto')
        self.assertFalse(any(m.to == ['personal@example.com'] for m in mail.outbox))

    def test_schedule_y_cola(self):
        from django.conf import settings
        schedule = settings.CELERY_BEAT_SCHEDULE['rrhh-cumpleanos-diario']
        self.assertEqual(schedule['task'],'rrhh.tasks.avisar_cumpleanos')
        self.assertEqual(schedule['schedule'].hour,{8})
        self.assertEqual(settings.CELERY_TASK_ROUTES['rrhh.tasks.avisar_cumpleanos']['queue'],'notificaciones')
