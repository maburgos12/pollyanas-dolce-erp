from datetime import timedelta
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase, Client
from django.urls import reverse
from core.navigation import build_nav_groups
from rrhh.models import PeriodoEmpleadoMes, EvaluacionEmpleadoMes, Empleado
from rrhh import tests_empleado_mes as backend
from rrhh import services_empleado_mes as s
from rrhh.models_empleado_mes import CRITERIOS


class EmpleadoMesHTTPTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        backend.EmpleadoMesTests.setUpTestData.__func__(cls)

    def setUp(self):
        self.p = s.abrir_periodo(self.rh, self.hoy.replace(day=1), self.hoy)
        self.url = reverse('rrhh:empleado_mes_detalle', args=[self.p.pk])

    def test_vistas_y_navegacion_sin_acceso_rrhh_general(self):
        for user in (self.rh, self.dg, self.jefes['PRODUCCION'], self.jefes['ADMINISTRACION']):
            self.client.force_login(user)
            response = self.client.get(reverse('rrhh:empleado_mes'))
            self.assertContains(response, 'Empleado del mes')
            self.assertNotContains(response, 'Empleado del mes CEDIS')
            self.assertEqual(self.client.get(self.url).status_code, 200)
        ventas = get_user_model().objects.create_user('ventas_em_mes')
        ventas.groups.add(Group.objects.get_or_create(name='Ventas')[0])
        Empleado.objects.create(nombre='Jefa ventas', codigo='EMVT', usuario_erp=ventas, departamento='VENTAS', nivel_organizacional='JEFATURA')
        self.client.force_login(ventas)
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertNotIn('/rrhh/empleado-del-mes/', str(build_nav_groups(ventas, '/')))
        self.assertIn('/rrhh/empleado-del-mes/', str(build_nav_groups(self.jefes['PRODUCCION'], '/')))

    def test_ciclo_formularios_y_aislamiento(self):
        enviados = []
        for dep in s.DEPARTAMENTOS:
            self.client.force_login(self.jefes[dep])
            for empleado in self.equipos[dep][:2]:
                response = self.client.post(self.url, {'accion':'postular', 'empleado':empleado.pk})
                self.assertEqual(response.status_code, 302)
                ev = EvaluacionEmpleadoMes.objects.get(periodo=self.p, empleado=empleado)
                url = reverse('rrhh:empleado_mes_evaluacion', args=[ev.pk])
                self.assertContains(self.client.get(url), 'Guardar borrador')
                datos = {'accion':'enviar','version':ev.version,'reconocimiento':'no','recomienda':'si','justificacion':'Buen desempeño comprobado', **{f'nota_{k}': '5' for k in CRITERIOS}}
                self.assertEqual(self.client.post(url, datos).status_code, 302)
                self.assertContains(self.client.get(url), '70 / 70 puntos')
                enviados.append(ev)
        self.client.force_login(self.jefes['ADMINISTRACION'])
        url_ajeno = reverse('rrhh:empleado_mes_evaluacion', args=[enviados[0].pk])
        self.assertEqual(self.client.get(url_ajeno).status_code,404)
        self.assertEqual(self.client.post(url_ajeno, {'accion':'guardar'}).status_code,404)
        self.client.force_login(self.dg)
        self.assertEqual(self.client.post(self.url, {'accion':'elegir'}).status_code,403)
        self.client.force_login(self.rh)
        self.assertEqual(self.client.post(self.url, {'accion':'elegir','evaluacion':enviados[0].pk,'motivo':'Desempate por evidencia de colaboración','version':0}).status_code,302)
        self.p.refresh_from_db()
        self.assertContains(self.client.get(self.url),'GANADOR SELECCIONADO')
        self.assertEqual(self.client.post(self.url, {'accion':'premio','tipo':'vales','detalle':'Aprobado por RH','importe':'500.00','version':self.p.version}).status_code,302)
        self.p.refresh_from_db()
        self.assertEqual(self.client.post(self.url, {'accion':'entregar','fecha':self.hoy.isoformat(),'responsable':'RH','comprobante':'Acuse local ficticio','version':self.p.version}).status_code,302)
        self.assertContains(self.client.get(self.url),'Entrega registrada')

    def test_errores_conservan_captura_y_csrf(self):
        e = s.postular(self.jefes['PRODUCCION'],self.p.pk,self.equipos['PRODUCCION'][0].pk)
        url = reverse('rrhh:empleado_mes_evaluacion',args=[e.pk])
        self.client.force_login(self.jefes['PRODUCCION'])
        r = self.client.post(url,{'accion':'enviar','version':0,'nota_metas':4,'justificacion':'Conservar este texto','reconocimiento':'no','recomienda':'si'})
        self.assertContains(r,'Conservar este texto')
        self.assertContains(r,'Completa los 14 criterios')
        e.refresh_from_db()
        self.assertEqual(e.estado,'borrador')
        csrf = Client(enforce_csrf_checks=True)
        csrf.force_login(self.jefes['PRODUCCION'])
        response = csrf.post(url,{'accion':'guardar'})
        self.assertEqual(response.status_code,302)
        self.assertIn('/login/',response.url)
        e.refresh_from_db()
        self.assertEqual(e.version,0)

    def test_plazo_vencido_conserva_texto_en_formulario(self):
        e = s.postular(self.jefes['PRODUCCION'], self.p.pk, self.equipos['PRODUCCION'][0].pk)
        PeriodoEmpleadoMes.objects.filter(pk=self.p.pk).update(fecha_limite=self.hoy-timedelta(days=1))
        self.client.force_login(self.jefes['PRODUCCION'])
        response = self.client.post(reverse('rrhh:empleado_mes_evaluacion',args=[e.pk]),{'accion':'guardar','version':0,'nota_metas':4,'justificacion':'Conservar fuera de plazo','reconocimiento':'no'})
        self.assertContains(response, 'Conservar fuera de plazo')
        self.assertContains(response, 'El plazo terminó')
        self.assertNotContains(response, 'value="enviar"')
        e.refresh_from_db()
        self.assertEqual(e.version,0)
