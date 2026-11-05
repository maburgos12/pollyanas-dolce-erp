from django.contrib.auth import get_user_model
from django.test import TestCase
from core.models import Sucursal
from rrhh.models import Empleado
from rrhh.services_personnel_normalization import build_personnel_normalization_plan


class PersonalFuenteUnicaTests(TestCase):
    def setUp(self):
        self.client.force_login(get_user_model().objects.create_superuser('fuente', password='test-only'))
        self.sucursal = Sucursal.objects.create(codigo='FUENTE', nombre='Sucursal prueba')

    def test_asignacion_por_departamento_no_oculta_cajas_ni_incluye_hornos(self):
        caja = Empleado.objects.create(nombre='Caja', area='PRODUCCION', departamento='VENTAS',
            puesto_operativo='CAJAS', sucursal_ref=self.sucursal)
        Empleado.objects.create(nombre='Hornos', area='VENTAS', departamento='PRODUCCION',
            puesto_operativo='HORNOS', sucursal_ref=self.sucursal)
        Empleado.objects.create(nombre='Baja', departamento='VENTAS', activo=False, sucursal_ref=self.sucursal)
        response = self.client.get('/api/rrhh/empleados/', {'departamento':'ventas','sucursal_id':self.sucursal.pk})
        self.assertEqual(response.status_code, 200)
        self.assertEqual([r['id'] for r in response.json()['results']], [caja.pk])
        self.assertEqual(response.json()['results'][0]['puesto_presentacion'], 'Cajas')
        self.assertEqual(self.client.get('/api/rrhh/empleados/', {'area':'VENTAS'}).json()['count'], 1)
        caja.sucursal_ref = None
        caja.save(update_fields=['sucursal_ref'])
        self.assertEqual(self.client.get('/api/rrhh/empleados/sin-asignar/',
            {'departamento':'VENTAS'}).json()['results'][0]['id'], caja.pk)

    def test_catalogo_no_exige_sobrescribir_departamento_amplio(self):
        e = Empleado.objects.create(nombre='Caja', area='VENTAS', departamento='VENTAS',
            puesto_operativo='cajas', sucursal_ref=self.sucursal, sucursal=self.sucursal.nombre)
        proposals = build_personnel_normalization_plan()['proposals']
        self.assertNotIn(f'employee.position_to_area:{e.pk}', [p['key'] for p in proposals])
        e.refresh_from_db()
        self.assertEqual(e.area, 'VENTAS')
        self.assertEqual(e.puesto_operativo, 'CAJAS')

    def test_organizacion_muestra_jefe_existente_sin_reactivar_nomina(self):
        director = Empleado.objects.create(nombre='Director', nivel_organizacional='DIRECCION', activo=False)
        Empleado.objects.create(nombre='Jefa', nivel_organizacional='JEFATURA', jefe_directo=director, puesto_operativo='CAJAS')
        from django.urls import reverse
        response = self.client.get(reverse('rrhh:rrhh_organizacion'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Director')
        self.assertEqual(response.context['equipo_jefatura'][0].funcion_presentacion, 'Cajas')
        director.refresh_from_db()
        self.assertFalse(director.activo)

    def test_editar_conserva_direccion_existente_sin_admitir_jefaturas_inactivas(self):
        from rrhh.views import _resolver_jefe_directo_desde_post
        from django.core.exceptions import ValidationError
        director = Empleado.objects.create(nombre='Director', nivel_organizacional='DIRECCION', activo=False)
        jefa = Empleado.objects.create(nombre='Jefa', departamento='VENTAS', jefe_directo=director)
        self.assertEqual(_resolver_jefe_directo_desde_post({'jefe_directo':str(director.pk)},
            {'departamento':'VENTAS'}, jefa), director.pk)
        baja = Empleado.objects.create(nombre='Baja', nivel_organizacional='JEFATURA', activo=False)
        with self.assertRaises(ValidationError):
            _resolver_jefe_directo_desde_post({'jefe_directo':str(baja.pk)}, {'departamento':'VENTAS'}, jefa)

    def test_organizacion_reutiliza_puesto_especifico_sin_inventar_funcion(self):
        jefa = Empleado.objects.create(nombre='Jefa con puesto', puesto='Jefe de Ventas',
            nivel_organizacional='JEFATURA', departamento='VENTAS')
        vacio = Empleado.objects.create(nombre='Sin información', puesto='', puesto_operativo='')
        response = self.client.get('/rrhh/organizacion/')
        rows = {e.pk: e for e in response.context['empleados']}
        self.assertEqual(rows[jefa.pk].funcion_presentacion, 'Jefe de Ventas')
        self.assertEqual(rows[vacio.pk].funcion_presentacion, 'Puesto pendiente')
        jefa.refresh_from_db()
        self.assertEqual(jefa.puesto_operativo, '')
