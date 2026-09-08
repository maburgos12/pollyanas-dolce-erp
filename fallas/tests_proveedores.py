from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from core.models import AuditLog, UserModuleAccess
from mantenimiento.models import ProveedorServicio
from .serializers import CambioEstatusSerializer


class ProveedoresFallasTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username='gestor')
        UserModuleAccess.objects.create(user=self.user, module='fallas.gestion', access='manage')
        self.client.force_login(self.user)
        self.url = reverse('fallas_api:proveedores-servicio')

    def test_lista_solo_servicios_activos(self):
        activo = ProveedorServicio.objects.create(nombre='Electricista')
        ProveedorServicio.objects.create(nombre='Inactivo', activo=False)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual([p['id'] for p in response.json()], [activo.pk])

    def test_alta_campos_y_auditoria(self):
        response = self.client.post(self.url, {'nombre': '  Taller   Eléctrico ', 'contacto': 'Ana',
            'telefono': '6871234567', 'especialidad': 'Electricidad', 'notas': 'Atención con cita'})
        self.assertEqual(response.status_code, 201)
        proveedor = ProveedorServicio.objects.get(pk=response.json()['proveedor']['id'])
        self.assertEqual(proveedor.nombre, 'Taller Eléctrico')
        self.assertEqual(proveedor.contacto, 'Ana')
        self.assertEqual(proveedor.telefono, '6871234567')
        self.assertEqual(proveedor.especialidad, 'Electricidad')
        self.assertEqual(proveedor.notas, 'Atención con cita')
        self.assertTrue(AuditLog.objects.filter(user=self.user, model='mantenimiento.ProveedorServicio',
            object_id=str(proveedor.pk), action='CREATE').exists())

    def test_duplicado_normalizado_no_modifica_datos_existentes(self):
        proveedor = ProveedorServicio.objects.create(nombre='Taller Eléctrico', telefono='123')
        for nombre in ['taller electrico', ' TALLER   ELÉCTRICO ', 'Taller Eléctrico']:
            response = self.client.post(self.url, {'nombre': nombre, 'telefono': '999'})
            self.assertEqual(response.status_code, 409)
        proveedor.refresh_from_db()
        self.assertEqual(proveedor.telefono, '123')
        self.assertEqual(ProveedorServicio.objects.count(), 1)

    def test_inactivo_no_se_reactiva_al_darlo_de_alta(self):
        proveedor = ProveedorServicio.objects.create(nombre='Técnico', activo=False)
        response = self.client.post(self.url, {'nombre': 'Tecnico'})
        self.assertEqual(response.status_code, 409)
        proveedor.refresh_from_db()
        self.assertFalse(proveedor.activo)

    def test_validacion_campos(self):
        for data in [{'nombre': ''}, {'nombre': '   '}, {'nombre': 'a' * 201},
                     {'nombre': 'Taller', 'telefono': '1' * 31}]:
            self.assertEqual(self.client.post(self.url, data).status_code, 400)
        self.assertFalse(ProveedorServicio.objects.exists())

    def test_permiso_gestion_obligatorio(self):
        UserModuleAccess.objects.filter(user=self.user).delete()
        for method in [self.client.get, self.client.post]:
            self.assertEqual(method(self.url, {'nombre': 'Taller'}).status_code, 403)
        self.assertFalse(ProveedorServicio.objects.exists())

    def test_id_resuelve_nombre_canonico(self):
        proveedor = ProveedorServicio.objects.create(nombre='Taller Eléctrico')
        serializer = CambioEstatusSerializer(data={'proveedor_servicio_id': proveedor.pk, 'proveedor_servicio': 'Incompleto'})
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data['proveedor_servicio'], proveedor.nombre)
        self.assertNotIn('proveedor_servicio_id', serializer.validated_data)

    def test_id_inexistente_o_inactivo_rechazado(self):
        proveedor = ProveedorServicio.objects.create(nombre='Inactivo', activo=False)
        for pk in [proveedor.pk, proveedor.pk + 100, 0]:
            serializer = CambioEstatusSerializer(data={'proveedor_servicio_id': pk})
            self.assertFalse(serializer.is_valid())

    def test_cliente_historico_conserva_contrato(self):
        serializer = CambioEstatusSerializer(data={'proveedor_servicio': 'Técnico histórico'})
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data['proveedor_servicio'], 'Técnico histórico')
