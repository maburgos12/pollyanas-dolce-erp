from datetime import date, timedelta
from uuid import uuid4
from django.urls import reverse
from django.test import TestCase
from core.models import UserProfile, Sucursal
from inventario import tests_conteos_views as fixtures
from inventario.models_conteos import AccesoConteoSucursal, ConteoSucursal


class ConteoSesionTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        fixtures.ConteoViewsTests.setUpTestData.__func__(cls)

    url = fixtures.ConteoViewsTests.url

    def body(self, **overrides):
        data={'fecha':(date.today()+timedelta(days=1)).isoformat(),'titulo':'Mi cierre',
              'request_id':str(uuid4()),'articulos':[f'p{self.product.pk}'],
              f'unidad_p{self.product.pk}':'PZA',f'fuente_p{self.product.pk}':'Catálogo verificado'}
        data.update(overrides)
        return data

    def test_app_preparation_has_session_identity_without_selectors(self):
        self.client.force_login(self.operator)
        response=self.client.get(reverse('operacion:conteos_app:preparar'))
        self.assertEqual(response.status_code,200)
        self.assertContains(response,self.branch.nombre)
        self.assertNotContains(response,'name="sucursal"')
        self.assertNotContains(response,'name="responsable"')
        self.assertNotContains(response,self.other.username)

    def test_operator_can_create_only_self_count_from_session(self):
        self.client.force_login(self.operator)
        response=self.client.post(reverse('operacion:conteos_app:preparar'),self.body())
        self.assertEqual(response.status_code,302)
        count=ConteoSucursal.objects.get(titulo='Mi cierre')
        self.assertEqual(count.sucursal_id,self.branch.pk)
        self.assertEqual(count.responsable_id,self.operator.pk)
        self.assertEqual(count.creado_por_id,self.operator.pk)

    def test_forged_identity_is_rejected_without_writes(self):
        self.client.force_login(self.operator)
        for forged in [{'sucursal':'999999'},{'responsable':self.other.pk}]:
            response=self.client.post(reverse('operacion:conteos_app:preparar'),self.body(**forged))
            self.assertEqual(response.status_code,403)
        self.assertFalse(ConteoSucursal.objects.filter(titulo='Mi cierre').exists())

    def test_app_admin_without_branch_cannot_select_other_branch(self):
        self.client.force_login(self.admin)
        response=self.client.get(reverse('operacion:conteos_app:lista'))
        self.assertContains(response,'Sin sucursal asignada')
        self.assertNotContains(response,self.count.titulo)
        self.assertEqual(self.client.post(reverse('operacion:conteos_app:preparar'),self.body(sucursal=self.branch.pk,responsable=self.operator.pk)).status_code,403)
        self.assertEqual(self.client.get(self.url()).status_code,404)

    def test_profile_change_revokes_mobile_access_even_with_cross_branch_grant(self):
        branch=Sucursal.objects.create(codigo='CT-OTHER',nombre='Otra sucursal')
        UserProfile.objects.filter(user=self.operator).update(sucursal=branch)
        AccesoConteoSucursal.objects.create(user=self.operator,sucursal=self.branch,activo=True,capturar=True,revisar=True)
        self.client.force_login(self.operator)
        for name in ['detalle','exportar']:
            self.assertEqual(self.client.get(self.url(name)).status_code,404)
        self.assertEqual(self.client.post(self.url('accion'),{'action':'guardar','version':1,'request_id':uuid4()}).status_code,404)
        response=self.client.get(reverse('operacion:conteos_app:lista'),{'sucursal':self.branch.pk})
        self.assertNotContains(response,self.count.titulo)

    def test_service_rechecks_session_identity(self):
        from inventario.services_conteos import preparar_conteo, ConteoError
        for actor, branch, responsible in [(self.operator,self.branch,self.other),(self.admin,self.branch,self.operator)]:
            with self.assertRaises(ConteoError):
                preparar_conteo(actor=actor,sucursal=branch,responsable=responsible,desde_app=True,
                    fecha=date.today(),titulo='Rechazado',items=[],request_id=uuid4())
        self.assertFalse(ConteoSucursal.objects.filter(titulo='Rechazado').exists())

    def test_async_forgery_returns_error_toast(self):
        self.client.force_login(self.operator)
        response=self.client.post(reverse('operacion:conteos_app:preparar'),self.body(responsable=self.other.pk),HTTP_X_REQUESTED_WITH='XMLHttpRequest')
        self.assertEqual(response.status_code,403)
        self.assertFalse(response.json()['ok'])
        self.assertEqual(response.json()['toast']['type'],'error')

    def test_profile_removal_blocks_existing_count(self):
        self.client.force_login(self.operator)
        UserProfile.objects.filter(user=self.operator).update(sucursal=None)
        self.assertEqual(self.client.get(self.url()).status_code,404)
        response=self.client.post(reverse('operacion:conteos_app:preparar'),self.body(),HTTP_X_REQUESTED_WITH='XMLHttpRequest')
        self.assertEqual(response.status_code,403)
        self.assertFalse(response.json()['ok'])
