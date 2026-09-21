from datetime import timedelta
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from core.models import Sucursal
from inventario import tests_conteos_units as fixtures
from inventario.conteos_catalog import codigos_habituales
from pos_bridge.models import PointBranch, PointDailySale, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.product_count_units import COUNT_UNIT_KEY, catalog_count_unit


class BranchCountCatalogTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        fixtures.AutomaticCountUnitTests.setUpTestData.__func__(cls)
        cls.seasonal = PointProduct.objects.create(external_id='old-valentine', sku='004493', name='Bollo San Valentín', metadata={COUNT_UNIT_KEY:catalog_count_unit({'Codigo':'004493','Unidad':'PZA'})})
        PointDailySale.objects.create(branch=cls.point_branch, product=cls.seasonal, sale_date=timezone.localdate()-timedelta(days=200), quantity=10)

    def test_default_is_branch_habitual_not_historical_catalog(self):
        self.client.force_login(self.operator)
        response=self.client.get(reverse('operacion:conteos_app:preparar'))
        self.assertContains(response, self.product.name)
        self.assertNotContains(response, self.seasonal.name)
        self.assertTrue(response.context['catalogo'][0]['selected'])
        response=self.client.get(reverse('operacion:conteos_app:preparar'), {'q':'004493'})
        self.assertContains(response, self.seasonal.name)
        self.assertFalse(response.context['catalogo'][0]['selected'])

    def test_latest_completed_inventory_zero_does_not_resurrect_previous_stock(self):
        now=timezone.now()
        for hours, stock in ((2,5),(1,0)):
            job=PointSyncJob.objects.create(job_type='inventory',status='SUCCESS')
            PointInventorySnapshot.objects.create(branch=self.point_branch,product=self.seasonal,sync_job=job,stock=stock,captured_at=now-timedelta(hours=hours))
        self.assertNotIn('004493',codigos_habituales(self.branch))
        fresh=PointProduct.objects.create(external_id='new-stock',sku='NEW',name='Nuevo')
        PointInventorySnapshot.objects.create(branch=self.point_branch,product=fresh,sync_job=job,stock=1,captured_at=now-timedelta(hours=1))
        self.assertIn('NEW',codigos_habituales(self.branch))

    def test_recent_sales_include_only_positive_codes_from_the_selected_branch(self):
        today=timezone.localdate()
        sold=PointProduct.objects.create(external_id='recent-sold',sku='RECENT',name='Vendido')
        zero=PointProduct.objects.create(external_id='recent-zero',sku='ZERO',name='Sin venta')
        other=PointProduct.objects.create(external_id='other-branch',sku='OTHER',name='Otra sucursal')
        other_branch=Sucursal.objects.create(codigo='SALE-OTHER',nombre='Otra sucursal ventas')
        other_point=PointBranch.objects.create(external_id='sale-other',name='Otra sucursal ventas',erp_branch=other_branch)
        PointDailySale.objects.create(branch=self.point_branch,product=sold,sale_date=today-timedelta(days=30),quantity=1)
        PointDailySale.objects.create(branch=self.point_branch,product=zero,sale_date=today,quantity=0)
        PointDailySale.objects.create(branch=other_point,product=other,sale_date=today,quantity=1)
        codes=codigos_habituales(self.branch)
        self.assertIn('RECENT',codes)
        self.assertNotIn('ZERO',codes)
        self.assertNotIn('OTHER',codes)
        self.assertNotIn('004493',codes)

    def test_branch_without_activity_has_no_whole_catalog_fallback(self):
        branch=Sucursal.objects.create(codigo='EMPTY',nombre='Sin movimiento')
        self.assertEqual(codigos_habituales(branch),set())
        self.assertEqual(codigos_habituales(None),set())

    def test_operator_cannot_request_another_branch_catalog(self):
        other=Sucursal.objects.create(codigo='OTHER',nombre='Otra')
        point=PointBranch.objects.create(external_id='other',name='Otra',erp_branch=other)
        PointDailySale.objects.create(branch=point,product=self.seasonal,sale_date=timezone.localdate(),quantity=1)
        self.client.force_login(self.operator)
        response=self.client.get(reverse('operacion:conteos_app:preparar'),{'sucursal':other.pk})
        self.assertNotContains(response,self.seasonal.name)
        self.assertEqual(response.context['catalogo_sucursal'],self.branch)
