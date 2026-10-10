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

    def test_daily_scope_does_not_use_old_sales_or_positive_stock_alone(self):
        PointDailySale.objects.create(branch=self.point_branch, product=self.seasonal,
                                     sale_date=timezone.localdate()-timedelta(days=10), quantity=1)
        job=PointSyncJob.objects.create(job_type='inventory',status='SUCCESS')
        PointInventorySnapshot.objects.create(branch=self.point_branch,product=self.seasonal,
                                              sync_job=job,stock=5,captured_at=timezone.now())
        self.assertNotIn(self.seasonal.sku,codigos_habituales(self.branch,diario=True))
        self.assertIn(self.seasonal.sku,codigos_habituales(self.branch))

    def test_daily_bread_counts_received_input_not_sale_presentations(self):
        from maestros.models import Insumo, UnidadMedida
        from recetas.models import Receta, LineaReceta
        from pos_bridge.models import PointTransferLine
        unit=UnidadMedida.objects.create(codigo='pza',nombre='Pieza')
        bread=Insumo.objects.create(nombre='PAN DE MUERTO HORNEADO',codigo_point='PMH028',
                                   tipo_item=Insumo.TIPO_INTERNO,unidad_base=unit)
        PointTransferLine.objects.create(origin_branch=self.point_branch,destination_branch=self.point_branch,
            erp_destination_branch=self.branch,registered_at=timezone.now(),received_at=timezone.now(),
            transfer_external_id='bread',detail_external_id='bread-1',source_hash='bread',
            item_code='PMH028',item_name=bread.nombre,is_insumo=True,is_received=True,received_quantity=8)
        for code,name in [('0124','Pan de Muerto'),('02PANMUERTOL','Pan de Muerto Lotus')]:
            recipe=Receta.objects.create(nombre=name,codigo_point=code,tipo=Receta.TIPO_PRODUCTO_FINAL,hash_contenido=code)
            LineaReceta.objects.create(receta=recipe,insumo=bread,insumo_texto=bread.nombre,cantidad=1,unidad=unit)
            product=PointProduct.objects.create(external_id=code,sku=code,name=name,metadata={COUNT_UNIT_KEY:catalog_count_unit({'Codigo':code,'Unidad':'PZA'})})
            PointDailySale.objects.create(branch=self.point_branch,product=product,sale_date=timezone.localdate(),quantity=1)
        self.client.force_login(self.operator)
        response=self.client.get(reverse('operacion:conteos_app:preparar'))
        codes={row['codigo'] for row in response.context['catalogo']}
        self.assertIn('PMH028',codes)
        self.assertNotIn('0124',codes)
        self.assertNotIn('02PANMUERTOL',codes)
        self.assertNotContains(response,'value="8"')
        response=self.client.get(reverse('operacion:conteos_app:preparar'),{'q':'0124','tipo':'diario'})
        self.assertNotIn('0124',{row['codigo'] for row in response.context['catalogo']})
        response=self.client.get(reverse('operacion:conteos_app:preparar'),{'q':'muerto','tipo':'diario'})
        self.assertEqual({row['codigo'] for row in response.context['catalogo']},{'PMH028'})

    def test_daily_excludes_monthly_resale_but_search_keeps_it_available(self):
        from pos_bridge.models import PointProductCategory
        PointProductCategory.objects.create(codigo_point=self.product.sku,nombre=self.product.name,category='REVENTA')
        self.client.force_login(self.operator)
        response=self.client.get(reverse('operacion:conteos_app:preparar'))
        self.assertFalse(response.context['catalogo'])
        response=self.client.get(reverse('operacion:conteos_app:preparar'),{'tipo':'producto'})
        self.assertIn(self.product.sku,{row['codigo'] for row in response.context['catalogo']})
        response=self.client.get(reverse('operacion:conteos_app:preparar'),{'tipo':'diario','q':self.product.sku})
        self.assertIn(self.product.sku,{row['codigo'] for row in response.context['catalogo']})

    def test_new_slice_conversion_is_suggested_without_a_sale(self):
        from pos_bridge.models import PointConversionLine
        PointConversionLine.objects.create(branch=self.point_branch,erp_branch=self.branch,
            movement_external_id='slice',source_hash='slice',movement_at=timezone.now(),
            item_name='Rebanada nueva',item_code='NEW-SLICE',quantity=6)
        self.assertIn('NEW-SLICE',codigos_habituales(self.branch,diario=True))

    def test_daily_excludes_unclassified_accessories_and_bottled_drinks(self):
        from inventario.views_conteos import _catalogo
        for code,name,category in [('CND00095','VELA ESPAGUETI COLORES','Alegría'),
                                   ('0174','24 Velas en Caja','Alegría'),
                                   ('0170','Pirotecnia Alegría Ch','Alegría'),
                                   ('04524','Tarjeta Cumpleaños','Otros postres'),
                                   ('0235','Coca Cola 450ml','Coca-cola'),
                                   ('GLOW2','Glow 2','Glow'),
                                   ('GRANMARK','ESPAGUETI ROSA PERLADO','Granmark'),
                                   ('BOOK','Recetario Doña Cuca','Otros postres'),
                                   ('DELIVERY','Servicio Domicilio 2','Otros postres'),
                                   ('ADDON','EXTRA 100','Otros postres'),
                                   ('CREAM','Litro crema','Vasos Preparados Grande')]:
            product=PointProduct.objects.create(external_id=code,sku=code,name=name,category=category,
                metadata={COUNT_UNIT_KEY:catalog_count_unit({'Codigo':code,'Unidad':'PZA'})})
            PointDailySale.objects.create(branch=self.point_branch,product=product,sale_date=timezone.localdate(),quantity=1)
            self.assertNotIn(code,{row['codigo'] for row in _catalogo('diario','',self.branch)})
            self.assertIn(code,{row['codigo'] for row in _catalogo('producto','',self.branch)})
            self.assertIn(code,{row['codigo'] for row in _catalogo('diario',code,self.branch)})

    def test_operator_cannot_request_another_branch_catalog(self):
        other=Sucursal.objects.create(codigo='OTHER',nombre='Otra')
        point=PointBranch.objects.create(external_id='other',name='Otra',erp_branch=other)
        PointDailySale.objects.create(branch=point,product=self.seasonal,sale_date=timezone.localdate(),quantity=1)
        self.client.force_login(self.operator)
        response=self.client.get(reverse('operacion:conteos_app:preparar'),{'sucursal':other.pk})
        self.assertNotContains(response,self.seasonal.name)
        self.assertEqual(response.context['catalogo_sucursal'],self.branch)
