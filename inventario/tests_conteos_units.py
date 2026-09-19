from datetime import date, timedelta
from uuid import uuid4

from django.test import TestCase
from django.urls import reverse

from inventario import tests_conteos_views as fixtures
from inventario.services_conteos import preparar_conteo, ConteoError
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointProduct
from pos_bridge.services.product_count_units import COUNT_UNIT_KEY, catalog_count_unit


class AutomaticCountUnitTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        fixtures.ConteoViewsTests.setUpTestData.__func__(cls)
        from pos_bridge.models import PointBranch, PointDailySale
        from django.utils import timezone
        cls.point_branch = PointBranch.objects.create(external_id='unit-branch', name='Prueba', erp_branch=cls.branch)
        PointDailySale.objects.create(branch=cls.point_branch, product=cls.product, sale_date=timezone.localdate(), quantity=1)

    def prepare(self, item):
        return preparar_conteo(actor=self.operator, sucursal=self.branch, responsable=self.operator,
                              desde_app=True, fecha=date.today()+timedelta(days=1), titulo='Solo cantidad',
                              items=[item], request_id=uuid4())

    def test_server_uses_official_unit_despite_forged_operator_fields(self):
        count = self.prepare({'producto_id':self.product.pk, 'unidad':'kg', 'fuente_unidad':'Inventado'})
        line = count.lineas.get()
        self.assertEqual(line.unidad, 'PZA')
        self.assertEqual(line.fuente_unidad, 'Unidad del catálogo oficial Point')
        self.assertEqual(count.eventos.get().payload['articulos'][0]['unidad'], 'PZA')

    def test_unitless_or_wrong_identity_cannot_be_prepared_even_with_manual_text(self):
        for metadata in ({}, {COUNT_UNIT_KEY:catalog_count_unit({'Codigo':'OTRO','Unidad':'PZA'})}):
            PointProduct.objects.filter(pk=self.product.pk).update(metadata=metadata)
            with self.assertRaisesMessage(ConteoError, 'falta la unidad del catálogo'):
                self.prepare({'producto_id':self.product.pk, 'unidad':'PZA','fuente_unidad':'Conteo físico'})

    def test_insumo_uses_its_base_unit_without_operator_fields(self):
        for code in ('kg', 'lt'):
            unit, _ = UnidadMedida.objects.get_or_create(codigo=code)
            insumo = Insumo.objects.create(nombre=f'Insumo {code}', codigo_point=f'UNIT-{code}', unidad_base=unit)
            count = self.prepare({'insumo_id':insumo.pk})
            self.assertEqual(count.lineas.get().unidad, code)

    def test_preparation_exposes_no_manual_unit_or_provenance_inputs(self):
        self.client.force_login(self.operator)
        response = self.client.get(reverse('operacion:conteos_app:preparar'))
        self.assertContains(response, 'PZA')
        self.assertNotContains(response, 'name="unidad_')
        self.assertNotContains(response, 'name="fuente_')
        self.assertNotContains(response, 'Fuente de la unidad')

    def test_operator_prepares_compact_identity_only_and_captures_decimal_in_litres(self):
        self.product.metadata = {COUNT_UNIT_KEY:catalog_count_unit({'Codigo':self.product.sku,'Unidad':'LT'})}
        self.product.save(update_fields=['metadata'])
        self.client.force_login(self.operator)
        import json
        response = self.client.post(reverse('operacion:conteos_app:preparar'), {
            'fecha':(date.today()+timedelta(days=1)).isoformat(), 'titulo':'Litros',
            'request_id':uuid4(), 'articulos_json':json.dumps([{'producto_id':self.product.pk}])})
        self.assertEqual(response.status_code,302)
        from inventario.models_conteos import ConteoSucursal
        from inventario.services_conteos import ejecutar_accion
        count = ConteoSucursal.objects.get(titulo='Litros')
        ejecutar_accion(conteo_id=count.pk,actor=self.operator,action='iniciar',version=1,request_id=uuid4(),payload={})
        line = count.lineas.get()
        ejecutar_accion(conteo_id=count.pk,actor=self.operator,action='guardar',version=2,request_id=uuid4(),
                        payload={'lecturas':{str(line.pk):{'cantidad':'2.5'}}})
        self.assertEqual(str(line.lecturas.get().cantidad),'2.500000')
        page = self.client.get(response['Location'])
        self.assertContains(page, 'Cantidad de Producto conteo en LT')
        self.assertContains(page, 'value="2.5"')
        self.assertNotContains(page, 'value="2.500000"')

    def test_inventory_refresh_preserves_unit_evidence(self):
        from pos_bridge.services.sync_service import PointSyncService
        # Inventory and sales do not carry catalog units; their refresh must
        # not erase the separately verified catalog evidence.
        service = PointSyncService.__new__(PointSyncService)
        service._upsert_product({'external_id':self.product.external_id,'sku':self.product.sku,
                                 'name':self.product.name,'category':'Prueba','metadata':{'inventory':True}})
        self.product.refresh_from_db()
        self.assertEqual(self.prepare({'producto_id':self.product.pk}).lineas.get().unidad, 'PZA')

    def test_official_sales_refreshes_preserve_counting_unit(self):
        from pos_bridge.services.official_sales_backfill_service import OfficialSalesBackfillService
        from pos_bridge.services.sales_pipeline.rebuild_service import PointSalesRebuildService
        from pos_bridge.services.product_count_units import product_count_unit
        for klass, method in ((OfficialSalesBackfillService,'_resolve_product'),
                              (PointSalesRebuildService,'_resolve_point_product')):
            getattr(klass.__new__(klass), method)(sku=self.product.sku,name=self.product.name,category='Prueba')
            self.product.refresh_from_db()
            self.assertEqual(product_count_unit(self.product)[0], 'PZA')
