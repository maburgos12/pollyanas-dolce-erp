"""Business source and authorization contracts; no provider calls."""
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.test import TestCase, override_settings
from rest_framework.exceptions import ValidationError
from core.models import Sucursal, UserProfile, UserModuleAccess
from recetas.models import Receta
from reportes.models import ProductoCostoOperativoMensual, ProductoSucursalContribucionMensual
from api import ai_gateway_business_reads as bridge


@override_settings(AI_AGENT_READ_ENABLED=True)
class ProductReadsTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_superuser('product-reader', password='test')
        self.branch = Sucursal.objects.create(codigo='PM1', nombre='Matriz prueba')
        self.other = Sucursal.objects.create(codigo='PM2', nombre='Otra prueba')
        UserProfile.objects.create(user=self.actor, sucursal=self.branch)
        self.recipe = Receta.objects.create(nombre='Pastel prueba', hash_contenido='margin-test')
        self.period = date(2026, 9, 1)
        override = override_settings(AI_AGENT_PILOT_USER_ID=self.actor.pk)
        override.enable(); self.addCleanup(override.disable)

    def invoke(self, key, args):
        self.assertIn(key, bridge.allowed_keys(self.actor))
        return bridge.invoke(user=self.actor, tool_key=key, arguments=args)['result']

    def costs(self):
        return ProductoCostoOperativoMensual.objects.create(periodo=self.period, receta=self.recipe,
            costo_mp_unit=30, mano_obra_prod_unit=10, indirecto_prod_unit=5, empaque_prod_unit=5,
            costo_fabricacion_unit=50, metadata={'cost_source':'MONTHLY_HISTORICAL', 'cost_source_period':'2026-09-01'})

    def contribution(self, branch=None):
        return ProductoSucursalContribucionMensual.objects.create(periodo=self.period, receta=self.recipe,
            sucursal=branch or self.branch, unidades_vendidas=10, venta_total=1000, asp=100,
            costo_producto_unit=50,costo_producto_total=500,gasto_comercial_total=100,
            contribucion_total=400,margen_contribucion_pct=Decimal('0.4'))

    def margin(self, **extra):
        return self.invoke('erp.get_product_monthly_margin', {'receta_id':self.recipe.pk, 'period':'2026-09-01',**extra})

    def test_margin_preserves_source_and_converts_fraction_to_percent(self):
        self.costs(); self.contribution()
        result = self.margin(branch='PM1')
        row = result['payload']['items'][0]
        self.assertEqual(Decimal(row['margin_percent']), Decimal('40'))
        self.assertEqual(Decimal(row['contribution']), Decimal('400'))
        self.assertFalse(result['payload']['net_profit_available'])
        self.assertIn('actualizado_en', row)
        self.assertEqual(result['payload']['cost']['source_period'], '2026-09-01')

    def test_missing_or_guarded_cost_does_not_become_high_margin(self):
        self.contribution()
        self.assertIsNone(self.margin()['payload']['items'][0]['margin_percent'])
        cost = self.costs(); cost.metadata['guardrail_applied']=True; cost.save()
        self.assertIsNone(self.margin()['payload']['items'][0]['margin_percent'])

    def test_unknown_month_is_not_zero_and_invalid_period_rejected(self):
        self.assertEqual(self.margin()['status'], 'no_data')
        with self.assertRaises(ValidationError): self.margin(period='2026-09-02')

    def test_branch_scope_and_permission_are_rechecked(self):
        self.costs(); self.contribution(); self.contribution(self.other)
        UserProfile.objects.filter(user=self.actor).update(modo_captura_sucursal=True)
        rows = self.margin()['payload']['items']
        self.assertEqual([r['branch_code'] for r in rows], ['PM1'])
        with self.assertRaises(PermissionDenied): self.margin(branch='PM2')
        self.actor.is_superuser=False; self.actor.save()
        UserModuleAccess.objects.create(user=self.actor,module='recetas',access='view')
        self.assertNotIn('erp.get_product_monthly_margin',bridge.allowed_keys(self.actor))

    def test_product_sales_use_canonical_source_and_report_truncation(self):
        grouped={'source':'authoritative','source_detail':'verified','coverage_accepted':False,
            'coverage_reason':'partial','rows':[{'product_name':'Pastel A','recipe_id':self.recipe.pk,
            'product_code':'01','point_product_id':None,'recipe_name':'Pastel prueba',
            'total_sales':Decimal('123.45'),'total_quantity':Decimal('3')},
            {'product_name':'Pastel B','recipe_id':None,'product_code':None,'point_product_id':None,
             'recipe_name':'','total_sales':Decimal('5'),'total_quantity':Decimal('1')}]}
        with patch('ventas.services.sales_read_service.get_sales_range_grouped',return_value=grouped) as source:
            result=self.invoke('erp.get_product_sales',{'start_date':'2026-09-01','end_date':'2026-09-30','branch':'PM1','limit':1})
        self.assertEqual(source.call_args.kwargs['sucursales'],[self.branch.pk])
        self.assertTrue(result['payload']['truncated'])
        self.assertFalse(result['payload']['coverage_accepted'])
        self.assertEqual(result['payload']['items'][0]['total_sales'],'123.45')
        self.assertEqual(result['payload']['items'][0]['recipe_id'],self.recipe.pk)

    def test_read_revocation_blocks_materialized_result(self):
        from api import ai_gateway_product_reads as products
        original=products.product_monthly_margin
        def revoked(*args,**kwargs):
            result=original(*args,**kwargs)
            get_user_model().objects.filter(pk=self.actor.pk).update(is_active=False)
            return result
        with patch.object(products,'product_monthly_margin',side_effect=revoked), self.assertRaises(PermissionDenied):
            self.margin()

    def test_stale_cost_and_missing_component_withhold_margin(self):
        cost=self.costs();self.contribution()
        cost.mano_obra_prod_unit=0;cost.save()
        row=self.margin()['payload']['items'][0]
        self.assertIsNone(row['contribution'])
        self.assertTrue(row['warnings'])

    def test_product_sales_without_rows_are_unknown(self):
        with patch('ventas.services.sales_read_service.get_sales_range_grouped',return_value={'rows':[], 'coverage_accepted':False}):
            result=self.invoke('erp.get_product_sales',{'start_date':'2026-09-01','end_date':'2026-09-30'})
        self.assertEqual(result['status'],'no_data')
        self.assertEqual(result['payload']['items'],[])

    def test_canonical_fact_sales_keep_identity_and_ignore_other_branch(self):
        from reportes.models import FactVentaDiaria
        for branch,amount in ((self.branch,Decimal('123.45')),(self.other,Decimal('900'))):
            FactVentaDiaria.objects.create(fecha=self.period,sucursal=branch,receta=self.recipe,
                producto_clave='PM-SKU',producto_nombre='Pastel prueba',cantidad=3,venta_total=amount,
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE)
        result=self.invoke('erp.get_product_sales',{'start_date':'2026-09-01','end_date':'2026-09-01','branch':'PM1'})
        row=result['payload']['items'][0]
        self.assertEqual(Decimal(row['total_sales']),Decimal('123.45'))
        self.assertEqual(row['recipe_id'],self.recipe.pk)
        self.assertEqual(Decimal(row['average_sale_price']),Decimal('41.15'))
