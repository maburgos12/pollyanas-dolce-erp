from datetime import date
from unittest.mock import patch
from django.test import TestCase, RequestFactory
from django.contrib.auth import get_user_model
from django.template.loader import render_to_string
from reportes.commercial_analytics import commercial_panel_from_request
from ventas import models

class ContextRenderingTests(TestCase):
    def test_bi_notes_are_fresh_outside_numeric_cache_and_escape(self):
        self.assertTrue(hasattr(models, 'ContextoComercial'), 'Falta bitácora comercial')
        actor = get_user_model().objects.create_user('render-dg', is_superuser=True)
        from ventas.services.operational_context import register_contexts
        entry = dict(clave='SAFE', fecha='2026-11-21', tipo='cierre_empresa', titulo='<script>bad</script>', descripcion='Declaración DG', evidencia=['Fuente <img>'])
        request = RequestFactory().get('/', {'sales_year':2026, 'sales_month':11, 'sales_branch':9})
        cached = {'branch_options':[], 'current':123}
        with patch('reportes.commercial_analytics.settings.RUNNING_TESTS', False), patch('reportes.commercial_analytics.get_or_set_versioned_cache', return_value=cached):
            self.assertEqual(commercial_panel_from_request(request)['operational_contexts'], [])
            preview=register_contexts(entries=[entry], actor_id=actor.pk)
            register_contexts(entries=[entry], actor_id=actor.pk, apply=True, digest=preview['digest'])
            panel=commercial_panel_from_request(request)
            self.assertEqual(panel['current'], 123)
            self.assertEqual(len(panel['operational_contexts']), 1)
            self.assertNotIn('operational_contexts', cached)
        html=render_to_string('reportes/partials/operational_context.html', {'operational_contexts':panel['operational_contexts']})
        self.assertIn('&lt;script&gt;', html)
        self.assertNotIn('<script>', html)
        self.assertIn('empresa', html)
        self.assertIn('SAFE', html)
        for month in [10,12]:
            with patch('reportes.commercial_analytics._build_panel', return_value={'branch_options':[]}):
                self.assertEqual(commercial_panel_from_request(RequestFactory().get('/', {'sales_year':2026,'sales_month':month}))['operational_contexts'], [])

    def test_sales_documentary_month_is_independent_and_preserves_params(self):
        from reportes.operational_context import sales_context_from_request
        request = RequestFactory().get('/reportes/ventas/?months=12&tag=a&tag=b&contexto_mes=2026-11')
        context = sales_context_from_request(request, visible_date=date(2026,10,15))
        self.assertEqual(context['operational_context_period'], '2026-11')
        self.assertEqual(context['operational_context_params'], [('months','12'),('tag','a'),('tag','b')])
        invalid=sales_context_from_request(RequestFactory().get('/', {'contexto_mes':'2026-13'}),visible_date=date(2026,10,15))
        self.assertEqual(invalid['operational_context_period'], '2026-10')
        self.assertIn('inválido', invalid['operational_context_error'])

    def test_sales_and_bi_views_render_shared_context_once_with_existing_permissions(self):
        from contextlib import ExitStack
        from reportes import views
        from django.core.exceptions import PermissionDenied
        actor=get_user_model().objects.create_user('view-dg',is_superuser=True)
        from ventas.services.operational_context import register_contexts
        entries=[dict(clave='VIEW-NOTE',fecha='2026-11-21',tipo='cierre_empresa',titulo='Referencia visible',descripcion='Alcance empresa',evidencia=['Declaración DG'])]
        preview=register_contexts(entries=entries,actor_id=actor.pk)
        register_contexts(entries=entries,actor_id=actor.pk,apply=True,digest=preview['digest'])
        request=RequestFactory().get('/reportes/ventas/',{'contexto_mes':'2026-11'})
        request.user=actor
        with ExitStack() as stack:
            for name in ['_bi_daily_sales_snapshot','build_sales_forecast_panel','build_closed_yoy_panel','_ventas_historicas_bi_summary','_sales_refresh_status']:
                stack.enter_context(patch('reportes.views.'+name,return_value={}))
            for name in ['_bi_branch_weekday_comparisons','_bi_product_weekday_comparisons']:
                stack.enter_context(patch('reportes.views.'+name,return_value=[]))
            response=views.ventas(request)
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.content.decode().count('id="contexto-comercial"'),1)
        self.assertIn('VIEW-NOTE',response.content.decode())
        request=RequestFactory().get('/reportes/bi/',{'sales_year':2026,'sales_month':11})
        request.user=actor
        response=views.bi(request)
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.content.decode().count('id="contexto-comercial"'),1)
        self.assertIn('VIEW-NOTE',response.content.decode())
        actor.is_superuser=False
        actor.save()
        with self.assertRaises(PermissionDenied):
            views.bi(request)

    def test_bi_detail_full_year_shows_both_general_notes_once(self):
        from reportes import views
        from ventas.services.operational_context import register_contexts
        actor=get_user_model().objects.create_user('detail-dg',is_superuser=True)
        entries=[dict(clave='YEAR-FRIDAY',fecha='2026-11-20',tipo='afectacion_parcial',titulo='Viernes declarado',descripcion='Sucursales exactas desconocidas',evidencia=['DG']),dict(clave='YEAR-SATURDAY',fecha='2026-11-21',tipo='cierre_empresa',titulo='Sábado declarado',descripcion='Actividad cancelada por seguridad',evidencia=['DG']),dict(clave='OTHER-YEAR',fecha='2025-11-21',tipo='cierre_empresa',titulo='Otra referencia',descripcion='Fuera del año',evidencia=['DG'])]
        preview=register_contexts(entries=entries,actor_id=actor.pk)
        register_contexts(entries=entries,actor_id=actor.pk,apply=True,digest=preview['digest'])
        request=RequestFactory().get('/reportes/bi/',{'detalle':1,'sales_year':2026,'sales_month':0,'sales_branch':999})
        request.user=actor
        panel=commercial_panel_from_request(request)
        self.assertEqual([note['clave'] for note in panel['operational_contexts']],['YEAR-FRIDAY','YEAR-SATURDAY'])
        self.assertTrue(all(note['alcance']=='empresa_general_informativo' for note in panel['operational_contexts']))
        executive={key:{} for key in ['forecast_panel','profitability_panel','branch_contribution_panel','branch_pricing_panel','budget_operating_panel','production_sales_panel','central_flow_panel','inventory_ledger_panel']}
        executive['yoy_panel']={'current_year':2026,'prev_year':2025,'prev2_year':2024}
        with patch('reportes.views.build_executive_bi_panels',return_value=executive), patch('reportes.views._bi_closed_weekday_comparison',return_value=None), patch('reportes.views._bi_daily_sales_snapshot',return_value={}):
            response=views.bi(request)
        self.assertEqual(response.status_code,200)
        html=response.content.decode()
        section=html.split('id="contexto-comercial"',1)[1].split('</section>',1)[0]
        self.assertEqual(html.count('id="contexto-comercial"'),1)
        self.assertEqual(section.count('YEAR-FRIDAY'),1)
        self.assertEqual(section.count('YEAR-SATURDAY'),1)
        self.assertNotIn('OTHER-YEAR',html)
        self.assertIn('Contexto general de empresa',section)
        self.assertIn('incluso al filtrar una sucursal',section)
        self.assertIn('id="detalle-operativo"',html)
