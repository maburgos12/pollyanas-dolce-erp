from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase


class DashboardLoadingTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser('dashboard-loading', '', 'local-test-only')
        self.client.force_login(self.user)

    @patch('core.views._build_canonical_inventory_dashboard_metrics')
    @patch('core.views._dashboard_materialized_executive_context')
    def test_pwa_dashboard_returns_loading_screen_before_calculations(self, materialized, inventory):
        response = self.client.get('/dashboard/?source=pwa', follow=True)
        self.assertContains(response, 'Cargando datos del Dashboard')
        self.assertContains(response, 'data-dashboard-loading')
        self.assertIn('no-store', response['Cache-Control'])
        materialized.assert_not_called()
        inventory.assert_not_called()

    @patch('reportes.commercial_analytics.commercial_panel_from_request', return_value=None)
    @patch('core.views._dashboard_materialized_executive_context')
    def test_fragment_uses_existing_data_and_templates(self, materialized, commercial):
        materialized.return_value = {
            'dashboard_exec_ready': True,
            'daily_sales_snapshot': {'date': date(2026, 10, 11), 'total_amount': 91234.56},
            'forecast_panel': {'weekly_rows': []}, 'yoy_panel': {'rows': []},
        }
        response = self.client.get('/dashboard/?sales_month=9', HTTP_X_ERP_DASHBOARD='fragment')
        self.assertContains(response, 'data-dashboard-content')
        self.assertContains(response, '91,234.56')
        self.assertContains(response, 'data-dashboard-assets')
        self.assertNotContains(response, '<aside class="sidebar"')
        self.assertIn('no-store', response['Cache-Control'])
        self.assertEqual(commercial.call_args.args[0].GET['sales_month'], '9')
        response = self.client.get('/dashboard/')
        self.assertContains(response, '<aside class="sidebar"')
        self.assertContains(response, '91,234.56')

    def test_loading_and_fragment_require_session_and_dashboard_permission(self):
        self.client.logout()
        for headers in ({}, {'HTTP_X_ERP_DASHBOARD': 'fragment'}):
            self.assertRedirects(self.client.get('/dashboard/?dashboard_loading=1', **headers), '/login/', fetch_redirect_response=False)
        user = get_user_model().objects.create_user('loading-no-access')
        self.client.force_login(user)
        for headers in ({}, {'HTTP_X_ERP_DASHBOARD': 'fragment'}):
            response = self.client.get('/dashboard/?dashboard_loading=1', **headers)
            self.assertEqual(response.status_code, 302)
            self.assertNotIn('dashboard', response['Location'])
