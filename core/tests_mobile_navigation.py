from unittest.mock import patch
from urllib.parse import quote

from django.contrib.auth import get_user_model
from django.test import TestCase


class MobileNavigationTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser('mobile-check', 'mobile@example.invalid', 'local-test-only')
        self.client.force_login(self.user)

    @patch('core.views._build_canonical_inventory_dashboard_metrics')
    @patch('core.views._dashboard_materialized_executive_context')
    def test_icon_restores_menu_page_without_building_dashboard(self, materialized, inventory):
        self.client.cookies[f'erp_resume_{self.user.pk}'] = quote('/rrhh/gafetes/?page=2&q=private-search', safe='')
        response = self.client.get('/dashboard/?source=pwa')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'], '/rrhh/gafetes/?page=2#erp-resume')
        materialized.assert_not_called()
        inventory.assert_not_called()

    def test_icon_rejects_foreign_action_and_other_user_routes(self):
        for target in ('https://example.invalid/', '//example.invalid/', '/logout/', '/notificaciones/marcar-todas/', '/rrhh/gafetes/../permisos/', 'https://[invalid'):
            with self.subTest(target=target):
                self.client.cookies[f'erp_resume_{self.user.pk}'] = quote(target, safe='')
                response = self.client.get('/dashboard/?source=pwa')
                self.assertEqual(response['Location'], '/dashboard/?dashboard_loading=1')
        self.client.cookies.pop(f'erp_resume_{self.user.pk}')
        self.client.cookies['erp_resume_999999'] = quote('/rrhh/gafetes/', safe='')
        self.assertEqual(self.client.get('/dashboard/?source=pwa')['Location'], '/dashboard/?dashboard_loading=1')

    def test_logout_clears_navigation_cookie(self):
        response = self.client.get('/logout/')
        self.assertEqual(response.cookies[f'erp_resume_{self.user.pk}']['max-age'], 0)

    @patch('core.views.get_dashboard_sales_dataset', return_value={'daily_sales_snapshot': {'date': None}})
    @patch('core.views.get_materialized_dashboard_full_payload', return_value=None)
    @patch('core.views._sales_refresh_status', return_value={'button_disabled': False, 'status_title': 'Actualizado'})
    def test_cut_poll_returns_fragments_without_building_full_dashboard(self, status, payload, snapshot):
        response = self.client.get('/dashboard/?cut_refresh=1')
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()['busy'])
        self.assertIn('data-cut-fragment="sales"', response.json()['html'])
        self.assertIn('data-cut-fragment="tickets"', response.json()['html'])
        self.assertIn('no-store', response['Cache-Control'])
        self.assertNotIn('value="/dashboard/?cut_refresh=1"', response.json()['html'])
        payload.assert_not_called()

    def test_launch_and_cut_poll_require_session(self):
        self.client.logout()
        for path in ('/dashboard/?source=pwa', '/dashboard/?cut_refresh=1'):
            self.assertEqual(self.client.get(path).status_code, 302)

    @patch('core.views._can_open_dashboard', return_value=True)
    def test_cut_poll_respects_management_permission(self, can_open):
        user = get_user_model().objects.create_user('view-only')
        self.client.force_login(user)
        self.assertEqual(self.client.get('/dashboard/?cut_refresh=1').status_code, 403)

    @patch('core.views.get_dashboard_sales_dataset', return_value={'daily_sales_snapshot': {'date': None}})
    @patch('core.views._sales_refresh_status', return_value={'button_disabled': False, 'last_status': 'ERROR', 'status_message': 'Falló la actualización'})
    def test_failed_refresh_is_not_reported_as_success(self, status, sales):
        payload = self.client.get('/dashboard/?cut_refresh=1').json()
        self.assertEqual(payload['toast']['type'], 'error')
        self.assertNotIn('Corte actualizado.', payload['html'])
