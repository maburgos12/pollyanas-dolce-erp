from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test.utils import CaptureQueriesContext

from pos_bridge.models import PointProduct
from rest_framework.test import APITestCase
from pos_bridge.tests import test_live_sale_price_api as price_tests


class CustomerCatalogApiTests(APITestCase):
    catalog_url = '/api/pos-bridge/products/customer-catalog/'

    def setUp(self):
        self.client.force_authenticate(get_user_model().objects.create_user(username="catalog-reader"))
        for target in ("pos_bridge.config.load_point_bridge_settings",
                       "pos_bridge.services.point_http_client.PointHttpSessionClient.login"):
            mocked = patch(target)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.detail = {}
        mocked = patch("pos_bridge.services.point_http_client.PointHttpSessionClient.get_product_detail",
                       side_effect=lambda product_id: self.detail)
        self.get_detail = mocked.start()
        self.addCleanup(mocked.stop)
        self.rows = [{'PK_Producto': 901, 'Codigo': 'NEW901', 'Nombre': 'Producto nuevo',
                      'Categoria': 'Galletas', 'Unidad': 'Pza', 'Cost_U': 20, 'metadata': {'private': True}}]
        self.products_patch = patch('pos_bridge.services.point_http_client.PointHttpSessionClient.get_products',
                                    side_effect=lambda **kwargs: self.rows)
        self.get_products = self.products_patch.start()
        self.addCleanup(self.products_patch.stop)

    def read(self, **params):
        return self.client.get("/api/pos-bridge/products/sale-price/", params)

    def catalog(self, **params):
        return self.client.get(self.catalog_url, params)

    def test_new_final_product_public_projection_without_master_or_writes(self):
        with CaptureQueriesContext(connection) as queries:
            response = self.catalog(q='nuevo')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['status'], 'VERIFIED')
        self.assertTrue(response.data['complete'])
        self.assertTrue(response.data['is_fresh'])
        self.assertEqual(response.data['products'], [{
            'point_product_id': '901', 'product_code': 'NEW901', 'product_name': 'Producto nuevo',
            'category': 'Galletas', 'presentation': None,
        }])
        self.get_products.assert_called_once_with(text_art='nuevo', activo=True)
        self.assertFalse(PointProduct.objects.filter(sku='NEW901').exists())
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')) for q in queries))
        self.assertNotIn('Cost_U', str(response.data))
        self.get_detail.assert_not_called()

    def test_explicit_inactive_insumo_service_or_complement_excluded(self):
        for key, value in [('Activo', False), ('IsInsumo', True), ('Servicio', True), ('Complemento', True)]:
            with self.subTest(key=key):
                self.rows = [{**self.rows[0], key: value}]
                self.assertEqual(self.catalog().data['products'], [])
                self.rows = [{k: v for k, v in self.rows[0].items() if k != key}]

    def test_duplicate_codes_and_conflicting_ids_fail_closed_but_same_names_remain(self):
        row = self.rows[0]
        self.rows = [row, {**row, 'PK_Producto': 902}]
        self.assertEqual(self.catalog().status_code, 502)
        self.rows = [row, {**row, 'Codigo': 'NEW902'}]
        self.assertEqual(self.catalog().status_code, 502)
        self.rows = [row, {**row, 'PK_Producto': 902, 'Codigo': 'NEW902'}]
        self.assertEqual(len(self.catalog().data['products']), 2)

    def test_point_cap_is_reported_partial(self):
        self.rows = [{**self.rows[0], 'PK_Producto': i+1, 'Codigo': str(i)} for i in range(150)]
        self.assertFalse(self.catalog().data['complete'])

    def test_bad_input_identity_and_source_failure_are_unknown(self):
        self.assertEqual(self.catalog(q='x'*121).status_code, 400)
        self.get_products.assert_not_called()
        original = self.rows[0]
        for key, value in [('PK_Producto', True), ('Codigo', ''), ('Nombre', ''), ('PK_Producto', '01')]:
            self.rows = [{**original, key: value}]
            response = self.catalog()
            self.assertEqual(response.status_code, 502)
            self.assertEqual(response.data['status'], 'UNKNOWN')
            self.assertFalse(response.data['is_fresh'])
            self.assertEqual(response.data['products'], [])
        self.get_products.side_effect = TimeoutError('secret')
        response = self.catalog()
        self.assertEqual(response.status_code, 503)
        self.assertNotIn('secret', str(response.data))

    def test_catalog_lock_and_authentication(self):
        with patch('pos_bridge.services.point_account_session_lock.point_account_session_lock', price_tests.unavailable_lock):
            self.assertEqual(self.catalog().status_code, 503)
        self.get_products.assert_not_called()
        self.client.force_authenticate(None)
        self.assertEqual(self.catalog().status_code, 401)

    def test_new_exact_sku_live_price_without_master(self):
        self.detail = {'PK_Producto': 901, 'Codigo': 'NEW901', 'Nombre': 'Producto nuevo',
                       'Activo': True, 'Precio_default': 90}
        response = self.read(product_code='NEW901')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['amount'], '90')
        self.get_detail.assert_called_once_with('901')
        self.assertFalse(PointProduct.objects.filter(sku='NEW901').exists())

    def test_new_sku_detail_cannot_reclassify_as_internal(self):
        self.detail = {'PK_Producto': 901, 'Codigo': 'NEW901', 'Nombre': 'Producto nuevo',
                       'Activo': True, 'Precio_default': 90, 'IsInsumo': True}
        self.assertEqual(self.read(product_code='NEW901').status_code, 502)

    def test_catalog_deadline_restored_and_expired_source_not_verified(self):
        from pos_bridge.services.catalog_recipe_execution import DEADLINE
        import time
        token = DEADLINE.set(time.monotonic() - 1)
        try:
            response = self.catalog()
            self.assertEqual(response.status_code, 503)
            self.assertFalse(response.data['is_fresh'])
            self.get_products.assert_not_called()
        finally:
            DEADLINE.reset(token)

    def test_restricted_existing_token_can_read_only_public_catalog_get(self):
        from django.contrib.auth.models import Group
        from rest_framework.authtoken.models import Token
        from integraciones.sales_read_policy import SALES_GROUP
        user = get_user_model().objects.create_user(username='maya-catalog')
        user.groups.add(Group.objects.get_or_create(name=SALES_GROUP)[0])
        token = Token.objects.create(user=user)
        self.client.force_authenticate(None)
        self.client.credentials(HTTP_AUTHORIZATION=f'Token {token.key}')
        self.assertEqual(self.catalog().status_code, 200)
        self.assertEqual(self.client.post(self.catalog_url, {}).status_code, 403)
        self.assertEqual(self.client.get('/api/pos-bridge/products/').status_code, 403)
        self.assertEqual(self.client.get('/api/pos-bridge/products/1/recipe/').status_code, 403)

    def test_partial_sku_search_cannot_prove_unique_live_price(self):
        self.rows = [{**self.rows[0], 'PK_Producto': i+1, 'Codigo': 'NEW901' if i == 0 else str(i)}
                     for i in range(150)]
        response = self.read(product_code='NEW901')
        self.assertEqual(response.status_code, 409)
        self.get_detail.assert_not_called()

    def test_public_identity_field_bounds_match_consumer_contract(self):
        original = self.rows[0]
        for field, value in [('PK_Producto', '9'*31), ('Nombre', 'n'*201), ('Categoria', 'c'*121)]:
            with self.subTest(field=field):
                self.rows = [{**original, field: value}]
                self.assertEqual(self.catalog().status_code, 502)
