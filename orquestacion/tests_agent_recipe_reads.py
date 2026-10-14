"""Recipe discovery preserves ambiguity, permissions and saved-cost provenance."""
from decimal import Decimal
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from api import ai_gateway_business_reads as bridge
from api import ai_gateway_recipe_reads as recipes
from core.models import Sucursal, UserProfile, AuditLog
from maestros.models import Insumo, UnidadMedida
from recetas.models import Receta, LineaReceta, RecetaCostoVersion, RecetaCodigoPointAlias


@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True)
class RecipeReadTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.actor = get_user_model().objects.create_superuser('recipe-reader', password='test')
        cls.branch = Sucursal.objects.create(codigo='RTEST', nombre='Sucursal prueba')
        UserProfile.objects.create(user=cls.actor, sucursal=cls.branch)
        cls.unit = UnidadMedida.objects.create(codigo='rkg', nombre='Kg prueba')
        cls.recipe = Receta.objects.create(nombre='Pastel limón', nombre_normalizado='pastel limon', hash_contenido='recipe-read-1', rendimiento_cantidad=2, rendimiento_unidad=cls.unit)
        cls.other = Receta.objects.create(nombre='Pastel limón chico', nombre_normalizado='pastel limon chico', hash_contenido='recipe-read-2')
        cls.input = Insumo.objects.create(nombre='Harina prueba', unidad_base=cls.unit)
        LineaReceta.objects.create(receta=cls.recipe, insumo=cls.input, insumo_texto='Harina', cantidad=Decimal('1.250000'), unidad=cls.unit)
        LineaReceta.objects.create(receta=cls.recipe, insumo_texto='<script>dato no confiable</script>', cantidad=None, tipo_linea=LineaReceta.TIPO_SUBSECCION, posicion=1)
        RecetaCostoVersion.objects.create(receta=cls.recipe, version_num=1, hash_snapshot='r1', costo_mp=Decimal('12.345678'), costo_total=Decimal('12.345678'))
        RecetaCostoVersion.objects.create(receta=cls.recipe, version_num=2, hash_snapshot='r2', creado_en=timezone.now()+timedelta(days=1), costo_total=999)

    def setUp(self):
        setting = override_settings(AI_AGENT_PILOT_USER_ID=self.actor.pk)
        setting.enable(); self.addCleanup(setting.disable)

    def invoke(self, key, arguments):
        return bridge.invoke(user=self.actor, tool_key=key, arguments=arguments)['result']

    def test_search_candidates_and_truncation_do_not_select_first(self):
        data = self.invoke('erp.search_recipes', {'q':'limon', 'limit':1})['payload']
        self.assertEqual(len(data['items']), 1)
        self.assertTrue(data['truncated'])
        self.assertTrue(data['selection_required'])
        self.assertEqual(self.invoke('erp.search_recipes', {'q':'inexistente'})['status'], 'no_data')

    def test_active_alias_does_not_duplicate_recipe(self):
        RecetaCodigoPointAlias.objects.create(receta=self.recipe, codigo_point='R-ALIAS', codigo_point_normalizado='r-alias')
        data = self.invoke('erp.search_recipes', {'q':'R-ALIAS'})['payload']
        self.assertEqual([r['receta_id'] for r in data['items']], [self.recipe.pk])
        RecetaCodigoPointAlias.objects.update(activo=False)
        self.assertEqual(self.invoke('erp.search_recipes', {'q':'R-ALIAS'})['status'], 'no_data')

    def test_context_preserves_decimal_null_status_and_saved_dates_without_mutation(self):
        before = list(RecetaCostoVersion.objects.values())
        data = self.invoke('erp.get_recipe_context', {'receta_id':self.recipe.pk})['payload']
        self.assertEqual(data['cost_versions'][0]['costo_total'], '12.345678')
        self.assertEqual(len(data['cost_versions']), 1)  # future versions excluded
        self.assertIsNone(data['items'][1]['cantidad'])
        self.assertEqual(data['items'][0]['estado_vinculacion'], LineaReceta.STATUS_REJECTED)
        self.assertFalse(data['freshness']['ingredient_cost_date_available'])
        self.assertIsNotNone(data['freshness']['latest_saved_cost_at'])
        self.assertEqual(list(RecetaCostoVersion.objects.values()), before)
        self.assertTrue(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ').exists())

    def test_missing_cost_is_unknown_not_zero_and_context_is_bounded(self):
        data = self.invoke('erp.get_recipe_context', {'receta_id':self.other.pk})['payload']
        self.assertFalse(data['saved_cost_available'])
        self.assertEqual(data['cost_versions'], [])
        self.assertIsNone(data['freshness']['latest_saved_cost_at'])
        self.assertTrue(self.invoke('erp.get_recipe_context', {'receta_id':self.recipe.pk,'limit':1})['payload']['truncated'])
        self.assertEqual(self.invoke('erp.get_recipe_context', {'receta_id':999999})['status'], 'no_data')

    def test_strict_contract_rejects_sql_actor_and_invalid_types(self):
        for key, args in [('erp.search_recipes', {'q':' '}), ('erp.search_recipes', {'q':'pastel','sql':'SELECT 1'}),
                          ('erp.get_recipe_context', {'receta_id':True}), ('erp.get_recipe_context', {'receta_id':self.recipe.pk,'cost_limit':21}),
                          ('erp.get_recipe_context', {'receta_id':self.recipe.pk,'user_id':self.actor.pk})]:
            with self.subTest(args=args), self.assertRaises(ValidationError): self.invoke(key,args)

    def test_denied_domain_is_neither_discovered_nor_read(self):
        with patch.object(recipes,'can_view_recetas',return_value=False), patch.object(recipes,'get_recipe_context') as read:
            self.assertNotIn('erp.get_recipe_context', bridge.allowed_keys(self.actor))
            with self.assertRaises(PermissionDenied): self.invoke('erp.get_recipe_context', {'receta_id':self.recipe.pk})
            read.assert_not_called()

    def test_permission_revocation_during_read_suppresses_result(self):
        original = recipes.get_recipe_context
        with patch.object(recipes,'can_view_recetas',return_value=True) as allowed:
            def revoke(actor, arguments):
                data = original(actor, arguments)
                allowed.return_value = False
                return data
            with patch.object(recipes,'get_recipe_context',side_effect=revoke), self.assertRaises(PermissionDenied):
                self.invoke('erp.get_recipe_context', {'receta_id':self.recipe.pk})
        self.assertTrue(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ',payload__result_status='denied').exists())
