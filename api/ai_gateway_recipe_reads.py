"""Read recipe declarations and saved costs without recalculating or saving them."""
from django.db.models import Q
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from api.ai_gateway_assets import StrictArguments, StrictIntegerField, StrictStringField
from core.access import can_view_recetas
from recetas.models import Receta, RecetaCostoVersion
from recetas.utils.normalizacion import normalizar_nombre


class RecipeSearchArguments(StrictArguments):
    q = StrictStringField(max_length=180)
    limit = StrictIntegerField(required=False, min_value=1, max_value=25, default=8)

    def validate_q(self, value):
        if not normalizar_nombre(value):
            raise ValidationError('Indica un nombre o código de receta.')
        return value.strip()


class RecipeContextArguments(StrictArguments):
    receta_id = StrictIntegerField(min_value=1, max_value=9223372036854775807)
    limit = StrictIntegerField(required=False, min_value=1, max_value=100, default=50)
    cost_limit = StrictIntegerField(required=False, min_value=1, max_value=20, default=5)


SERIALIZERS = {
    'erp.search_recipes': RecipeSearchArguments,
    'erp.get_recipe_context': RecipeContextArguments,
}
UNITS = {'erp.search_recipes': 'recipe_candidate', 'erp.get_recipe_context': 'recipe_and_saved_cost_versions'}
RELATIONSHIPS = {
    'erp.search_recipes': ['Receta por nombre o código Point, incluidos alias activos declarados. Coincidencias no prueban equivalencia de productos.'],
    'erp.get_recipe_context': ['Receta → líneas → insumo y unidad; receta → versiones guardadas de costo.'],
}
SOURCE = 'Recetas, líneas y versiones de costo guardadas; no implica costo vigente ni margen de venta.'


def _identity(recipe):
    return {'receta_id': recipe.pk, 'nombre': recipe.nombre, 'codigo_point': recipe.codigo_point,
            'tipo': recipe.tipo, 'modo_costeo': recipe.modo_costeo,
            'familia': recipe.familia, 'categoria': recipe.categoria,
            'rendimiento_cantidad': recipe.rendimiento_cantidad,
            'rendimiento_unidad': recipe.rendimiento_unidad.codigo if recipe.rendimiento_unidad else None}


def search_recipes(_actor, arguments):
    query = arguments['q']
    normalized = normalizar_nombre(query)
    criteria = Q(nombre__icontains=query) | Q(nombre_normalizado__icontains=normalized) | Q(codigo_point__iexact=query)
    criteria |= Q(codigos_point_aliases__codigo_point__iexact=query, codigos_point_aliases__activo=True)
    rows = list(Receta.objects.select_related('rendimiento_unidad').filter(criteria).distinct().order_by('nombre', 'pk')[:arguments['limit']+1])
    truncated = len(rows) > arguments['limit']
    return {'status': 'ok' if rows else 'no_data', 'sources': ['recetas.Receta', 'recetas.RecetaCodigoPointAlias'],
            'payload': {'items': [_identity(row) for row in rows[:arguments['limit']]],
                        'returned': min(len(rows), arguments['limit']), 'truncated': truncated,
                        'selection_required': len(rows) > 1,
                        'limitations': ['Resultados candidatos; seleccionar la receta y presentación antes de atribuir costos a un producto.']}}


def get_recipe_context(_actor, arguments):
    recipe = Receta.objects.select_related('rendimiento_unidad').filter(pk=arguments['receta_id']).first()
    if recipe is None:
        return {'status': 'no_data', 'sources': ['recetas.Receta'], 'payload': {'recipe': None, 'items': [], 'truncated': False}}
    rows = list(recipe.lineas.select_related('insumo', 'unidad').order_by('posicion', 'pk')[:arguments['limit']+1])
    costs = list(RecetaCostoVersion.objects.filter(receta=recipe, creado_en__lte=timezone.now()).order_by('-version_num', '-pk')[:arguments['cost_limit']+1])
    items = [{'linea_id': row.pk, 'posicion': row.posicion, 'tipo_linea': row.tipo_linea, 'etapa': row.etapa,
              'insumo_id': row.insumo_id, 'insumo': row.insumo.nombre if row.insumo else None,
              'texto_original': row.insumo_texto, 'cantidad': row.cantidad,
              'unidad': row.unidad.codigo if row.unidad else None, 'unidad_original': row.unidad_texto,
              'estado_vinculacion': row.match_status,
              'costo_unitario_snapshot': row.costo_unitario_snapshot} for row in rows[:arguments['limit']]]
    fields = ('version_num', 'costo_mp', 'costo_mo', 'costo_indirecto', 'costo_total', 'lote_referencia',
              'rendimiento_cantidad', 'rendimiento_unidad', 'costo_por_unidad_rendimiento', 'fuente', 'creado_en')
    return {'status': 'ok', 'sources': ['recetas.Receta', 'recetas.LineaReceta', 'recetas.RecetaCostoVersion'],
            'payload': {'recipe': _identity(recipe), 'items': items, 'returned': len(items),
                        'truncated': len(rows) > arguments['limit'],
                        'cost_versions': [{name: getattr(row, name) for name in fields} for row in costs[:arguments['cost_limit']]],
                        'costs_truncated': len(costs) > arguments['cost_limit'], 'currency': 'MXN',
                        'saved_cost_available': bool(costs),
                        'freshness': {'read_live': True, 'latest_saved_cost_at': costs[0].creado_en if costs else None,
                                      'ingredient_cost_date_available': False},
                        'limitations': ['Versiones guardadas: no acreditan costo vigente ni costos completos de operación.',
                                        'Ingredientes actuales y versiones históricas no constituyen una fotografía del mismo momento.',
                                        'Subsecciones no deben sumarse otra vez al componente principal.',
                                        'Vínculos pendientes o rechazados no prueban equivalencia; cantidades y costos desconocidos permanecen null.',
                                        'No incluye precio de venta ni cálculo de margen.']}}


def definitions():
    from api.ai_gateway_services import AIToolDefinition
    descriptions = (
        ('erp.search_recipes', 'Buscar recetas y productos elaborados', 'Busca recetas por nombre o código y devuelve candidatos con rendimiento. Obtén su contexto antes de analizar ingredientes o costos.', search_recipes),
        ('erp.get_recipe_context', 'Ingredientes y costos guardados de receta', 'Consulta una receta identificada: ingredientes, cantidades, unidades, estado de vinculación y versiones guardadas de costo con fecha. No recalcula ni cambia la receta. No son precios ni márgenes.', get_recipe_context),
    )
    return {key: AIToolDefinition(key=key, name=name, description=description, operation_type='read',
                                 data_domain='costing', branch_scoped=False, requires_approval=False,
                                 access_check=can_view_recetas, handler=handler,
                                 argument_schema=SERIALIZERS[key].argument_schema())
            for key, name, description, handler in descriptions}
