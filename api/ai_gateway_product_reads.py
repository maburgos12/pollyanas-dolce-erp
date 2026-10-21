"""Bounded product reads using canonical sales and saved monthly cost sources."""
from decimal import Decimal
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from api.ai_gateway_assets import StrictArguments, StrictDateField, StrictIntegerField, StrictStringField
from core.access import can_view_recetas, can_view_rentabilidad, can_view_reportes
from recetas.models import Receta
from recetas.utils.normalizacion import normalizar_nombre
from reportes.models import ProductoCostoOperativoMensual, ProductoSucursalContribucionMensual


class ProductSalesArguments(StrictArguments):
    start_date = StrictDateField()
    end_date = StrictDateField()
    branch = StrictStringField(required=False, max_length=180)
    q = StrictStringField(required=False, max_length=180)
    limit = StrictIntegerField(required=False, min_value=1, max_value=100, default=20)

    def validate(self, attrs):
        if not 0 <= (attrs['end_date']-attrs['start_date']).days < 366:
            raise ValidationError('El rango debe ser ordenado y abarcar como máximo 366 días.')
        if 'q' in attrs and not normalizar_nombre(attrs['q']):
            raise ValidationError('Indica un nombre o código de producto.')
        return attrs


class ProductMarginArguments(StrictArguments):
    receta_id = StrictIntegerField(min_value=1, max_value=9223372036854775807)
    period = StrictDateField()
    branch = StrictStringField(required=False, max_length=180)
    limit = StrictIntegerField(required=False, min_value=1, max_value=100, default=20)

    def validate_period(self, value):
        if value.day != 1 or value > timezone.localdate().replace(day=1):
            raise ValidationError('Indica el primer día de un mes actual o pasado.')
        return value


SERIALIZERS = {'erp.get_product_sales':ProductSalesArguments,
               'erp.get_product_monthly_margin':ProductMarginArguments}
UNITS = {'erp.get_product_sales':'product_sales_range',
         'erp.get_product_monthly_margin':'saved_recipe_branch_month_contribution'}
RELATIONSHIPS = {
    'erp.get_product_sales':['Venta por producto, fecha y sucursal; recipe_id declarado por la fuente permite consultar receta e ingredientes. No vincular por parecido del nombre.'],
    'erp.get_product_monthly_margin':['Receta identificada → costo mensual de fabricación → contribución por sucursal del mismo mes. No es utilidad neta ni precio vigente.'],
}
SOURCES = {'erp.get_product_sales':'ventas.services.sales_read_service.get_sales_range_grouped',
           'erp.get_product_monthly_margin':'reportes.ProductoCostoOperativoMensual; reportes.ProductoSucursalContribucionMensual'}


def can_read_margin(actor):
    return can_view_recetas(actor) and can_view_rentabilidad(actor)


def product_sales(_actor, arguments, *, branch_ids):
    from ventas.services.sales_read_service import get_sales_range_grouped
    data = get_sales_range_grouped(start_date=arguments['start_date'], end_date=arguments['end_date'],
        dimension='product', sucursales=branch_ids, coverage_policy='prefer_complete')
    query = normalizar_nombre(arguments.get('q',''))
    rows = [row for row in data['rows'] if not query or query in normalizar_nombre(row.get('product_name',''))
            or query == normalizar_nombre(row.get('product_code') or '')]
    items=[]
    for row in rows[:arguments['limit']]:
        item={name:row.get(name) for name in ('product_name','product_code','point_product_id','recipe_id','recipe_name','total_sales','total_quantity')}
        quantity, sales = item['total_quantity'], item['total_sales']
        item['average_sale_price'] = sales/quantity if sales is not None and quantity is not None and quantity > 0 else None
        items.append(item)
    return {'status':'ok' if rows else 'no_data','sources':[SOURCES['erp.get_product_sales']], 'payload':{
        'items':items,'returned':len(items),'truncated':len(rows)>arguments['limit'],
        **{name:data.get(name) for name in ('source','source_detail','coverage_accepted','coverage_reason')},
        'limitations':['Orden por venta registrada; lista acotada, no es total empresa.',
                       'Precio promedio vendido del periodo, no precio vigente ni promedio por ticket.',
                       'No encontrar filas no demuestra venta cero. Identidad ausente no autoriza vincular por nombre.']}}


def product_monthly_margin(_actor, arguments, *, branch_ids):
    period=arguments['period']
    recipe=Receta.objects.filter(pk=arguments['receta_id']).first()
    if recipe is None:
        return {'status':'no_data','sources':['recetas.Receta'],'payload':{'recipe':None,'items':[]}}
    cost=ProductoCostoOperativoMensual.objects.filter(receta=recipe,periodo=period).first()
    meta=cost.metadata if cost and isinstance(cost.metadata,dict) else {}
    warnings=[]
    fields={'materials':'costo_mp_unit','labor':'mano_obra_prod_unit','overhead':'indirecto_prod_unit','packaging':'empaque_prod_unit'}
    if not cost or cost.costo_fabricacion_unit <= 0:
        warnings.append('Sin costo de fabricación verificable; no equivale a costo cero.')
    elif any(getattr(cost,field) <= 0 for field in fields.values()):
        warnings.append('Hay componentes sin importe positivo; cobertura de costo sin confirmar.')
    if not meta.get('cost_source') or meta.get('cost_source_period') != period.isoformat():
        warnings.append('La fuente del costo no acredita el mismo mes.')
    if meta.get('guardrail_applied'):
        warnings.append('El costo tiene un ajuste de control; revisar antes de interpretar el margen.')
    rows=ProductoSucursalContribucionMensual.objects.filter(receta=recipe,periodo=period).select_related('sucursal')
    if branch_ids is not None: rows=rows.filter(sucursal_id__in=branch_ids)
    rows=list(rows.order_by('sucursal__codigo','pk')[:arguments['limit']+1])
    items=[]
    for row in rows[:arguments['limit']]:
        issues=list(warnings)
        if cost and row.costo_producto_unit != cost.costo_fabricacion_unit:
            issues.append('Costo aplicado y costo mensual difieren; revisar el cálculo guardado.')
        if cost and row.actualizado_en < cost.actualizado_en:
            issues.append('El costo se actualizó después de la contribución.')
        if row.venta_total <= 0 or row.unidades_vendidas <= 0:
            issues.append('Sin venta y unidades positivas para interpretar el margen.')
        items.append({'branch_code':row.sucursal.codigo,'branch_name':row.sucursal.nombre,
            'sales':row.venta_total,'quantity':row.unidades_vendidas,'average_sale_price':row.asp,
            'product_cost':row.costo_producto_total,'commercial_expense':row.gasto_comercial_total,
            'contribution':None if issues else row.contribucion_total,
            'margin_percent':None if issues else row.margen_contribucion_pct*Decimal('100'),
            'warnings':issues,'actualizado_en':row.actualizado_en})
    return {'status':'ok' if rows else 'no_data','sources':['recetas.Receta',*SOURCES['erp.get_product_monthly_margin'].split('; ')],
        'payload':{'recipe':{'receta_id':recipe.pk,'nombre':recipe.nombre,'codigo_point':recipe.codigo_point},
            'period':period,'items':items,'returned':len(items),'truncated':len(rows)>arguments['limit'],
            'cost':({**{name:getattr(cost,field) for name,field in fields.items()},
                'manufacturing':cost.costo_fabricacion_unit,'actualizado_en':cost.actualizado_en,
                'source':meta.get('cost_source'),'source_period':meta.get('cost_source_period')} if cost else None),
            'net_profit_available':False,
            'limitations':['Cálculo mensual guardado, no recalculado. La fecha de consulta no acredita actualización del cierre.',
                'No es utilidad neta: no certifica cobertura completa de gastos, mermas, impuestos ni otros componentes.',
                'Costo unitario empresarial; ventas y contribución limitadas a las sucursales autorizadas.',
                'Ausencia de registros no equivale a cero; margen no disponible cuando el costo no es verificable.']}}


def definitions():
    from api.ai_gateway_services import AIToolDefinition
    specs=(('erp.get_product_sales','Ventas por producto','Consulta ventas, cantidades y precio promedio vendido por producto en un rango y sucursal. Devuelve identidad de receta si la fuente la declara para continuar con ingredientes y costos.',can_view_reportes,product_sales,'sales'),
           ('erp.get_product_monthly_margin','Contribución mensual de producto','Consulta costo de fabricación y contribución guardada por receta y sucursal para un mes. Identifica primero la receta; period es el primer día del mes. No recalcula, no es utilidad neta; costos incompletos bloquean el margen.',can_read_margin,product_monthly_margin,'costing'))
    return {key:AIToolDefinition(key=key,name=name,description=description,operation_type='read',data_domain=domain,
        branch_scoped=True,requires_approval=False,access_check=check,handler=handler,argument_schema=SERIALIZERS[key].argument_schema())
        for key,name,description,check,handler,domain in specs}
