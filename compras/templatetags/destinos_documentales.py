from django import template
from django.urls import reverse
from compras.services_destinos_documentales import actor_actual, destinos_autorizados, vinculos_visibles

register = template.Library()


def _estado_peticion(context, user):
    """Una instantánea por petición y actor; no sobrevive cambios de sesión."""
    if not getattr(user, 'is_authenticated', False) or not getattr(user, 'pk', None):
        return None
    request = context.get('request')
    if request is not None:
        if not hasattr(request, '_destinos_documentales_por_usuario'):
            request._destinos_documentales_por_usuario = {}
        cache = request._destinos_documentales_por_usuario
    else:
        # render_context conserva su capa durante los {% with %} de cada fila.
        cache = context.render_context.setdefault('destinos_documentales_por_usuario', {})
    if user.pk not in cache:
        cache[user.pk] = {'actor': actor_actual(user)}
    return cache[user.pk]


@register.simple_tag(takes_context=True)
def consulta_compras_destino(context, user, tipo, destino_id):
    estado = _estado_peticion(context, user)
    if estado is None:
        return ''
    if 'visibles' not in estado:
        # ponytail: O(n) vínculos autorizados por request; si crece el histórico,
        # consultar sólo los IDs de destino presentes en la pantalla.
        estado['visibles'] = {
            (v.tipo, v.destino_original_id) for v in vinculos_visibles(estado['actor'])
        }
    if (tipo, destino_id) in estado['visibles']:
        return reverse('compras:compras_del_destino', args=[tipo, destino_id])
    return ''


@register.simple_tag(takes_context=True)
def puede_consultar_destinos(context, user):
    estado = _estado_peticion(context, user)
    if estado is None:
        return False
    if 'puede_consultar' not in estado:
        actor = estado['actor']
        estado['puede_consultar'] = (
            destinos_autorizados(actor, 'ACTIVO').exists()
            or destinos_autorizados(actor, 'ORDEN').exists()
        )
    return estado['puede_consultar']


@register.simple_tag(takes_context=True)
def consulta_procedencia_activo(context, user, activo_id):
    estado = _estado_peticion(context, user)
    if estado is None:
        return ''
    if 'procedencias' not in estado:
        from compras.services_procedencia_adquisicion import procedencias_visibles
        estado['procedencias'] = {v.activo_id for v in procedencias_visibles(estado['actor'])}
    if activo_id in estado['procedencias']:
        return reverse('compras:procedencia_del_activo', args=[activo_id])
    return ''


@register.simple_tag(takes_context=True)
def recepciones_procedencia_item(context, user, item_id):
    estado = _estado_peticion(context, user)
    if estado is None:
        return []
    if 'recepciones_procedencia' not in estado:
        from collections import defaultdict
        from core.access import can_view_inventario
        from compras.access_departamentales import _areas_lectura_solicitudes
        from compras.models import RecepcionItemDepartamental
        estado['recepciones_procedencia'] = defaultdict(list)
        actor = estado['actor']
        if can_view_inventario(actor):
            # Sólo artículos de la pantalla; una consulta para todas sus recepciones.
            solicitud = context.get('solicitud')
            items = solicitud.items.all() if solicitud is not None else context.get('items', [])
            ids = [item.pk for item in items] or [item_id]
            recepciones = RecepcionItemDepartamental.objects.select_related('linea_orden__intento').filter(linea_orden__item_id__in=ids)
            areas = _areas_lectura_solicitudes(actor)
            if areas is not None:
                recepciones = recepciones.filter(linea_orden__item__solicitud__area_id__in=areas)
            for recepcion in recepciones:
                estado['recepciones_procedencia'][recepcion.linea_orden.item_id].append(recepcion)
    return estado['recepciones_procedencia'].get(item_id, [])
