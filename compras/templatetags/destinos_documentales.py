from django import template
from django.urls import reverse
from compras.services_destinos_documentales import actor_actual, destinos_autorizados, vinculos_visibles

register = template.Library()


def _estado_peticion(context, user):
    """Una instantánea por petición y actor; no sobrevive cambios de sesión."""
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
    if 'puede_consultar' not in estado:
        actor = estado['actor']
        estado['puede_consultar'] = (
            destinos_autorizados(actor, 'ACTIVO').exists()
            or destinos_autorizados(actor, 'ORDEN').exists()
        )
    return estado['puede_consultar']
