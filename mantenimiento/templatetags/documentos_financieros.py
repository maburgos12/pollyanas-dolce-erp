from django import template
from django.core.exceptions import PermissionDenied
from django.urls import reverse
from mantenimiento.services_documentos_financieros import TIPOS, actor_actual, fuentes_autorizadas, trabajos_autorizados

register = template.Library()


@register.simple_tag(takes_context=True)
def consulta_documentos_trabajo(context, user, tipo, trabajo_id):
    if not getattr(user, "is_authenticated", False) or not getattr(user, "pk", None) or not trabajo_id:
        return ""
    request = context.get("request")
    if request is None:
        return ""
    if not hasattr(request, "_documentos_trabajo_scope"):
        request._documentos_trabajo_scope = {}
    if user.pk not in request._documentos_trabajo_scope:
        try:
            actor = actor_actual(user)
            puede = any(fuentes_autorizadas(actor, t).exists() for t, _ in TIPOS)
            # ponytail: IDs autorizados por request; paginar por IDs presentes si
            # el volumen de trabajos deja de caber en memoria.
            scope = {t: set(trabajos_autorizados(actor, t).values_list("pk", flat=True)) for t in ("orden", "falla")} if puede else {}
        except PermissionDenied:
            scope = {}
        request._documentos_trabajo_scope[user.pk] = scope
    if trabajo_id in request._documentos_trabajo_scope[user.pk].get(tipo, set()):
        return reverse("mantenimiento:documentos-trabajo", args=[tipo, trabajo_id])
    return ""
