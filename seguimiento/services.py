from __future__ import annotations

from django.contrib.auth import get_user_model

from recetas.utils.normalizacion import normalizar_nombre
from rrhh.models import Empleado


def empleado_de_usuario(user):
    if not user or not user.is_authenticated:
        return None

    email = (getattr(user, "email", "") or "").strip()
    if email:
        empleado = Empleado.objects.filter(activo=True, email__iexact=email).first()
        if empleado:
            return empleado

    nombre = (user.get_full_name() or user.username or "").strip()
    nombre_norm = normalizar_nombre(nombre)
    if not nombre_norm:
        return None
    return Empleado.objects.filter(activo=True, nombre_normalizado=nombre_norm).first()


def usuarios_para_empleado(empleado):
    if not empleado:
        return get_user_model().objects.none()

    User = get_user_model()
    qs = User.objects.none()
    if empleado.email:
        qs = qs | User.objects.filter(email__iexact=empleado.email)
    nombre_norm = empleado.nombre_normalizado or normalizar_nombre(empleado.nombre or "")
    if nombre_norm:
        matches = [user.pk for user in User.objects.all() if normalizar_nombre(user.get_full_name() or user.username) == nombre_norm]
        qs = qs | User.objects.filter(pk__in=matches)
    return qs.distinct()
