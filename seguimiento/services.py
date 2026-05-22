from __future__ import annotations

from django.contrib.auth import get_user_model

from recetas.utils.normalizacion import normalizar_nombre
from rrhh.models import Empleado


def _tokens(value: str) -> set[str]:
    return {token for token in normalizar_nombre(value or "").replace(".", " ").split() if len(token) > 1}


def _user_area_hints(user) -> set[str]:
    hints = set()
    for group in getattr(user, "groups").all():
        hints.update(_tokens(group.name))
    profile = getattr(user, "userprofile", None)
    if profile:
        hints.update(_tokens(getattr(getattr(profile, "departamento", None), "nombre", "")))
        hints.update(_tokens(getattr(getattr(profile, "sucursal", None), "nombre", "")))
    return hints


def _score_empleado_para_usuario(empleado: Empleado, user, user_tokens: set[str], area_hints: set[str]) -> int:
    empleado_tokens = _tokens(empleado.nombre_normalizado or empleado.nombre)
    if not user_tokens or not empleado_tokens:
        return 0
    overlap = user_tokens.intersection(empleado_tokens)
    if not overlap:
        return 0

    score = len(overlap) * 10
    if user_tokens.issubset(empleado_tokens):
        score += 30
    if empleado_tokens == user_tokens:
        score += 50

    area_tokens = _tokens(f"{empleado.area} {empleado.puesto} {empleado.sucursal}")
    if area_hints and area_tokens.intersection(area_hints):
        score += 20
    return score


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
    empleado = Empleado.objects.filter(activo=True, nombre_normalizado=nombre_norm).first()
    if empleado:
        return empleado

    user_tokens = _tokens(f"{user.get_full_name()} {user.username}")
    area_hints = _user_area_hints(user)
    scored = []
    for candidato in Empleado.objects.filter(activo=True).only(
        "id",
        "nombre",
        "nombre_normalizado",
        "email",
        "area",
        "puesto",
        "sucursal",
        "activo",
    ):
        score = _score_empleado_para_usuario(candidato, user, user_tokens, area_hints)
        if score:
            scored.append((score, candidato.id, candidato))
    if not scored:
        return None
    scored.sort(key=lambda item: (-item[0], item[1]))
    return scored[0][2]


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
