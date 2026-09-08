"""Autorización vigente y específica de conteos físicos por sucursal."""
from django.contrib.auth import get_user_model
from django.db.models import Q
from core.access import can_manage_submodule, is_admin_or_dg
from core.models import UserProfile, sucursales_operativas_q
from .models_conteos import AccesoConteoSucursal, ConteoSucursal


def _activo(user):
    return bool(getattr(user, 'is_authenticated', False) and get_user_model().objects.filter(pk=user.pk, is_active=True).exists())


def puede_coordinar(user):
    return _activo(user) and (is_admin_or_dg(user) or can_manage_submodule(user, 'inventario', 'conteos_sucursales'))


def autorizado_sucursal(user, sucursal):
    return _activo(user) and sucursal.esta_operativa() and (UserProfile.objects.filter(user=user, sucursal=sucursal).exists() or AccesoConteoSucursal.objects.filter(user=user, sucursal=sucursal, activo=True, capturar=True).exists())


def puede_capturar(user, conteo):
    if not _activo(user) or not conteo.sucursal.esta_operativa():
        return False
    return puede_coordinar(user) or AccesoConteoSucursal.objects.filter(user=user, sucursal=conteo.sucursal, activo=True, capturar=True).exists() or (conteo.responsable_id == user.pk and autorizado_sucursal(user, conteo.sucursal))


def puede_revisar(user, conteo):
    return _activo(user) and conteo.sucursal.esta_operativa() and (puede_coordinar(user) or AccesoConteoSucursal.objects.filter(user=user, sucursal=conteo.sucursal, activo=True, revisar=True).exists())


def conteos_visibles(user):
    qs = ConteoSucursal.objects.all()
    if not _activo(user): return qs.none()
    if puede_coordinar(user): return qs
    grants = AccesoConteoSucursal.objects.filter(user=user, activo=True).filter(Q(capturar=True)|Q(revisar=True)).values('sucursal_id')
    profile = UserProfile.objects.filter(user=user).values('sucursal_id')
    from core.models import Sucursal
    operational = Sucursal.objects.filter(sucursales_operativas_q()).values('pk')
    return qs.filter(Q(sucursal_id__in=grants)|Q(responsable=user,sucursal_id__in=profile), sucursal_id__in=operational)


def sucursal_app(user):
    """La app de sucursal siempre usa la asignación vigente del perfil."""
    if not _activo(user):
        return None
    profile = UserProfile.objects.select_related('sucursal').filter(user=user).first()
    branch = profile.sucursal if profile else None
    return branch if branch and branch.esta_operativa() else None
