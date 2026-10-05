from core.access import can_manage_compras, ROLE_DG, has_any_role
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable


AREA_ADMINISTRACION = "administracion"


def puede_gestionar_compras_departamentales(user) -> bool:
    """Autoriza la bandeja sin ampliar acceso a compras de insumos."""
    if not (user and user.is_authenticated and getattr(user, "is_active", True)):
        return False
    if can_manage_compras(user):
        return True
    if not getattr(user, "pk", None):
        return False

    codigos_cacheados = getattr(user, "_areas_presupuesto_codigos", None)
    if codigos_cacheados is not None:
        return AREA_ADMINISTRACION in codigos_cacheados

    return AreaPresupuestoResponsable.objects.filter(
        usuario=user,
        puede_capturar=True,
        area__activa=True,
        area__codigo=AREA_ADMINISTRACION,
    ).exists()


def _areas_usuario(user):
    return AreaPresupuesto.objects.filter(
        activa=True,
        responsables__usuario=user,
        responsables__puede_capturar=True,
    ).distinct()


def _es_direccion(user):
    return (
        user.is_superuser
        or has_any_role(user, ROLE_DG)
        or user.has_perm("compras.decidir_exceso_compra_departamental")
    )


def _areas_lectura_solicitudes(user):
    """None significa lectura global; el resto conserva las áreas de lectura."""
    if puede_gestionar_compras_departamentales(user) or _es_direccion(user):
        return None
    # Lectura permite áreas inactivas; la captura usa _areas_usuario por separado.
    return AreaPresupuestoResponsable.objects.filter(
        usuario=user, puede_capturar=True,
    ).values_list('area_id', flat=True)


def _puede_ver_solicitud(user, solicitud):
    areas = _areas_lectura_solicitudes(user)
    return areas is None or areas.filter(area_id=solicitud.area_id).exists()


def _puede_enviar_solicitud(user, solicitud):
    # Mismo alcance que la captura: responsables activos del área y Compras.
    return (
        puede_gestionar_compras_departamentales(user)
        or _areas_usuario(user).filter(pk=solicitud.area_id).exists()
    )
