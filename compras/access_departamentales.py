from core.access import can_manage_compras
from reportes.models import AreaPresupuestoResponsable


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
