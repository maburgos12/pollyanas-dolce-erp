from core.access import (
    can_view_audit,
    can_manage_crm,
    can_manage_compras,
    can_manage_inventario,
    can_view_crm,
    can_view_compras,
    can_view_inventario,
    can_view_maestros,
    can_view_recetas,
    can_view_reportes,
)


def ui_access(request):
    user = getattr(request, "user", None)
    return {
        "ui_access": {
            "can_view_maestros": can_view_maestros(user),
            "can_view_recetas": can_view_recetas(user),
            "can_view_compras": can_view_compras(user),
            "can_manage_compras": can_manage_compras(user),
            "can_view_inventario": can_view_inventario(user),
            "can_manage_inventario": can_manage_inventario(user),
            "can_view_reportes": can_view_reportes(user),
            "can_view_audit": can_view_audit(user),
            "can_view_crm": can_view_crm(user),
            "can_manage_crm": can_manage_crm(user),
        }
    }
