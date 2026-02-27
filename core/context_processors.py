from core.access import (
    can_capture_piso,
    can_view_audit,
    can_manage_users,
    can_manage_crm,
    can_manage_compras,
    can_manage_inventario,
    can_manage_logistica,
    can_manage_rrhh,
    can_view_crm,
    can_view_compras,
    can_view_inventario,
    can_view_logistica,
    can_view_maestros,
    can_view_recetas,
    can_view_rrhh,
    can_view_reportes,
    is_branch_capture_only,
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
            "can_manage_users": can_manage_users(user),
            "can_view_crm": can_view_crm(user),
            "can_manage_crm": can_manage_crm(user),
            "can_view_logistica": can_view_logistica(user),
            "can_manage_logistica": can_manage_logistica(user),
            "can_view_rrhh": can_view_rrhh(user),
            "can_manage_rrhh": can_manage_rrhh(user),
            "can_capture_piso": can_capture_piso(user),
            "branch_capture_only": is_branch_capture_only(user),
        }
    }
