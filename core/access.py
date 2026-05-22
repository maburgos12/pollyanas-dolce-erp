from django.contrib.auth.models import AbstractBaseUser

ROLE_DG = "DG"
ROLE_ADMIN = "ADMIN"
ROLE_COMPRAS = "COMPRAS"
ROLE_ALMACEN = "ALMACEN"
ROLE_PRODUCCION = "PRODUCCION"
ROLE_VENTAS = "VENTAS"
ROLE_LOGISTICA = "LOGISTICA"
ROLE_RRHH = "RRHH"
ROLE_LECTURA = "LECTURA"
ROLE_REPARTIDOR = "repartidor"
ROLE_BONOS_PRODUCCION_CAPTURA = "bonos_produccion_captura"

ROLE_ORDER = [
    ROLE_DG,
    ROLE_ADMIN,
    ROLE_COMPRAS,
    ROLE_ALMACEN,
    ROLE_PRODUCCION,
    ROLE_VENTAS,
    ROLE_LOGISTICA,
    ROLE_RRHH,
    ROLE_LECTURA,
]

ACCESS_NONE = "none"
ACCESS_VIEW = "view"
ACCESS_MANAGE = "manage"
ACCESS_LEVELS = {
    ACCESS_NONE: 0,
    ACCESS_VIEW: 1,
    ACCESS_MANAGE: 2,
}

ACCESS_MODULES = [
    ("direccion", "Dirección"),
    ("ventas", "Ventas"),
    ("crm", "CRM"),
    ("produccion", "Producción"),
    ("mantenimiento", "Mantenimiento"),
    ("seguimiento", "Seguimiento personal"),
    ("logistica", "Logística"),
    ("fallas", "Fallas / Mantenimiento"),
    ("mermas", "Mermas"),
    ("compras", "Compras"),
    ("inventario", "Inventario"),
    ("recetas", "Recetas"),
    ("maestros", "Maestros"),
    ("reportes", "Reportes"),
    ("activos", "Activos"),
    ("rrhh", "RRHH"),
    ("control", "Control"),
    ("auditoria", "Auditoría"),
    ("sistema", "Sistema"),
]

ACCESS_SUBMODULES = {
    "direccion": [
        ("dashboard", "Dashboard"),
        ("operacion_dg", "Operación DG"),
        ("bi", "BI ejecutivo"),
        ("cierre_diario", "Cierre diario"),
        ("producido_vendido", "Producido vs Vendido"),
        ("proyectos_inversion", "Proyectos inversión"),
        ("rentabilidad", "Rentabilidad"),
    ],
    "ventas": [
        ("eventos", "Eventos"),
        ("tendencias", "Tendencias"),
        ("pronostico", "Pronóstico"),
        ("bonos", "Bonos ventas"),
    ],
    "crm": [
        ("dashboard", "Dashboard"),
        ("clientes", "Clientes"),
        ("pedidos", "Pedidos"),
    ],
    "produccion": [
        ("plan", "Plan de producción"),
        ("reabasto_cedis", "Reabasto CEDIS"),
        ("consolidado_cedis", "Consolidado CEDIS"),
        ("cedis_semanal", "Producción CEDIS semanal"),
        ("bonos", "Bonos producción"),
    ],
    "mantenimiento": [
        ("dashboard", "Dashboard"),
        ("bandeja", "Bandeja de seguimiento"),
        ("app", "App mantenimiento"),
    ],
    "seguimiento": [
        ("mi_tablero", "Mis minutas, proyectos y compromisos"),
    ],
    "logistica": [
        ("dashboard", "Dashboard"),
        ("ejecutivo", "Ejecutivo"),
        ("tickets", "Tickets"),
        ("flota", "Flota"),
        ("rutas", "Rutas"),
        ("unidades", "Unidades"),
        ("reportes", "Reportes"),
        ("bitacoras", "Bitácoras"),
        ("capturas", "Capturas"),
    ],
    "fallas": [
        ("dashboard", "Dashboard"),
        ("reportar", "Reportar falla"),
        ("mis_reportes", "Mis reportes"),
        ("gestion", "Gestión de fallas"),
    ],
    "mermas": [
        ("dashboard", "Dashboard"),
        ("captura", "Captura de sucursal"),
        ("recepcion", "Recepción CEDIS"),
    ],
    "compras": [
        ("dashboard", "Dashboard"),
        ("solicitudes", "Solicitudes"),
        ("ordenes", "Órdenes"),
        ("recepciones", "Recepciones"),
    ],
    "inventario": [
        ("dashboard", "Dashboard"),
        ("existencias", "Existencias"),
        ("movimientos", "Movimientos"),
        ("ajustes", "Ajustes"),
        ("alertas", "Alertas"),
        ("conteo_fisico", "Conteo físico"),
    ],
    "recetas": [
        ("catalogo", "Catálogo de recetas"),
        ("costeo", "Costeo"),
        ("margenes", "Monitor de márgenes"),
        ("matching", "Matching"),
        ("mrp", "MRP"),
    ],
    "maestros": [
        ("proveedores", "Proveedores"),
        ("insumos", "Insumos"),
        ("point", "Revisión Point"),
    ],
    "reportes": [
        ("consumo", "Consumo"),
        ("ventas", "Ventas"),
        ("financiero", "Financiero"),
        ("faltantes", "Faltantes"),
        ("presupuesto", "Presupuesto maestro"),
    ],
    "activos": [
        ("dashboard", "Dashboard"),
        ("catalogo", "Activos"),
        ("planes", "Planes"),
        ("ordenes", "Órdenes"),
        ("reportes", "Reportes"),
    ],
    "rrhh": [
        ("dashboard", "Capital Humano"),
        ("asistencias", "Asistencias"),
        ("horas_extra", "Horas extra"),
        ("permisos", "Permisos"),
        ("prestamos", "Préstamos"),
        ("importar_checador", "Importar checador"),
        ("empleados", "Empleados"),
        ("nomina", "Nómina"),
        ("asignacion_sucursal", "Asignación sucursal"),
    ],
    "control": [
        ("discrepancias", "Discrepancias"),
        ("captura_movil", "Captura móvil"),
    ],
    "auditoria": [
        ("bitacora", "Bitácora"),
    ],
    "sistema": [
        ("usuarios", "Usuarios y accesos"),
        ("orquestacion", "Orquestación"),
        ("ia", "IA privada"),
        ("integraciones", "Integraciones"),
        ("horarios_especiales", "Horarios Especiales"),
    ],
}

LOCK_BY_MODULE = {
    "maestros": "lock_maestros",
    "recetas": "lock_recetas",
    "produccion": "lock_recetas",
    "compras": "lock_compras",
    "inventario": "lock_inventario",
    "direccion": "lock_reportes",
    "reportes": "lock_reportes",
    "control": "lock_reportes",
    "ventas": "lock_crm",
    "crm": "lock_crm",
    "logistica": "lock_logistica",
    "fallas": "lock_logistica",
    "mermas": "lock_logistica",
    "rrhh": "lock_rrhh",
    "auditoria": "lock_auditoria",
    "sistema": "lock_auditoria",
}


def _group_names(user: AbstractBaseUser) -> set[str]:
    if not user or not user.is_authenticated:
        return set()
    cached = getattr(user, "_group_names_cache", None)
    if cached is not None:
        return set(cached)
    prefetched = getattr(user, "_prefetched_objects_cache", {}) or {}
    prefetched_groups = prefetched.get("groups")
    if prefetched_groups is not None:
        names = {str(group.name) for group in prefetched_groups}
        setattr(user, "_group_names_cache", frozenset(names))
        return names
    names = set(user.groups.values_list("name", flat=True))
    setattr(user, "_group_names_cache", frozenset(names))
    return names


def has_any_role(user: AbstractBaseUser, *roles: str) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return bool(_group_names(user).intersection(set(roles)))


def _is_locked(user: AbstractBaseUser, lock_field: str) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return False
    profile = getattr(user, "userprofile", None)
    if not profile:
        return False
    return bool(getattr(profile, lock_field, False))


def is_repartidor_only(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated or user.is_superuser or user.is_staff:
        return False
    if _explicit_access_map(user).get("mermas.recepcion") == ACCESS_MANAGE:
        return False
    if _explicit_access_map(user).get("mantenimiento") in {ACCESS_VIEW, ACCESS_MANAGE}:
        return False
    groups = _group_names(user)
    if not (ROLE_REPARTIDOR in groups or hasattr(user, "repartidor_logistica")):
        return False
    elevated = {
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_COMPRAS,
        ROLE_ALMACEN,
        ROLE_PRODUCCION,
        ROLE_VENTAS,
        ROLE_LOGISTICA,
        ROLE_RRHH,
        ROLE_LECTURA,
        "compras_logistica",
        "supervisor_logistica",
        "personal_sucursal",
    }
    return not bool(groups.intersection(elevated))


def primary_role(user: AbstractBaseUser) -> str:
    groups = _group_names(user)
    for role in ROLE_ORDER:
        if role in groups:
            return role
    return ""


def _normalize_access(value: str) -> str:
    value = (value or ACCESS_NONE).strip().lower()
    if value not in ACCESS_LEVELS:
        return ACCESS_NONE
    return value


def _max_access(*values: str) -> str:
    best = ACCESS_NONE
    for value in values:
        value = _normalize_access(value)
        if ACCESS_LEVELS[value] > ACCESS_LEVELS[best]:
            best = value
    return best


def _explicit_access_map(user: AbstractBaseUser) -> dict[str, str]:
    if not user or not user.is_authenticated:
        return {}
    cached = getattr(user, "_module_access_map_cache", None)
    if cached is not None:
        return dict(cached)
    from core.models import UserModuleAccess

    access_map = {
        item.module: _normalize_access(item.access)
        for item in UserModuleAccess.objects.filter(user=user).only("module", "access")
    }
    setattr(user, "_module_access_map_cache", dict(access_map))
    return access_map


def is_mermas_only(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated or user.is_superuser or user.is_staff:
        return False
    groups = _group_names(user)
    allowed_app_groups = {ROLE_REPARTIDOR.lower(), ROLE_REPARTIDOR}
    try:
        from mermas.models import PersonalEnviosSucursal

        if PersonalEnviosSucursal.objects.filter(user=user, activo=True).exists() and groups.issubset(allowed_app_groups):
            return True
    except Exception:
        pass
    active_modules = {
        module
        for module, access in _explicit_access_map(user).items()
        if _normalize_access(access) != ACCESS_NONE
    }
    return bool(active_modules) and all(module.split(".", 1)[0] == "mermas" for module in active_modules) and groups.issubset(allowed_app_groups)


def _role_module_access(user: AbstractBaseUser, module: str) -> str:
    module = (module or "").strip().lower()
    groups = {name.upper() for name in _group_names(user)}
    if ROLE_DG in groups or ROLE_ADMIN in groups:
        return ACCESS_MANAGE
    if ROLE_LECTURA in groups:
        return ACCESS_VIEW
    if ROLE_COMPRAS in groups:
        if module == "compras":
            return ACCESS_MANAGE
        if module in {"maestros", "recetas", "inventario", "reportes"}:
            return ACCESS_VIEW
    if ROLE_ALMACEN in groups:
        if module == "inventario":
            return ACCESS_MANAGE
        if module in {"maestros", "recetas", "compras"}:
            return ACCESS_VIEW
    if ROLE_PRODUCCION in groups:
        if module == "produccion":
            return ACCESS_MANAGE
        if module in {"recetas", "reportes"}:
            return ACCESS_VIEW
    if ROLE_VENTAS in groups:
        if module in {"ventas", "crm"}:
            return ACCESS_MANAGE
    if ROLE_LOGISTICA in groups:
        if module == "logistica":
            return ACCESS_MANAGE
    if ROLE_RRHH in groups and module == "rrhh":
        return ACCESS_MANAGE
    if module == "seguimiento" and groups.intersection({role.upper() for role in ROLE_ORDER}):
        return ACCESS_MANAGE
    return ACCESS_NONE


def _module_locked(user: AbstractBaseUser, module: str) -> bool:
    lock_field = LOCK_BY_MODULE.get((module or "").split(".", 1)[0])
    return bool(lock_field and _is_locked(user, lock_field))


def get_module_access(user: AbstractBaseUser, module: str) -> str:
    """
    Retorna 'none', 'view' o 'manage' para un usuario y modulo.

    Prioridad:
    1. Superuser: manage
    2. Permiso explicito en UserModuleAccess
    3. Fallback derivado del rol actual
    """
    if not user or not user.is_authenticated:
        return ACCESS_NONE
    if user.is_superuser:
        return ACCESS_MANAGE

    module = (module or "").strip().lower()
    if _module_locked(user, module):
        return ACCESS_NONE
    explicit = _explicit_access_map(user)
    if module in explicit:
        return explicit[module]
    return _role_module_access(user, module)


def get_effective_module_access(user: AbstractBaseUser, module: str) -> str:
    access = get_module_access(user, module)
    if access != ACCESS_NONE:
        return access
    children = [
        get_submodule_access(user, module, submodule)
        for submodule, _label in ACCESS_SUBMODULES.get(module, [])
    ]
    return _max_access(access, *children)


def get_submodule_access(user: AbstractBaseUser, module: str, submodule: str) -> str:
    if not user or not user.is_authenticated:
        return ACCESS_NONE
    if user.is_superuser:
        return ACCESS_MANAGE
    module = (module or "").strip().lower()
    submodule = (submodule or "").strip().lower()
    if _module_locked(user, module):
        return ACCESS_NONE
    if module in {"produccion", "ventas"} and submodule == "bonos":
        if module == "produccion" and has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_RRHH, ROLE_PRODUCCION):
            return ACCESS_MANAGE
        if module == "ventas" and has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_RRHH, ROLE_VENTAS):
            return ACCESS_MANAGE
        return ACCESS_NONE
    key = f"{module}.{submodule}"
    explicit = _explicit_access_map(user)
    if key in explicit:
        return explicit[key]
    if module in explicit:
        return explicit[module]
    return _role_module_access(user, module)


def can_view_module(user: AbstractBaseUser, module: str) -> bool:
    return get_effective_module_access(user, module) in (ACCESS_VIEW, ACCESS_MANAGE)


def can_manage_module(user: AbstractBaseUser, module: str) -> bool:
    return get_effective_module_access(user, module) == ACCESS_MANAGE


def can_view_submodule(user: AbstractBaseUser, module: str, submodule: str) -> bool:
    return get_submodule_access(user, module, submodule) in (ACCESS_VIEW, ACCESS_MANAGE)


def can_manage_submodule(user: AbstractBaseUser, module: str, submodule: str) -> bool:
    return get_submodule_access(user, module, submodule) == ACCESS_MANAGE


def can_view(user: AbstractBaseUser, module: str) -> bool:
    return can_view_module(user, module)


def can_manage(user: AbstractBaseUser, module: str) -> bool:
    return can_manage_module(user, module)


def can_view_compras(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "compras")


def can_manage_compras(user: AbstractBaseUser) -> bool:
    return can_manage_module(user, "compras")


def can_view_inventario(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "inventario")


def can_manage_inventario(user: AbstractBaseUser) -> bool:
    return can_manage_module(user, "inventario")


def can_view_reportes(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "reportes") or can_view_module(user, "direccion")


def can_view_rentabilidad(user: AbstractBaseUser) -> bool:
    return can_view_submodule(user, "direccion", "rentabilidad")


def can_manage_rentabilidad(user: AbstractBaseUser) -> bool:
    return can_manage_submodule(user, "direccion", "rentabilidad")


def can_view_product_closure(user: AbstractBaseUser) -> bool:
    return can_view_reportes(user)


def can_build_product_closure(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_PRODUCCION, ROLE_ALMACEN) and not _is_locked(
        user, "lock_reportes"
    )


def can_rebuild_product_closure(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_reportes")


def can_lock_product_closure(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_reportes")


def can_view_maestros(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "maestros")


def can_view_recetas(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "recetas")


def can_view_audit(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "auditoria")


def can_manage_users(user: AbstractBaseUser) -> bool:
    return can_manage_submodule(user, "sistema", "usuarios")


def can_view_orquestacion(user: AbstractBaseUser) -> bool:
    return can_view_submodule(user, "sistema", "orquestacion")


def can_manage_orquestacion(user: AbstractBaseUser) -> bool:
    return can_manage_submodule(user, "sistema", "orquestacion")


def can_view_crm(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "crm")


def can_manage_crm(user: AbstractBaseUser) -> bool:
    return can_manage_module(user, "crm")


def can_view_ventas_eventos(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "ventas")


def can_view_ventas(user: AbstractBaseUser) -> bool:
    return can_view_ventas_eventos(user)


def can_manage_ventas_eventos(user: AbstractBaseUser) -> bool:
    return can_manage_module(user, "ventas")


def can_view_logistica(user: AbstractBaseUser) -> bool:
    if is_repartidor_only(user):
        return False
    return can_view_module(user, "logistica")


def can_manage_logistica(user: AbstractBaseUser) -> bool:
    return can_manage_module(user, "logistica")


def can_view_rrhh(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "rrhh")


def can_manage_rrhh(user: AbstractBaseUser) -> bool:
    return can_manage_module(user, "rrhh")


def can_view_activos(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "activos")


def can_view_control(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "control")


def can_view_sistema(user: AbstractBaseUser) -> bool:
    return can_view_module(user, "sistema")


def can_capture_piso(user: AbstractBaseUser) -> bool:
    if is_repartidor_only(user):
        return False
    return has_any_role(
        user,
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_ALMACEN,
        ROLE_PRODUCCION,
        ROLE_VENTAS,
        ROLE_LOGISTICA,
    ) and not _is_locked(user, "lock_captura_piso")


def is_branch_capture_only(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated or user.is_superuser:
        return False
    profile = getattr(user, "userprofile", None)
    if not profile:
        return False
    return bool(getattr(profile, "modo_captura_sucursal", False))


def is_bonos_produccion_capture_only(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated or user.is_superuser or user.is_staff:
        return False
    groups = _group_names(user)
    allowed_groups = {ROLE_BONOS_PRODUCCION_CAPTURA}
    active_modules = {
        module
        for module, access in _explicit_access_map(user).items()
        if _normalize_access(access) != ACCESS_NONE
    }
    return bool(groups.intersection(allowed_groups)) and groups.issubset(allowed_groups) and not active_modules


def _get_role_label(user: AbstractBaseUser) -> str:
    if not user or not user.is_authenticated:
        return ""
    if user.is_superuser:
        return "Superusuario"

    role_map = {
        ROLE_DG: "Director General",
        ROLE_ADMIN: "Administrador",
        ROLE_COMPRAS: "Compras",
        ROLE_ALMACEN: "Almacen",
        ROLE_PRODUCCION: "Produccion",
        ROLE_VENTAS: "Ventas",
        ROLE_LOGISTICA: "Logistica",
        ROLE_RRHH: "RRHH",
        ROLE_LECTURA: "Solo lectura",
        ROLE_REPARTIDOR: "Repartidor",
    }
    groups = _group_names(user)
    for role, label in role_map.items():
        if role in groups:
            return label
    return "Usuario"
