from core.access import can_view_submodule


NAV_GROUPS = [
    {
        "key": "direccion",
        "label": "Dirección",
        "items": [
            ("direccion", "dashboard", "Dashboard", "/dashboard/", ["/dashboard/"]),
            ("direccion", "operacion_dg", "Operación DG", "/recetas/dg-operacion/", ["/recetas/dg-operacion/"]),
            ("direccion", "bi", "BI ejecutivo", "/reportes/bi/", ["/reportes/bi/"]),
            ("direccion", "cierre_diario", "Cierre diario", "/reportes/cierre-operativo/", ["/reportes/cierre-operativo/"]),
            ("direccion", "producido_vendido", "Producido vs Vendido", "/reportes/produccion/", ["/reportes/produccion/"]),
            ("direccion", "proyectos_inversion", "Proyectos inversión", "/reportes/proyectos-inversion/", ["/reportes/proyectos-inversion/", "/reportes/sucursales/"]),
            ("direccion", "rentabilidad", "Rentabilidad", "/rentabilidad/", ["/rentabilidad/"]),
        ],
    },
    {
        "key": "comercial",
        "label": "Comercial",
        "items": [
            ("ventas", "pronostico", "Pronóstico", "/ventas/pronostico/", ["/ventas/pronostico/", "/ventas/"]),
            ("ventas", "bonos", "Bonos ventas", "/bonos-ventas/app/", ["/bonos-ventas/"]),
            ("crm", "dashboard", "CRM", "/crm/dashboard/", ["/crm/dashboard/"]),
            ("crm", "clientes", "Clientes", "/crm/clientes/", ["/crm/clientes/"]),
            ("crm", "pedidos", "Pedidos", "/crm/pedidos/", ["/crm/pedidos/"]),
            ("fallas", "dashboard", "Fallas", "/fallas/", ["/fallas/"]),
            ("fallas", "reportar", "Reportar falla", "/fallas/reportar/", ["/fallas/reportar/"]),
            ("fallas", "mis_reportes", "Mis reportes", "/fallas/mis-reportes/", ["/fallas/mis-reportes/"]),
        ],
    },
    {
        "key": "produccion",
        "label": "Producción",
        "items": [
            ("produccion", "plan", "Plan de producción", "/recetas/plan-produccion/", ["/recetas/plan-produccion/"]),
            ("produccion", "bonos", "Bonos producción", "/bonos-produccion/dashboard/", ["/bonos-produccion/"]),
            ("produccion", "reabasto_cedis", "Reabasto CEDIS", "/recetas/reabasto-cedis/", ["/recetas/reabasto-cedis/"]),
            ("produccion", "consolidado_cedis", "Consolidado CEDIS", "/recetas/consolidado-cedis/", ["/recetas/consolidado-cedis/"]),
            ("produccion", "cedis_semanal", "Producción CEDIS semanal", "/recetas/produccion-cedis/semanal/", ["/recetas/produccion-cedis/semanal/"]),
            ("recetas", "catalogo", "Recetas", "/recetas/", ["/recetas/"]),
            ("recetas", "costeo", "Costeo", "/recetas/costeo/", ["/recetas/costeo/", "/recetas/drivers-costeo/"]),
            ("recetas", "margenes", "Monitor de márgenes", "/recetas/monitor-margenes/", ["/recetas/monitor-margenes/"]),
            ("recetas", "mrp", "MRP", "/recetas/mrp/", ["/recetas/mrp/"]),
        ],
    },
    {
        "key": "operacion",
        "label": "Operación",
        "items": [
            (
                "maestros",
                "insumos",
                "Insumos",
                "/maestros/insumos/",
                ["/maestros/insumos/", "/maestros/insumo/"],
            ),
            (
                "maestros",
                "proveedores",
                "Proveedores",
                "/maestros/proveedores/",
                ["/maestros/proveedores/", "/maestros/proveedor/"],
            ),
            (
                "compras",
                "dashboard",
                "Compras",
                "/compras/dashboard/",
                ["/compras/"],
            ),
            (
                "inventario",
                "dashboard",
                "Inventario",
                "/inventario/dashboard/",
                ["/inventario/"],
            ),
            (
                "maestros",
                "point",
                "Sincronización Point",
                "/maestros/point-pendientes/",
                ["/maestros/point-pendientes/"],
            ),
        ],
    },
    {
        "key": "logistica",
        "label": "Logística",
        "items": [
            ("logistica", "dashboard", "Dashboard", "/logistica/dashboard/", ["/logistica/dashboard/"]),
            ("logistica", "ejecutivo", "Ejecutivo", "/logistica/ejecutivo/", ["/logistica/ejecutivo/"]),
            ("logistica", "tickets", "Tickets", "/logistica/tickets/", ["/logistica/tickets/"]),
            ("logistica", "flota", "Flota", "/logistica/flota/", ["/logistica/flota/"]),
            ("logistica", "rutas", "Rutas", "/logistica/rutas/", ["/logistica/rutas/"]),
            ("logistica", "unidades", "Unidades", "/logistica/unidades/", ["/logistica/unidades/"]),
            ("logistica", "reportes", "Reportes", "/logistica/reportes/", ["/logistica/reportes/"]),
            ("logistica", "bitacoras", "Bitácoras", "/logistica/bitacoras/", ["/logistica/bitacoras/"]),
            ("logistica", "capturas", "Capturas", "/logistica/capturas/", ["/logistica/capturas/"]),
            ("mermas", "dashboard", "Mermas", "/mermas/", ["/mermas/"]),
            ("mermas", "captura", "Captura merma", "/mermas/nuevo/", ["/mermas/nuevo/"]),
        ],
    },
    {
        "key": "administracion",
        "label": "Administración",
        "items": [
            ("activos", "dashboard", "Activos", "/activos/dashboard/", ["/activos/dashboard/"]),
            ("activos", "catalogo", "Catálogo activos", "/activos/activos/", ["/activos/activos/"]),
            ("activos", "planes", "Planes", "/activos/planes/", ["/activos/planes/"]),
            ("activos", "ordenes", "Órdenes activos", "/activos/ordenes/", ["/activos/ordenes/"]),
            ("activos", "reportes", "Reportes activos", "/activos/reportes/", ["/activos/reportes/"]),
            ("rrhh", "dashboard", "Capital Humano", "/rrhh/dashboard/", ["/rrhh/dashboard/"]),
            ("rrhh", "asistencias", "Asistencias", "/rrhh/asistencias/", ["/rrhh/asistencias/"]),
            ("rrhh", "horas_extra", "Horas extra", "/rrhh/horas-extra/", ["/rrhh/horas-extra/"]),
            ("rrhh", "permisos", "Permisos", "/rrhh/permisos/", ["/rrhh/permisos/"]),
            ("rrhh", "prestamos", "Préstamos", "/rrhh/prestamos/", ["/rrhh/prestamos/"]),
            ("rrhh", "importar_checador", "Importar checador", "/rrhh/importar-checador/", ["/rrhh/importar-checador/"]),
            ("rrhh", "empleados", "Empleados", "/rrhh/empleados/", ["/rrhh/empleados/"]),
            ("rrhh", "nomina", "Nómina", "/rrhh/nomina/", ["/rrhh/nomina/"]),
            ("rrhh", "asignacion_sucursal", "Asignación sucursal", "/rrhh/asignacion-sucursal/", ["/rrhh/asignacion-sucursal/"]),
            ("control", "discrepancias", "Control", "/control/discrepancias/", ["/control/discrepancias/"]),
            ("control", "captura_movil", "Captura móvil", "/control/captura-movil/", ["/control/captura-movil/"]),
            ("auditoria", "bitacora", "Bitácora", "/auditoria/", ["/auditoria/"]),
        ],
    },
    {
        "key": "sistema",
        "label": "Sistema",
        "items": [
            ("sistema", "usuarios", "Usuarios y accesos", "/usuarios-accesos/", ["/usuarios-accesos/"]),
            ("sistema", "orquestacion", "Orquestación", "/orquestacion/", ["/orquestacion/"]),
            ("sistema", "ia", "IA privada", "/ia-privada/", ["/ia-privada/"]),
            ("sistema", "integraciones", "Integraciones", "/integraciones/", ["/integraciones/"]),
            (
                "sistema",
                "horarios_especiales",
                "Horarios Especiales",
                "/horarios-especiales/",
                ["/horarios-especiales/"],
            ),
        ],
    },
]


def build_nav_groups(user, current_path: str) -> list[dict]:
    groups = []
    current_path = current_path or ""
    visible_groups = []
    best_match_len = 0
    for group in NAV_GROUPS:
        items = []
        for module, submodule, label, url, prefixes in group["items"]:
            if not can_view_submodule(user, module, submodule):
                continue
            match_len = max((len(prefix) for prefix in prefixes if current_path.startswith(prefix)), default=0)
            best_match_len = max(best_match_len, match_len)
            items.append(
                {
                    "label": label,
                    "url": url,
                    "active": False,
                    "_match_len": match_len,
                    "module": module,
                    "submodule": submodule,
                    "initial": label[:1],
                }
            )
        if items:
            visible_groups.append(
                {
                    "key": group["key"],
                    "label": group["label"],
                    "items": items,
                    "active": False,
                }
            )
    for group in visible_groups:
        active_group = False
        for item in group["items"]:
            item["active"] = bool(best_match_len and item.pop("_match_len") == best_match_len)
            active_group = active_group or item["active"]
        group["active"] = active_group
        groups.append(group)
    return groups
