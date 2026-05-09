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
            ("ventas", "pronostico", "Pronóstico", "/recetas/plan-produccion/", ["/recetas/plan-produccion/"]),
            ("ventas", "eventos", "Eventos", "/ventas/eventos/", ["/ventas/eventos/"]),
            ("ventas", "tendencias", "Tendencias", "/ventas/tendencias/", ["/ventas/tendencias/"]),
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
            ("produccion", "reabasto_cedis", "Reabasto CEDIS", "/recetas/reabasto-cedis/", ["/recetas/reabasto-cedis/"]),
            ("produccion", "consolidado_cedis", "Consolidado CEDIS", "/recetas/consolidado-cedis/", ["/recetas/consolidado-cedis/"]),
            ("produccion", "cedis_semanal", "Producción CEDIS semanal", "/recetas/produccion-cedis/semanal/", ["/recetas/produccion-cedis/semanal/"]),
            ("recetas", "catalogo", "Recetas", "/recetas/", ["/recetas/"]),
            ("recetas", "costeo", "Costeo", "/recetas/costeo/", ["/recetas/costeo/", "/recetas/drivers-costeo/"]),
            ("recetas", "margenes", "Monitor de márgenes", "/recetas/monitor-margenes/", ["/recetas/monitor-margenes/"]),
            ("recetas", "matching", "Matching", "/recetas/matching/pendientes/", ["/recetas/matching/"]),
            ("recetas", "mrp", "MRP", "/recetas/mrp/", ["/recetas/mrp/"]),
        ],
    },
    {
        "key": "operacion",
        "label": "Operación",
        "items": [
            ("maestros", "proveedores", "Proveedores", "/maestros/proveedores/", ["/maestros/proveedores/"]),
            ("maestros", "insumos", "Insumos", "/maestros/insumos/", ["/maestros/insumos/"]),
            ("maestros", "point", "Revisión Point", "/maestros/point-pendientes/", ["/maestros/point-pendientes/"]),
            ("compras", "dashboard", "Compras", "/compras/dashboard/", ["/compras/dashboard/"]),
            ("compras", "solicitudes", "Solicitudes", "/compras/solicitudes/", ["/compras/solicitudes/"]),
            ("compras", "ordenes", "Órdenes", "/compras/ordenes/", ["/compras/ordenes/"]),
            ("compras", "recepciones", "Recepciones", "/compras/recepciones/", ["/compras/recepciones/"]),
            ("inventario", "dashboard", "Inventario", "/inventario/dashboard/", ["/inventario/dashboard/"]),
            ("inventario", "existencias", "Existencias", "/inventario/existencias/", ["/inventario/existencias/"]),
            ("inventario", "movimientos", "Movimientos", "/inventario/movimientos/", ["/inventario/movimientos/"]),
            ("inventario", "ajustes", "Ajustes", "/inventario/ajustes/", ["/inventario/ajustes/"]),
            ("inventario", "alertas", "Alertas", "/inventario/alertas/", ["/inventario/alertas/"]),
            ("inventario", "conteo_fisico", "Conteo físico", "/inventario/conteo-fisico/", ["/inventario/conteo-fisico/"]),
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
            ("rrhh", "empleados", "Empleados", "/rrhh/empleados/", ["/rrhh/empleados/"]),
            ("rrhh", "nomina", "Nómina", "/rrhh/nomina/", ["/rrhh/nomina/"]),
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
    for group in NAV_GROUPS:
        items = []
        active_group = False
        for module, submodule, label, url, prefixes in group["items"]:
            if not can_view_submodule(user, module, submodule):
                continue
            active = any(current_path.startswith(prefix) for prefix in prefixes)
            active_group = active_group or active
            items.append(
                {
                    "label": label,
                    "url": url,
                    "active": active,
                    "module": module,
                    "submodule": submodule,
                    "initial": label[:1],
                }
            )
        if items:
            groups.append(
                {
                    "key": group["key"],
                    "label": group["label"],
                    "items": items,
                    "active": active_group,
                }
            )
    return groups
