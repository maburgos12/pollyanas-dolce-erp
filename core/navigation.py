from core.access import can_review_seguimiento_global, can_view_reportes, can_view_submodule


def _puede_capturar_presupuesto(user) -> bool:
    """Responsables de área de presupuesto (o perfiles de reportes).

    Import perezoso: core no debe depender de reportes al cargar.
    """
    if not (user and user.is_authenticated):
        return False
    if can_view_reportes(user):
        return True
    from reportes.models import AreaPresupuestoResponsable

    return AreaPresupuestoResponsable.objects.filter(
        usuario=user, puede_capturar=True, area__activa=True
    ).exists()


NAV_GROUPS = [
    {
        "key": "mi_trabajo",
        "label": "Mi trabajo",
        "url": "/seguimiento/",
        "items": [
            ("seguimiento", "minutas", "Minutas", "/seguimiento/minutas/", ["/seguimiento/minutas/"]),
            ("seguimiento", "proyectos", "Proyectos", "/seguimiento/proyectos/", ["/seguimiento/proyectos/"]),
            ("seguimiento", "compromisos", "Compromisos", "/seguimiento/compromisos/", ["/seguimiento/compromisos/"]),
            ("seguimiento", "calendario", "Mi calendario", "/seguimiento/calendario/", ["/seguimiento/calendario/"]),
        ],
    },
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
            (
                "ventas",
                "pronostico",
                "Pronóstico",
                "/ventas/pronostico/?tab=pronosticos",
                ["/ventas/pronostico/?tab=pronosticos", "/ventas/pronostico/", "/ventas/"],
            ),
            (
                "ventas",
                "pronostico",
                "Proyecciones",
                "/ventas/pronostico/?tab=proyecciones",
                ["/ventas/pronostico/?tab=proyecciones"],
            ),
            ("ventas", "bonos", "Bonos ventas", "/bonos-ventas/dashboard/", ["/bonos-ventas/"]),
            ("ventas", "visitas_sucursal", "Visitas a sucursal", "/visitas-sucursal/", ["/visitas-sucursal/"]),
            ("crm", "dashboard", "CRM", "/crm/dashboard/", ["/crm/dashboard/"]),
            ("crm", "clientes", "Clientes", "/crm/clientes/", ["/crm/clientes/"]),
            ("crm", "pedidos", "Pedidos", "/crm/pedidos/", ["/crm/pedidos/"]),
        ],
    },
    {
        "key": "fallas",
        "label": "Fallas",
        "items": [
            ("fallas", "dashboard", "Reportes de fallas", "/fallas/", ["/fallas/"]),
            ("fallas", "reportar", "Reportar falla", "/fallas/reportar/", ["/fallas/reportar/"]),
            ("fallas", "mis_reportes", "Reportes", "/fallas/mis-reportes/", ["/fallas/mis-reportes/"]),
            ("fallas", "categorias", "Categorías", "/fallas/?tab=categorias", ["/fallas/?tab=categorias"]),
        ],
    },
    {
        "key": "produccion",
        "label": "Producción",
        "items": [
            ("produccion", "plan", "Plan de producción", "/recetas/plan-produccion/", ["/recetas/plan-produccion/"]),
            (
                "produccion",
                "calculo_insumos",
                "Cálculo de insumos",
                "/recetas/plan-produccion/?seccion=calculo_insumos#calculo-insumos",
                ["/recetas/plan-produccion/?seccion=calculo_insumos"],
            ),
            ("produccion", "bonos", "Bonos producción", "/bonos-produccion/dashboard/", ["/bonos-produccion/"]),
            ("produccion", "reabasto_cedis", "Reabasto CEDIS", "/recetas/reabasto-cedis/", ["/recetas/reabasto-cedis/"]),
            ("produccion", "consolidado_cedis", "Consolidado CEDIS", "/recetas/consolidado-cedis/", ["/recetas/consolidado-cedis/"]),
            ("produccion", "cedis_semanal", "Producción CEDIS semanal", "/recetas/produccion-cedis/semanal/", ["/recetas/produccion-cedis/semanal/"]),
            (
                "produccion",
                "mano_obra_area",
                "Mano de obra por área",
                "/reportes/mano-obra-area/reporte/",
                ["/reportes/mano-obra-area/"],
            ),
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
        "key": "mantenimiento",
        "label": "Mantenimiento",
        "items": [
            ("mantenimiento", "dashboard", "Mantenimiento", "/mantenimiento/", ["/mantenimiento/"]),
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
            ("logistica", "reportes", "Reportes", "/logistica/reportes/", ["/logistica/reportes/"]),
            ("logistica", "bitacoras", "Bitácoras", "/logistica/bitacoras/", ["/logistica/bitacoras/"]),
            ("logistica", "rutas", "Rutas", "/logistica/rutas/", ["/logistica/rutas/"]),
            ("logistica", "rutas", "Control rutas", "/logistica/rutas/control/", ["/logistica/rutas/control/"]),
            ("logistica", "rutas", "Puntos", "/logistica/rutas/puntos/", ["/logistica/rutas/puntos/"]),
            ("logistica", "unidades", "Unidades", "/logistica/unidades/", ["/logistica/unidades/"]),
            ("logistica", "capturas", "Capturas", "/logistica/capturas/", ["/logistica/capturas/"]),
            ("mermas", "dashboard", "Mermas", "/mermas/", ["/mermas/"]),
            ("mermas", "captura", "Captura merma", "/mermas/nuevo/", ["/mermas/nuevo/"]),
        ],
    },
    {
        "key": "capital_humano",
        "label": "Capital Humano",
        "items": [
            ("rrhh", "dashboard", "Indicadores", "/rrhh/indicadores/", ["/rrhh/indicadores/"]),
            ("rrhh", "organizacion", "Organización", "/rrhh/organizacion/", ["/rrhh/organizacion/"]),
            ("rrhh", "catalogos", "Catálogos", "/rrhh/catalogos/", ["/rrhh/catalogos/"]),
            ("rrhh", "empleados", "Empleados", "/rrhh/empleados/", ["/rrhh/empleados/"]),
            ("rrhh", "permisos", "Permisos", "/rrhh/permisos/", ["/rrhh/permisos/"]),
            ("rrhh", "suspensiones", "Suspensiones", "/rrhh/suspensiones/", ["/rrhh/suspensiones/"]),
            ("rrhh", "incapacidades", "Incapacidades", "/rrhh/incapacidades/", ["/rrhh/incapacidades/"]),
            ("rrhh", "vacaciones", "Vacaciones", "/rrhh/vacaciones/", ["/rrhh/vacaciones/"]),
            ("rrhh", "horas_extra", "Horas extra", "/rrhh/horas-extra/", ["/rrhh/horas-extra/"]),
            ("rrhh", "asistencias", "Asistencias", "/rrhh/asistencias/", ["/rrhh/asistencias/"]),
            ("rrhh", "reporte_asistencia", "Reporte asistencia", "/rrhh/reporte-asistencia/", ["/rrhh/reporte-asistencia/"]),
            ("rrhh", "vacantes", "Vacantes", "/rrhh/vacantes/", ["/rrhh/vacantes/"]),
            ("rrhh", "prestamos", "Préstamos", "/rrhh/prestamos/", ["/rrhh/prestamos/"]),
            ("rrhh", "nomina", "Nómina", "/rrhh/nomina/", ["/rrhh/nomina/"]),
            ("rrhh", "prenomina", "Prenómina", "/rrhh/prenomina/", ["/rrhh/prenomina/"]),
            ("rrhh", "importar_checador", "Checador", "/rrhh/importar-checador/", ["/rrhh/importar-checador/"]),
            ("rrhh", "asignacion_sucursal", "Asignación sucursal", "/rrhh/asignacion-sucursal/", ["/rrhh/asignacion-sucursal/"]),
        ],
    },
    {
        "key": "administracion",
        "label": "Administración",
        "items": [
            ("activos", "dashboard", "Activos", "/activos/dashboard/", ["/activos/dashboard/"]),
            ("activos", "seguimiento", "Bandeja Compras", "/activos/seguimiento/", ["/activos/seguimiento/"]),
            ("activos", "catalogo", "Catálogo activos", "/activos/activos/", ["/activos/activos/"]),
            ("activos", "planes", "Planes", "/activos/planes/", ["/activos/planes/"]),
            ("activos", "ordenes", "Órdenes activos", "/activos/ordenes/", ["/activos/ordenes/"]),
            ("activos", "reportes", "Reportes activos", "/activos/reportes/", ["/activos/reportes/"]),
            ("control", "discrepancias", "Control", "/control/discrepancias/", ["/control/discrepancias/"]),
            ("control", "captura_movil", "Captura móvil", "/control/captura-movil/", ["/control/captura-movil/"]),
            ("conciliacion", "bancaria", "Conciliación bancaria", "/conciliacion/bancaria/", ["/conciliacion/bancaria/"]),
            ("conciliacion", "fiscal", "SAT fiscal", "/sat/", ["/sat/"]),
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
            ("sistema", "syncfy", "Bancos Syncfy", "/syncfy/bancos/", ["/syncfy/bancos/"]),
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
        # Entradas con permiso propio (no dependen del acceso por módulo):
        # la captura de presupuesto la usan responsables de área que pueden
        # no tener el módulo de reportes; el tablero es de dirección.
        if group["key"] == "mi_trabajo" and user and user.is_authenticated:
            from reportes.views_presupuesto_real import _puede_subir_cedulas

            if _puede_subir_cedulas(user):
                url_cedula = "/reportes/presupuesto-real/cedula-imss/"
                match_len = len(url_cedula) if current_path.startswith(url_cedula) else 0
                best_match_len = max(best_match_len, match_len)
                items.append(
                    {
                        "label": "Cédulas IMSS",
                        "url": url_cedula,
                        "active": False,
                        "_match_len": match_len,
                        "module": "reportes",
                        "submodule": "cedula_imss",
                        "initial": "I",
                    }
                )
        if group["key"] == "mi_trabajo" and _puede_capturar_presupuesto(user):
            url_captura = "/reportes/presupuesto-real/captura/"
            match_len = len(url_captura) if current_path.startswith(url_captura) else 0
            best_match_len = max(best_match_len, match_len)
            items.append(
                {
                    "label": "Captura de presupuesto",
                    "url": url_captura,
                    "active": False,
                    "_match_len": match_len,
                    "module": "reportes",
                    "submodule": "presupuesto_real_captura",
                    "initial": "C",
                }
            )
        if group["key"] == "direccion" and user and user.is_authenticated and can_view_reportes(user):
            url_tablero = "/reportes/presupuesto-vs-real/"
            match_len = len(url_tablero) if current_path.startswith(url_tablero) else 0
            best_match_len = max(best_match_len, match_len)
            items.append(
                {
                    "label": "Presupuesto vs Real",
                    "url": url_tablero,
                    "active": False,
                    "_match_len": match_len,
                    "module": "reportes",
                    "submodule": "presupuesto_vs_real",
                    "initial": "P",
                }
            )
            url_estado = "/reportes/estado-resultados/"
            match_len = len(url_estado) if current_path.startswith(url_estado) else 0
            best_match_len = max(best_match_len, match_len)
            items.append(
                {
                    "label": "Estado de resultados",
                    "url": url_estado,
                    "active": False,
                    "_match_len": match_len,
                    "module": "reportes",
                    "submodule": "estado_resultados",
                    "initial": "E",
                }
            )
        if items:
            group_url = group.get("url")
            group_active = bool(
                group_url
                and current_path.rstrip("/") == group_url.rstrip("/")
            )
            visible_groups.append(
                {
                    "key": group["key"],
                    "label": group["label"],
                    "url": group_url,
                    "items": items,
                    "active": group_active,
                }
            )
    if user and user.is_authenticated:
        try:
            from core.models import Notificacion

            notificaciones_pendientes = Notificacion.objects.filter(usuario=user, leida=False).count()
        except Exception:
            notificaciones_pendientes = 0
        match_len = len("/notificaciones/") if current_path.startswith("/notificaciones/") else 0
        best_match_len = max(best_match_len, match_len)
        mi_trabajo = next((group for group in visible_groups if group["key"] == "mi_trabajo"), None)
        if mi_trabajo is None:
            mi_trabajo = {"key": "mi_trabajo", "label": "Mi trabajo", "items": [], "active": False}
            visible_groups.insert(0, mi_trabajo)
        mi_trabajo["badge_count"] = notificaciones_pendientes
        mi_trabajo["items"].insert(
            0,
            {
                "label": "Notificaciones",
                "url": "/notificaciones/",
                "active": False,
                "_match_len": match_len,
                "module": "core",
                "submodule": "notificaciones",
                "initial": "N",
                "badge_count": notificaciones_pendientes,
            },
        )
        try:
            from rrhh.api_views import empleado_de_usuario

            empleado_actual = empleado_de_usuario(user)
        except Exception:
            empleado_actual = None
        if empleado_actual:
            match_len = len("/rrhh/app/") if current_path.startswith("/rrhh/app/") else 0
            best_match_len = max(best_match_len, match_len)
            mi_trabajo["items"].append(
                {
                    "label": "Mis solicitudes",
                    "url": "/rrhh/app/",
                    "active": False,
                    "_match_len": match_len,
                    "module": "rrhh",
                    "submodule": "autoservicio",
                    "initial": "S",
                }
            )
        try:
            from rrhh.services_vacantes import can_solicitar_vacantes

            puede_solicitar_vacantes = can_solicitar_vacantes(user)
        except Exception:
            puede_solicitar_vacantes = False
        if puede_solicitar_vacantes:
            match_len = len("/rrhh/vacantes/") if current_path.startswith("/rrhh/vacantes/") else 0
            best_match_len = max(best_match_len, match_len)
            mi_trabajo["items"].append(
                {
                    "label": "Solicitar vacante",
                    "url": "/rrhh/vacantes/nueva/",
                    "active": False,
                    "_match_len": match_len,
                    "module": "rrhh",
                    "submodule": "vacantes",
                    "initial": "V",
                }
            )
        try:
            from rrhh.models import Prestamo
            from rrhh.services_prestamos import prestamos_jefe_q

            tiene_prestamos_por_autorizar = Prestamo.objects.filter(
                prestamos_jefe_q(user),
                estado=Prestamo.ESTADO_SOLICITADO,
            ).exists()
        except Exception:
            tiene_prestamos_por_autorizar = False
        if tiene_prestamos_por_autorizar:
            match_len = len("/rrhh/prestamos/") if current_path.startswith("/rrhh/prestamos/") else 0
            best_match_len = max(best_match_len, match_len)
            mi_trabajo["items"].append(
                {
                    "label": "Préstamos por autorizar",
                    "url": "/rrhh/prestamos/",
                    "active": False,
                    "_match_len": match_len,
                    "module": "rrhh",
                    "submodule": "prestamos",
                    "initial": "P",
                }
            )
        try:
            from rrhh.services_vacantes import vacantes_por_autorizar_count

            vacantes_por_autorizar = vacantes_por_autorizar_count(user)
        except Exception:
            vacantes_por_autorizar = 0
        if vacantes_por_autorizar:
            match_len = len("/rrhh/vacantes/") if current_path.startswith("/rrhh/vacantes/") else 0
            best_match_len = max(best_match_len, match_len)
            mi_trabajo["items"].append(
                {
                    "label": "Vacantes por autorizar",
                    "url": "/rrhh/vacantes/",
                    "active": False,
                    "_match_len": match_len,
                    "module": "rrhh",
                    "submodule": "vacantes",
                    "initial": "V",
                    "badge_count": vacantes_por_autorizar,
                }
            )
        try:
            from rrhh.models import SolicitudVacaciones
            from rrhh.services_vacaciones import vacaciones_jefe_q

            vacaciones_por_autorizar = SolicitudVacaciones.objects.filter(
                vacaciones_jefe_q(user),
                estado=SolicitudVacaciones.ESTADO_SOLICITADA,
            ).count()
        except Exception:
            vacaciones_por_autorizar = 0
        if vacaciones_por_autorizar:
            match_len = len("/rrhh/vacaciones/") if current_path.startswith("/rrhh/vacaciones/") else 0
            best_match_len = max(best_match_len, match_len)
            mi_trabajo["items"].append(
                {
                    "label": "Vacaciones por autorizar",
                    "url": "/rrhh/vacaciones/",
                    "active": False,
                    "_match_len": match_len,
                    "module": "rrhh",
                    "submodule": "vacaciones",
                    "initial": "V",
                    "badge_count": vacaciones_por_autorizar,
                }
            )
        try:
            from rrhh.models import HoraExtra

            horas_extra_qs = HoraExtra.objects.filter(estado=HoraExtra.ESTADO_PENDIENTE)
            if not user.is_superuser:
                horas_extra_qs = horas_extra_qs.filter(jefe_directo=user)
            horas_extra_por_autorizar = horas_extra_qs.count()
        except Exception:
            horas_extra_por_autorizar = 0
        if horas_extra_por_autorizar:
            match_len = len("/rrhh/horas-extra/") if current_path.startswith("/rrhh/horas-extra/") else 0
            best_match_len = max(best_match_len, match_len)
            mi_trabajo["items"].append(
                {
                    "label": "Horas extra por autorizar",
                    "url": "/rrhh/horas-extra/",
                    "active": False,
                    "_match_len": match_len,
                    "module": "rrhh",
                    "submodule": "horas_extra",
                    "initial": "H",
                    "badge_count": horas_extra_por_autorizar,
                }
            )
        if can_review_seguimiento_global(user):
            match_len = len("/seguimiento/panel/") if current_path.startswith("/seguimiento/panel/") else 0
            best_match_len = max(best_match_len, match_len)
            mi_trabajo["items"].append(
                {
                    "label": "Panel de acuerdos",
                    "url": "/seguimiento/panel/",
                    "active": False,
                    "_match_len": match_len,
                    "module": "seguimiento",
                    "submodule": "panel_dg",
                    "initial": "P",
                }
            )
    for group in visible_groups:
        active_group = False
        for item in group["items"]:
            item["active"] = bool(best_match_len and item.pop("_match_len") == best_match_len)
            active_group = active_group or item["active"]
        group["active"] = group["active"] or active_group
        groups.append(group)
    return groups
