import ast
from pathlib import Path

from django.core.checks import Error, Warning, register
from django.utils import timezone

from logistica.pwa_compat import v59_compat_deadline


LOGISTICA_DIR = Path(__file__).resolve().parent
PWA_TEMPLATE = LOGISTICA_DIR / "templates" / "logistica" / "pwa.html"
PWA_SERVICE_WORKER = LOGISTICA_DIR / "static" / "logistica" / "pwa" / "sw.js"
PROJECT_DIR = LOGISTICA_DIR.parent
CRITICAL_PARADA_FIELDS = {
    "estado", "entrega_estado", "entrega_confirmada_en", "entrega_confirmada_por",
    "hora_llegada_real", "hora_salida_real", "revision_entrega_estado",
}
# Esta lista es deliberadamente por funcion, no por archivo: agregar un helper nuevo
# a uno de estos modulos no le concede permiso implicito para mutar estado operativo.
ALLOWED_CRITICAL_WRITERS = {
    "logistica/services_entregas.py": {"confirmar_entrega_parada", "revisar_entrega_excepcional"},
    "logistica/services_rutas_control.py": {"_marcar_visitada_por_permanencia"},
    "logistica/services_carga_ruta.py": {"registrar_recarga_cedis"},
}


@register()
def logistica_v59_compat_window(app_configs, **kwargs):
    try:
        deadline = v59_compat_deadline()
    except ValueError as exc:
        return [Error(str(exc), id="logistica.E911")]
    if deadline is not None and timezone.now() > deadline:
        return [Warning(
            "La ventana de compatibilidad PWA v59 ya vencio.",
            hint="Deja LOGISTICA_PWA_V59_COMPAT_UNTIL vacia para deshabilitarla explicitamente.",
            id="logistica.W911",
        )]
    return []


def critical_parada_writes_in_source(source: str, relative_path: str) -> list[tuple[int, str]]:
    tree = ast.parse(source, filename=relative_path)
    findings = []
    parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    def is_allowed(node):
        allowed_functions = ALLOWED_CRITICAL_WRITERS.get(relative_path, set())
        current = node
        while current is not None:
            if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return current.name in allowed_functions
            current = parents.get(current)
        return False

    def add(node, field):
        if field in CRITICAL_PARADA_FIELDS and not is_allowed(node):
            findings.append((node.lineno, field))

    def string_values(node):
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return [item.value for item in node.elts if isinstance(item, ast.Constant) and isinstance(item.value, str)]
        if isinstance(node, ast.Dict):
            return [key.value for key in node.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)]
        return []

    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Attribute):
                    add(node, target.attr)
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id == "setattr" and len(node.args) >= 2:
            field_arg = node.args[1]
            if isinstance(field_arg, ast.Constant) and isinstance(field_arg.value, str):
                add(node, field_arg.value)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "bulk_update" and len(node.args) >= 2:
            for field in string_values(node.args[1]):
                add(node, field)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "update":
            for keyword in node.keywords:
                if keyword.arg:
                    add(node, keyword.arg)
                else:
                    for field in string_values(keyword.value):
                        add(node, field)
        if isinstance(node.func, ast.Attribute) and node.func.attr in {"execute", "executemany"} and node.args:
            sql = node.args[0]
            if isinstance(sql, ast.Constant) and isinstance(sql.value, str):
                normalized = sql.value.lower()
                if "update" in normalized and "paradaruta" in normalized:
                    for field in CRITICAL_PARADA_FIELDS:
                        if field.lower() in normalized:
                            add(node, field)
    return findings


@register()
def logistica_critical_writer_guard(app_configs, **kwargs):
    errors = []
    for root in (PROJECT_DIR / "logistica", PROJECT_DIR / "api"):
        for path in root.rglob("*.py"):
            relative = path.relative_to(PROJECT_DIR).as_posix()
            if "migrations/" in relative or path.name.startswith("test"):
                continue
            try:
                findings = critical_parada_writes_in_source(path.read_text(encoding="utf-8"), relative)
            except (OSError, SyntaxError):
                continue
            for line, field in findings:
                errors.append(Error(
                    f"Escritura directa de ParadaRuta.{field} fuera del servicio autorizado: {relative}:{line}",
                    hint="Mueve la transición a services_entregas, services_rutas_control o services_carga_ruta.",
                    id="logistica.E910",
                ))
    return errors

REMOVED_PWA_MARKERS = {
    "cierreCombustibleExcepcionUnica": "referencia a excepcion de cierre de Jorge eliminada",
    "cierreJorgeModal": "popup de cierre especial de Jorge eliminado",
    "avisoKmJorgeModal": "popup operativo de Jorge eliminado",
    "cierreJorgePermitido": "bandera residual de excepcion de Jorge eliminada",
    "pd_cierre_jorge": "storage key de popup de cierre de Jorge eliminado",
    "pd_aviso_km_jorge": "storage key de popup operativo de Jorge eliminado",
    "pd_combustible_recordatorio": "recordatorio diario no critico de combustible eliminado",
    "Ya puedes registrar gasolina durante el turno": "popup recordatorio no critico de combustible eliminado",
    "Aviso unico para Jorge": "texto de popup especifico de Jorge eliminado",
    "Aviso único para Jorge": "texto de popup especifico de Jorge eliminado",
    "Revisa tu cierre de turno": "texto de popup operativo de Jorge eliminado",
}

REQUIRED_TEMPLATE_MARKERS = {
    "route-control-v64-route-invariants": "versionado exacto del service worker para forzar actualizacion de la PWA",
}

REQUIRED_SERVICE_WORKER_MARKERS = {
    "pollyanas-logistica-pwa-v64-route-invariants": "cache versionado de la PWA",
    'event.request.mode === "navigate"': "estrategia network-first para navegacion",
    'url.pathname === "/logistica/app/"': "estrategia network-first para el app shell",
}


def _read_text(path):
    try:
        return path.read_text(encoding="utf-8"), None
    except OSError as exc:
        return None, exc


@register()
def logistica_pwa_guardrails(app_configs, **kwargs):
    errors = []

    template_text, template_error = _read_text(PWA_TEMPLATE)
    if template_error:
        errors.append(
            Error(
                f"No se pudo leer la PWA de logistica: {template_error}",
                hint="Verifica que logistica/templates/logistica/pwa.html exista.",
                id="logistica.E901",
            )
        )
        return errors

    for marker, reason in REMOVED_PWA_MARKERS.items():
        if marker in template_text:
            errors.append(
                Error(
                    f"Marcador eliminado presente en pwa.html: {marker}",
                    hint=f"Retira este marcador; razon: {reason}.",
                    id="logistica.E902",
                )
            )

    for marker, reason in REQUIRED_TEMPLATE_MARKERS.items():
        if marker not in template_text:
            errors.append(
                Error(
                    f"Falta marcador requerido en pwa.html: {marker}",
                    hint=f"Este marcador protege el flujo de cierre de turno; razon: {reason}.",
                    id="logistica.E903",
                )
            )

    service_worker_text, service_worker_error = _read_text(PWA_SERVICE_WORKER)
    if service_worker_error:
        errors.append(
            Error(
                f"No se pudo leer el service worker de logistica: {service_worker_error}",
                hint="Verifica que logistica/static/logistica/pwa/sw.js exista.",
                id="logistica.E904",
            )
        )
        return errors

    for marker, reason in REQUIRED_SERVICE_WORKER_MARKERS.items():
        if marker not in service_worker_text:
            errors.append(
                Error(
                    f"Falta marcador requerido en sw.js: {marker}",
                    hint=f"Este marcador evita que los celulares queden usando una PWA vieja; razon: {reason}.",
                    id="logistica.E905",
                )
            )

    return errors
