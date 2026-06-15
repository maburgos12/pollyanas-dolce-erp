from pathlib import Path

from django.core.checks import Error, register


LOGISTICA_DIR = Path(__file__).resolve().parent
PWA_TEMPLATE = LOGISTICA_DIR / "templates" / "logistica" / "pwa.html"
PWA_SERVICE_WORKER = LOGISTICA_DIR / "static" / "logistica" / "pwa" / "sw.js"

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
    "route-control-v20": "versionado del service worker para forzar actualizacion de la PWA",
}

REQUIRED_SERVICE_WORKER_MARKERS = {
    "pollyanas-logistica-pwa-v20-sw-no-cache": "cache versionado de la PWA",
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
