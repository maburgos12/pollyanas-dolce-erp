from __future__ import annotations

from django.conf import settings
from django.middleware.csrf import get_token
from django.shortcuts import redirect

from core.access import is_branch_capture_only


ERP_BUILD_TAG = "2026.03.13-enterprise-01"


class CanonicalLocalHostMiddleware:
    """
    Fuerza un solo host local para evitar sesiones, caché y cookies divergentes
    entre localhost y 127.0.0.1 durante desarrollo.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        host = request.get_host()
        canonical = getattr(settings, "CANONICAL_LOCAL_HOST", "127.0.0.1:8002")

        if host.startswith("localhost") and host != canonical:
            query = request.META.get("QUERY_STRING", "")
            target = f"{request.scheme}://{canonical}{request.path}"
            if query:
                target = f"{target}?{query}"
            return redirect(target, permanent=False)

        return self.get_response(request)


class BranchCaptureOnlyMiddleware:
    """
    Restringe usuarios en modo captura sucursal a su módulo operativo mínimo.
    """

    ALLOWED_PREFIXES = (
        "/recetas/reabasto-cedis/",
        "/logout/",
        "/login/",
        "/static/",
        "/favicon.ico",
    )

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        user = getattr(request, "user", None)
        path = request.path or "/"

        if (
            user
            and user.is_authenticated
            and is_branch_capture_only(user)
            and not any(path.startswith(prefix) for prefix in self.ALLOWED_PREFIXES)
        ):
            return redirect("/recetas/reabasto-cedis/captura/")

        return self.get_response(request)


class EnsureCSRFCookieOnHtmlMiddleware:
    """
    Fuerza la emisión de cookie CSRF en vistas HTML seguras para evitar
    sesiones que navegan bien pero fallan en el primer POST por falta de cookie.
    """

    SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        accept = (request.headers.get("Accept") or "").lower()
        wants_html = "text/html" in accept or "*/*" in accept or not accept

        if request.method in self.SAFE_METHODS and wants_html:
            get_token(request)

        response = self.get_response(request)
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type:
            response["X-ERP-Build"] = ERP_BUILD_TAG
        return response
