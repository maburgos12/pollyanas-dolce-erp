from __future__ import annotations

from django.shortcuts import redirect

from core.access import is_branch_capture_only


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
            return redirect("/recetas/reabasto-cedis/")

        return self.get_response(request)

