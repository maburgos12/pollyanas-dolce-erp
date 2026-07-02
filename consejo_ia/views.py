from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render

from core.access import can_view_consejo_ia

from .models import ConsejoConsulta
from .services import ROLES, analizar_pregunta

PREGUNTA_MIN_LENGTH = 10


def _require_view_consejo_ia(user):
    if not can_view_consejo_ia(user):
        raise PermissionDenied("No tienes permisos para ver el Consejo Estratégico de IA")


@login_required
def consejo_ia_home(request):
    _require_view_consejo_ia(request.user)

    consulta = None
    resultados_roles = None
    error = None

    if request.method == "POST":
        pregunta = (request.POST.get("pregunta") or "").strip()
        if len(pregunta) < PREGUNTA_MIN_LENGTH:
            error = f"Escribe una pregunta más completa (mínimo {PREGUNTA_MIN_LENGTH} caracteres)."
        else:
            consulta = analizar_pregunta(pregunta, usuario=request.user)
            resultados_roles = [
                {"nombre": rol["nombre"], "respuesta": consulta.respuestas_json.get(rol["codigo"], {})}
                for rol in ROLES
            ]

    historial = ConsejoConsulta.objects.all()[:20]

    return render(
        request,
        "consejo_ia/home.html",
        {
            "consulta": consulta,
            "resultados_roles": resultados_roles,
            "error": error,
            "historial": historial,
        },
    )
