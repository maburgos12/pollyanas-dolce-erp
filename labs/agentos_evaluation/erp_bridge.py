"""Trusted subprocess bridge to existing gateway; isolated PostgreSQL only."""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if os.environ.get("DATABASE_URL") != "postgresql://postgres:postgres@127.0.0.1:56673/pastelerias_erp":
    raise RuntimeError("evaluation_database_required")
import django
django.setup()

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from rest_framework.exceptions import ValidationError
from api.ai_gateway_services import invoke_read_shadow_tool
from activos.models import Activo
from core.models import Sucursal, UserProfile


def seed():
    User = get_user_model()
    manager, _ = User.objects.get_or_create(username="agentos-lab-manager", defaults={"is_superuser": True, "is_staff": True})
    operator, _ = User.objects.get_or_create(username="agentos-lab-operator")
    for code, name in [("LAB-A", "Matriz laboratorio"), ("LAB-B", "Guamúchil laboratorio")]:
        branch, _ = Sucursal.objects.get_or_create(codigo=code, defaults={"nombre": name})
        Activo.objects.get_or_create(codigo=code + "-3", defaults={"nombre": "Batidora 3", "sucursal": branch,
            "estado": "MANTENIMIENTO" if code == "LAB-A" else "OPERATIVO",
            "notas": "Datos sintéticos; no corresponden a equipos reales."})
        if code == "LAB-A":
            UserProfile.objects.get_or_create(user=operator, defaults={"sucursal": branch})
    return {"manager": manager.pk, "operator": operator.pk, "fixture_assets": 2}


if __name__ == "__main__":
    request = json.load(sys.stdin)
    if request.get("action") == "seed":
        result = seed()
    else:
        manager = get_user_model().objects.get(username="agentos-lab-manager")
        settings.AI_GATEWAY_ASSETS_ENABLED = True
        settings.AI_AGENT_PILOT_USER_ID = manager.pk
        actor = request.get("actor")
        names = {"lab-manager": "agentos-lab-manager", "lab-operator": "agentos-lab-operator"}
        try:
            if actor not in names:
                raise PermissionDenied("unknown_evaluation_actor")
            user = get_user_model().objects.get(username=names[actor])
            result = invoke_read_shadow_tool(user=user, tool_key=request.get("tool"), arguments=request.get("arguments"), mode="READ")
            result["environment"] = "SYNTHETIC_LOCAL_FIXTURES"
        except PermissionDenied:
            result = {"status": "denied", "error": "permission_denied"}
        except ValidationError:
            result = {"status": "error", "error": "invalid_arguments"}
    print(json.dumps(result, ensure_ascii=False, default=str))
