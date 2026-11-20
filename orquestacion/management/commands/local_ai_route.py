import json
import sys

from django.core.management.base import BaseCommand, CommandError
from orquestacion.services import local_router
from orquestacion.models import AgentTask


class Command(BaseCommand):
    help = "Asignación privada JSON de solicitudes de agentes a IA local, sin proveedor pagado."

    def add_arguments(self, parser):
        parser.add_argument("action", choices=["enable", "disable", "submit", "claim", "finish", "heartbeat", "status"])

    def handle(self, *args, **options):
        try:
            action = options["action"]
            if action in {"enable", "disable"}:
                result = local_router.configure(enabled=action == "enable")
            elif action == "status":
                result = local_router.panel()
            else:
                raw = sys.stdin.read(16385)
                if len(raw) > 16384:
                    raise ValueError("payload_too_large")
                request = json.loads(raw)
                if not isinstance(request, dict):
                    raise ValueError("invalid_request")
                result = getattr(local_router, action)(**request)
            self.stdout.write(json.dumps(result, ensure_ascii=False, allow_nan=False))
        except (ValueError, TypeError, KeyError, AgentTask.DoesNotExist) as exc:
            reason = str(exc) if type(exc) is ValueError and str(exc) in {"invalid_output", "invalid_usage", "expired_lease", "invalid_error"} else "unknown"
            raise CommandError(json.dumps({"router_error": reason})) from exc
