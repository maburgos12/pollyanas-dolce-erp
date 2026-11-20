import json
import sys

from django.core.management.base import BaseCommand, CommandError
from orquestacion.services import local_ai


class Command(BaseCommand):
    help = "Cola técnica Mac-IA-02; interfaz JSON privada para el ejecutor del VPS."

    def add_arguments(self, parser):
        parser.add_argument("action", choices=["enable", "disable", "enqueue", "claim", "finish", "heartbeat", "status"])

    def handle(self, *args, **options):
        action = options["action"]
        try:
            if action in {"enable", "disable"}:
                result = local_ai.configure(enabled=action == "enable")
            elif action == "finish":
                raw = sys.stdin.read(16385)
                if len(raw) > 16384:
                    raise ValueError("payload_too_large")
                result = local_ai.finish(**json.loads(raw))
            elif action == "heartbeat":
                result = local_ai.heartbeat(sys.stdin.read(32).strip())
            else:
                result = {"enqueue": local_ai.enqueue, "claim": local_ai.claim, "status": local_ai.panel}[action]()
            self.stdout.write(json.dumps(result, default=str, ensure_ascii=False))
        except (ValueError, TypeError, KeyError) as exc:
            raise CommandError("Invalid local job request") from exc
