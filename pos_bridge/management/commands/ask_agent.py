from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from pos_bridge.services.agent_query_service import PosAgentQueryService


class Command(BaseCommand):
    help = "Consulta al agente interno de pos_bridge en lenguaje natural."

    def add_arguments(self, parser):
        parser.add_argument("query", type=str, help="Pregunta en lenguaje natural.")
        parser.add_argument("--json", action="store_true", help="Imprime tambien el payload estructurado.")

    def handle(self, *args, **options):
        query = options["query"]
        show_json = bool(options.get("json"))

        service = PosAgentQueryService()
        result = service.process_query(query=query)

        self.stdout.write(result["answer"])
        self.stdout.write(f"Tipo: {result.get('query_type', 'general')}")
        if show_json:
            self.stdout.write(json.dumps(result.get("data", {}), indent=2, ensure_ascii=False, default=str))
