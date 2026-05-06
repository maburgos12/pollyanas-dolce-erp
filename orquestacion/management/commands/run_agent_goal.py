from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from orquestacion.services.agent_runtime import Goal, resolve_runtime_actor, run_agent_goal


class Command(BaseCommand):
    help = "Ejecuta el runtime mínimo de un agente real sobre un objetivo explícito."

    def add_arguments(self, parser):
        parser.add_argument("--goal", required=True, help="Tipo de objetivo registrado en orquestacion.")
        parser.add_argument("--event-id", type=int, required=True, help="ID de entidad a revisar.")
        parser.add_argument(
            "--agent-code",
            default="",
            help="Código de agente a forzar. Si se omite, el runtime infiere el agente correcto por goal_type.",
        )
        parser.add_argument(
            "--requested-action",
            default="review",
            choices=["review", "publish_if_safe"],
            help="Acción que el loop debe intentar después de validar bloqueos.",
        )
        parser.add_argument("--username", default="", help="Usuario que dispara la ejecución para trazabilidad.")
        parser.add_argument("--objective", default="", help="Descripción humana del objetivo.")

    def handle(self, *args, **options):
        actor = resolve_runtime_actor(str(options.get("username") or "").strip())
        if options.get("username") and actor is None:
            raise CommandError(f"No existe el usuario '{options['username']}'.")

        goal = Goal(
            goal_type=str(options["goal"]).strip(),
            objective=(str(options.get("objective") or "").strip() or "Ejecutar objetivo de agente"),
            agent_code=str(options.get("agent_code") or "").strip(),
            entity_type="",
            entity_id=int(options["event_id"]),
            requested_action=str(options.get("requested_action") or "review").strip(),
        )
        try:
            result = run_agent_goal(goal, actor=actor)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            self.style.SUCCESS(
                json.dumps(
                    {
                        "run_id": result.run_id,
                        "task_id": result.task_id,
                        "status": result.status,
                        "decision": result.decision,
                        "blocking_findings": [finding.as_dict() for finding in result.blocking_findings],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        )
