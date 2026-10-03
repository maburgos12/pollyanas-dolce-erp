from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

from orquestacion.services.agent_runtime import Goal, resolve_runtime_actor, run_agent_goal


class Command(BaseCommand):
    help = "Ejecuta el runtime mínimo de un agente real sobre un objetivo explícito."

    def add_arguments(self, parser):
        parser.add_argument("--goal", required=True, help="Tipo de objetivo registrado en orquestacion.")
        parser.add_argument("--event-id", type=int, required=True, help="ID de entidad a revisar.")
        parser.add_argument("--entity-type", default="", help="Modelo del objetivo; conciliación requiere ProductInventoryAuditCase.")
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
        parser.add_argument('--plan-month', action='store_true', help='Plan agrupado del mes del expediente, sin reinvestigar ni ejecutar.')
        parser.add_argument('--batch-size', type=int, default=10, help='Máximo de revisiones propuestas (1–10).')
        parser.add_argument('--after-case-id', type=int, default=0, help='Cursor del plan anterior.')
        parser.add_argument('--expected-plan-fingerprint', default='', help='Huella del plan anterior para reanudar sin omisiones.')

    def handle(self, *args, **options):
        if not options['plan_month'] and (options['after_case_id'] or options['expected_plan_fingerprint']
                                           or options['batch_size'] != 10):
            raise CommandError('Opciones de lote requieren --plan-month.')
        if options['plan_month'] and options['goal'] != 'reconciliation_guard':
            raise CommandError('--plan-month requiere reconciliation_guard.')
        actor = resolve_runtime_actor(str(options.get("username") or "").strip())
        if options.get("username") and actor is None:
            raise CommandError(f"No existe el usuario '{options['username']}'.")

        goal = Goal(
            goal_type=str(options["goal"]).strip(),
            objective=(str(options.get("objective") or "").strip() or "Ejecutar objetivo de agente"),
            agent_code=str(options.get("agent_code") or "").strip(),
            entity_type=str(options.get("entity_type") or "").strip(),
            entity_id=int(options["event_id"]),
            requested_action=str(options.get("requested_action") or "review").strip(),
            metadata=({'mode': 'plan_month', 'batch_size': options['batch_size'],
                       'after_case_id': options['after_case_id'],
                       'expected_plan_fingerprint': options['expected_plan_fingerprint']}
                      if options['plan_month'] else {}),
        )
        try:
            result = run_agent_goal(goal, actor=actor, base_dir=settings.BASE_DIR)
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
                        "next_step": result.observation.get("next_step"),
                        "mode": result.observation.get('mode', 'review'),
                        "plan": result.observation.get('plan'),
                        "blocking_findings": [finding.as_dict() for finding in result.blocking_findings],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        )
