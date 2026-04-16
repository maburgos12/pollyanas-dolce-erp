from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import Departamento
from orquestacion.catalog import AGENTS, CAPABILITIES, RULES
from orquestacion.models import AgentCapability, AgentDefinition, OrchestrationRule


class Command(BaseCommand):
    help = "Carga o actualiza el catalogo inicial de agentes, capacidades y reglas de orquestacion."

    @transaction.atomic
    def handle(self, *args, **options):
        self._ensure_departments()
        agent_map = self._seed_agents()
        self._seed_capabilities(agent_map)
        self._seed_rules(agent_map)
        self.stdout.write(self.style.SUCCESS("Catalogo inicial de orquestacion sembrado correctamente."))

    def _ensure_departments(self) -> None:
        defaults = [
            ("OPS", "Operaciones"),
            ("VENTAS", "Ventas"),
            ("PROD", "Produccion"),
            ("COMPRAS", "Compras"),
            ("ADMIN", "Administracion"),
        ]
        for codigo, nombre in defaults:
            Departamento.objects.get_or_create(codigo=codigo, defaults={"nombre": nombre})

    def _seed_agents(self) -> dict[str, AgentDefinition]:
        agent_map: dict[str, AgentDefinition] = {}
        for payload in AGENTS:
            payload = dict(payload)
            department_code = payload.pop("owner_department_code", "")
            owner_department = None
            if department_code:
                owner_department = Departamento.objects.filter(codigo=department_code).first()
            agent, _ = AgentDefinition.objects.update_or_create(
                code=payload["code"],
                defaults={**payload, "owner_department": owner_department},
            )
            agent_map[agent.code] = agent
        return agent_map

    def _seed_capabilities(self, agent_map: dict[str, AgentDefinition]) -> None:
        for payload in CAPABILITIES:
            payload = dict(payload)
            agent_code = payload.pop("agent_code")
            agent = agent_map[agent_code]
            AgentCapability.objects.update_or_create(
                agent=agent,
                capability_key=payload["capability_key"],
                resource_key=payload["resource_key"],
                defaults=payload,
            )

    def _seed_rules(self, agent_map: dict[str, AgentDefinition]) -> None:
        for payload in RULES:
            payload = dict(payload)
            primary_agent_code = payload.pop("primary_agent_code")
            secondary_agent_code = payload.pop("secondary_agent_code", "")
            primary_agent = agent_map[primary_agent_code]
            secondary_agent = agent_map.get(secondary_agent_code) if secondary_agent_code else None
            OrchestrationRule.objects.update_or_create(
                code=payload["code"],
                defaults={
                    **payload,
                    "primary_agent": primary_agent,
                    "secondary_agent": secondary_agent,
                },
            )

