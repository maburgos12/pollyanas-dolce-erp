from django.core.management.base import BaseCommand, CommandError

from orquestacion.models import AgentDefinition, OrchestrationRule
from orquestacion.services.hours_intent import AGENT
from orquestacion.services.local_router import RULE


class Command(BaseCommand):
    help = 'Activa o desactiva la interpretación local de correcciones Telegram.'

    def add_arguments(self, parser):
        parser.add_argument('action', choices=['enable', 'disable'])

    def handle(self, *args, **options):
        enabled = options['action'] == 'enable'
        if enabled and not OrchestrationRule.objects.filter(code=RULE, is_active=True).exists():
            raise CommandError('El router IA local no está activo.')
        agent, _ = AgentDefinition.objects.get_or_create(code=AGENT, defaults={
            'name': 'Horarios de sucursales por Telegram',
            'domain': 'operacion',
            'status': 'active' if enabled else 'paused',
            'description': 'Interpreta correcciones para una vista previa; no publica horarios.',
        })
        agent.status = 'active' if enabled else 'paused'
        agent.save(update_fields=['status', 'updated_at'])
        self.stdout.write('Interpretación local de horarios: ' + ('activa.' if enabled else 'pausada.'))
