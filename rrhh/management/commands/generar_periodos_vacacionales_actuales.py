import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from rrhh.models import Empleado
from rrhh.services_vacaciones_aniversarios import asegurar_periodos_actuales


class Command(BaseCommand):
    help = 'Simula o genera únicamente el último aniversario cumplido del personal activo.'

    def add_arguments(self, parser):
        parser.add_argument('--empleado-id', type=int, action='append')
        parser.add_argument('--ejecutar', action='store_true')
        parser.add_argument('--actor-id', type=int)
        parser.add_argument('--referencia', default='comando-aniversarios')

    def handle(self, *args, **options):
        ids = options['empleado_id']
        if ids is not None:
            existentes = set(Empleado.objects.filter(pk__in=ids, activo=True).values_list('pk', flat=True))
            if existentes != set(ids):
                raise CommandError('Hay empleados inexistentes o inactivos en el alcance solicitado.')
        actor = None
        if options['actor_id']:
            actor = get_user_model().objects.filter(pk=options['actor_id'], is_active=True).first()
            if actor is None:
                raise CommandError('Actor inexistente o inactivo.')
        resultados = asegurar_periodos_actuales(
            empleado_ids=ids, ejecutar=options['ejecutar'], actor=actor, referencia=options['referencia'],
        )
        self.stdout.write(json.dumps(resultados, ensure_ascii=False))
        if any(r['estado'] == 'revision' for r in resultados):
            raise CommandError('Hay aniversarios pendientes de conciliación; consulta el resultado por empleado.')
