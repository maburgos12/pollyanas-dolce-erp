"""Simula por defecto. Aplicar exige actor DG y digest vigente exacto."""
import json
from pathlib import Path
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.management.base import BaseCommand, CommandError
from rrhh.services_contingencia import previsualizar_contingencia, aplicar_contingencia


class Command(BaseCommand):
    help = 'Simular/aplicar un permiso empresarial de contingencia por días completos.'

    def add_arguments(self, parser):
        parser.add_argument('--archivo', required=True, help='JSON de fuente y alcance explícitos.')
        parser.add_argument('--actor', required=True, type=int, help='ID de usuario DG activo; nunca se elige automáticamente.')
        parser.add_argument('--accion', choices=['borrador','aprobar','revocar'], default='aprobar')
        parser.add_argument('--aplicar', action='store_true')
        parser.add_argument('--digest', default='')

    def handle(self, *args, **options):
        try:
            datos = json.loads(Path(options['archivo']).read_text())
            actor = get_user_model().objects.get(pk=options['actor'])
            if options['aplicar']:
                if not options['digest']:
                    raise CommandError('--aplicar exige --digest de una simulación revisada.')
                permiso = aplicar_contingencia(datos,actor,digest=options['digest'],accion=options['accion'])
                resultado = dict(id=permiso.pk,clave=permiso.clave,estado=permiso.estado,revision=permiso.revision)
            else:
                resultado = previsualizar_contingencia(datos,actor,accion=options['accion'])
            self.stdout.write(json.dumps(resultado,ensure_ascii=False,sort_keys=True))
        except (OSError, ValueError, TypeError, get_user_model().DoesNotExist, PermissionDenied, ValidationError) as exc:
            raise CommandError(str(exc)) from exc
