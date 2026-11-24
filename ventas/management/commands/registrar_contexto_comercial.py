import json
from pathlib import Path
from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import ValidationError, PermissionDenied, ObjectDoesNotExist
from ventas.services.operational_context import register_contexts


class Command(BaseCommand):
    help = 'Previsualiza una bitácora comercial inmutable; aplicar requiere digest fresco y DG activa.'

    def add_arguments(self, parser):
        parser.add_argument('--archivo', required=True)
        parser.add_argument('--actor', required=True, type=int)
        parser.add_argument('--aplicar', action='store_true')
        parser.add_argument('--digest')

    def handle(self, *args, **options):
        try:
            entries = json.loads(Path(options['archivo']).read_text(encoding='utf-8'))
            result = register_contexts(entries=entries, actor_id=options['actor'], apply=options['aplicar'], digest=options['digest'])
        except (OSError, ValueError, ValidationError, PermissionDenied, ObjectDoesNotExist) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True))
