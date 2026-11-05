import json
from django.core.management.base import BaseCommand, CommandError
from inventario.services_documentary_audit import build_report, month, refresh_report


class Command(BaseCommand):
    help = 'Conciliación documental sin cambios operativos. --guardar conserva únicamente el expediente.'

    def add_arguments(self, parser):
        parser.add_argument('--mes', required=True)
        parser.add_argument('--guardar', action='store_true')

    def handle(self, *args, **options):
        try:
            period = month(options['mes'])
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        if options['guardar']:
            snapshot = refresh_report(period)
            if snapshot is None:
                raise CommandError('Otra corrida del mismo periodo está en curso.')
            result = {'snapshot_id': snapshot.pk, 'fingerprint': snapshot.fingerprint, 'coverage': snapshot.payload['coverage'], 'cases': len(snapshot.payload['rows'])}
        else:
            result = build_report(period)
        self.stdout.write(json.dumps(result, ensure_ascii=False))
