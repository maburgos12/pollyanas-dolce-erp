import json

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from orquestacion.services.erp_system_map import build_system_map


class Command(BaseCommand):
    help = 'Emite JSON técnico de declaraciones Django y referencias del registro, sin leer datos operativos.'

    def handle(self, *args, **options):
        if connection.vendor != 'postgresql':
            raise CommandError('Este proyecto requiere PostgreSQL.')
        self.stdout.write(json.dumps(build_system_map(), ensure_ascii=False, indent=2, sort_keys=True))
