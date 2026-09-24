"""Previsualiza la carga cerrada de seis jornadas administrativas de 2026."""

import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from rrhh.services_jornadas_administrativas_2026 import (
    ConfiguracionJornadasError, configurar_jornadas_administrativas_2026,
)


class Command(BaseCommand):
    help = "Previsualiza o aplica con huella fresca seis jornadas administrativas Sep-Dic 2026."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--actor-username")
        parser.add_argument("--expected-fingerprint")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        actor = None
        if aplicar:
            if not options["actor_username"] or not options["expected_fingerprint"]:
                raise CommandError("--apply exige --actor-username y --expected-fingerprint.")
            actor = get_user_model().objects.filter(username=options["actor_username"], is_active=True).first()
            if actor is None:
                raise CommandError("El actor no existe o está inactivo.")
        try:
            resultado = configurar_jornadas_administrativas_2026(
                aplicar=aplicar, actor=actor,
                expected_fingerprint=options["expected_fingerprint"],
            )
        except ConfiguracionJornadasError as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(resultado, ensure_ascii=False, sort_keys=True, indent=2))
