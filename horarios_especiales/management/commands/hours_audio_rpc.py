"""Private stdin RPC, invoked only through a root-owned forced-command wrapper."""
import json
import sys
from django.core.management.base import BaseCommand, CommandError
from horarios_especiales.services import telegram_audio as audio
from horarios_especiales.services.telegram_bot import load_config


class Command(BaseCommand):
    help = 'RPC privado y acotado de voz; sin argumentos ni comandos arbitrarios.'

    def handle(self, *args, **options):
        try:
            raw = sys.stdin.read(8193)
            if len(raw.encode()) > 8192:
                raise ValueError()
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError()
            if payload == {'action':'claim'}:
                result = audio.claim(load_config())
            elif payload.get('action') == 'finish' and set(payload) == {'action', 'id', 'lease', 'sha256', 'text'}:
                result = audio.finish(load_config(), {key:value for key,value in payload.items() if key != 'action'})
            else:
                raise ValueError()
        except Exception:
            raise CommandError('Solicitud de audio inválida o no disponible.') from None
        self.stdout.write(json.dumps(result, ensure_ascii=True))
