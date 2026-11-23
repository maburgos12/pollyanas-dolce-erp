import json
from django.core.management.base import BaseCommand, CommandError
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from horarios_especiales.services.telegram_bot import load_config, members, scoped_config, actor_for


class Command(BaseCommand):
    help = 'Registra polling privado y reconciliación desactivados; --enable requiere activación explícita.'

    def add_arguments(self, parser):
        parser.add_argument('--config', help='Directorio privado que también debe configurar el worker mediante HOURS_TELEGRAM_CONFIG_DIR.')
        parser.add_argument('--enable', action='store_true')
        parser.add_argument('--validate-only', action='store_true', help='Valida identidades y permisos sin escribir tareas Beat.')

    def handle(self, *args, **options):
        try:
            config = load_config(options['config'])
        except (OSError, ValueError, KeyError):
            raise CommandError('Configuración privada incompleta o insegura.') from None
        from django.core.exceptions import PermissionDenied
        try:
            for identity in members(config):
                if identity.get('approve_own') is False:
                    continue
                actor_for(scoped_config(config, identity))
        except PermissionDenied:
            raise CommandError('Cada usuario habilitado debe existir, estar activo y tener autorización de horarios.') from None
        if options['validate_only']:
            count = sum(identity.get('approve_own') is not False for identity in members(config))
            self.stdout.write(f'Configuración válida: {count} usuarios habilitados. Sin cambios operativos.')
            return
        # One runtime directory per worker; --config never serializes credentials into beat.
        for name, task, seconds, expires in [
            ('hours-private-telegram-poll', 'horarios_especiales.telegram_poll', 15, 30),
            ('hours-approved-reconcile', 'horarios_especiales.reconcile_approved', 900, 840),
        ]:
            interval, _ = IntervalSchedule.objects.get_or_create(every=seconds, period=IntervalSchedule.SECONDS)
            PeriodicTask.objects.update_or_create(name=name, defaults={'task': task, 'interval': interval,
                'queue': 'hours_control', 'exchange': None, 'routing_key': None, 'priority': None,
                'expires': None, 'expire_seconds': expires, 'crontab': None, 'solar': None, 'clocked': None, 'enabled': options['enable'], 'kwargs': json.dumps({'config_dir': options['config']}) if task.endswith('telegram_poll') and options['config'] else '{}'})
        self.stdout.write('Horarios Telegram: ' + ('activado explícitamente.' if options['enable'] else 'registrado DESACTIVADO.'))
