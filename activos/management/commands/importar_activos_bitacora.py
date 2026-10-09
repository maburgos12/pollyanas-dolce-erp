from __future__ import annotations

import json
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management.base import BaseCommand, CommandError
from mantenimiento.services_capturas_equipos import CapturaEquipoError

from activos.services_importacion import _id, aplicar_importacion
from activos.utils.bitacora_import import leer_archivo, preview_bitacora


class Command(BaseCommand):
    help = 'Vista previa de CSV/XLSX; apply sólo con actor y decisiones expresamente revisadas.'

    def add_arguments(self, parser):
        parser.add_argument('archivo', type=str)
        parser.add_argument('--sheet', default='')
        parser.add_argument('--dry-run', action='store_true', help='Compatibilidad: siempre vista previa sin apply.')
        parser.add_argument('--skip-servicios', action='store_true', help='Compatibilidad de lectura; no crea equipos.')
        parser.add_argument('--apply', action='store_true')
        parser.add_argument('--actor', help='PK explícita del usuario autorizado.')
        parser.add_argument('--decisiones', help='JSON revisado con archivo_sha256, hoja, revisado=true y decisiones.')

    def handle(self, *args, **options):
        archivo = Path(options['archivo']).expanduser().resolve()
        if not archivo.is_file():
            raise CommandError('El archivo no existe.')
        try:
            nombre, contenido = leer_archivo(archivo)
            fuente = SimpleUploadedFile(nombre, contenido)
            if not options['apply']:
                resultado = preview_bitacora(fuente, sheet_name=options['sheet'])
            else:
                if options['dry_run'] or options['skip_servicios'] or not options['actor'] or not options['decisiones']:
                    raise CommandError('Apply exige --actor y --decisiones revisadas; no admite dry-run ni skip-servicios.')
                actor_id = _id(options['actor'], 'un actor')
                actor = get_user_model().objects.filter(pk=actor_id).first()
                if not actor:
                    raise CommandError('El actor no está disponible.')
                path = Path(options['decisiones']).expanduser()
                with path.open('rb') as stream:
                    raw = stream.read(4 * 1024 * 1024 + 1)
                if len(raw) > 4 * 1024 * 1024:
                    raise CommandError('El archivo de decisiones supera 4 MB.')
                revisado = json.loads(raw)
                if not isinstance(revisado, dict):
                    raise CommandError('La revisión debe ser un objeto JSON con archivo, hoja y decisiones.')
                preview = preview_bitacora(fuente, sheet_name=options['sheet'])
                if (revisado.get('revisado') is not True or revisado.get('archivo_sha256') != preview['sha256']
                        or revisado.get('hoja') != preview['sheet_name']):
                    raise CommandError('Revisa las decisiones de este archivo exacto y su hoja antes de aplicar.')
                resultado = aplicar_importacion(usuario=actor, archivo=fuente,
                    sheet_name=options['sheet'], decisiones=revisado.get('decisiones'), confirmado=True)
        except (ValueError, OSError, PermissionDenied, CapturaEquipoError) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(resultado, ensure_ascii=False, indent=2))
