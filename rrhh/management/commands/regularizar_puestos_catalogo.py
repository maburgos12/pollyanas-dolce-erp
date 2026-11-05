import hashlib
import json

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import AuditLog
from rrhh.models import Empleado
from rrhh.services_catalogos import etiquetas_puestos, puesto_canonico


class Command(BaseCommand):
    help = "Revisa y regulariza etiquetas de puestos ya identificados en el catálogo, sin cambiar funciones ni estatus."

    def add_arguments(self, parser):
        parser.add_argument('--apply', action='store_true')
        parser.add_argument('--expected-sha256')

    @transaction.atomic
    def handle(self, *args, **options):
        catalogo = etiquetas_puestos()
        empleados = Empleado.objects.order_by('pk')
        if options['apply']:
            empleados = empleados.select_for_update()
        cambios = []
        for empleado in empleados:
            nuevo = puesto_canonico(empleado, catalogo)
            if nuevo and nuevo != empleado.puesto:
                cambios.append(dict(id=empleado.pk, nombre=empleado.nombre,
                    funcion=empleado.puesto_operativo, anterior=empleado.puesto, nuevo=nuevo))
        digest = hashlib.sha256(json.dumps(cambios, sort_keys=True).encode()).hexdigest()
        if options['apply']:
            if options['expected_sha256'] != digest:
                raise CommandError('El plan cambió o no se indicó su SHA256; vuelve a revisar el dry-run.')
            for cambio in cambios:
                Empleado.objects.filter(pk=cambio['id']).update(puesto=cambio['nuevo'])
                AuditLog.objects.create(action='UPDATE', model='rrhh.Empleado',
                    object_id=str(cambio['id']), payload={
                        'origen': 'regularizar_puestos_catalogo', 'sha256_plan': digest,
                        'antes': {'puesto': cambio['anterior']},
                        'despues': {'puesto': cambio['nuevo']},
                        'funcion_existente': cambio['funcion']})
        self.stdout.write(json.dumps(dict(aplicado=options['apply'], sha256=digest,
            cambios=cambios), ensure_ascii=False))
