from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.services_presupuesto_fuentes import corregir_agua_purificada


class Command(BaseCommand):
    help = "Separa agua de personal del ingrediente y revierte el doble AUTO auditado."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Persiste la corrección; sin esta opción solo muestra una simulación.",
        )

    def handle(self, *args, **options):
        aplicar = options["apply"]
        with transaction.atomic():
            resultado = corregir_agua_purificada()
            if not aplicar:
                transaction.set_rollback(True)
        modo = "APLICADO" if aplicar else "SIMULACIÓN"
        self.stdout.write(
            self.style.SUCCESS(
                f"[{modo}] rubro personal={resultado.rubro_personal_id}; "
                f"ingrediente={resultado.rubro_ingrediente_id}; "
                f"líneas corregidas={resultado.lineas_corregidas}; "
                f"reglas desactivadas={resultado.reglas_desactivadas}."
            )
        )
