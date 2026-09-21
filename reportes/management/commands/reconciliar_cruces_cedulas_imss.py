"""Revisa cruces pendientes de expedientes IMSS ya aplicados."""

from contextlib import nullcontext

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.services_cedula_reconciliacion import reconciliar_cruces_aplicados


class Command(BaseCommand):
    help = "Previsualiza o aplica cruces nuevos por vigencia laboral documentada."

    def add_arguments(self, parser):
        parser.add_argument("--expediente", type=int, action="append", required=True)
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        ids = options["expediente"]
        if len(ids) != len(set(ids)):
            raise CommandError("No repitas identificadores de expediente.")
        if any(expediente_id <= 0 for expediente_id in ids):
            raise CommandError("Los identificadores de expediente deben ser positivos.")
        resultados = []
        contexto = transaction.atomic() if options["apply"] else nullcontext()
        with contexto:
            for expediente_id in ids:
                try:
                    resultados.append(reconciliar_cruces_aplicados(
                        expediente_id, aplicar=options["apply"], usuario=None
                    ))
                except Exception as exc:
                    raise CommandError(f"Expediente #{expediente_id}: {exc}") from exc
        for resultado in resultados:
            estado = "APLICADO" if options["apply"] else "DRY-RUN"
            self.stdout.write(
                f"{estado} | expediente={resultado.expediente_id} | "
                f"nuevos_cruces={resultado.nuevos_cruces} | "
                f"pendientes={resultado.pendientes} | "
                f"monto_atribuido={resultado.monto_atribuido:.2f}"
            )
