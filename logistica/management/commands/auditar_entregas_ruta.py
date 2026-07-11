import json
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from logistica.services_auditoria_entregas import auditar_entregas_ruta


class Command(BaseCommand):
    help = "Diagnostica inconsistencias de entrega sin corregir hechos operativos."

    def add_arguments(self, parser):
        parser.add_argument("--ruta-id", type=int)
        parser.add_argument("--fecha", help="Fecha de ruta en formato AAAA-MM-DD")
        modo = parser.add_mutually_exclusive_group()
        modo.add_argument("--dry-run", action="store_true", help="Reporta hallazgos sin crear alertas (predeterminado)")
        modo.add_argument("--crear-alertas", action="store_true", help="Crea únicamente alertas idempotentes")

    def handle(self, *args, **options):
        if not options["ruta_id"] and not options["fecha"]:
            raise CommandError("Indica --fecha o --ruta-id para acotar la auditoría.")
        fecha = None
        if options["fecha"]:
            try:
                fecha = date.fromisoformat(options["fecha"])
            except ValueError as exc:
                raise CommandError("--fecha debe usar el formato AAAA-MM-DD") from exc
        resumen = auditar_entregas_ruta(
            ruta_id=options["ruta_id"],
            fecha=fecha,
            dry_run=not options["crear_alertas"],
        )
        modo = "alertas" if options["crear_alertas"] else "dry-run"
        self.stdout.write(f"Auditoría de entregas ({modo})")
        self.stdout.write(json.dumps(resumen, ensure_ascii=False, sort_keys=True, default=str))
