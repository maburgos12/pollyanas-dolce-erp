import json
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from logistica.services_auditoria_entregas import auditar_entregas_ruta


class Command(BaseCommand):
    help = "Diagnostica inconsistencias de entrega sin corregir hechos operativos."

    def add_arguments(self, parser):
        parser.add_argument("--ruta-id", type=int)
        parser.add_argument("--fecha", help="Fecha de ruta en formato AAAA-MM-DD")
        parser.add_argument("--dry-run", action="store_true", help="Reporta hallazgos sin crear alertas")

    def handle(self, *args, **options):
        fecha = None
        if options["fecha"]:
            try:
                fecha = date.fromisoformat(options["fecha"])
            except ValueError as exc:
                raise CommandError("--fecha debe usar el formato AAAA-MM-DD") from exc
        resumen = auditar_entregas_ruta(
            ruta_id=options["ruta_id"],
            fecha=fecha,
            dry_run=options["dry_run"],
        )
        modo = "dry-run" if options["dry_run"] else "alertas"
        self.stdout.write(f"Auditoría de entregas ({modo})")
        self.stdout.write(json.dumps(resumen, ensure_ascii=False, sort_keys=True, default=str))
