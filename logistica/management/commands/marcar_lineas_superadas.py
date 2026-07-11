import json

from django.core.management.base import BaseCommand

from logistica.services_carga_ruta import marcar_lineas_checklist_superadas_historicas


class Command(BaseCommand):
    help = (
        "Marca retroactivamente como SUPERADA las líneas de checklist de carga "
        "duplicadas por el mismo producto/parada/folio. Nunca cruza folios "
        "distintos ni toca grupos con más de una línea ya resuelta."
    )

    def add_arguments(self, parser):
        modo = parser.add_mutually_exclusive_group()
        modo.add_argument("--dry-run", action="store_true", help="Reporta hallazgos sin escribir (predeterminado)")
        modo.add_argument("--aplicar", action="store_true", help="Marca SUPERADA las líneas encontradas")

    def handle(self, *args, **options):
        dry_run = not options["aplicar"]
        resumen = marcar_lineas_checklist_superadas_historicas(dry_run=dry_run)
        modo = "dry-run" if dry_run else "aplicado"
        self.stdout.write(f"Marcar líneas superadas ({modo})")
        self.stdout.write(
            json.dumps(
                {
                    "grupos_afectados": resumen.grupos_afectados,
                    "lineas_superadas": resumen.lineas_superadas,
                    "grupos_ambiguos": resumen.grupos_ambiguos,
                    "detalle_ambiguos": resumen.detalle_ambiguos,
                },
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
        )
