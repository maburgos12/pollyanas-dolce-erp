from __future__ import annotations

from django.core.management.base import BaseCommand
from unidecode import unidecode

from maestros.models import Insumo, InsumoAlias


OPERATIVE_ALIAS_MAP = {
    "Bañado 3 Leches": "Mezcla 3 Leches",
    "Bañado": "Mezcla 3 Leches",
    "Flan": "Flan 3 Pecados",
    "Betún": "Betún dream whip",
    "Ganach Chocolate": "Betún de Chocolate Original Dolce",
    "Mermelada": "Mermelada Fresa",
    "Betún Chocolate Original": "Betún de Chocolate Original Dolce",
    "Pan Zanahoria": "Pan de Zanahoria",
    "Nuez": "Nuez Picada",
    "Chocolate": "Chocolate Original",
    "Crema de Lotus": "Crema Lotus",
    "Crema Fresas": "Batida crema para fresas",
    "Galleta de Red Velvet": "Galleta de Chispas de Red Velvet",
    "Galleta Nutella": "Galleta Relleno de Nutella",
    "Gragea": "Gragea Colores",
    "Jugo Limón": "Limón",
    "Caja": "CAJA G",
    "Faja": "Faja",
}


class Command(BaseCommand):
    help = (
        "Crea/actualiza alias operativos de recetas para estabilizar matching "
        "en cargas recurrentes."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios. Sin esta bandera corre en dry-run.",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options.get("apply"))
        created = 0
        updated = 0
        missing_targets: list[tuple[str, str]] = []

        for alias_name, target_name in OPERATIVE_ALIAS_MAP.items():
            target = Insumo.objects.filter(nombre=target_name).first()
            if not target:
                missing_targets.append((alias_name, target_name))
                continue

            normalized = " ".join(unidecode(alias_name).lower().split())
            alias = InsumoAlias.objects.filter(nombre_normalizado=normalized).first()
            if not alias:
                if apply_changes:
                    InsumoAlias.objects.create(nombre=alias_name, insumo=target)
                created += 1
                continue

            if alias.insumo_id == target.id and alias.nombre == alias_name:
                continue

            if apply_changes:
                alias.insumo = target
                alias.nombre = alias_name
                alias.save(update_fields=["insumo", "nombre", "nombre_normalizado"])
            updated += 1

        self.stdout.write("Bootstrap aliases operativos")
        self.stdout.write(f"  - mapa total: {len(OPERATIVE_ALIAS_MAP)}")
        self.stdout.write(f"  - creados: {created}")
        self.stdout.write(f"  - actualizados: {updated}")
        if missing_targets:
            self.stdout.write("  - target insumo faltante:")
            for alias_name, target_name in missing_targets:
                self.stdout.write(f"    * {alias_name} -> {target_name}")

        if not apply_changes:
            self.stdout.write("Dry-run: no se escribieron cambios. Usa --apply para confirmar.")
