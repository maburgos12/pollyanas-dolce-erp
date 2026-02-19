from __future__ import annotations

import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from maestros.models import Insumo, InsumoAlias
from recetas.utils.normalizacion import normalizar_nombre


class Command(BaseCommand):
    help = "Carga aliases de insumos desde CSV para unificar nombres de captura entre formatos/usuarios."

    def add_arguments(self, parser):
        parser.add_argument("csvpath", type=str, help="Ruta CSV")
        parser.add_argument("--dry-run", action="store_true", help="Simula la carga sin guardar cambios")

    @transaction.atomic
    def handle(self, *args, **options):
        csv_path = Path(options["csvpath"]).expanduser()
        if not csv_path.exists():
            raise CommandError(f"Archivo no encontrado: {csv_path}")

        created = 0
        updated = 0
        skipped = 0
        errors = 0

        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            required_any = {"alias", "nombre_origen"}
            if not (set(reader.fieldnames or []) & required_any):
                raise CommandError(
                    "CSV inválido. Requiere al menos una columna alias o nombre_origen, "
                    "y una referencia de insumo (insumo_id o insumo_nombre)."
                )

            for i, row in enumerate(reader, start=2):
                alias = (row.get("alias") or row.get("nombre_origen") or "").strip()
                if not alias:
                    skipped += 1
                    continue

                insumo = None
                insumo_id_raw = (row.get("insumo_id") or "").strip()
                if insumo_id_raw.isdigit():
                    insumo = Insumo.objects.filter(id=int(insumo_id_raw)).first()

                if not insumo:
                    insumo_nombre = (row.get("insumo_nombre") or row.get("insumo") or "").strip()
                    if insumo_nombre:
                        insumo = (
                            Insumo.objects.filter(nombre_normalizado=normalizar_nombre(insumo_nombre))
                            .order_by("id")
                            .first()
                        )

                if not insumo:
                    errors += 1
                    self.stdout.write(self.style.WARNING(f"Fila {i}: no se pudo resolver insumo para alias '{alias}'"))
                    continue

                alias_norm = normalizar_nombre(alias)
                if not alias_norm:
                    skipped += 1
                    continue

                obj, was_created = InsumoAlias.objects.get_or_create(
                    nombre_normalizado=alias_norm,
                    defaults={"nombre": alias[:250], "insumo": insumo},
                )
                if was_created:
                    created += 1
                else:
                    changed = False
                    if obj.insumo_id != insumo.id:
                        obj.insumo = insumo
                        changed = True
                    if obj.nombre != alias[:250]:
                        obj.nombre = alias[:250]
                        changed = True
                    if changed:
                        obj.save(update_fields=["insumo", "nombre"])
                        updated += 1
                    else:
                        skipped += 1

        if options["dry_run"]:
            transaction.set_rollback(True)

        mode = "DRY-RUN" if options["dry_run"] else "APLICADO"
        self.stdout.write(self.style.SUCCESS(f"Carga de aliases completada ({mode})"))
        self.stdout.write(f"  - creados: {created}")
        self.stdout.write(f"  - actualizados: {updated}")
        self.stdout.write(f"  - omitidos: {skipped}")
        self.stdout.write(f"  - errores: {errors}")
