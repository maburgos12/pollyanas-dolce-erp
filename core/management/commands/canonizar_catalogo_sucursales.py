from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from core.branch_catalog import POINT_NETWORK_BRANCH_CODES, canonical_branch_catalog_name
from core.models import Sucursal


class Command(BaseCommand):
    help = "Canoniza los nombres del catálogo operativo de sucursales usando el formato oficial."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica los cambios. Sin este flag solo reporta el diff.",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options["apply"])
        rows = []
        queryset = Sucursal.objects.filter(codigo__in=POINT_NETWORK_BRANCH_CODES).order_by("codigo")
        for sucursal in queryset:
            target_name = canonical_branch_catalog_name(sucursal.codigo, sucursal.nombre)
            if sucursal.nombre == target_name:
                continue
            rows.append((sucursal, target_name))

        if not rows:
            self.stdout.write(self.style.SUCCESS("Catálogo ya canonizado."))
            return

        self.stdout.write("| código | actual | canonico |")
        self.stdout.write("| --- | --- | --- |")
        for sucursal, target_name in rows:
            self.stdout.write(f"| {sucursal.codigo} | {sucursal.nombre} | {target_name} |")

        if not apply_changes:
            self.stdout.write(self.style.WARNING("Dry-run: sin cambios. Usa --apply para escribir."))
            return

        with transaction.atomic():
            for sucursal, target_name in rows:
                sucursal.nombre = target_name
                sucursal.save(update_fields=["nombre"])

        self.stdout.write(self.style.SUCCESS(f"Catálogo canonizado: {len(rows)} sucursales actualizadas."))
