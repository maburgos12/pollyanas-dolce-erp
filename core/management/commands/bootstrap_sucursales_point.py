from __future__ import annotations

from django.core.management.base import BaseCommand

from core.models import Sucursal


SUCURSALES_POINT = [
    ("COLOSIO", "Colosio"),
    ("CRUCERO", "Crucero"),
    ("EL_TUNEL", "EL TUNEL"),
    ("LAS_GLORIAS", "Las Glorias"),
    ("LEYVA", "Leyva"),
    ("MATRIZ", "Matriz"),
    ("PAYAN", "Payán"),
    ("PLAZA_NIO", "Plaza Nío"),
]


class Command(BaseCommand):
    help = "Crea/actualiza catálogo base de sucursales Point."

    def add_arguments(self, parser):
        parser.add_argument(
            "--only-missing",
            action="store_true",
            help="Solo crea sucursales faltantes (no actualiza nombres existentes).",
        )

    def handle(self, *args, **options):
        only_missing = bool(options.get("only_missing"))
        created = 0
        updated = 0

        for codigo, nombre in SUCURSALES_POINT:
            obj, was_created = Sucursal.objects.get_or_create(
                codigo=codigo,
                defaults={"nombre": nombre, "activa": True},
            )
            if was_created:
                created += 1
                continue

            if only_missing:
                continue

            changes = []
            if obj.nombre != nombre:
                obj.nombre = nombre
                changes.append("nombre")
            if not obj.activa:
                obj.activa = True
                changes.append("activa")
            if changes:
                obj.save(update_fields=changes)
                updated += 1

        total = Sucursal.objects.filter(activa=True).count()
        self.stdout.write(self.style.SUCCESS("Sucursales Point listas"))
        self.stdout.write(f"  - creadas: {created}")
        self.stdout.write(f"  - actualizadas: {updated}")
        self.stdout.write(f"  - activas totales: {total}")
