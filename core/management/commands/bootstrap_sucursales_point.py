from __future__ import annotations

from django.core.management.base import BaseCommand

from core.models import Sucursal


SUCURSALES_POINT = [
    {"codigo": "COLOSIO", "nombre": "Colosio", "activa": True},
    {"codigo": "CRUCERO", "nombre": "Crucero", "activa": True},
    {"codigo": "EL_TUNEL", "nombre": "EL TUNEL", "activa": True},
    {"codigo": "GUAMUCHIL", "nombre": "Guamuchil", "activa": False, "fecha_apertura": None},
    {"codigo": "LAS_GLORIAS", "nombre": "Las Glorias", "activa": True},
    {"codigo": "LEYVA", "nombre": "Leyva", "activa": True},
    {"codigo": "MATRIZ", "nombre": "Matriz", "activa": True},
    {"codigo": "PAYAN", "nombre": "Payán", "activa": True},
    {"codigo": "PLAZA_NIO", "nombre": "Plaza Nío", "activa": True},
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

        for item in SUCURSALES_POINT:
            codigo = item["codigo"]
            nombre = item["nombre"]
            activa = bool(item.get("activa", True))
            fecha_apertura = item.get("fecha_apertura")
            obj, was_created = Sucursal.objects.get_or_create(
                codigo=codigo,
                defaults={"nombre": nombre, "activa": activa, "fecha_apertura": fecha_apertura},
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
            if obj.activa != activa:
                obj.activa = activa
                changes.append("activa")
            if obj.fecha_apertura != fecha_apertura:
                obj.fecha_apertura = fecha_apertura
                changes.append("fecha_apertura")
            if changes:
                obj.save(update_fields=changes)
                updated += 1

        total = Sucursal.objects.filter(activa=True).count()
        self.stdout.write(self.style.SUCCESS("Sucursales Point listas"))
        self.stdout.write(f"  - creadas: {created}")
        self.stdout.write(f"  - actualizadas: {updated}")
        self.stdout.write(f"  - activas totales: {total}")
