from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand

from maestros.models import CostoInsumo, Insumo, Proveedor


OPERATIVE_MAP = {
    # destino_sin_costo: fuente_con_costo
    "Betún Azúcar Glass Chocolate": "Betún de Chocolate Azúcar Glass",
    "Pan Chocolate": "Pan de Chocolate Deleite Dawn",
    "Crumble Nuez": "Masa Base Pay con Nuez",
    "Ganache Crunch": "Cobertura Crunch",
}


class Command(BaseCommand):
    help = (
        "Bootstrap de costos por mapa operativo explícito (destino -> fuente) "
        "para casos donde no existe evidencia suficiente automática."
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Aplica cambios. Por default dry-run.")
        parser.add_argument(
            "--proveedor-auto",
            type=str,
            default="AUTO MAPA OPERATIVO",
            help="Proveedor para costos creados por mapa operativo.",
        )

    def handle(self, *args, **options):
        proveedor_nombre = (options["proveedor_auto"] or "AUTO MAPA OPERATIVO").strip()

        proposals: list[tuple[Insumo, Insumo, Decimal]] = []
        for target_name, source_name in OPERATIVE_MAP.items():
            target = Insumo.objects.filter(nombre=target_name).first()
            source = Insumo.objects.filter(nombre=source_name).first()
            if not target or not source:
                self.stdout.write(
                    self.style.WARNING(
                        f"Omitido por catálogo faltante: target='{target_name}' source='{source_name}'"
                    )
                )
                continue
            latest = (
                CostoInsumo.objects.filter(insumo=source)
                .order_by("-fecha", "-id")
                .values_list("costo_unitario", flat=True)
                .first()
            )
            if latest is None:
                self.stdout.write(self.style.WARNING(f"Omitido sin costo fuente: {source_name}"))
                continue
            has_target_cost = CostoInsumo.objects.filter(insumo=target).exists()
            if has_target_cost:
                self.stdout.write(f"Ya tiene costo: {target_name}")
                continue
            proposals.append((target, source, Decimal(str(latest))))

        self.stdout.write("Bootstrap costos por mapa operativo")
        self.stdout.write(f"  - propuestas: {len(proposals)}")
        for target, source, costo in proposals:
            self.stdout.write(f"    * {target.nombre} <= {source.nombre} | costo={costo}")

        if not options["apply"]:
            self.stdout.write("Dry-run: no se crearon costos. Usa --apply para confirmar.")
            return

        proveedor, _ = Proveedor.objects.get_or_create(nombre=proveedor_nombre, defaults={"activo": True})
        today = date.today()
        created = 0
        for target, source, costo in proposals:
            source_hash = hashlib.sha256(
                f"AUTO_MAPA:{target.id}:{source.id}:{today.isoformat()}:{costo}".encode("utf-8")
            ).hexdigest()
            _, was_created = CostoInsumo.objects.get_or_create(
                source_hash=source_hash,
                defaults={
                    "insumo": target,
                    "proveedor": proveedor,
                    "fecha": today,
                    "moneda": "MXN",
                    "costo_unitario": costo,
                    "raw": {
                        "fuente": "AUTO_MAPA_OPERATIVO",
                        "source_insumo_id": source.id,
                        "source_insumo_nombre": source.nombre,
                    },
                },
            )
            if was_created:
                created += 1

        self.stdout.write(self.style.SUCCESS(f"Costos creados: {created}"))
