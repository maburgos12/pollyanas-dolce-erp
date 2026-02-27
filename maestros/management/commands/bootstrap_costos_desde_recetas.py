from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from statistics import median

from django.core.management.base import BaseCommand
from django.db.models import Q

from maestros.models import CostoInsumo, Insumo, Proveedor
from recetas.models import LineaReceta


class Command(BaseCommand):
    help = (
        "Genera costos base para insumos sin costo usando evidencia de líneas de receta "
        "(costo_linea_excel / cantidad)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios. Sin esta bandera corre en dry-run.",
        )
        parser.add_argument(
            "--min-evidencias",
            type=int,
            default=1,
            help="Mínimo de líneas válidas requeridas por insumo (default: 1).",
        )
        parser.add_argument(
            "--proveedor-auto",
            type=str,
            default="AUTO COSTEO RECETA",
            help="Nombre de proveedor para costos auto generados.",
        )

    def handle(self, *args, **options):
        min_evidencias = max(1, int(options["min_evidencias"]))
        proveedor_nombre = (options["proveedor_auto"] or "AUTO COSTEO RECETA").strip()

        insumos_sin_costo = Insumo.objects.filter(activo=True).exclude(
            id__in=CostoInsumo.objects.values_list("insumo_id", flat=True).distinct()
        )
        insumo_ids = list(insumos_sin_costo.values_list("id", flat=True))
        if not insumo_ids:
            self.stdout.write("No hay insumos activos sin costo base.")
            return

        lineas = (
            LineaReceta.objects.filter(insumo_id__in=insumo_ids)
            .filter(cantidad__gt=0, costo_linea_excel__gt=0)
            .values("insumo_id", "cantidad", "costo_linea_excel")
        )

        evidencias: dict[int, list[Decimal]] = {}
        for row in lineas.iterator():
            try:
                qty = Decimal(str(row["cantidad"]))
                line_cost = Decimal(str(row["costo_linea_excel"]))
                if qty <= 0 or line_cost <= 0:
                    continue
                unit_cost = (line_cost / qty).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                if unit_cost <= 0:
                    continue
            except (InvalidOperation, ZeroDivisionError):
                continue
            evidencias.setdefault(int(row["insumo_id"]), []).append(unit_cost)

        propuestas: list[tuple[Insumo, Decimal, int]] = []
        for insumo in insumos_sin_costo.order_by("nombre"):
            vals = evidencias.get(insumo.id, [])
            if len(vals) < min_evidencias:
                continue
            costo_mediana = Decimal(str(median(vals))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            if costo_mediana <= 0:
                continue
            propuestas.append((insumo, costo_mediana, len(vals)))

        self.stdout.write("Bootstrap costos desde recetas")
        self.stdout.write(f"  - insumos sin costo evaluados: {len(insumo_ids)}")
        self.stdout.write(f"  - insumos con evidencia suficiente: {len(propuestas)}")
        if propuestas:
            self.stdout.write("  - muestra:")
            for insumo, costo, n in propuestas[:20]:
                self.stdout.write(f"    * {insumo.nombre} -> {costo} (evidencias={n})")

        if not options["apply"]:
            self.stdout.write("Dry-run: no se crearon costos. Usa --apply para confirmar.")
            return

        proveedor, _ = Proveedor.objects.get_or_create(nombre=proveedor_nombre, defaults={"activo": True})
        created = 0
        today = date.today()
        for insumo, costo, n in propuestas:
            source_hash = hashlib.sha256(
                f"AUTO_RECETA:{insumo.id}:{today.isoformat()}:{costo}:{n}".encode("utf-8")
            ).hexdigest()
            _, was_created = CostoInsumo.objects.get_or_create(
                source_hash=source_hash,
                defaults={
                    "insumo": insumo,
                    "proveedor": proveedor,
                    "fecha": today,
                    "moneda": "MXN",
                    "costo_unitario": costo,
                    "raw": {
                        "fuente": "AUTO_RECETA_MEDIANA",
                        "evidencias": n,
                    },
                },
            )
            if was_created:
                created += 1

        self.stdout.write(self.style.SUCCESS(f"Costos creados: {created}"))
