from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from django.core.management.base import BaseCommand
from django.db.models import Q

from recetas.models import LineaReceta


class Command(BaseCommand):
    help = (
        "Completa costo_unitario_snapshot en líneas ligadas cuando falta snapshot "
        "y existe costo_linea_excel + cantidad (>0)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios. Sin esta bandera corre en dry-run.",
        )
        parser.add_argument(
            "--receta",
            type=str,
            default="",
            help="Filtra por nombre de receta (contains).",
        )

    def handle(self, *args, **options):
        qs = (
            LineaReceta.objects.filter(insumo__isnull=False)
            .filter(Q(costo_unitario_snapshot__isnull=True) | Q(costo_unitario_snapshot__lte=0))
            .filter(cantidad__gt=0, costo_linea_excel__gt=0)
            .select_related("receta", "insumo")
            .order_by("receta__nombre", "posicion")
        )
        receta_filter = (options.get("receta") or "").strip()
        if receta_filter:
            qs = qs.filter(receta__nombre__icontains=receta_filter)

        candidates = []
        skipped = 0
        for linea in qs.iterator():
            try:
                qty = Decimal(str(linea.cantidad))
                line_cost = Decimal(str(linea.costo_linea_excel))
                if qty <= 0:
                    skipped += 1
                    continue
                snapshot = (line_cost / qty).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                if snapshot <= 0:
                    skipped += 1
                    continue
                candidates.append((linea, snapshot))
            except (InvalidOperation, ZeroDivisionError):
                skipped += 1

        self.stdout.write("Backfill snapshot desde costo/qty")
        self.stdout.write(f"  - candidatas evaluadas: {qs.count()}")
        self.stdout.write(f"  - inferibles: {len(candidates)}")
        self.stdout.write(f"  - omitidas: {skipped}")
        if candidates:
            self.stdout.write("  - muestra:")
            for linea, snapshot in candidates[:15]:
                self.stdout.write(
                    f"    * {linea.receta.nombre} | pos={linea.posicion} | "
                    f"{linea.insumo_texto} -> snapshot={snapshot}"
                )

        if not options["apply"]:
            self.stdout.write("Dry-run: no se actualizaron líneas. Usa --apply para confirmar.")
            return

        updated = 0
        for linea, snapshot in candidates:
            linea.costo_unitario_snapshot = snapshot
            linea.save(update_fields=["costo_unitario_snapshot"])
            updated += 1

        self.stdout.write(self.style.SUCCESS(f"Líneas actualizadas: {updated}"))
