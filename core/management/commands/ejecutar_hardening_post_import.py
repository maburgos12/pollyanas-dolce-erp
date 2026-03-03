from __future__ import annotations

from django.core.management import BaseCommand, call_command

from maestros.models import CostoInsumo, Proveedor


class Command(BaseCommand):
    help = (
        "Orquesta hardening post-import: aliases operativos, rematch de recetas, "
        "inferencia de cantidades/snapshots, costos automáticos y normalización "
        "de inventario/proveedor."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--proveedor-fallback",
            default="AUTO SIN PROVEEDOR",
            help="Proveedor asignado a costos históricos sin proveedor.",
        )
        parser.add_argument(
            "--min-evidencias-costo",
            type=int,
            default=1,
            help="Mínimo de evidencias para bootstrap_costos_desde_recetas.",
        )

    def handle(self, *args, **options):
        proveedor_fallback_nombre = str(options["proveedor_fallback"]).strip() or "AUTO SIN PROVEEDOR"
        min_evidencias = int(options["min_evidencias_costo"])

        self.stdout.write(self.style.SUCCESS("Hardening post-import"))

        self.stdout.write("1) Alias operativos")
        call_command("bootstrap_aliases_operativos", apply=True)

        self.stdout.write("2) Costos por mapa operativo")
        call_command("bootstrap_costos_mapa_operativo", apply=True)

        self.stdout.write("3) Costos por evidencia en recetas")
        call_command("bootstrap_costos_desde_recetas", apply=True, min_evidencias=min_evidencias)

        self.stdout.write("4) Rematch de líneas de receta")
        call_command("rematch_lineas_receta", include_needs_review=True, apply=True)

        self.stdout.write("5) Inferencia de cantidad desde costo")
        call_command(
            "inferir_cantidad_lineas_desde_costo",
            apply=True,
            min_match_score=0,
            relax_piece_rule=True,
            use_linecost_as_qty_when_tiny=True,
        )

        self.stdout.write("6) Backfill de snapshots")
        call_command("backfill_linea_snapshots")
        call_command("backfill_linea_snapshots_from_linecost", apply=True)

        self.stdout.write("7) Existencias faltantes")
        call_command("backfill_existencias_insumos", apply=True)

        self.stdout.write("8) Proveedor fallback para costos sin proveedor")
        proveedor_fallback, _ = Proveedor.objects.get_or_create(
            nombre=proveedor_fallback_nombre,
            defaults={"activo": True},
        )
        updated = CostoInsumo.objects.filter(proveedor__isnull=True).update(proveedor=proveedor_fallback)
        self.stdout.write(f"  - costos actualizados: {updated}")

        self.stdout.write("9) Auditoría final")
        call_command("auditar_flujo_erp")

        self.stdout.write(self.style.SUCCESS("Hardening post-import completado"))
