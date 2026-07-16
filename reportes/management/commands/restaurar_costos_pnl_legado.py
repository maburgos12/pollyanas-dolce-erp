"""Restaura los Costos ene–may 2026 del P&L al valor del Excel.

La regla CONSUMO_MP total_empresa pisó los valores legados con el consumo
del ERP, que antes de junio 2026 está incompleto (marzo/abril parciales) y
con ajustes erróneos (mayo: Desmoldante en −$2.3M). Los valores aquí son
los importados del Excel GENERAL de administración (cuadre verificado).
La regla ya trae filtro desde=2026-06, así que no vuelve a pisarlos.
"""

from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import LineaPresupuestoMensual

VALORES_LEGADO = {
    "2026-01-01": Decimal("1438275.97"),
    "2026-02-01": Decimal("1138956.59"),
    "2026-03-01": Decimal("1247660.66"),
    "2026-04-01": Decimal("1445329.69"),
    "2026-05-01": Decimal("1495586.22"),
}


class Command(BaseCommand):
    help = "Restaura Costos insumos/productos ene–may 2026 al valor legado del Excel"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        restauradas = 0
        for periodo, valor in VALORES_LEGADO.items():
            linea = LineaPresupuestoMensual.objects.filter(
                rubro__area__codigo="resultados",
                rubro__concepto="Costos insumos/productos",
                periodo=periodo,
            ).first()
            if linea is None:
                self.stdout.write(self.style.WARNING(f"  {periodo}: línea no encontrada"))
                continue
            if linea.fuente_real.startswith("MANUAL:"):
                self.stdout.write(f"  {periodo}: captura manual, no se toca")
                continue
            if linea.monto_real == valor and linea.fuente_real == "AUTO:LEGADO":
                self.stdout.write(f"  {periodo}: ya restaurado")
                continue
            self.stdout.write(
                f"  {periodo}: {linea.monto_real} ({linea.fuente_real}) → {valor} (AUTO:LEGADO)"
            )
            restauradas += 1
            if not dry_run:
                linea.monto_real = valor
                linea.fuente_real = "AUTO:LEGADO"
                metadata = dict(linea.metadata or {})
                metadata.pop("sin_datos_fuente", None)
                metadata["restaurado_motivo"] = "consumo ERP no confiable antes de jun-2026"
                linea.metadata = metadata
                linea.save(update_fields=["monto_real", "fuente_real", "metadata", "actualizado_en"])
        if dry_run:
            transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(self.style.SUCCESS(f"[{modo}] restauradas: {restauradas}"))
