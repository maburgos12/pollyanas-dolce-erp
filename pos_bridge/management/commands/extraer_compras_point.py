"""Extrae compras de Point (con cantidad) y las persiste como CostoInsumo.

Paso previo a `importar_compras_point_a_kardex`, que convierte esas compras en
entradas de inventario. Dry-run por defecto.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.point_purchase_cost_import_service import PointPurchaseCostImportService
from pos_bridge.services.point_purchase_extraction_service import PointPurchaseExtractionService


def _parse_date(value: str, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        raise CommandError(f"--{label} debe tener formato YYYY-MM-DD (recibido: {value!r}).")


class Command(BaseCommand):
    help = (
        "Extrae compras de Point en un rango de fechas y las guarda como CostoInsumo con "
        "cantidad. Dry-run por defecto; usa --apply para guardar."
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Guarda las compras en la BD.")
        parser.add_argument("--desde", help="Fecha inicial (YYYY-MM-DD). Default: hace 30 días.")
        parser.add_argument("--hasta", help="Fecha final (YYYY-MM-DD). Default: hoy.")
        parser.add_argument(
            "--todas-las-sucursales",
            action="store_true",
            dest="todas_sucursales",
            help="Incluye compras de sucursales distintas de Almacén (por defecto se omiten).",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options["apply"])
        hasta = _parse_date(options["hasta"], "hasta") if options.get("hasta") else date.today()
        desde = (
            _parse_date(options["desde"], "desde")
            if options.get("desde")
            else hasta - timedelta(days=30)
        )
        if desde > hasta:
            raise CommandError("--desde no puede ser posterior a --hasta.")

        if not apply_changes:
            self.stdout.write(
                self.style.WARNING("── DRY-RUN: no se guarda nada. Usa --apply para confirmar.\n")
            )

        self.stdout.write(f"Consultando compras Point de {desde} a {hasta}...")
        purchases, extraction = PointPurchaseExtractionService().fetch_purchases(
            desde=desde,
            hasta=hasta,
            solo_almacen=not bool(options["todas_sucursales"]),
        )

        self.stdout.write(f"Compras en el rango : {extraction.purchases_seen}")
        self.stdout.write(f"Compras de almacén  : {extraction.purchases_kept}")
        self.stdout.write(f"Renglones de compra : {extraction.lines_kept}")
        for branch, total in sorted(extraction.branches_skipped.items()):
            self.stdout.write(f"  omitida sucursal {branch}: {total}")

        if not purchases:
            self.stdout.write(self.style.WARNING("\nPoint no entregó compras de almacén en el rango."))
            return

        if not apply_changes:
            self.stdout.write("")
            for purchase in purchases[:15]:
                self.stdout.write(
                    f"  {purchase['purchase_date']} folio {purchase['folio'] or 's/f'} "
                    f"({purchase['supplier'][:40]}): {len(purchase['lines'])} renglones"
                )
            if len(purchases) > 15:
                self.stdout.write(f"  ... y {len(purchases) - 15} compras más")
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("DRY-RUN: nada se guardó."))
            return

        result = PointPurchaseCostImportService().persist_purchases(purchases)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Costos creados      : {result.created}"))
        self.stdout.write(f"Ya existentes       : {result.existing}")
        self.stdout.write(f"Artículos sin match : {result.unresolved}")
        if result.unresolved_articles:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("Artículos sin insumo en el ERP:"))
            for article in result.unresolved_articles[:30]:
                self.stdout.write(f"  {article}")
            if len(result.unresolved_articles) > 30:
                self.stdout.write(f"  ... y {len(result.unresolved_articles) - 30} más")
