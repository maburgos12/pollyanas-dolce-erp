from __future__ import annotations

from django.core.management.base import BaseCommand

from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureService


class Command(BaseCommand):
    help = "Captura costo unitario desde Point -> Existencias -> ALMACEN y opcionalmente lo persiste en CostoInsumo."

    def add_arguments(self, parser):
        parser.add_argument("--branch", default="ALMACEN", help="Sucursal de existencias a consultar (default: ALMACEN).")
        parser.add_argument("--query", action="append", default=[], help="Texto a buscar en el catálogo de insumos.")
        parser.add_argument("--code", action="append", default=[], help="Código Point exacto a buscar.")
        parser.add_argument("--apply", action="store_true", help="Persiste el costo en CostoInsumo.")

    def handle(self, *args, **options):
        branch = (options.get("branch") or "ALMACEN").strip()
        queries = list(options.get("query") or [])
        codes = list(options.get("code") or [])

        service = PointInventoryCostCaptureService()
        rows = service.capture_matches(branch_hint=branch, queries=queries, point_codes=codes)

        self.stdout.write("Captura de costo desde Point/Existencias")
        self.stdout.write(f"  - branch: {branch}")
        self.stdout.write(f"  - matches: {len(rows)}")
        for row in rows[:20]:
            self.stdout.write(
                f"    * [{row.point_code}] {row.point_name} | categoria={row.category_name} | "
                f"cantidad={row.quantity} {row.unit} | costo_unitario={row.unit_cost} | total={row.total_cost}"
            )

        if not options.get("apply"):
            self.stdout.write("Dry-run: usa --apply para persistir costo.")
            return

        created = 0
        skipped = 0
        for row in rows:
            _cost, was_created, status = service.persist_cost_row(row)
            if was_created:
                created += 1
            else:
                skipped += 1
                self.stdout.write(f"    - omitido [{row.point_code}] {row.point_name}: {status}")

        self.stdout.write(self.style.SUCCESS(f"Costos creados: {created}"))
        self.stdout.write(f"  - omitidos: {skipped}")
