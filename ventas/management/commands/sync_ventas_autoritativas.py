from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from ventas.services.sales_truth import sync_authoritative_from_vps


def _previous_month_period(today: date | None = None) -> str:
    today = today or date.today()
    if today.month == 1:
        return f"{today.year - 1}-12"
    return f"{today.year}-{today.month - 1:02d}"


class Command(BaseCommand):
    help = "Sincroniza VentaAutoritativaPoint desde PointSalesDailyProductFact"

    def add_arguments(self, parser):
        parser.add_argument("--periodo", default="", help="Periodo YYYY-MM. Default: mes anterior completo.")
        parser.add_argument("--sucursal", type=int, default=None, help="ID de sucursal ERP. Default: todas.")
        parser.add_argument("--dry-run", action="store_true", help="Muestra qué se crearía sin persistir.")

    def handle(self, *args, **options):
        periodo = (options.get("periodo") or "").strip() or _previous_month_period()
        sucursal_id = options.get("sucursal")
        dry_run = bool(options.get("dry_run"))
        try:
            result = sync_authoritative_from_vps(periodo, sucursal_id=sucursal_id, dry_run=dry_run)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(f"Periodo: {result['periodo']}")
        self.stdout.write(f"Sucursal: {sucursal_id or 'todas'}")
        self.stdout.write(f"Registros fuente (PointSalesDailyProductFact): {result['source_count']}")
        if dry_run:
            self.stdout.write(f"[DRY-RUN] Se crearían {result['creados']} registros")
            self.stdout.write(f"[DRY-RUN] Se actualizarían {result['actualizados']} registros")
            if result["examples"]:
                self.stdout.write("[DRY-RUN] Primeros 5 registros:")
                for row in result["examples"]:
                    self.stdout.write(
                        "  "
                        f"{row['sale_date']} | {row['branch']} | {row['product_code']} | "
                        f"{row['point_name']} | qty={row['quantity']} | venta={row['total_amount']}"
                    )
        else:
            self.stdout.write(f"Creados: {result['creados']}")
            self.stdout.write(f"Actualizados: {result['actualizados']}")
        self.stdout.write(f"Errores: {len(result['errores'])}")
        for error in result["errores"]:
            self.stdout.write(f"  - {error}")
