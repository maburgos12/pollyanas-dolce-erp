from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, time
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.utils import timezone

from reportes.models import StockMensualSucursal


def _parse_period(value: str) -> date:
    try:
        parsed = datetime.strptime(str(value or "").strip(), "%Y-%m").date()
    except ValueError as exc:
        raise CommandError("--period debe tener formato YYYY-MM.") from exc
    return date(parsed.year, parsed.month, 1)


class Command(BaseCommand):
    help = "Genera stock mensual de apertura/cierre por sucursal y producto desde PointInventorySnapshot."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Periodo a generar en formato YYYY-MM.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula sin persistir cambios.")

    def handle(self, *args, **options):
        period = _parse_period(options["period"])
        dry_run = bool(options.get("dry_run"))
        month_end = date(period.year, period.month, monthrange(period.year, period.month)[1])

        opening_at = self._closest_snapshot_at(period)
        closing_at = self._closest_snapshot_at(month_end)
        if opening_at is None:
            raise CommandError(f"No hay snapshots de inventario para calcular apertura {period:%Y-%m}.")
        if closing_at is None:
            raise CommandError(f"No hay snapshots de inventario para calcular cierre {period:%Y-%m}.")

        opening_rows = self._snapshot_rows_for_day(opening_at.date())
        closing_rows = self._snapshot_rows_for_day(closing_at.date())
        keys = set(opening_rows) | set(closing_rows)
        now = timezone.now()
        objects = []
        for sucursal_id, product_id in sorted(keys):
            opening = opening_rows.get((sucursal_id, product_id))
            closing = closing_rows.get((sucursal_id, product_id))
            objects.append(
                StockMensualSucursal(
                    periodo=period,
                    sucursal_id=sucursal_id,
                    producto_id=product_id,
                    stock_apertura=Decimal(str(opening["stock"] if opening else 0)),
                    stock_cierre=Decimal(str(closing["stock"] if closing else 0)),
                    fuente_apertura=opening["captured_at"] if opening else opening_at,
                    fuente_cierre=closing["captured_at"] if closing else closing_at,
                    creado_en=now,
                    actualizado_en=now,
                    metadata={
                        "source": "PointInventorySnapshot",
                        "opening_snapshot_id": opening["id"] if opening else None,
                        "closing_snapshot_id": closing["id"] if closing else None,
                        "opening_target_date": period.isoformat(),
                        "closing_target_date": month_end.isoformat(),
                        "opening_effective_date": opening_at.date().isoformat(),
                        "closing_effective_date": closing_at.date().isoformat(),
                    },
                )
            )

        self.stdout.write(f"Periodo: {period:%Y-%m}")
        self.stdout.write(f"Apertura objetivo={period.isoformat()} fuente={opening_at.isoformat()} filas={len(opening_rows)}")
        self.stdout.write(f"Cierre objetivo={month_end.isoformat()} fuente={closing_at.isoformat()} filas={len(closing_rows)}")
        self.stdout.write(f"Registros a {'simular' if dry_run else 'upsert'}: {len(objects)}")
        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run: no se modificaron datos."))
            return

        with transaction.atomic():
            StockMensualSucursal.objects.bulk_create(
                objects,
                batch_size=1000,
                update_conflicts=True,
                update_fields=[
                    "stock_apertura",
                    "stock_cierre",
                    "fuente_apertura",
                    "fuente_cierre",
                    "metadata",
                    "actualizado_en",
                ],
                unique_fields=["periodo", "sucursal", "producto"],
            )
        self.stdout.write(self.style.SUCCESS(f"Stock mensual generado: {len(objects)} registros."))

    def _closest_snapshot_at(self, target_date: date):
        target_start = timezone.make_aware(datetime.combine(target_date, time.min), timezone.get_current_timezone())
        target_end = timezone.make_aware(datetime.combine(target_date, time.max), timezone.get_current_timezone())
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT captured_at
                FROM pos_bridge_inventory_snapshots
                WHERE captured_at <= %s
                ORDER BY captured_at DESC, id DESC
                LIMIT 1
                """,
                [target_end],
            )
            before = cursor.fetchone()
            cursor.execute(
                """
                SELECT captured_at
                FROM pos_bridge_inventory_snapshots
                WHERE captured_at >= %s
                ORDER BY captured_at ASC, id ASC
                LIMIT 1
                """,
                [target_start],
            )
            after = cursor.fetchone()
        candidates = [row[0] for row in [before, after] if row]
        if not candidates:
            return None
        return min(candidates, key=lambda value: abs(value - target_start))

    def _snapshot_rows_for_day(self, source_date: date) -> dict[tuple[int, int], dict[str, object]]:
        day_start = timezone.make_aware(datetime.combine(source_date, time.min), timezone.get_current_timezone())
        day_end = timezone.make_aware(datetime.combine(source_date, time.max), timezone.get_current_timezone())
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (b.erp_branch_id, s.product_id)
                    b.erp_branch_id,
                    s.product_id,
                    s.stock,
                    s.captured_at,
                    s.id
                FROM pos_bridge_inventory_snapshots s
                JOIN pos_bridge_branches b ON b.id = s.branch_id
                WHERE s.captured_at >= %s
                  AND s.captured_at <= %s
                  AND b.erp_branch_id IS NOT NULL
                ORDER BY b.erp_branch_id, s.product_id, s.captured_at DESC, s.id DESC
                """,
                [day_start, day_end],
            )
            rows = cursor.fetchall()
        return {
            (row[0], row[1]): {
                "stock": row[2],
                "captured_at": row[3],
                "id": row[4],
            }
            for row in rows
        }
