from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.utils import timezone

from pos_bridge.models.snapshot import PointInventoryCutStorage, PointInventorySnapshot


_CUT = (
    "id", "branch_id", "product_id", "sync_job_id", "captured_at",
    "stock", "min_stock", "max_stock", "raw_payload", "shared_payload_id",
)


class Command(BaseCommand):
    help = "Verifica o compacta un lote de saldos de inventario de un dia cerrado de Mazatlan."

    def add_arguments(self, parser):
        parser.add_argument("day", type=date.fromisoformat)
        parser.add_argument("--after-id", type=int, default=0)
        parser.add_argument("--batch-size", type=int, default=500)
        parser.add_argument("--max-batches", type=int, default=1)
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, day, after_id, batch_size, max_batches, apply, **options):
        mazatlan = ZoneInfo("America/Mazatlan")
        if day >= timezone.localdate(timezone=mazatlan) or after_id < 0 or not 1 <= batch_size <= 2000:
            raise CommandError("Elige un dia cerrado, cursor no negativo y lote de 1 a 2000 cortes.")
        if not 1 <= max_batches <= 1000:
            raise CommandError("Ejecuta entre 1 y 1000 lotes por invocacion.")
        window = (
            datetime.combine(day, time.min, mazatlan),
            datetime.combine(day + timedelta(days=1), time.min, mazatlan),
        )
        query = PointInventoryCutStorage.objects.filter(
            captured_at__gte=window[0], captured_at__lt=window[1], state__isnull=True,
        ).order_by("pk").values_list(*_CUT)
        for _ in range(max_batches):
            selected, updated, after_id = self._batch(query.filter(pk__gt=after_id), batch_size, apply, after_id)
            self.stdout.write(f"day={day} selected={selected} updated={updated} next_id={after_id}")
            if not selected:
                break

    def _batch(self, query, batch_size, apply, after_id):
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '25s'")
                cursor.execute("SET LOCAL lock_timeout = '2s'")
                batch = list((query.select_for_update() if apply else query)[:batch_size])
                updated = 0
                if apply and batch:
                    ids = [row[0] for row in batch]
                    # Save the logical rows before releasing the legacy values.
                    cursor.execute("""
                        UPDATE public.pos_bridge_inventory_cuts
                        SET state_id = public.pb_resolve_inventory_daily_state(
                            captured_at, branch_id, product_id, stock, min_stock, max_stock
                        ), stock = NULL, min_stock = NULL, max_stock = NULL
                        WHERE id = ANY(%s) AND state_id IS NULL
                    """, [ids])
                    updated = cursor.rowcount
                    restored = list(PointInventorySnapshot.objects.filter(
                        pk__in=ids,
                    ).order_by("pk").values_list(*_CUT))
                    if updated != len(ids) or restored != batch:
                        raise CommandError("Los cortes reconstruidos no coinciden; se revirtio el lote.")
        next_id = batch[-1][0] if batch else after_id
        return len(batch), updated, next_id
