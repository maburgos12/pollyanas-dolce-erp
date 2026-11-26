from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, models, transaction
from django.db.models.functions import Cast
from django.utils import timezone

from pos_bridge.models.snapshot import PointInventoryCutStorage
from pos_bridge.services.inventory_payload_store import store_exact_inventory_payloads
from pos_bridge.utils.exceptions import PersistenceError


class Command(BaseCommand):
    help = "Verifica o compacta un lote de evidencia de inventario de un dia cerrado de Mazatlan."

    def add_arguments(self, parser):
        parser.add_argument("day", type=date.fromisoformat)
        parser.add_argument("--after-id", type=int, default=0)
        parser.add_argument("--batch-size", type=int, default=500)
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, day, after_id, batch_size, apply, **options):
        mazatlan = ZoneInfo("America/Mazatlan")
        if day >= timezone.localdate(timezone=mazatlan) or after_id < 0 or not 1 <= batch_size <= 2000:
            raise CommandError("Elige un dia cerrado, cursor no negativo y lote de 1 a 2000 filas.")
        if apply and not settings.POINT_INVENTORY_SHARE_PAYLOADS:
            raise CommandError("Habilita primero el escritor y verifica los lectores de evidencia compartida.")

        begin = datetime.combine(day, time.min, mazatlan)
        end = datetime.combine(day + timedelta(days=1), time.min, mazatlan)
        query = (
            PointInventoryCutStorage.objects.filter(
                captured_at__gte=begin, captured_at__lt=end, pk__gt=after_id, shared_payload__isnull=True,
            )
            .exclude(raw_payload={})
            .annotate(document=Cast("raw_payload", models.TextField()))
            .order_by("pk")
            .values_list("pk", "document")
        )
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout = '25s'")
                    cursor.execute("SET LOCAL lock_timeout = '2s'")
                    batch = list((query.select_for_update() if apply else query)[:batch_size])
                    updated = 0
                    if apply and batch:
                        ids, documents = map(list, zip(*batch, strict=True))
                        payload_ids = store_exact_inventory_payloads(documents)
                        cursor.execute("""
                            UPDATE pos_bridge_inventory_cuts s
                            SET shared_payload_id = d.id, raw_payload = '{}'::jsonb
                            FROM unnest(%s::bigint[], %s::bigint[]) AS changes(snapshot_id, payload_id)
                            JOIN pos_bridge_inventory_payloads d ON d.id = changes.payload_id
                            WHERE s.id = changes.snapshot_id AND s.shared_payload_id IS NULL
                              AND s.raw_payload::text = d.payload::text
                        """, [ids, payload_ids])
                        updated = cursor.rowcount
                        if updated != len(ids):
                            raise CommandError("La evidencia no coincide; no se aplico el lote.")
        except PersistenceError as exc:
            raise CommandError(str(exc)) from exc
        next_id = batch[-1][0] if batch else after_id
        self.stdout.write(f"day={day} selected={len(batch)} updated={updated} next_id={next_id}")
