from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.product_month_closure_service import ProductMonthClosureError, ProductMonthClosureService


class Command(BaseCommand):
    help = "Construye cierre mensual teorico de producto terminado usando movimientos Point materializados."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True, help="Mes a construir en formato YYYY-MM.")
        parser.add_argument("--rebuild", action="store_true", help="Reconstruye el mes si ya existe y no esta bloqueado.")
        parser.add_argument("--lock-after-build", action="store_true", help="Bloquea el cierre al terminar.")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar el build.")
        parser.add_argument("--approval-note", default="", help="Nota de aprobacion o comentario operacional.")
        parser.add_argument("--approval-reason", default="", help="Motivo corto para build/lock.")

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        service = ProductMonthClosureService()
        try:
            closure = service.build(
                month=options["month"],
                rebuild=bool(options.get("rebuild")),
                lock_after_build=bool(options.get("lock_after_build")),
                built_by=actor,
                approval_note=options.get("approval_note") or "",
                approval_reason=options.get("approval_reason") or "",
                approval_channel="command",
            )
        except ProductMonthClosureError as exc:
            raise CommandError(str(exc)) from exc

        total_ending = sum((line.inventario_final_teorico for line in closure.lines.all()), start=0)
        payload = {
            "closure_id": closure.id,
            "month": closure.month_start.strftime("%Y-%m"),
            "status": closure.status,
            "opening_source": closure.opening_source,
            "line_count": closure.lines.count(),
            "total_ending_inventory": str(total_ending),
            "notes": closure.notes,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
