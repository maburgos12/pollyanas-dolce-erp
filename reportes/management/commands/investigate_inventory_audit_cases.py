from datetime import datetime

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from pos_bridge.services.branch_inventory_traceability_service import (
    canonical_point_branch_identity,
)
from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.point_http_client import PointHttpSessionClient
from reportes.models import ProductInventoryAuditCase
from reportes.services_inventory_audit_agent import InventoryAuditAgent
from reportes.services_inventory_traceability import InventoryAuditMaterializer


class Command(BaseCommand):
    help = "Investiga casos guardados y, por excepción, reutiliza el historial Point."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True, help="Mes en formato YYYY-MM.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Calcula resultados sin modificar casos ni crear notificaciones.",
        )
        parser.add_argument(
            "--refresh-point-history",
            action="store_true",
            help="Consulta una vez el historial Point de los casos con diferencia.",
        )
        parser.add_argument(
            "--case-id",
            action="append",
            type=int,
            default=[],
            help="Limita la investigación o consulta Point a uno o más expedientes.",
        )
        parser.add_argument("--no-notify", action="store_true", help="Actualiza investigaciones sin enviar avisos.")

    def handle(self, *args, **options):
        try:
            month = datetime.strptime(options["month"], "%Y-%m").date().replace(day=1)
        except ValueError as exc:
            raise CommandError("--month debe usar el formato YYYY-MM.") from exc

        if options["refresh_point_history"] and not options["dry_run"]:
            self._refresh_point_history(month, case_ids=options["case_id"])
            return

        counters = InventoryAuditAgent().run_month(
            month, dry_run=options["dry_run"], case_ids=options["case_id"] or None,
            notify=not options["no_notify"],
        )
        mode = "VISTA PREVIA" if options["dry_run"] else "APLICADO"
        self.stdout.write(f"{mode} · {month:%Y-%m}")
        self.stdout.write(
            " · ".join(f"{key}={value}" for key, value in counters.items())
        )

    def _refresh_point_history(self, month, *, case_ids):
        branch_aliases, _ = canonical_point_branch_identity()
        cases = (
            ProductInventoryAuditCase.objects.filter(
                month=month,
                branch_id__in=set(branch_aliases.values()),
            )
            .exclude(difference=0)
            .select_related("branch", "product")
            .order_by("id")
        )
        if case_ids:
            cases = cases.filter(id__in=case_ids)
        cases = list(cases)
        captured = 0
        errors = 0
        with point_account_session_lock(wait=True) as acquired:
            if acquired is False:
                raise CommandError("Point está ocupado con otra sincronización de cuenta.")
            with PointHttpSessionClient(load_point_bridge_settings()) as client:
                client.login()
                service = AuditStockHistoryService(client=client)
                for case in cases:
                    try:
                        service.capture(case.branch, case.product, month)
                        captured += 1
                    except Exception as exc:
                        errors += 1
                        self.stderr.write(
                            f"caso={case.id} historial_no_disponible={exc}"
                        )

        counts = InventoryAuditMaterializer().reconcile_existing_cases_from_point_history(
            month,
            case_ids=[case.id for case in cases],
        )
        self.stdout.write(
            f"POINT · {month:%Y-%m} · historiales={captured} · errores={errors}"
        )
        self.stdout.write(
            "CONCILIADO · "
            + " · ".join(f"{key}={value}" for key, value in counts.items())
        )
