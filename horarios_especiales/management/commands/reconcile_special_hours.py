import json

from django.core.management.base import BaseCommand
from django.core.exceptions import PermissionDenied
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from horarios_especiales.models import SolicitudHorarioEspecial
from horarios_especiales.services.execution import execute_request


class Command(BaseCommand):
    help = "Reconcilia una vez las proyecciones aprobadas pendientes (horizonte móvil de 90 días)."

    def add_arguments(self, parser):
        parser.add_argument("--request-id", type=int)

    def handle(self, *args, **options):
        requests = SolicitudHorarioEspecial.objects.filter(canonical_payload__schema_version=2,
            status__in=["APROBADO", "EJECUTADO", "FALLIDO"], approved_at__isnull=False, cancelled_at__isnull=True)
        if options["request_id"]:
            requests = requests.filter(pk=options["request_id"])
        count = failed = 0
        for obj in requests.order_by("pk").iterator():
            if count >= 100:
                break
            if not options["request_id"] and obj.execution_summary_json.get("projection_expired"):
                continue
            due = parse_datetime(obj.execution_summary_json.get("next_reconcile_at") or "")
            if not options["request_id"] and due and due > timezone.now():
                continue
            try:
                summary = execute_request(request_obj=obj)
                failed += summary["failed"]
                count += 1
                self.stdout.write(json.dumps({"request_id": obj.pk, **summary}, ensure_ascii=False))
            except (ValueError, PermissionDenied) as exc:
                failed += 1
                self.stderr.write(f"Solicitud {obj.pk}: {exc}")
        self.stdout.write(json.dumps({"processed": count, "failed": failed}))
