from __future__ import annotations

import json
from datetime import date

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from reportes.models import ProyectoInversion
from reportes.services_investment_projects import ProyectoInversionRefreshService


class Command(BaseCommand):
    help = "Refresca snapshots mensuales de proyectos de inversion activos o en recuperacion."

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-id",
            action="append",
            type=int,
            default=[],
            help="ID de proyecto especifico. Puede repetirse.",
        )
        parser.add_argument("--until", help="Fecha limite YYYY-MM-DD. Default: fecha local actual.")

    def handle(self, *args, **options):
        until = self._parse_until(options.get("until"))
        project_ids = list(dict.fromkeys(options.get("project_id") or []))
        statuses = [ProyectoInversion.ESTATUS_ACTIVO, ProyectoInversion.ESTATUS_EN_RECUPERACION]
        projects = ProyectoInversion.objects.filter(estatus__in=statuses).order_by("id")
        if project_ids:
            projects = projects.filter(id__in=project_ids)

        service = ProyectoInversionRefreshService()
        refreshed: list[dict[str, object]] = []
        for project in projects:
            result = service.refresh_project(project, until=until)
            refreshed.append(
                {
                    "project_id": result.project_id,
                    "snapshots_updated": result.snapshots_updated,
                    "latest_period": result.latest_period.isoformat() if result.latest_period else None,
                    "project_status": result.project_status,
                    "data_gaps": result.data_gaps,
                }
            )

        payload = {
            "until": until.isoformat(),
            "project_ids": project_ids,
            "projects_refreshed": len(refreshed),
            "snapshots_created": sum(int(item["snapshots_updated"]) for item in refreshed),
            "results": refreshed,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))

    def _parse_until(self, raw_until: str | None) -> date:
        if not raw_until:
            return timezone.localdate()
        try:
            return date.fromisoformat(str(raw_until).strip())
        except ValueError as exc:
            raise CommandError("--until debe venir en formato YYYY-MM-DD.") from exc
