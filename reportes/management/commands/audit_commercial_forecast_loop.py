from __future__ import annotations

from datetime import date, timedelta
from io import StringIO
from pathlib import Path

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone

from reportes.views import (
    _present_daily_sales_branch_ids,
    _required_daily_sales_branches,
    _sales_source_context,
    _validated_zero_daily_sales_branch_codes,
)
from ventas.models import EventoVenta, EventoVentaDetailSnapshot, EventoVentaProjectionArtifact


class Command(BaseCommand):
    help = (
        "Audita el loop comercial recurrente: completitud diaria válida, alineación "
        "forecast/snapshot/artifacts y guard estacional."
    )

    def add_arguments(self, parser):
        parser.add_argument("--date", type=str, help="Fecha de referencia YYYY-MM-DD")
        parser.add_argument("--days-back", type=int, default=30, help="Ventana hacia atrás para completitud diaria")
        parser.add_argument("--skip-seasonal-audit", action="store_true", help="No correr la auditoría estacional 10/10")
        parser.add_argument("--write-report", action="store_true", help="Escribe reporte markdown en output/forecast_audits")

    def handle(self, *args, **options):
        reference_date = date.fromisoformat(options["date"]) if options.get("date") else timezone.localdate()
        days_back = max(1, int(options.get("days_back") or 30))
        source = _sales_source_context()
        latest_sales_date = source.get("latest_date") or reference_date
        if latest_sales_date > reference_date:
            latest_sales_date = reference_date
        window_start = latest_sales_date - timedelta(days=days_back - 1)

        completeness_rows: list[dict[str, object]] = []
        holes: list[dict[str, object]] = []
        cursor = window_start
        while cursor <= latest_sales_date:
            required = _required_daily_sales_branches(cursor)
            present_ids = _present_daily_sales_branch_ids(source=source, target_date=cursor)
            valid_zero_codes = _validated_zero_daily_sales_branch_codes(cursor)
            missing = [
                item
                for item in required
                if int(item["branch_id"]) not in present_ids and str(item["branch_code"] or "") not in valid_zero_codes
            ]
            completeness_rows.append(
                {
                    "date": cursor,
                    "required": len(required),
                    "present": len(required) - len(missing),
                    "missing_codes": [str(item["branch_code"] or "") for item in missing],
                }
            )
            if missing:
                holes.append(completeness_rows[-1])
            cursor += timedelta(days=1)

        tracked_statuses = [
            EventoVenta.STATUS_MODELADO,
            EventoVenta.STATUS_LISTO_REVISION,
            EventoVenta.STATUS_PENDIENTE_DG,
        ]
        events = list(
            EventoVenta.objects.filter(status__in=tracked_statuses).order_by("main_date", "id")
        )
        event_rows: list[dict[str, object]] = []
        for event in events:
            snapshot = (
                EventoVentaDetailSnapshot.objects.filter(sales_event=event)
                .order_by("-snapshot_version")
                .first()
            )
            artifact_count = EventoVentaProjectionArtifact.objects.filter(
                sales_event=event,
                forecast_version=event.version,
            ).count()
            snapshot_version = int(snapshot.snapshot_version) if snapshot else None
            event_rows.append(
                {
                    "code": event.code,
                    "name": event.name,
                    "status": event.status,
                    "version": int(event.version),
                    "snapshot_version": snapshot_version,
                    "artifacts": int(artifact_count),
                    "aligned": bool(snapshot_version == int(event.version) and artifact_count > 0),
                }
            )

        seasonal_stdout = ""
        if not options.get("skip_seasonal_audit"):
            seasonal_buffer = StringIO()
            call_command(
                "audit_seasonal_event_forecasts",
                enforce_status=True,
                write_report=True,
                stdout=seasonal_buffer,
            )
            seasonal_stdout = seasonal_buffer.getvalue().strip()

        report_path: Path | None = None
        if options.get("write_report"):
            report_dir = Path(settings.BASE_DIR) / "output" / "forecast_audits"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / f"commercial_forecast_loop_audit_{reference_date.isoformat()}.md"
            report_path.write_text(
                self._build_report(
                    reference_date=reference_date,
                    latest_sales_date=latest_sales_date,
                    completeness_rows=completeness_rows,
                    holes=holes,
                    event_rows=event_rows,
                    seasonal_stdout=seasonal_stdout,
                ),
                encoding="utf-8",
            )

        self.stdout.write(
            self.style.SUCCESS(
                "Loop comercial auditado "
                f"ventas_holes={len(holes)} eventos={len(event_rows)}"
            )
        )
        if report_path is not None:
            self.stdout.write(self.style.SUCCESS(f"Reporte: {report_path}"))

    def _build_report(
        self,
        *,
        reference_date: date,
        latest_sales_date: date,
        completeness_rows: list[dict[str, object]],
        holes: list[dict[str, object]],
        event_rows: list[dict[str, object]],
        seasonal_stdout: str,
    ) -> str:
        lines = [
            "# Auditoría recurrente del loop comercial",
            "",
            f"- Ejecutada: {timezone.now().isoformat()}",
            f"- Fecha de referencia: `{reference_date.isoformat()}`",
            f"- Última fecha visible de ventas: `{latest_sales_date.isoformat()}`",
            f"- Huecos diarios válidos pendientes: `{len(holes)}`",
            "",
            "## Completitud diaria",
            "",
            "| Fecha | Requeridas | Presentes | Faltantes |",
            "|---|---:|---:|---|",
        ]
        for row in completeness_rows:
            missing_codes = ", ".join(row["missing_codes"]) if row["missing_codes"] else "ninguno"
            lines.append(
                f"| {row['date'].isoformat()} | {row['required']} | {row['present']} | {missing_codes} |"
            )

        lines.extend(
            [
                "",
                "## Eventos vigilados",
                "",
                "| Evento | Estado | Versión | Snapshot | Artifacts | Alineado |",
                "|---|---|---:|---:|---:|---|",
            ]
        )
        for row in event_rows:
            lines.append(
                f"| {row['code']} | {row['status']} | {row['version']} | "
                f"{row['snapshot_version'] if row['snapshot_version'] is not None else 'sin snapshot'} | "
                f"{row['artifacts']} | {'sí' if row['aligned'] else 'no'} |"
            )

        if seasonal_stdout:
            lines.extend(
                [
                    "",
                    "## Salida de auditoría estacional",
                    "",
                    "```text",
                    seasonal_stdout,
                    "```",
                ]
            )

        return "\n".join(lines) + "\n"
