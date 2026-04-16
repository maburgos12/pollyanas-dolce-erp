from __future__ import annotations

from pathlib import Path
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum

from ventas.models import (
    EventoVenta,
    EventoVentaFinancial,
    EventoVentaProducto,
    EventoVentaProjectionArtifact,
    EventoVentaSucursal,
)


class Command(BaseCommand):
    help = (
        "Sincroniza canasta/ventana desde el evento fuente y reprocesa forecast, financieros "
        "y artifacts de eventos estacionales en una sola corrida auditable."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-code",
            default="DÍADELASMADRES-260510-001",
            help="Código del evento fuente para copiar productos y sucursales.",
        )
        parser.add_argument(
            "--event-code",
            action="append",
            dest="event_codes",
            help="Código de evento a reprocesar. Repetible. Si se omite, usa Niño/Madres/Padre.",
        )
        parser.add_argument(
            "--skip-postmortem",
            action="store_true",
            default=True,
            help="Omite postmortem para eventos futuros.",
        )

    def handle(self, *args, **options):
        source_code = options["source_code"]
        event_codes = options.get("event_codes") or [
            "DÍADELNIÑO-260430-001",
            "DÍADELASMADRES-260510-001",
            "DÍADELPADRE-260621-001",
        ]
        skip_postmortem = bool(options.get("skip_postmortem"))

        source_event = EventoVenta.objects.filter(code=source_code).first()
        if not source_event:
            raise CommandError(f"No existe el evento fuente {source_code}.")
        actor = source_event.approved_by or source_event.created_by
        if actor is None:
            raise CommandError("No se encontró usuario actor para regenerar artifacts.")

        from ventas.views import _event_financial_dataset, _refresh_event_detail_snapshot, _reprocess_event_for_audit

        product_links = list(source_event.products.filter(is_active=True).select_related("product"))
        branch_links = list(source_event.branches.filter(is_active=True).select_related("branch", "comparable_branch"))
        if not product_links or not branch_links:
            raise CommandError("El evento fuente no tiene productos o sucursales activas para sincronizar.")

        start_offset = (source_event.analysis_start_date - source_event.main_date).days
        end_offset = (source_event.analysis_end_date - source_event.main_date).days

        for code in event_codes:
            event = EventoVenta.objects.filter(code=code).first()
            if not event:
                raise CommandError(f"No existe el evento {code}.")

            self.stdout.write(self.style.MIGRATE_HEADING(f"Procesando {event.code}"))
            self._sync_products(event, product_links)
            self._sync_branches(event, branch_links)
            event.analysis_start_date = event.main_date + timedelta(days=start_offset)
            event.analysis_end_date = event.main_date + timedelta(days=end_offset)
            event.save(update_fields=["analysis_start_date", "analysis_end_date", "updated_at"])

            execution = _reprocess_event_for_audit(
                event,
                actor,
                skip_purchases=False,
                skip_postmortem=skip_postmortem,
            )
            event.refresh_from_db()
            _refresh_event_detail_snapshot(event, generated_by=actor)
            event.refresh_from_db()

            main_qty = (
                event.forecasts.filter(forecast_date=event.main_date).aggregate(total=Sum("final_forecast"))["total"]
            )
            base = event.financials.filter(scenario="BASE").first()
            day_summary = _event_financial_dataset(
                event,
                event.forecasts.all(),
                start_date=event.main_date,
                end_date=event.main_date,
            )["summary"]
            current_artifacts = list(
                event.projection_artifacts.filter(forecast_version=event.version).order_by("export_type")
            )
            missing_files = [artifact.file_name for artifact in current_artifacts if not Path(artifact.file_path).exists()]

            self.stdout.write(f"- productos activos: {event.products.filter(is_active=True).count()}")
            self.stdout.write(f"- sucursales activas: {event.branches.filter(is_active=True).count()}")
            self.stdout.write(f"- forecast filas: {event.forecasts.count()}")
            self.stdout.write(f"- forecast día principal: {main_qty}")
            self.stdout.write(f"- ventas día principal: {day_summary['sales']}")
            self.stdout.write(f"- ventas ventana BASE: {base.estimated_sales if base else 'N/A'}")
            self.stdout.write(f"- artifacts vigentes: {len(current_artifacts)}")
            if missing_files:
                raise CommandError(f"Artifacts faltantes en disco para {event.code}: {missing_files}")
            if len(current_artifacts) != 5:
                raise CommandError(f"Se esperaban 5 artifacts vigentes para {event.code} y se encontraron {len(current_artifacts)}.")
            self.stdout.write(self.style.SUCCESS(f"Evento {event.code} cerrado correctamente."))
            self.stdout.write(f"  execution={execution}")
            self.stdout.write(f"  artifacts={[artifact.export_type for artifact in current_artifacts]}")

    def _sync_products(self, event: EventoVenta, source_links: list[EventoVentaProducto]) -> None:
        source_by_product = {link.product_id: link for link in source_links}
        existing_links = {link.product_id: link for link in event.products.all()}

        for product_id, link in existing_links.items():
            should_be_active = product_id in source_by_product
            new_source_type = source_by_product[product_id].source_type if should_be_active else link.source_type
            new_reason = source_by_product[product_id].inclusion_reason if should_be_active else link.inclusion_reason
            updates = []
            if link.is_active != should_be_active:
                link.is_active = should_be_active
                updates.append("is_active")
            if should_be_active and link.source_type != new_source_type:
                link.source_type = new_source_type
                updates.append("source_type")
            if should_be_active and link.inclusion_reason != new_reason:
                link.inclusion_reason = new_reason
                updates.append("inclusion_reason")
            if updates:
                link.save(update_fields=updates)

        for product_id, source_link in source_by_product.items():
            if product_id in existing_links:
                continue
            EventoVentaProducto.objects.create(
                sales_event=event,
                product=source_link.product,
                source_type=source_link.source_type,
                inclusion_reason=source_link.inclusion_reason,
                is_active=True,
            )

    def _sync_branches(self, event: EventoVenta, source_links: list[EventoVentaSucursal]) -> None:
        source_by_branch = {link.branch_id: link for link in source_links}
        existing_links = {link.branch_id: link for link in event.branches.all()}

        for branch_id, link in existing_links.items():
            should_be_active = branch_id in source_by_branch
            source_link = source_by_branch.get(branch_id)
            updates = []
            if link.is_active != should_be_active:
                link.is_active = should_be_active
                updates.append("is_active")
            if should_be_active and link.comparable_branch_id != source_link.comparable_branch_id:
                link.comparable_branch = source_link.comparable_branch
                updates.append("comparable_branch")
            if updates:
                link.save(update_fields=updates)

        for branch_id, source_link in source_by_branch.items():
            if branch_id in existing_links:
                continue
            EventoVentaSucursal.objects.create(
                sales_event=event,
                branch=source_link.branch,
                comparable_branch=source_link.comparable_branch,
                is_active=True,
            )
