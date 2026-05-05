from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from ventas.models import EventoVenta, EventoVentaForecast
from ventas.services.forecasting_v9 import V9_MODEL_VERSION, generate_event_forecast_v9, summarize_v9_rows


class Command(BaseCommand):
    help = "Genera forecast v9 explicito por producto+sucursal+dia para un EventoVenta."

    def add_arguments(self, parser):
        parser.add_argument("--evento", required=True, help="Codigo del evento a procesar.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula y reporta sin persistir.")
        parser.add_argument("--replace", action="store_true", help="Borra forecasts existentes del evento y guarda v9.")
        parser.add_argument(
            "--append",
            action="store_true",
            help="Agrega v9 sin borrar. Si ya existe la llave evento/sucursal/producto/fecha, se omite por restriccion unica.",
        )

    def handle(self, *args, **options):
        event_code = options["evento"]
        dry_run = bool(options["dry_run"])
        replace = bool(options["replace"])
        append = bool(options["append"]) or not replace

        if replace and options["append"]:
            raise CommandError("Usa solo una opcion: --replace o --append.")

        event = EventoVenta.objects.filter(code=event_code).first()
        if not event:
            raise CommandError(f"No existe EventoVenta con code={event_code!r}.")

        self.stdout.write(f"START {event.code} model={V9_MODEL_VERSION} dry_run={dry_run} replace={replace} append={append}")
        rows = generate_event_forecast_v9(event)
        summary = summarize_v9_rows(rows)
        self._print_summary(summary)

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY_RUN: no se modifico la base de datos."))
            return

        instances = [
            EventoVentaForecast(
                sales_event=event,
                branch=row["branch"],
                product=row["product"],
                forecast_date=row["forecast_date"],
                base_demand=row["base_demand"],
                event_uplift=row["event_uplift"],
                trend_adjustment=row["trend_adjustment"],
                final_forecast=row["final_forecast"],
                conservative_forecast=row["conservative_forecast"],
                aggressive_forecast=row["aggressive_forecast"],
                confidence_score=0,
                model_version=V9_MODEL_VERSION,
                explanation_json=row["explanation_json"],
            )
            for row in rows
        ]

        with transaction.atomic():
            deleted = 0
            if replace:
                deleted, _ = EventoVentaForecast.objects.filter(sales_event=event).delete()
            before = EventoVentaForecast.objects.filter(sales_event=event, model_version=V9_MODEL_VERSION).count()
            EventoVentaForecast.objects.bulk_create(instances, ignore_conflicts=append and not replace)
            after = EventoVentaForecast.objects.filter(sales_event=event, model_version=V9_MODEL_VERSION).count()

        created = max(after - before, 0)
        skipped = max(len(instances) - created, 0) if append and not replace else 0
        if skipped:
            self.stdout.write(
                self.style.WARNING(
                    f"APPEND omitio {skipped} filas porque EventoVentaForecast es unico por evento/sucursal/producto/fecha. "
                    "Usa --replace para sustituir v8 por v9."
                )
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"END {event.code} rows {len(rows)} products {summary['products_processed']} created {created} deleted {deleted}"
            )
        )

    def _print_summary(self, summary):
        self.stdout.write(f"PRODUCTOS_PROCESADOS: {summary['products_processed']}")
        self.stdout.write(f"FALLBACKS: {summary['fallback_counts']}")
        self.stdout.write(f"ALERTAS: {summary['alert_counts']}")
        self.stdout.write(f"TOTAL_UNIDADES_FORECAST: {summary['total_units']}")
        self.stdout.write("COMPARATIVO_V8_VS_V9_POR_FAMILIA:")
        for family, values in summary["family_comparison"].items():
            self.stdout.write(f"  {family}: v8={values['v8']} v9={values['v9']}")
