from django.core.management.base import BaseCommand, CommandError

from inventario.services_auditoria_insumos import ConsumoInsumoAuditService, parse_period


class Command(BaseCommand):
    help = "Calcula auditoría mensual de consumo teórico vs real de insumos."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Periodo en formato YYYY-MM.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula y muestra resultados sin persistir.")

    def handle(self, *args, **options):
        try:
            periodo = parse_period(options["period"])
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        summary = ConsumoInsumoAuditService().calcular_periodo(periodo, dry_run=options["dry_run"])
        by_alerta = summary.by_alerta
        self.stdout.write(
            self.style.SUCCESS(
                f"Auditoría de insumos · {periodo:%Y-%m} · dry_run={summary.dry_run} · filas={summary.total}"
            )
        )
        self.stdout.write(
            "Alertas: "
            f"OK={by_alerta.get('OK', 0)} | "
            f"MERMA={by_alerta.get('MERMA', 0)} | "
            f"FALTANTE={by_alerta.get('FALTANTE', 0)} | "
            f"SIN_DATOS={by_alerta.get('SIN_DATOS', 0)}"
        )
        self.stdout.write(
            "Fuentes costo: "
            + " | ".join(f"{source}={count}" for source, count in sorted(summary.costo_fuentes.items()))
        )
        self.stdout.write(
            "Razones: "
            + (
                " | ".join(f"{reason}={count}" for reason, count in sorted(summary.razones.items()))
                if summary.razones
                else "sin anomalías de unidad/costo"
            )
        )
        self.stdout.write("")
        self.stdout.write("Top 5 desviaciones por costo absoluto:")
        self.stdout.write("Insumo | Teórico | Real | Dif | Dif % | Dif costo | Alerta")
        for row in summary.top_diferencias:
            self.stdout.write(
                f"{row.insumo.nombre} | "
                f"{row.consumo_teorico} {row.unidad} | "
                f"{row.consumo_real} {row.unidad} | "
                f"{row.diferencia_unidades} | "
                f"{row.diferencia_pct}% | "
                f"${row.diferencia_costo} | "
                f"{row.alerta}"
            )
