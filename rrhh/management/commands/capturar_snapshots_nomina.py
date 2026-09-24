from datetime import date

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from rrhh.models import NominaLinea


def parse_periodo(value: str) -> date:
    try:
        periodo = date.fromisoformat(f"{value}-01")
    except (TypeError, ValueError) as exc:
        raise CommandError("--periodo debe usar el formato YYYY-MM.") from exc
    if periodo.strftime("%Y-%m") != value:
        raise CommandError("--periodo debe usar el formato YYYY-MM.")
    return periodo


class Command(BaseCommand):
    help = "Previsualiza o captura explícitamente snapshots faltantes de nómina."

    def add_arguments(self, parser):
        parser.add_argument("--periodo", required=True, help="Mes de nómina YYYY-MM")
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        periodo = parse_periodo(options["periodo"])
        aplicar = options["apply"]
        with transaction.atomic():
            lineas = list(
                NominaLinea.objects.select_for_update(of=("self",))
                .filter(
                    periodo__fecha_fin__year=periodo.year,
                    periodo__fecha_fin__month=periodo.month,
                )
                .select_related("empleado", "empleado__sucursal_ref", "periodo")
                .order_by("periodo__fecha_inicio", "empleado_id", "id")
            )
            parciales = [
                linea
                for linea in lineas
                if bool(linea.sucursal_snapshot_id) != bool(linea.departamento_snapshot)
            ]
            if parciales:
                ids = ", ".join(str(linea.pk) for linea in parciales)
                raise CommandError(
                    f"Hay snapshots parciales que requieren revisión manual: {ids}."
                )
            pendientes = [
                linea
                for linea in lineas
                if not linea.sucursal_snapshot_id and not linea.departamento_snapshot
            ]
            invalidas = [
                linea
                for linea in pendientes
                if not linea.empleado.sucursal_ref_id or not linea.empleado.departamento
            ]
            if invalidas:
                ids = ", ".join(str(linea.pk) for linea in invalidas)
                raise CommandError(
                    "No se puede capturar la ficha actual de líneas sin sucursal o "
                    f"departamento: {ids}."
                )

            self.stdout.write(
                f"periodo={periodo:%Y-%m} lineas={len(lineas)} "
                f"pendientes={len(pendientes)} completas={len(lineas) - len(pendientes)}"
            )
            for linea in pendientes:
                self.stdout.write(
                    f"linea={linea.pk} empleado={linea.empleado_id} "
                    f"sucursal_actual={linea.empleado.sucursal_ref_id} "
                    f"departamento_actual={linea.empleado.departamento}"
                )

            if not aplicar:
                self.stdout.write("DRY-RUN: sin cambios")
                return

            capturado_en = timezone.now()
            for linea in pendientes:
                linea.sucursal_snapshot_id = linea.empleado.sucursal_ref_id
                linea.departamento_snapshot = linea.empleado.departamento
                linea.snapshot_origen = NominaLinea.SNAPSHOT_CAPTURA_EXPLICITA
                linea.snapshot_capturado_en = capturado_en
            if pendientes:
                NominaLinea.objects.bulk_update(
                    pendientes,
                    [
                        "sucursal_snapshot",
                        "departamento_snapshot",
                        "snapshot_origen",
                        "snapshot_capturado_en",
                    ],
                    batch_size=500,
                )
            self.stdout.write(
                self.style.SUCCESS(f"APLICADO: snapshots capturados={len(pendientes)}")
            )
