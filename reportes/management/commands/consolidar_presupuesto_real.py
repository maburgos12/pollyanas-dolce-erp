"""Consolida el monto real por rubro×periodo desde las fuentes ERP mapeadas."""

from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from reportes.models import LineaPresupuestoMensual
from reportes.services_presupuesto_real import (
    PresupuestoRealConsolidacionService,
    migrar_fuentes_legadas,
)


def _parse_periodo(raw: str) -> date:
    try:
        year, month = raw.split("-")
        return date(int(year), int(month), 1)
    except (ValueError, AttributeError):
        raise CommandError(f"Periodo inválido '{raw}' (formato YYYY-MM)")


def _iter_meses(desde: date, hasta: date):
    actual = desde
    while actual <= hasta:
        yield actual
        actual = date(actual.year + (actual.month == 12), actual.month % 12 + 1, 1)


class Command(BaseCommand):
    help = "Llena LineaPresupuestoMensual.monto_real desde las fuentes mapeadas."

    def add_arguments(self, parser):
        parser.add_argument("--periodo", help="Mes único YYYY-MM")
        parser.add_argument("--desde", help="Inicio de rango YYYY-MM")
        parser.add_argument("--hasta", help="Fin de rango YYYY-MM")
        parser.add_argument(
            "--presupuesto-version",
            dest="presupuesto_version",
            default=LineaPresupuestoMensual.VERSION_ORIGINAL,
            help="ORIGINAL o REVISADO (no confundir con --version, reservado por Django).",
        )
        parser.add_argument("--areas", nargs="*", help="Códigos de área a consolidar")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--migrar-legado",
            action="store_true",
            help="Migra fuente_real heredados al namespace AUTO:/MANUAL: antes de consolidar.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        if options["periodo"]:
            periodos = [_parse_periodo(options["periodo"])]
        elif options["desde"] and options["hasta"]:
            periodos = list(_iter_meses(_parse_periodo(options["desde"]), _parse_periodo(options["hasta"])))
        else:
            raise CommandError("Indica --periodo o el rango --desde/--hasta")

        if options["migrar_legado"]:
            migrados = migrar_fuentes_legadas(dry_run=dry_run)
            for legado, n in migrados.items():
                self.stdout.write(f"namespace legado '{legado}': {n} líneas")

        service = PresupuestoRealConsolidacionService()
        for periodo in periodos:
            summary = service.consolidar(
                periodo=periodo,
                version=options["presupuesto_version"],
                areas=options["areas"] or None,
                dry_run=dry_run,
            )
            data = summary.as_dict()
            modo = "DRY-RUN" if dry_run else "OK"
            self.stdout.write(
                f"[{modo}] {data['periodo']} v{data['version']}: "
                f"actualizadas={data['actualizadas']} sin_cambio={data['sin_cambio']} "
                f"manual_protegidas={data['protegidas_manual']} sin_regla={data['sin_regla']} "
                f"sin_datos={data['sin_datos_fuente']}"
            )
            for err in summary.errores:
                self.stdout.write(self.style.ERROR(f"  ERROR {err}"))
            if dry_run:
                for cambio in summary.detalle[:40]:
                    self.stdout.write(
                        f"  {cambio['rubro']}: {cambio['anterior']} -> {cambio['nuevo']} ({cambio['fuente']})"
                    )
                if len(summary.detalle) > 40:
                    self.stdout.write(f"  ... y {len(summary.detalle) - 40} cambios más")
