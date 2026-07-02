from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from reportes.services_nomina_produccion import sincronizar_mano_obra_produccion


def _parse_period(value: str) -> date:
    try:
        year, month = value.split("-", 1)
        return date(int(year), int(month), 1)
    except ValueError as exc:
        raise CommandError(f"Periodo inválido '{value}'. Usa YYYY-MM.") from exc


def _months_between(desde: date, hasta: date) -> list[date]:
    if hasta < desde:
        raise CommandError("--hasta debe ser igual o posterior a --desde.")
    months = []
    current = desde
    while current <= hasta:
        months.append(current)
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


class Command(BaseCommand):
    help = (
        "Sincroniza mano de obra de producción en GastoOperativoMensual desde "
        "rrhh.NominaLinea (depto PRODUCCION), reemplazando cargas manuales legacy "
        "del mismo mes/categoría."
    )

    def add_arguments(self, parser):
        parser.add_argument("--periodo", help="Mes único, formato YYYY-MM.")
        parser.add_argument("--desde", help="Inicio de rango, formato YYYY-MM.")
        parser.add_argument("--hasta", help="Fin de rango, formato YYYY-MM.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula y muestra sin persistir.")

    def handle(self, *args, **options):
        periodo_option = options.get("periodo")
        desde_option = options.get("desde")
        hasta_option = options.get("hasta")
        dry_run = bool(options.get("dry_run"))

        if periodo_option and (desde_option or hasta_option):
            raise CommandError("Usa --periodo o --desde/--hasta, no ambos.")
        if bool(desde_option) != bool(hasta_option):
            raise CommandError("--desde y --hasta deben usarse juntos.")
        if not periodo_option and not desde_option:
            raise CommandError("Especifica --periodo YYYY-MM o --desde/--hasta YYYY-MM.")

        if periodo_option:
            meses = [_parse_period(periodo_option)]
        else:
            meses = _months_between(_parse_period(desde_option), _parse_period(hasta_option))

        for mes in meses:
            resumen = sincronizar_mano_obra_produccion(mes, dry_run=dry_run)
            if not resumen.escrito and not dry_run:
                self.stdout.write(self.style.WARNING(f"{mes:%Y-%m}: {resumen.motivo}"))
                continue
            estado = "DRY-RUN" if dry_run else "OK"
            self.stdout.write(
                f"{mes:%Y-%m} [{estado}]: monto=${resumen.monto:,.2f} "
                f"filas_legacy_borradas={resumen.filas_legacy_borradas} "
                f"external_key={resumen.external_key or '-'} "
                f"{('· ' + resumen.motivo) if resumen.motivo else ''}"
            )
