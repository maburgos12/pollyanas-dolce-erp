from __future__ import annotations

from datetime import datetime

from django.core.management.base import BaseCommand, CommandError

from inventario.services_consumo_bom import ConsumoInsumoAutoService, parse_period


class Command(BaseCommand):
    help = "Genera movimientos CONSUMO calculados por BOM desde producción real Point."

    def add_arguments(self, parser):
        parser.add_argument("--period", help="Periodo en formato YYYY-MM.")
        parser.add_argument("--desde", help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--hasta", help="Fecha final YYYY-MM-DD.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula y muestra resultados sin persistir.")

    def handle(self, *args, **options):
        period = options.get("period")
        desde = options.get("desde")
        hasta = options.get("hasta")
        if period and (desde or hasta):
            raise CommandError("Usa --period o --desde/--hasta, no ambos.")
        if period:
            try:
                periodo = parse_period(period)
            except ValueError as exc:
                raise CommandError(str(exc)) from exc
            summary = ConsumoInsumoAutoService().generar_consumos_periodo(periodo.strftime("%Y-%m"), dry_run=options["dry_run"])
        else:
            if not desde or not hasta:
                raise CommandError("Debes enviar --period YYYY-MM o --desde YYYY-MM-DD --hasta YYYY-MM-DD.")
            try:
                fecha_inicio = datetime.strptime(desde, "%Y-%m-%d").date()
                fecha_fin = datetime.strptime(hasta, "%Y-%m-%d").date()
            except ValueError as exc:
                raise CommandError("Las fechas deben tener formato YYYY-MM-DD.") from exc
            if fecha_fin < fecha_inicio:
                raise CommandError("--hasta no puede ser menor que --desde.")
            summary = ConsumoInsumoAutoService().generar_consumos_produccion(
                fecha_inicio,
                fecha_fin,
                dry_run=options["dry_run"],
            )

        self.stdout.write(
            self.style.SUCCESS(
                "Consumos BOM · "
                f"{summary.fecha_inicio:%Y-%m-%d}..{summary.fecha_fin:%Y-%m-%d} · "
                f"dry_run={summary.dry_run}"
            )
        )
        self.stdout.write(f"Producciones procesadas: {summary.producciones_procesadas}")
        self.stdout.write(f"Líneas de producción procesadas: {summary.lineas_produccion_procesadas}")
        self.stdout.write(f"Recetas de venta-servicio procesadas: {summary.recetas_venta_servicio_procesadas}")
        self.stdout.write(f"Movimientos CONSUMO generados: {summary.movimientos_generados}")
        if not summary.dry_run:
            self.stdout.write(
                "Persistencia: "
                f"creados={summary.movimientos_creados} | "
                f"actualizados={summary.movimientos_actualizados} | "
                f"sin_cambio={summary.movimientos_sin_cambio}"
            )
        self.stdout.write(f"Insumos actualizados: {summary.insumos_actualizados}")
        self.stdout.write(
            "Omitidos: "
            f"sin_receta={summary.omitidos_sin_receta} | "
            f"bom_incompleto={summary.omitidos_bom_incompleto} | "
            f"unidad_incompatible={summary.omitidos_unidad_incompatible} | "
            f"reventa_sin_insumo={summary.omitidos_sin_insumo_reventa}"
        )
        self.stdout.write("")
        self.stdout.write("Top 5 insumos con mayor consumo calculado:")
        self.stdout.write("Insumo | Cantidad | Costo | Fuente | Origen")
        for item in summary.top_consumos:
            self.stdout.write(
                f"{item.insumo.nombre} | "
                f"{item.cantidad} {item.insumo.unidad_base.codigo if item.insumo.unidad_base_id else ''} | "
                f"${item.costo_total} | "
                f"{item.fuente} | "
                f"{item.origen}"
            )
