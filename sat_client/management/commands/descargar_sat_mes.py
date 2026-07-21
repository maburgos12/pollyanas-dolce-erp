from __future__ import annotations

import calendar
from datetime import date, timedelta

from django.core.management.base import BaseCommand, CommandError

from sat_client.models import SolicitudDescarga
from sat_client.tasks import _procesar_con_split, _solicitud_periodo_registrada


def _rango_mes(mes: str) -> tuple[date, date]:
    try:
        anio_str, mes_str = mes.split("-")
        anio, numero = int(anio_str), int(mes_str)
        inicio = date(anio, numero, 1)
    except (ValueError, TypeError) as exc:
        raise CommandError(f"Mes invalido '{mes}': usar formato YYYY-MM") from exc
    fin = date(anio, numero, calendar.monthrange(anio, numero)[1])
    ayer = date.today() - timedelta(days=1)
    if fin > ayer:
        # No cubrir el dia en curso: quedaria marcado como resuelto y la
        # descarga nocturna lo saltaria aunque lleguen CFDIs mas tarde.
        fin = ayer
    if inicio > fin:
        raise CommandError(f"El mes {mes} aun no tiene dias completos que descargar")
    return inicio, fin


class Command(BaseCommand):
    help = (
        "Backfill de CFDIs del SAT por mes completo (una solicitud por mes y "
        "direccion, en lugar de solicitudes diarias). Ej: "
        "descargar_sat_mes --mes 2026-01 --mes 2026-02"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--mes",
            action="append",
            required=True,
            help="Mes a descargar en formato YYYY-MM (repetible)",
        )
        parser.add_argument(
            "--direccion",
            choices=["emitidos", "recibidos", "ambas"],
            default="ambas",
        )
        parser.add_argument(
            "--forzar",
            action="store_true",
            help="Solicitar aunque el periodo ya tenga una solicitud registrada",
        )

    def handle(self, *args, **options):
        direcciones = (
            [SolicitudDescarga.DIRECCION_EMITIDOS, SolicitudDescarga.DIRECCION_RECIBIDOS]
            if options["direccion"] == "ambas"
            else [options["direccion"]]
        )
        total_nuevos = 0
        for mes in options["mes"]:
            inicio, fin = _rango_mes(mes)
            for direccion in direcciones:
                if not options["forzar"] and _solicitud_periodo_registrada(inicio, fin, direccion):
                    self.stdout.write(f"{mes} {direccion}: ya registrado, se omite (usa --forzar)")
                    continue
                self.stdout.write(f"{mes} {direccion}: solicitando {inicio} a {fin}...")
                resultados = _procesar_con_split(inicio, fin, direccion)
                descargados = sum(int(r["descargados"]) for r in resultados)
                nuevos = sum(int(r["nuevos"]) for r in resultados)
                total_nuevos += nuevos
                self.stdout.write(
                    self.style.SUCCESS(
                        f"{mes} {direccion}: {nuevos} nuevos de {descargados} descargados "
                        f"({len(resultados)} solicitud(es))"
                    )
                )
        self.stdout.write(self.style.SUCCESS(f"Backfill terminado: {total_nuevos} CFDIs nuevos"))
