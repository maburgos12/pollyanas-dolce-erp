from __future__ import annotations

from django.core.management.base import BaseCommand
from django.utils import dateparse

from conciliacion.services.sucursal_cfdi import guardar_resolucion_sucursal_cfdi, resolver_sucursal_cfdi
from sat_client.models import CfdiDescargado


class Command(BaseCommand):
    help = "Resuelve sucursal fiscal de CFDI a partir de los conceptos del XML."

    def add_arguments(self, parser):
        parser.add_argument("--fecha-inicial", required=True, help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--fecha-final", required=True, help="Fecha final YYYY-MM-DD.")
        parser.add_argument(
            "--tipo-cfdi",
            choices=[CfdiDescargado.TIPO_EMITIDO, CfdiDescargado.TIPO_RECIBIDO],
            default=CfdiDescargado.TIPO_EMITIDO,
        )
        parser.add_argument("--forma-pago", default="", help="Filtra por forma de pago SAT, por ejemplo 01.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula sin guardar resoluciones.")
        parser.add_argument("--limit", type=int, default=0, help="Limite opcional de CFDI a procesar.")

    def handle(self, *args, **options):
        fecha_inicial = dateparse.parse_date(options["fecha_inicial"])
        fecha_final = dateparse.parse_date(options["fecha_final"])
        if fecha_inicial is None or fecha_final is None:
            self.stderr.write(self.style.ERROR("Fechas invalidas. Usa YYYY-MM-DD."))
            return

        queryset = CfdiDescargado.objects.filter(
            fecha_emision__date__gte=fecha_inicial,
            fecha_emision__date__lte=fecha_final,
            tipo_cfdi=options["tipo_cfdi"],
        ).order_by("fecha_emision", "uuid")
        if options["forma_pago"]:
            queryset = queryset.filter(forma_pago=options["forma_pago"])
        if options["limit"]:
            queryset = queryset[: options["limit"]]

        dry_run = bool(options["dry_run"])
        total = 0
        resueltos = 0
        ambiguos = 0
        sin_coincidencia = 0
        por_sucursal: dict[str, int] = {}

        for cfdi in queryset.iterator():
            total += 1
            if dry_run:
                match = resolver_sucursal_cfdi(cfdi)
                fuente = match.fuente
                sucursal_codigo = match.sucursal_codigo
            else:
                resolucion = guardar_resolucion_sucursal_cfdi(cfdi)
                fuente = resolucion.fuente
                sucursal_codigo = resolucion.sucursal.codigo if resolucion.sucursal_id else ""

            if sucursal_codigo:
                resueltos += 1
                por_sucursal[sucursal_codigo] = por_sucursal.get(sucursal_codigo, 0) + 1
            elif fuente == "ambigua":
                ambiguos += 1
            else:
                sin_coincidencia += 1

        self.stdout.write(self.style.SUCCESS("Resolucion de sucursales CFDI finalizada"))
        self.stdout.write(f"  - procesados: {total}")
        self.stdout.write(f"  - resueltos: {resueltos}")
        self.stdout.write(f"  - ambiguos: {ambiguos}")
        self.stdout.write(f"  - sin coincidencia: {sin_coincidencia}")
        for codigo, count in sorted(por_sucursal.items()):
            self.stdout.write(f"  - {codigo}: {count}")
        if dry_run:
            self.stdout.write("  - modo: dry-run, sin guardar cambios")
