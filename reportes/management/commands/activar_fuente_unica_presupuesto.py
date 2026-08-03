from collections import defaultdict
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import ReglaFuenteRubro
from reportes.services_presupuesto_fuentes import corregir_agua_purificada


class Command(BaseCommand):
    help = "Audita y activa claves de fuente única en las reglas de presupuesto."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true")
        parser.add_argument(
            "--sin-corregir-agua",
            action="store_true",
            help="Omite la corrección conocida de agua (solo para diagnóstico).",
        )

    def handle(self, *args, **options):
        aplicar = options["apply"]
        with transaction.atomic():
            if not options["sin_corregir_agua"]:
                corregir_agua_purificada()

            reglas = list(
                ReglaFuenteRubro.objects.filter(activa=True)
                .select_related("rubro", "rubro__area", "rubro__sucursal")
                .order_by("id")
            )
            canonicas = defaultdict(list)
            distribuciones = defaultdict(Decimal)
            for regla in reglas:
                if regla.rubro.area.codigo in {"nomina", "resultados"}:
                    regla.modo_asignacion = ReglaFuenteRubro.MODO_CONTROL
                elif (regla.filtros or {}).get("porcentaje") is not None:
                    regla.modo_asignacion = ReglaFuenteRubro.MODO_DISTRIBUCION
                else:
                    regla.modo_asignacion = ReglaFuenteRubro.MODO_CANONICA
                regla.clave_fuente = regla.calcular_clave_fuente()
                if regla.modo_asignacion == ReglaFuenteRubro.MODO_CANONICA and regla.clave_fuente:
                    canonicas[regla.clave_fuente].append(regla)
                if regla.modo_asignacion == ReglaFuenteRubro.MODO_DISTRIBUCION:
                    distribuciones[regla.clave_fuente] += Decimal(
                        str((regla.filtros or {}).get("porcentaje", 0))
                    )

            duplicadas = [grupo for grupo in canonicas.values() if len(grupo) > 1]
            incompletas = {
                clave: total for clave, total in distribuciones.items() if total != Decimal("100")
            }
            if duplicadas:
                detalle = "; ".join(
                    ", ".join(f"#{r.pk} {r.rubro}" for r in grupo) for grupo in duplicadas
                )
                raise CommandError(f"Fuentes canónicas duplicadas: {detalle}")
            if incompletas:
                detalle = ", ".join(f"{clave[:10]}={total}%" for clave, total in incompletas.items())
                raise CommandError(f"Distribuciones distintas de 100%: {detalle}")

            for regla in reglas:
                regla.full_clean()
                regla.save(update_fields=["modo_asignacion", "clave_fuente", "actualizado_en"])
            if not aplicar:
                transaction.set_rollback(True)

        modo = "APLICADO" if aplicar else "SIMULACIÓN"
        self.stdout.write(
            self.style.SUCCESS(
                f"[{modo}] {len(reglas)} reglas activas auditadas; "
                f"{len(canonicas)} fuentes canónicas; {len(distribuciones)} distribuciones."
            )
        )
