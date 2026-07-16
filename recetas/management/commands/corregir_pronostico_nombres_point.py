"""Corrige los pronósticos de venta ligados a recetas que no son producto Point.

Dos casos (confirmados por dirección 2026-07-15):
1. Variantes de temporada / nombres viejos del Excel → se reasignan a la
   receta del producto Point vigente (sumando si el destino ya tiene
   proyección propia, como el caso San Valentín + regular).
2. Insumos producidos en Dolce (flanes y panes que forman parte del
   producto terminado, con su propia receta de materia prima) → NO son
   productos de venta: sus pronósticos se eliminan del comparativo.

Idempotente: re-correr después de un re-import de la proyección.
"""

from django.core.management.base import BaseCommand
from django.db import transaction

from recetas.models import PronosticoVenta, Receta

REASIGNACIONES = {
    "3 Pecados Mini SV": "Pastel 3 Pecados Mini",
    "Fresas Con Crema Mini SV": "Vaso Fresas con Crema Mini",  # descontinuado, pero es el producto correcto
    "Crunch Mini SV": "Pastel Crunch Mini",
    "Pastel Fresas con Crema San Valentín Chico": "Pastel de Fresas Con Crema Chico",
    "Pay de Guayaba Grande": "Sabor Guayaba Grande",
    "Pay de Guayaba Mediano": "Sabor Guayaba Mediano",
    "Pay de Guayaba R": "Sabor Guayaba Rebanada",
    "Pay de Queso con Fresa San Valentín Mediano": "Sabor Fresa Mediano Pay",
    "Pay de Queso con Fresa San Valentín Grande": "Sabor Fresa Grande Pay",
}

# Componentes producidos (insumo preparado dentro del producto terminado).
INSUMOS_INTERNOS = [
    "Flan 3 Pecados Chico",
    "Flan 3 Pecados Mediano",
    "Flan 3 Pecados Grande",
    "Pan Zanahoria Chico",
    "Pan Zanahoria Mediano",
    "Pan Zanahoria Grande",
    "Pan 3 Leches Mediano",
]


class Command(BaseCommand):
    help = "Reasigna pronósticos de venta a recetas Point vigentes y saca los insumos internos"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        movidos = eliminados = 0

        for origen_nombre, destino_nombre in REASIGNACIONES.items():
            destino = Receta.objects.filter(nombre__iexact=destino_nombre).first()
            if destino is None:
                self.stdout.write(self.style.WARNING(f"  SIN RECETA DESTINO: '{destino_nombre}'"))
                continue
            origenes = PronosticoVenta.objects.filter(
                receta__nombre__iexact=origen_nombre
            ).exclude(receta=destino)
            for pron in origenes:
                existente = PronosticoVenta.objects.filter(
                    receta=destino, periodo=pron.periodo
                ).first()
                if existente is not None:
                    if str(existente.fuente or "").startswith("MANUAL"):
                        self.stdout.write(self.style.WARNING(
                            f"  '{destino_nombre}' {pron.periodo}: destino MANUAL, no se suma"
                        ))
                        continue
                    self.stdout.write(
                        f"  {origen_nombre} {pron.periodo}: +{pron.cantidad} → '{destino_nombre}' "
                        f"(ya tenía {existente.cantidad})"
                    )
                    if not dry_run:
                        existente.cantidad += pron.cantidad
                        existente.save(update_fields=["cantidad", "actualizado_en"])
                        pron.delete()
                else:
                    self.stdout.write(
                        f"  {origen_nombre} {pron.periodo}: {pron.cantidad} → '{destino_nombre}'"
                    )
                    if not dry_run:
                        pron.receta = destino
                        pron.save(update_fields=["receta", "actualizado_en"])
                movidos += 1

        for nombre in INSUMOS_INTERNOS:
            qs = PronosticoVenta.objects.filter(receta__nombre__iexact=nombre)
            n = qs.count()
            if n:
                self.stdout.write(f"  eliminar {n} pronóstico(s) de insumo interno '{nombre}'")
                eliminados += n
                if not dry_run:
                    qs.delete()

        if dry_run:
            transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(self.style.SUCCESS(f"[{modo}] reasignados: {movidos} · eliminados: {eliminados}"))
