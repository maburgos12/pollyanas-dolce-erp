"""Crea renglones de Ventas (ppto 0) para familias vivas nunca presupuestadas.

Aprobado por dirección (2026-07-19): todo producto Point debe pertenecer a un
renglón. Los productos de año corrido que el Excel nunca presupuestó (solo
contaban en el ingreso total) reciben su renglón con presupuesto en 0; el
real vivo entra por sus overrides VENTA_POS del CSV de mapeo.

Renglones por producto (nombre exacto Point) y por categoría completa
(Granmark, Velas Sparklers, Accesorios de repostería, Plásticos — mismo
patrón que Alegría/Pillines). Idempotente y con --dry-run. Después de
correrlo: seed_reglas_fuente_rubro y consolidar_presupuesto_real.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from reportes.models import LineaPresupuestoMensual, RubroPresupuesto

# Nombres exactos de Point (productos) o de categoría Point (renglones de
# categoría completa). El real lo asignan los overrides del CSV de mapeo.
RENGLONES = [
    "Pastel de Fresas Con Crema R",
    "Pastel de 3 Leches Rebanada",
    "Pay de Plátano Grande",
    "Pay de Plátano Rebanada",
    "Pastel de Vainilla Grande",
    "Pastel de Vainilla Mediano",
    "Pastel de Vainilla Chico",
    "Piñatero Mini",
    "Pastel Piñatero Mediano",
    "Pastel Piñatero Grande",
    "Piñatero Chico",
    "Cheesecakes Lotus M",
    "Cheesecake Tortuga M",
    "Cheesecake Rol Canela M",
    "Litro crema",
    "Vaso con crema grande",
    "Granmark",
    "Velas Sparklers",
    "Accesorios de repostería",
    "Plásticos",
]


class Command(BaseCommand):
    help = "Crea renglones de Ventas con presupuesto 0 para familias vivas sin renglón."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        # Los periodos del año presupuestado se copian de un renglón existente.
        molde = RubroPresupuesto.objects.filter(
            area__codigo="ventas", concepto="Especiales/Temporada", activo=True
        ).first()
        if molde is None:
            raise CommandError("No existe el rubro 'Especiales/Temporada' (correr la reestructura primero).")
        periodos = list(molde.lineas_mensuales.values_list("periodo", flat=True))

        creados = 0
        with transaction.atomic():
            for concepto in RENGLONES:
                existe = RubroPresupuesto.objects.filter(
                    area__codigo="ventas", concepto=concepto, activo=True
                ).exists()
                if existe:
                    self.stdout.write(f"  ya existe: '{concepto}'")
                    continue
                if not dry_run:
                    rubro = RubroPresupuesto.objects.create(
                        area=molde.area,
                        concepto=concepto,
                        tipo=RubroPresupuesto.TIPO_INGRESO,
                        metadata={"source": "RENGLONES_FAMILIAS_VIVAS_2026-07-19"},
                        creado_en=timezone.now(),
                    )
                    for periodo in periodos:
                        LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=periodo)
                creados += 1
                self.stdout.write(f"  creado: '{concepto}' (ppto 0)")
            if dry_run:
                transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(f"[{modo}] {creados} renglones nuevos de {len(RENGLONES)}.")
