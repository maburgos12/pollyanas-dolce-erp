"""Renombra los rubros de Ventas a los nombres del catálogo Point.

Decisión de dirección: Point y la base de datos son las únicas fuentes de
verdad — los nombres heredados del Excel no deben quedar en el sistema.

Para cada rubro de Ventas con asignación POS (ReglaFuenteRubro VENTA_POS):
- un producto asignado    → el rubro toma el nombre exacto del producto Point;
- varios productos        → toma el nombre más corto (el producto base; las
                            variantes de temporada quedan visibles en la regla);
- categoría completa      → toma el nombre de la categoría Point;
- sin asignación          → conserva su nombre y se reporta.

El nombre anterior queda en metadata["nombre_excel"]: el re-import de la
proyección lo reconoce y no resucita el nombre viejo ni duplica rubros.
Idempotente; --dry-run para previsualizar.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import ReglaFuenteRubro, RubroPresupuesto


class Command(BaseCommand):
    help = "Renombra los rubros de Ventas a los nombres del catálogo Point."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        renombrados = 0
        sin_asignacion = 0
        colisiones = 0

        reglas = (
            ReglaFuenteRubro.objects.filter(
                tipo_fuente=ReglaFuenteRubro.FUENTE_VENTA_POS,
                activa=True,
                rubro__area__codigo="ventas",
                rubro__activo=True,
            )
            .select_related("rubro", "rubro__sucursal")
            .order_by("rubro__concepto")
        )

        with transaction.atomic():
            for regla in reglas:
                rubro = regla.rubro
                filtros = regla.filtros or {}
                productos = filtros.get("productos_pos") or []
                categoria = filtros.get("categoria_pos") or ""

                if productos:
                    nuevo = min(productos, key=len)
                elif categoria:
                    nuevo = categoria
                else:
                    sin_asignacion += 1
                    self.stdout.write(f"  SIN ASIGNACIÓN (conserva nombre): {rubro.concepto}")
                    continue

                if nuevo == rubro.concepto:
                    continue

                colision = (
                    RubroPresupuesto.objects.filter(
                        area=rubro.area,
                        concepto=nuevo,
                        codigo_cuenta=rubro.codigo_cuenta,
                        sucursal=rubro.sucursal,
                    )
                    .exclude(pk=rubro.pk)
                    .exists()
                )
                if colision:
                    colisiones += 1
                    self.stdout.write(
                        self.style.WARNING(
                            f"  COLISIÓN: '{rubro.concepto}' → '{nuevo}' ya existe; no se renombra"
                        )
                    )
                    continue

                self.stdout.write(f"  {rubro.concepto} → {nuevo}")
                renombrados += 1
                if not dry_run:
                    metadata = dict(rubro.metadata or {})
                    metadata.setdefault("nombre_excel", rubro.concepto)
                    rubro.concepto = nuevo
                    rubro.metadata = metadata
                    rubro.save(update_fields=["concepto", "metadata", "actualizado_en"])
            if dry_run:
                transaction.set_rollback(True)

        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(
            f"[{modo}] renombrados: {renombrados}, sin asignación: {sin_asignacion}, colisiones: {colisiones}"
        )
