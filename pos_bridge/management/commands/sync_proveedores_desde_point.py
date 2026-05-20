"""
Extrae el historial de compras desde Point y actualiza Insumo.proveedor_principal.

Lee el módulo /InventoryPurchases/tab_registro_compras de Point, agrega
los proveedores por artículo (proveedor más reciente) y actualiza el campo
proveedor_principal de cada Insumo en el ERP.

Dry-run por defecto. Usa --apply para guardar cambios.
"""
from __future__ import annotations

from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db import transaction

from maestros.models import Insumo, Proveedor
from pos_bridge.services.point_purchase_supplier_sync_service import (
    PointPurchaseSupplierSyncService,
)
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from recetas.utils.normalizacion import normalizar_nombre


class Command(BaseCommand):
    help = (
        "Extrae el historial de compras desde Point y asigna proveedor_principal a cada insumo. "
        "Dry-run por defecto. Usa --apply para guardar."
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Aplica cambios a la BD.")
        parser.add_argument(
            "--dias",
            type=int,
            default=365,
            help="Días hacia atrás para el rango de fechas de compras (default: 365).",
        )
        parser.add_argument(
            "--desde",
            default="",
            help="Fecha inicio ISO (YYYY-MM-DD). Sobreescribe --dias.",
        )
        parser.add_argument(
            "--hasta",
            default="",
            help="Fecha fin ISO (YYYY-MM-DD). Default: hoy.",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        apply = bool(options["apply"])
        dry = not apply

        if dry:
            self.stdout.write(self.style.WARNING(
                "── DRY-RUN: ningún cambio se guardará. Usa --apply para confirmar.\n"
            ))

        hasta: date = date.today()
        desde: date = hasta - timedelta(days=int(options["dias"]))

        if options["hasta"]:
            try:
                hasta = date.fromisoformat(options["hasta"].strip())
            except ValueError:
                self.stderr.write(f"Fecha --hasta inválida: {options['hasta']}")
                return

        if options["desde"]:
            try:
                desde = date.fromisoformat(options["desde"].strip())
            except ValueError:
                self.stderr.write(f"Fecha --desde inválida: {options['desde']}")
                return

        self.stdout.write(f"Rango de compras: {desde} → {hasta}")
        self.stdout.write("Conectando a Point (módulo de compras)...")

        service = PointPurchaseSupplierSyncService()
        identity = PointRecipeIdentityService()

        rows = service.scrape_purchase_rows(desde=desde, hasta=hasta)
        self.stdout.write(f"Point entregó {len(rows)} líneas de compra.\n")

        if not rows:
            self.stdout.write(self.style.WARNING(
                "Sin filas de compras. Verifica el rango de fechas o la conexión."
            ))
            if dry:
                transaction.set_rollback(True)
            return

        insumo_proveedor_map = service.build_insumo_supplier_map(rows)
        self.stdout.write(f"Insumos distintos en compras: {len(insumo_proveedor_map)}\n")

        updated = 0
        sin_match = 0
        sin_cambio = 0
        errores: list[str] = []

        for articulo_upper, proveedor_name in sorted(insumo_proveedor_map.items()):
            resolved = identity.resolve_insumo(point_name=articulo_upper)
            if resolved.insumo is None:
                sin_match += 1
                self.stdout.write(
                    self.style.WARNING(f"  SIN MATCH: {articulo_upper!r}")
                )
                continue

            insumo = resolved.insumo
            proveedor_norm = normalizar_nombre(proveedor_name)

            try:
                proveedor_obj, _ = Proveedor.objects.get_or_create(
                    nombre__iexact=proveedor_name,
                    defaults={"nombre": proveedor_name.strip().upper(), "activo": True},
                )
            except Exception as exc:
                errores.append(f"{articulo_upper}: {exc}")
                continue

            if insumo.proveedor_principal_id == proveedor_obj.id:
                sin_cambio += 1
                continue

            prev_nombre = insumo.proveedor_principal.nombre if insumo.proveedor_principal_id else "(ninguno)"
            self.stdout.write(
                self.style.SUCCESS(
                    f"  {insumo.nombre}: {prev_nombre!r} → {proveedor_obj.nombre!r}"
                )
            )

            if not dry:
                insumo.proveedor_principal = proveedor_obj
                insumo.save(update_fields=["proveedor_principal"])

            updated += 1

        self.stdout.write(f"\n{'─'*60}")
        self.stdout.write(f"  Líneas de compra     : {len(rows)}")
        self.stdout.write(f"  Insumos en compras   : {len(insumo_proveedor_map)}")
        self.stdout.write(f"  Actualizados         : {updated}")
        self.stdout.write(f"  Sin cambio           : {sin_cambio}")
        self.stdout.write(f"  Sin match ERP        : {sin_match}  ← no encontrados en catálogo ERP")
        if errores:
            self.stdout.write(self.style.ERROR(f"  Errores              : {len(errores)}"))
            for e in errores:
                self.stdout.write(self.style.ERROR(f"    · {e}"))

        if dry:
            transaction.set_rollback(True)
            self.stdout.write(self.style.WARNING("\nDRY-RUN completo. Usa --apply para guardar.\n"))
        else:
            self.stdout.write(self.style.SUCCESS(
                f"\n✓ Proveedores principales actualizados desde historial de compras Point.\n"
            ))
