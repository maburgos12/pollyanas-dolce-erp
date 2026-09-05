"""Convierte las compras históricas de Point en entradas del kardex de insumos.

Las compras de Point ya viven en el ERP como ``CostoInsumo`` con
``raw['source'] == 'POINT_COMPRAS_HISTORICAS'``: el importador de costos guardó
el precio y dejó ``raw['quantity']`` sin usar. Sin esas entradas el saldo
teórico de ``FactInventarioDiario`` descuenta consumo contra un kardex que nunca
recibió la materia prima, y deriva a negativos de millones de unidades.

Este comando cierra ese hueco escribiendo un ``MovimientoInventario`` de tipo
ENTRADA por cada línea de compra.

Es ledger histórico: NO toca ``ExistenciaInsumo``. El stock vivo se concilia por
separado con ``sync_inventario_desde_point``, que trata a Point ALMACÉN como
fuente de verdad absoluta; aplicar aquí un delta sobre el saldo actual sumaría
compras de hace meses que ya se consumieron.

Dry-run por defecto. Usa --apply para escribir.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from datetime import datetime, time
from decimal import Decimal, InvalidOperation

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from inventario.models import MovimientoInventario
from maestros.models import CostoInsumo
from pos_bridge.services.inventory_baseline import latest_point_almacen_baseline_run
from pos_bridge.services.unidades import cantidad_en_unidad_erp
from recetas.utils.costeo_snapshot import POINT_UNIT_ALIASES

PURCHASE_SOURCE = "POINT_COMPRAS_HISTORICAS"
ALMACEN_DESTINO = "ALMACEN_1"

# El matching artículo Point -> insumo ERP ya quedó resuelto y persistido en el FK
# de CostoInsumo; aquí solo se descarta el que se resolvió por similitud difusa.
UNTRUSTED_MATCH_METHODS = frozenset({"FUZZY"})


def _normalize_unit(raw_unit: str) -> str:
    return " ".join(str(raw_unit or "").strip().lower().split())


class Command(BaseCommand):
    help = (
        "Crea entradas de kardex a partir de las compras de Point ya importadas en "
        "CostoInsumo. Dry-run por defecto; usa --apply para escribir."
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe los movimientos en la BD.")
        parser.add_argument("--desde", help="Fecha mínima de compra (YYYY-MM-DD).")
        parser.add_argument("--hasta", help="Fecha máxima de compra (YYYY-MM-DD).")
        parser.add_argument(
            "--detalle",
            action="store_true",
            help="Imprime un renglón por compra en vez de solo el resumen.",
        )
        parser.add_argument(
            "--ignorar-baseline",
            action="store_true",
            dest="ignorar_baseline",
            help=(
                "Importa también compras anteriores al último corte de Point ALMACÉN. "
                "DUPLICA el inventario que ese corte ya absorbió: úsalo solo para "
                "reconstruir el histórico a propósito."
            ),
        )

    def handle(self, *args, **options):
        apply_changes = bool(options["apply"])
        detalle = bool(options["detalle"])
        ignorar_baseline = bool(options["ignorar_baseline"])

        # El corte de Point ALMACÉN fija el saldo contra el conteo físico real, así que
        # ya absorbió las compras previas que nunca entraron al kardex. Reinyectarlas
        # las cuenta dos veces, y el sobreconteo es proporcional al ajuste de cada
        # insumo (medido 2026-09: SUSTITUTO DE CREMA se ajustó +6.4M y sus compras
        # suman +8.5M; MEDIA CREMA se ajustó +663K contra +12.2M de compras).
        baseline = latest_point_almacen_baseline_run()
        corte = getattr(baseline, "finished_at", None) if baseline else None
        if corte and not ignorar_baseline:
            self.stdout.write(
                f"Último corte de Point ALMACÉN: {corte:%Y-%m-%d}. "
                "Solo se importan compras posteriores (--ignorar-baseline para forzar).\n"
            )

        queryset = (
            CostoInsumo.objects.filter(raw__source=PURCHASE_SOURCE)
            .select_related("insumo", "insumo__unidad_base", "proveedor")
            .order_by("fecha", "id")
        )
        if options.get("desde"):
            queryset = queryset.filter(fecha__gte=options["desde"])
        if options.get("hasta"):
            queryset = queryset.filter(fecha__lte=options["hasta"])

        if not apply_changes:
            self.stdout.write(
                self.style.WARNING("── DRY-RUN: no se escribe nada. Usa --apply para confirmar.\n")
            )

        planned: list[dict] = []
        blockers: list[dict] = []

        for costo in queryset.iterator():
            raw = costo.raw or {}
            insumo = costo.insumo
            etiqueta = f"{raw.get('article_name') or insumo.nombre} ({costo.fecha})"

            if corte and not ignorar_baseline and costo.fecha <= corte.date():
                blockers.append({"razon": "ANTERIOR_A_BASELINE", "detalle": f"{etiqueta}: corte {corte:%Y-%m-%d}"})
                continue

            method = str(raw.get("match_method") or "").upper()
            if method in UNTRUSTED_MATCH_METHODS:
                blockers.append({"razon": "MATCH_NO_CONFIABLE", "detalle": f"{etiqueta}: método {method}"})
                continue

            try:
                cantidad = Decimal(str(raw.get("quantity")))
            except (InvalidOperation, TypeError, ValueError):
                blockers.append({"razon": "CANTIDAD_INVALIDA", "detalle": f"{etiqueta}: {raw.get('quantity')!r}"})
                continue
            if cantidad <= 0:
                blockers.append({"razon": "CANTIDAD_NO_POSITIVA", "detalle": f"{etiqueta}: {cantidad}"})
                continue

            unidad_point = raw.get("unit")
            # Sin alias conocido, cantidad_en_unidad_erp devuelve la cantidad cruda sin
            # avisar: copiaría galones como mililitros. Se bloquea antes de convertir.
            if not POINT_UNIT_ALIASES.get(_normalize_unit(unidad_point)):
                blockers.append({"razon": "UNIDAD_DESCONOCIDA", "detalle": f"{etiqueta}: '{unidad_point}'"})
                continue

            convertida, nota = cantidad_en_unidad_erp(cantidad, unidad_point, insumo)
            if nota.startswith("UNIDAD INCOMPATIBLE"):
                blockers.append({"razon": "UNIDAD_INCOMPATIBLE", "detalle": f"{etiqueta}: {nota}"})
                continue

            planned.append(
                {
                    "costo": costo,
                    "insumo": insumo,
                    "cantidad_base": Decimal(str(convertida)),
                    "cantidad_point": cantidad,
                    "unidad_point": unidad_point,
                    "nota": nota,
                    "raw": raw,
                }
            )

        created = 0
        already = 0
        for item in planned:
            costo = item["costo"]
            raw = item["raw"]
            insumo = item["insumo"]
            # source_hash del CostoInsumo ya es único por (purchase_id, artículo,
            # cantidad, unidad, costo): sirve de llave de idempotencia derivada.
            source_hash = hashlib.sha256(f"point_purchase|{costo.source_hash}".encode()).hexdigest()

            if MovimientoInventario.objects.filter(source_hash=source_hash).exists():
                already += 1
                continue

            unidad_erp = insumo.unidad_base.codigo if insumo.unidad_base else item["unidad_point"]
            if detalle:
                self.stdout.write(
                    f"  {insumo.nombre}: +{item['cantidad_base']} {unidad_erp} "
                    f"({item['cantidad_point']} {item['unidad_point']}, {costo.fecha}, "
                    f"folio {raw.get('folio') or 's/f'})"
                )

            if apply_changes:
                with transaction.atomic():
                    MovimientoInventario.objects.create(
                        # Mediodía local: inmune al corrimiento de día que produciría
                        # medianoche al convertirse a UTC.
                        fecha=timezone.make_aware(datetime.combine(costo.fecha, time(12, 0))),
                        tipo=MovimientoInventario.TIPO_ENTRADA,
                        insumo=insumo,
                        cantidad=item["cantidad_base"],
                        almacen=ALMACEN_DESTINO,
                        referencia=f"POINT-COMPRA-{raw.get('folio') or costo.id}",
                        notas=str(raw.get("supplier") or "")[:255],
                        registrado_por="importar_compras_point_a_kardex",
                        source_hash=source_hash,
                        trazabilidad={
                            "source": PURCHASE_SOURCE,
                            "purchase_id": raw.get("purchase_id"),
                            "folio": raw.get("folio"),
                            "supplier": raw.get("supplier"),
                            "article_name": raw.get("article_name"),
                            "point_quantity": str(item["cantidad_point"]),
                            "point_unit": item["unidad_point"],
                            "conversion_note": item["nota"],
                            "costo_insumo_id": costo.id,
                            "unit_cost": raw.get("unit_cost"),
                        },
                    )
            created += 1

        self.stdout.write("")
        verbo = "creadas" if apply_changes else "por crear"
        self.stdout.write(self.style.SUCCESS(f"Entradas {verbo}: {created}"))
        self.stdout.write(f"Ya existentes (idempotencia): {already}")
        self.stdout.write(f"Bloqueadas: {len(blockers)}")

        if blockers:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("Bloqueos por razón:"))
            for razon, total in Counter(b["razon"] for b in blockers).most_common():
                self.stdout.write(f"  {razon}: {total}")
            self.stdout.write("")
            for blocker in blockers[:40]:
                self.stdout.write(f"  [{blocker['razon']}] {blocker['detalle']}")
            if len(blockers) > 40:
                self.stdout.write(f"  ... y {len(blockers) - 40} más")

        if not apply_changes:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("DRY-RUN: nada se guardó."))
