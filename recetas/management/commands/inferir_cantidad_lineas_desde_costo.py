from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from django.core.management.base import BaseCommand
from django.db.models import Q

from recetas.models import LineaReceta


class Command(BaseCommand):
    help = (
        "Infiere cantidad en líneas de receta ligadas cuando falta cantidad y "
        "existe costo_linea_excel + costo_unitario_snapshot."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica los cambios. Sin esta bandera corre en dry-run.",
        )
        parser.add_argument(
            "--max-cantidad",
            type=float,
            default=1000.0,
            help="Límite superior de cantidad inferida para evitar outliers (default: 1000).",
        )
        parser.add_argument(
            "--min-match-score",
            type=float,
            default=0.0,
            help="Score mínimo de matching para considerar inferencia (default: 0).",
        )
        parser.add_argument(
            "--only-auto",
            action="store_true",
            help="Solo evalúa líneas en AUTO_APPROVED.",
        )
        parser.add_argument(
            "--relax-piece-rule",
            action="store_true",
            help=(
                "Permite fracciones en líneas detectadas como pieza. "
                "Útil cuando el origen no trae unidad confiable."
            ),
        )
        parser.add_argument(
            "--use-linecost-as-qty-when-tiny",
            action="store_true",
            help=(
                "Si qty=costo_linea/snapshot resulta minúscula, usa costo_linea_excel "
                "como cantidad (fallback para archivos donde esa columna trae cantidad)."
            ),
        )

    def handle(self, *args, **options):
        max_cantidad = Decimal(str(options["max_cantidad"]))
        min_match_score = float(options["min_match_score"])
        candidates = (
            LineaReceta.objects.filter(insumo__isnull=False)
            .filter(Q(cantidad__isnull=True) | Q(cantidad__lte=0))
            .filter(costo_linea_excel__gt=0, costo_unitario_snapshot__gt=0)
            .select_related("receta", "insumo")
            .order_by("receta__nombre", "posicion")
        )
        if min_match_score > 0:
            candidates = candidates.filter(match_score__gte=min_match_score)
        if options.get("only_auto"):
            candidates = candidates.filter(match_status=LineaReceta.STATUS_AUTO)
        relax_piece_rule = bool(options.get("relax_piece_rule"))
        fallback_linecost_qty = bool(options.get("use_linecost_as_qty_when_tiny"))

        total = candidates.count()
        inferibles = []
        skipped = 0
        for linea in candidates.iterator():
            try:
                qty_raw = Decimal(str(linea.costo_linea_excel)) / Decimal(str(linea.costo_unitario_snapshot))
            except (InvalidOperation, ZeroDivisionError):
                skipped += 1
                continue

            if qty_raw <= 0 or qty_raw > max_cantidad:
                skipped += 1
                continue

            is_piece = self._is_piece_line(linea)
            if fallback_linecost_qty:
                # Algunos archivos legados guardan cantidad en costo_linea_excel.
                # Si la qty calculada por costo unitario es demasiado chica, toma el valor directo.
                try:
                    excel_value = Decimal(str(linea.costo_linea_excel))
                except InvalidOperation:
                    excel_value = Decimal("0")
                if qty_raw < Decimal("0.005") and excel_value > 0 and excel_value <= max_cantidad:
                    qty_raw = excel_value
            qty = self._normalize_qty(qty_raw, is_piece, relax_piece_rule)
            if qty is None:
                skipped += 1
                continue
            inferibles.append((linea, qty))

        self.stdout.write("Inferencia de cantidades por costo")
        self.stdout.write(f"  - candidatas evaluadas: {total}")
        self.stdout.write(f"  - inferibles: {len(inferibles)}")
        self.stdout.write(f"  - omitidas por outlier/error: {skipped}")

        if inferibles:
            self.stdout.write("  - muestra:")
            for linea, qty in inferibles[:15]:
                self.stdout.write(
                    f"    * {linea.receta.nombre} | pos={linea.posicion} | "
                    f"{linea.insumo_texto} -> cantidad={qty}"
                )

        if not options["apply"]:
            self.stdout.write("Dry-run: no se actualizaron líneas. Usa --apply para confirmar.")
            return

        updated = 0
        for linea, qty in inferibles:
            linea.cantidad = qty
            linea.save(update_fields=["cantidad"])
            updated += 1

        self.stdout.write(self.style.SUCCESS(f"Líneas actualizadas: {updated}"))

    def _is_piece_line(self, linea: LineaReceta) -> bool:
        unit_text = (linea.unidad_texto or "").strip().lower()
        piece_tokens = {"pza", "pz", "pieza", "piezas", "unidad", "und", "u"}
        if unit_text in piece_tokens:
            return True
        if linea.unidad and linea.unidad.tipo == "UNIT":
            return True
        if linea.insumo and linea.insumo.unidad_base and linea.insumo.unidad_base.tipo == "UNIT":
            return True
        return False

    def _normalize_qty(self, qty_raw: Decimal, is_piece: bool, relax_piece_rule: bool = False) -> Decimal | None:
        if is_piece and not relax_piece_rule:
            # En piezas evitamos fracciones improbables; solo aceptamos cercanía a entero.
            if qty_raw < Decimal("0.5"):
                return None
            nearest = qty_raw.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            if abs(qty_raw - nearest) > Decimal("0.25"):
                return None
            return nearest

        if qty_raw < Decimal("0.001"):
            return None
        return qty_raw.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
