from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q, Sum
from django.utils import timezone
from unidecode import unidecode

from pos_bridge.models import PointDailySale, PointProduct, PointProductCategory
from recetas.models import Receta


NORMALIZACION = {
    "GALLETAS": "Galletas",
    "PAN": "Pan",
    "MASAS": "Masas",
    "PASTELES": "Pastel",
    "Pasteles": "Pastel",
    "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)": "Betún y Rellenos",
}

FAMILY_RULES = [
    ("Pay", ["pay"]),
    ("Cheesecake", ["cheesecake"]),
    ("Bollo", ["bollo"]),
    ("Bebidas", ["latte", "cappuccino", "capuchino", "café", "cafe", "americano", "chocolate caliente", "chocola"]),
    ("Vasos Preparados", ["frappe", "vaso"]),
    ("Galletas", ["galleta"]),
    ("Pastel", ["pastel"]),
    ("Otros postres", []),
]

POINT_CLASS_RULES = [
    ("TOPPING", ["topping", "empaque pay"]),
    (
        "SERVICIO_ACCESORIO",
        ["servicio domicilio", "letrero", "pirotecnia", "chispas", "encendedor", "vela", "tarjeta de regalo", "extra 100"],
    ),
    ("REVENTA", ["coca-cola", "coca cola", "espagueti", "aderezo"]),
]

# Resoluciones revisadas contra el nombre de Point y los candidatos ERP vendidos.
# La llave usa el codigo Point normalizado (external_id o sku). No se usa matching
# por cercania cuando cruza familias distintas.
AMBIGUOS_RESUELTOS = {
    "116": 5,  # Bollo Chocolate -> Bollo Chocolate, no Navidad
    "117": 11,  # Bollo Vainilla -> Bollo Vainilla, no Navidad
    "118": 14,  # Bollo Zanahoria -> Bollo Zanahoria, no Navidad
    "125": 19,  # Galleta Chispas Chocolate -> Galleta Chispas Chocolate
    "1015": 141,  # Latte Vainilla -> Latte Vainilla
    "1003": 91,  # Pastel Lotus Chico -> Pastel Lotus Chico
    "1004": 95,  # Pastel Lotus Mediano -> Pastel Lotus Mediano
    "1002": 98,  # Pastel Lotus Mini -> Pastel Lotus Mini, nunca Pascua
    "445": 26,  # Pastel de 3 Leches Individual -> Pastel de 3 Leches Individual
    "100": 48,  # Pastel de Fresas Con Crema Mediano -> mismo producto
}

# No existe receta segura para esta venta: los candidatos detectados eran pasteles.
# Se fuerza como no-receta para clasificarla como accesorio/servicio.
AMBIGUOS_SIN_RECETA = {
    "310": "SERVICIO_ACCESORIO",  # TARJETA DE REGALO
}


@dataclass
class SoldRecipeRow:
    receta: Receta
    family: str


@dataclass
class UnmatchedPointRow:
    point_product: PointProduct
    qty: Decimal
    amount: Decimal
    suggested_type: str


@dataclass
class ClassificationSummary:
    normalizaciones: Counter[str] = field(default_factory=Counter)
    recipe_family_assignments: list[SoldRecipeRow] = field(default_factory=list)
    point_unmatched: list[UnmatchedPointRow] = field(default_factory=list)
    point_ambiguous: list[tuple[PointProduct, list[Receta]]] = field(default_factory=list)
    point_resolved_ambiguous: list[tuple[PointProduct, Receta]] = field(default_factory=list)
    point_forced_unmatched: list[tuple[PointProduct, str]] = field(default_factory=list)
    point_categories_created: Counter[str] = field(default_factory=Counter)
    point_categories_existing: Counter[str] = field(default_factory=Counter)
    point_categories_skipped: list[UnmatchedPointRow] = field(default_factory=list)


class Command(BaseCommand):
    help = "Clasifica solo productos vendidos en Point: familias ERP, sugerencias para Point sin receta y normalización de familias."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Simula cambios sin escribir en BD.")
        parser.add_argument(
            "--ejecutar",
            action="store_true",
            help="Aplica normalización, familias ERP vendidas y categorías Point sin receta.",
        )
        parser.add_argument("--days", type=int, default=30, help="Ventana de ventas Point a considerar.")

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        ejecutar = bool(options["ejecutar"])
        if dry_run and ejecutar:
            raise CommandError("Usa solo una opcion: --dry-run o --ejecutar.")
        if not dry_run and not ejecutar:
            dry_run = True
        days = int(options["days"] or 30)
        if days <= 0:
            raise CommandError("--days debe ser mayor a cero.")

        summary = self._process(days=days, dry_run=dry_run)
        self._print_summary(summary=summary, days=days, dry_run=dry_run, ejecutar=ejecutar)

    def _process(self, *, days: int, dry_run: bool) -> ClassificationSummary:
        summary = ClassificationSummary()
        fecha_corte = timezone.now().date() - timedelta(days=days)
        recetas_by_code = self._recipes_by_normalized_code()
        sold_products = self._sold_products_since(fecha_corte)

        with transaction.atomic():
            for source, target in NORMALIZACION.items():
                qs = Receta.objects.filter(familia=source)
                count = qs.count()
                if count:
                    summary.normalizaciones[target] += count
                    if not dry_run:
                        qs.update(familia=target)

            matched_recipe_ids: set[int] = set()
            for product in sold_products:
                candidates = self._match_recipes_for_product(product, recetas_by_code)
                if len(candidates) == 1:
                    receta = candidates[0]
                    matched_recipe_ids.add(int(receta.id))
                    if self._resolved_recipe_id_for_product(product):
                        summary.point_resolved_ambiguous.append((product, receta))
                elif len(candidates) > 1:
                    summary.point_ambiguous.append((product, candidates))
                elif self._forced_unmatched_type_for_product(product):
                    summary.point_forced_unmatched.append((product, self._forced_unmatched_type_for_product(product) or ""))

            recipes_to_classify = (
                Receta.objects.filter(
                    id__in=matched_recipe_ids,
                    tipo=Receta.TIPO_PRODUCTO_FINAL,
                )
                .filter(Q(familia="") | Q(familia__isnull=True))
                .order_by("nombre", "id")
            )
            for receta in recipes_to_classify:
                family = self._family_for_name(receta.nombre)
                summary.recipe_family_assignments.append(SoldRecipeRow(receta=receta, family=family))
                if not dry_run:
                    receta.familia = family
                    receta.save(update_fields=["familia"])

            sold_totals = self._sold_totals_since(fecha_corte)
            for product in sorted(sold_products, key=lambda item: (item.name, item.external_id)):
                candidates = self._match_recipes_for_product(product, recetas_by_code)
                if candidates:
                    continue
                qty, amount = sold_totals.get(int(product.id), (Decimal("0"), Decimal("0")))
                summary.point_unmatched.append(
                    UnmatchedPointRow(
                        point_product=product,
                        qty=qty,
                        amount=amount,
                        suggested_type=self._suggest_point_type(product),
                    )
                )

            self._persist_point_categories(summary=summary, dry_run=dry_run)

            if dry_run:
                transaction.set_rollback(True)

        return summary

    def _print_summary(self, *, summary: ClassificationSummary, days: int, dry_run: bool, ejecutar: bool) -> None:
        self.stdout.write(f"clasificar_productos_point · days={days} · dry_run={dry_run} · ejecutar={ejecutar}")
        self.stdout.write("")
        self.stdout.write("Normalizaciones de familias existentes:")
        if summary.normalizaciones:
            for family, count in sorted(summary.normalizaciones.items()):
                self.stdout.write(f"  {family}: {count}")
        else:
            self.stdout.write("  0")

        self.stdout.write("")
        self.stdout.write(f"Recetas ERP vendidas sin familia: {len(summary.recipe_family_assignments)}")
        by_family = Counter(row.family for row in summary.recipe_family_assignments)
        for family, count in sorted(by_family.items()):
            self.stdout.write(f"  {family}: {count}")
        self.stdout.write("Lista recetas vendidas sin familia:")
        for row in summary.recipe_family_assignments:
            self.stdout.write(f"  {row.receta.codigo_point} | {row.receta.nombre} -> {row.family}")

        self.stdout.write("")
        self.stdout.write(f"Point ambiguos resueltos por lista blanca: {len(summary.point_resolved_ambiguous)}")
        for product, receta in summary.point_resolved_ambiguous:
            self.stdout.write(
                f"  {product.external_id} | {product.sku or ''} | {product.name} -> "
                f"{receta.id}:{receta.codigo_point}:{receta.nombre}"
            )
        self.stdout.write(f"Point forzados como no-receta: {len(summary.point_forced_unmatched)}")
        for product, suggested_type in summary.point_forced_unmatched:
            self.stdout.write(f"  {product.external_id} | {product.sku or ''} | {product.name} -> {suggested_type}")

        self.stdout.write("")
        self.stdout.write(f"Point vendidos sin receta ERP: {len(summary.point_unmatched)}")
        point_by_type = Counter(row.suggested_type for row in summary.point_unmatched)
        for suggested_type, count in sorted(point_by_type.items()):
            self.stdout.write(f"  {suggested_type}: {count}")
        self.stdout.write("Lista Point vendidos sin receta con clasificación sugerida:")
        for row in summary.point_unmatched:
            product = row.point_product
            self.stdout.write(
                "  "
                f"{product.external_id} | {product.sku or ''} | {product.name} | "
                f"cat={product.category} | qty={row.qty} | amount={row.amount} -> {row.suggested_type}"
            )
        self.stdout.write("PointProductCategory:")
        if dry_run:
            creatable = Counter(
                row.suggested_type
                for row in summary.point_unmatched
                if row.suggested_type in self._valid_point_category_values()
            )
            for suggested_type, count in sorted(creatable.items()):
                self.stdout.write(f"  CREARIA {suggested_type}: {count}")
        else:
            for suggested_type, count in sorted(summary.point_categories_created.items()):
                self.stdout.write(f"  CREADOS {suggested_type}: {count}")
            for suggested_type, count in sorted(summary.point_categories_existing.items()):
                self.stdout.write(f"  EXISTENTES {suggested_type}: {count}")
        if summary.point_categories_skipped:
            self.stdout.write("PointProductCategory omitidos:")
            for row in summary.point_categories_skipped:
                product = row.point_product
                self.stdout.write(f"  {product.external_id} | {product.name} -> {row.suggested_type}")

        self.stdout.write("")
        self.stdout.write(f"Point vendidos con matching ambiguo: {len(summary.point_ambiguous)}")
        for product, candidates in summary.point_ambiguous[:60]:
            candidate_text = "; ".join(f"{recipe.id}:{recipe.codigo_point}:{recipe.nombre}" for recipe in candidates)
            self.stdout.write(f"  {product.external_id} | {product.sku or ''} | {product.name} -> {candidate_text}")

    def _recipes_by_normalized_code(self) -> dict[str, list[Receta]]:
        index: dict[str, list[Receta]] = defaultdict(list)
        for receta in Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).exclude(codigo_point="").order_by("id"):
            key = self._normalize_code(receta.codigo_point)
            if key:
                index[key].append(receta)
        return dict(index)

    def _sold_products_since(self, fecha_corte):
        product_ids = (
            PointDailySale.objects.filter(sale_date__gte=fecha_corte, product_id__isnull=False)
            .values_list("product_id", flat=True)
            .distinct()
        )
        return list(PointProduct.objects.filter(id__in=product_ids, active=True).order_by("name", "external_id"))

    def _sold_totals_since(self, fecha_corte) -> dict[int, tuple[Decimal, Decimal]]:
        rows = (
            PointDailySale.objects.filter(sale_date__gte=fecha_corte, product_id__isnull=False)
            .values("product_id")
            .annotate(qty=Sum("quantity"), amount=Sum("total_amount"))
        )
        return {
            int(row["product_id"]): (
                Decimal(str(row["qty"] or 0)),
                Decimal(str(row["amount"] or 0)),
            )
            for row in rows
        }

    def _match_recipes_for_product(self, product: PointProduct, recipes_by_code: dict[str, list[Receta]]) -> list[Receta]:
        if self._forced_unmatched_type_for_product(product):
            return []
        resolved_id = self._resolved_recipe_id_for_product(product)
        if resolved_id:
            receta = Receta.objects.filter(id=resolved_id, tipo=Receta.TIPO_PRODUCTO_FINAL).first()
            if receta:
                return [receta]
        keys = {self._normalize_code(product.external_id), self._normalize_code(product.sku)}
        keys.discard("")
        candidates: list[Receta] = []
        seen: set[int] = set()
        for key in keys:
            for receta in recipes_by_code.get(key, []):
                if int(receta.id) in seen:
                    continue
                candidates.append(receta)
                seen.add(int(receta.id))
        return candidates

    def _family_for_name(self, name: str) -> str:
        normalized = self._normalize_name(name)
        for family, tokens in FAMILY_RULES:
            if not tokens:
                continue
            for token in tokens:
                if self._normalize_name(token) in normalized:
                    return family
        return "Otros postres"

    def _suggest_point_type(self, product: PointProduct) -> str:
        forced_type = self._forced_unmatched_type_for_product(product)
        if forced_type:
            return forced_type
        haystack = self._normalize_name(f"{product.name} {product.category}")
        for suggested_type, tokens in POINT_CLASS_RULES:
            for token in tokens:
                if self._normalize_name(token) in haystack:
                    return suggested_type
        if product.category and self._normalize_name(product.category) in {"coca cola", "granmark", "plasticos", "regalos"}:
            return "REVENTA"
        if product.category and self._normalize_name(product.category) in {"alegria", "pillines", "accesorios de reposteria"}:
            return "SERVICIO_ACCESORIO"
        return "REVISAR_MAURICIO"

    def _persist_point_categories(self, *, summary: ClassificationSummary, dry_run: bool) -> None:
        valid_categories = self._valid_point_category_values()
        for row in summary.point_unmatched:
            if row.suggested_type not in valid_categories:
                summary.point_categories_skipped.append(row)
                continue
            if dry_run:
                continue
            product = row.point_product
            _, created = PointProductCategory.objects.get_or_create(
                codigo_point=(product.external_id or product.sku or "")[:50],
                defaults={
                    "nombre": (product.name or "")[:200],
                    "category": row.suggested_type,
                    "notas": "Clasificado automaticamente desde ventas Point sin receta ERP.",
                },
            )
            if created:
                summary.point_categories_created[row.suggested_type] += 1
            else:
                summary.point_categories_existing[row.suggested_type] += 1

    def _valid_point_category_values(self) -> set[str]:
        return {choice[0] for choice in PointProductCategory.CATEGORY_CHOICES}

    def _normalize_code(self, value: str | None) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if raw.isdigit():
            return raw.lstrip("0") or "0"
        return raw.upper()

    def _normalize_name(self, value: str | None) -> str:
        return " ".join(unidecode(value or "").casefold().strip().split())

    def _product_keys(self, product: PointProduct) -> set[str]:
        keys = {self._normalize_code(product.external_id), self._normalize_code(product.sku)}
        keys.discard("")
        return keys

    def _resolved_recipe_id_for_product(self, product: PointProduct) -> int | None:
        for key in self._product_keys(product):
            if key in AMBIGUOS_RESUELTOS:
                return AMBIGUOS_RESUELTOS[key]
        return None

    def _forced_unmatched_type_for_product(self, product: PointProduct) -> str:
        for key in self._product_keys(product):
            if key in AMBIGUOS_SIN_RECETA:
                return AMBIGUOS_SIN_RECETA[key]
        return ""
