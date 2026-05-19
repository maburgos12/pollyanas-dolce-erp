"""
Asigna categorías a insumos sin categoría usando reglas de keywords sobre nombre normalizado.
Por defecto es dry-run; usa --apply para guardar cambios.
Usa --normalizar para estandarizar a MAYÚSCULAS las categorías existentes.
"""
from __future__ import annotations

from unidecode import unidecode

from django.core.management.base import BaseCommand

from maestros.models import Insumo


# Reglas en orden de prioridad: la primera que haga match gana.
# Cada regla: (CATEGORÍA, [keywords en minúsculas sin acento])
RULES: list[tuple[str, list[str]]] = [
    # Limpieza primero (evitar falsos positivos con "bolsa" en empaques)
    ("LIMPIEZA", [
        "bactericida", "pinol", "power clean", "suavel", "papel de bano",
        "papel bano", "jabón", "jabon", "zote", "detergente", "cloro",
        "desinfectante", "ariel", "fabuloso", "escoba", "trapeador",
        "bolsa de basura", "bolsas de basura", "rollo toalla", "rollos toalla",
        "limpiador", "sanitizante",
    ]),
    # Bebidas
    ("BEBIDAS", [
        "nescafe", "cafe", "te ", " te ", "agua purif", "refresco", "jugo ",
        "licor", "ron ", "kirsch", "brandy", "presidente", "calahua",
        "kahlua", "baileys", "vodka", "tequila", "whisky",
    ]),
    # Huevos
    ("HUEVOS", ["huevo", "clara ", "yema "]),
    # Frutas
    ("FRUTAS", [
        "fresa", "piña", "pina ", "mango", "manzana", "pera ", "durazno",
        "cereza", "kiwi", "uva ", "arandano", "guayaba", "mora ", "coco",
        "chabacano", "datil", "ciruela", "blueberry", "zarzamora",
        "cocktail de fruta", "jugo de pina", "jugo de fresa",
    ]),
    # Chocolates y derivados
    ("CHOCOLATES", [
        "chocolate", "cocoa", "cacao", "nutella", "crunch", "snicker",
        "milkyway", "milky way", "kit kat", "ferrero", "bubulubu",
        "muibon", "deleite choco",
    ]),
    # Lácteos
    ("LACTEOS", [
        "leche", "crema ", "mantequilla", "margarina", "manteca",
        "queso", "yogurt", "yoghurt", "la lechera", "chantilly",
        "base chantilly", "polvo dream whip", "crema lotus", "lotus",
    ]),
    # Harinas y almidones
    ("HARINAS", [
        "harina", "almidon", "fecula", "maizena", "avena",
    ]),
    # Azúcares y mieles
    ("AZUCARES", [
        "azucar", "piloncillo", "miel karo", "miel de", "jarabe",
        "splenda", "stevia", "glucosa", "cajeta",
    ]),
    # Galletas base
    ("GALLETAS", [
        "galleta", "oreo", "ritz", "maria ", "canela",
    ]),
    # Rellenos, mermeladas y cremas de producto
    ("RELLENOS Y CREMAS", [
        "relleno", "mermelada", "polvo de merengue", "glaseado",
        "colorante mermelada", "vainilla dawn", "dona dawn",
    ]),
    # Colorantes y decoración
    ("DECORACION", [
        "colorante", "gragea", "perla ", "corazon", "granillo",
        "liston", "estrella", "confeti", "sugar",
    ]),
    # Masas y bases de producción interna
    ("MASAS", [
        "masa ", "base cheesecake", "base 3 leches", "bizcocho",
        "batido", "especial devils", "vainilla ", "dona ",
    ]),
    # Empaques (cajas, domos, charolas, moldes)
    ("EMPAQUES", [
        "domo", "charola", "caja carton", "caja pastel", "caja cheesecake",
        "base cheesecake", "aluminio pay", "alumino pay", "rebanada pay",
        "bisagra bollo", "molde ", "porta vasos", "tapa vasos",
        "caja ", "box ", "bolsa camiseta",
    ]),
    # Desechables (vasos, cucharas, platos, mangas, ligas)
    ("DESECHABLES", [
        "vaso", "cuchara", "plato ", "desechable", "cubierto",
        "cinta adhesiva", "liga no", "manga ", "bowl ",
        "8oz", "9oz", "12oz", "16oz",
    ]),
    # Etiquetas y papel
    ("ETIQUETAS", [
        "etiqueta", "papel cliche", "papelcliche", "sticker",
    ]),
    # Complementos (sal, vinagre, levadura, nueces, especias)
    ("COMPLEMENTOS", [
        "sal ", "vinagre", "levadura", "nuez", "almendra",
        "ajonjoli", "canela", "bicarbonato", "polvo hornear",
        "vainilla extracto", "extracto", "colorante",
    ]),
    # PAN (materias de panadería no cubiertas antes)
    ("PAN", [
        "pan ", "brioche", "croissant",
    ]),
    # Cupcakes y empaque de cupcakes (último para no solapar)
    ("EMPAQUES", [
        "cupcake", "1pz", "2pz", "4pz", "6pz", "12pz",
    ]),
]

# Normalización de categorías existentes → clave canónica
CATEGORY_NORMALIZATION: dict[str, str] = {
    "empaque": "EMPAQUES",
    "Empaque": "EMPAQUES",
    "EMPAQUE": "EMPAQUES",
    "desechables": "DESECHABLES",
    "DESECHABLE": "DESECHABLES",
    "ETIQUETA": "ETIQUETAS",
    "etiquetas": "ETIQUETAS",
    "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)": "RELLENOS Y CREMAS",
    "betun, cremas, rellenos (insumo producido)": "RELLENOS Y CREMAS",
    "pan": "PAN",
    "masas": "MASAS",
    "galletas": "GALLETAS",
    "lacteos": "LACTEOS",
    "huevos": "HUEVOS",
    "frutas": "FRUTAS",
    "harinas": "HARINAS",
    "azucares": "AZUCARES",
    "chocolates": "CHOCOLATES",
    "bebidas": "BEBIDAS",
    "complementos": "COMPLEMENTOS",
    "limpieza": "LIMPIEZA",
}


def _key(text: str) -> str:
    return unidecode(text or "").lower().strip()


def _match_category(nombre: str) -> str | None:
    k = _key(nombre)
    for category, keywords in RULES:
        for kw in keywords:
            if kw in k:
                return category
    return None


class Command(BaseCommand):
    help = (
        "Asigna categorías a insumos sin categoría usando reglas de keywords. "
        "Dry-run por defecto. Usa --apply para guardar."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios (guarda categorías asignadas).",
        )
        parser.add_argument(
            "--normalizar",
            action="store_true",
            help="Normaliza categorías existentes a forma canónica (MAYÚSCULAS).",
        )
        parser.add_argument(
            "--tipo",
            default="",
            help="Filtrar por tipo_item: MATERIA_PRIMA, INSUMO_INTERNO, EMPAQUE.",
        )
        parser.add_argument(
            "--solo-vacias",
            action="store_true",
            default=True,
            help="Solo procesar insumos sin categoría (default).",
        )

    def handle(self, *args, **options):
        apply_changes = options["apply"]
        normalizar = options["normalizar"]
        tipo_filter = (options.get("tipo") or "").strip().upper()
        dry = not apply_changes

        if dry:
            self.stdout.write(self.style.WARNING("── DRY-RUN: ningún cambio se guardará. Usa --apply para confirmar.\n"))

        qs = Insumo.objects.filter(activo=True)
        if tipo_filter:
            qs = qs.filter(tipo_item=tipo_filter)

        # ── Fase 1: normalizar categorías existentes ──────────────────────
        if normalizar:
            to_normalize = qs.exclude(categoria__exact="")
            cambios_norm = 0
            for insumo in to_normalize:
                canon = CATEGORY_NORMALIZATION.get(insumo.categoria)
                if canon and canon != insumo.categoria:
                    self.stdout.write(
                        f"  NORMALIZAR  {insumo.nombre!r:50s}  {insumo.categoria!r} → {canon!r}"
                    )
                    if not dry:
                        insumo.categoria = canon
                        insumo.save(update_fields=["categoria"])
                    cambios_norm += 1
            self.stdout.write(
                self.style.SUCCESS(f"\n  Normalizaciones: {cambios_norm}")
                if cambios_norm else self.style.SUCCESS("  Categorías existentes ya son canónicas.\n")
            )

        # ── Fase 2: asignar categorías a los que no tienen ────────────────
        sin_cat = qs.filter(categoria="")
        total = sin_cat.count()
        self.stdout.write(f"\nInsumos activos sin categoría: {total}\n")

        assigned: dict[str, list[str]] = {}
        skipped: list[str] = []

        for insumo in sin_cat.order_by("nombre"):
            cat = _match_category(insumo.nombre)
            if cat:
                assigned.setdefault(cat, []).append(insumo.nombre)
                if not dry:
                    insumo.categoria = cat
                    insumo.save(update_fields=["categoria"])
            else:
                skipped.append(insumo.nombre)

        # ── Reporte ───────────────────────────────────────────────────────
        self.stdout.write("\n── Asignaciones por categoría:\n")
        total_assigned = 0
        for cat, names in sorted(assigned.items()):
            self.stdout.write(self.style.SUCCESS(f"  {cat} ({len(names)}):"))
            for n in names:
                self.stdout.write(f"      · {n}")
            total_assigned += len(names)

        self.stdout.write(f"\n── Sin match ({len(skipped)}):")
        for n in skipped:
            self.stdout.write(self.style.WARNING(f"  ? {n}"))

        self.stdout.write(
            f"\nResumen: {total_assigned}/{total} insumos asignados"
            f" ({100*total_assigned//total if total else 0}% cobertura).\n"
        )
        if skipped:
            self.stdout.write(
                "Agrega keywords a RULES en categorizar_insumos.py para cubrir los restantes.\n"
            )
        if dry and total_assigned:
            self.stdout.write(
                self.style.WARNING("\nEjecuta con --apply para guardar los cambios.\n")
            )
