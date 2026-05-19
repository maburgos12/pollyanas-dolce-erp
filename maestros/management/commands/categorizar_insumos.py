"""
Asigna categorías a insumos sin categoría usando reglas de keywords sobre nombre normalizado.
Por defecto es dry-run; usa --apply para guardar cambios.
Usa --normalizar para estandarizar a MAYÚSCULAS las categorías existentes.
"""
from __future__ import annotations

from unidecode import unidecode

from django.core.management.base import BaseCommand

from maestros.models import Insumo


def _key(text: str) -> str:
    """Normaliza a minúsculas sin acentos con espacios delimitadores para matching seguro."""
    return " " + unidecode(text or "").lower().strip() + " "


def _match(k: str, keywords: list[str]) -> bool:
    return any(kw in k for kw in keywords)


# Reglas en orden estricto de prioridad (la primera que haga match gana).
# Keywords con espacios evitan falsos positivos por substring parcial.
RULES: list[tuple[str, list[str]]] = [
    # 1. Limpieza — debe ir primero para no confundirse con empaques o alimentos
    ("LIMPIEZA", [
        "bactericida", "pinol", "power clean", "suavel",
        "papel de bano", "papel higienico", "papel bano",
        " jabon ", "jabon liquido", "jabón", "zote", "detergente",
        " cloro ", "desinfectante", "ariel", "fabuloso",
        " escoba", "trapeador", "trapero",
        "bolsa de basura", "bolsas de basura",
        "rollo toalla", "rollos toalla", "toalla interdoblada",
        "toalla rollo", "toallas desinfectantes",
        "limpiador", "sanitizante", "desengrasante", "laysol",
        "fibra metalica", "fibra verde", "fibra esponja",
        " cepillo ", "cepillo para wc", "destapa cano",
        " guante", "guantes alimento", "guantes para alimento",
        "cubrebocas", " cofia ", "gorra basic", " mandil",
        "rollo anti-insecto", "rollo antiinsecto",
        "liquido limpiacristales", "antiseptico",
        "bactericida", "sorbato", "garrafon 19",
        "toalla rollo fapsa",
    ]),

    # 2. Colorantes y decoración — antes de cualquier alimento
    ("DECORACION", [
        " colorante", "liqua-gel", "liquagel", "liqua gel",
        " gragea", "perla ", "corazon perla", "mini corazon",
        " granillo", "confeti", "sugar art",
        "hoja de oro", " palillo", "letrero mom",
        "grajea tornasol", "grajea ",
        "liston ", "estrella deco", "deco azucar",
    ]),

    # 3. Bebidas alcohólicas y no alcohólicas (keywords específicos, sin "te" genérico)
    ("BEBIDAS", [
        "nescafe", " cafe ", "agua purificada", "agua purif",
        " jugo ", "refresco", "licor43", " licor ",
        " ron ", "kirsch", "brandy", "presidente tequila",
        "calahua", "kahlua", "baileys", "vodka",
        " tequila", "whisky", "presidente ",
    ]),

    # 4. Huevos
    ("HUEVOS", [" huevo", "clara de huevo", "yema de huevo"]),

    # 5. Frutas — keywords específicos
    ("FRUTAS", [
        " fresa", " piña", " pina ", "jugo de pina", "jugo de fresa",
        " mango ", " manzana", " pera ", " durazno", " cereza",
        " kiwi ", " uva ", " arandano", " guayaba", " mora ",
        " coco ", "coco rallado", " chabacano", " datil", " ciruela",
        " blueberry", "zarzamora", "cocktail de fruta",
        "mermelada de", "mermelada fresa", "mermelada zarzamora",
        " frambuesa",
    ]),

    # 6. Chocolates y derivados — antes de MASAS/PAN para capturar "Pan Chocolate"
    ("CHOCOLATES", [
        " chocolate ", "chocola", " cocoa", " cacao",
        " nutella", " crunch ", "snicker", "milkyway", "milky way",
        "kit kat", "ferrero", "bubulubu", " muibon",
        "kinder bueno", "kisses leche",
    ]),

    # 7. Lácteos
    ("LACTEOS", [
        " leche", " crema ", "mantequilla", "margarina", " manteca",
        " queso", " yogurt", "yoghurt", "la lechera", "chantilly",
        "base chantilly", "dream whip", " lotus",
        "3 leches",
    ]),

    # 8. Harinas y almidones
    ("HARINAS", [" harina", " almidon", " fecula", " maizena"]),

    # 9. Azúcares
    ("AZUCARES", [
        " azucar", "piloncillo", "miel karo", "miel de",
        " jarabe", "splenda", "stevia", " glucosa", " cajeta",
        " caramelo",
    ]),

    # 10. Galletas (base para pays y postres)
    ("GALLETAS", [" galleta", " oreo", " ritz"]),

    # 11. Rellenos, betunes, mermeladas de producción
    ("RELLENOS Y CREMAS", [
        " relleno", " betu", "polvo de merengue", " glaseado",
        " crema pastelera", " mezcla 3 leches", "cobertura 3",
        " flan ", "batida queso", "batida crema", "batida pay",
        " dream whip", "dona dawn", "vainilla dawn",
        "cobertura dona",
    ]),

    # 12. Complementos (sal, especias, aditivos, nueces, levadura)
    ("COMPLEMENTOS", [
        " sal ", "vinagre", " levadura", " nuez", " almendra",
        "ajonjoli", " canela", "bicarbonato", "polvo hornear",
        "extracto de vainilla", "esencia de", " avena",
        "grenetina", "goma xantana", "sorbato de",
        "desmoldante", " crumble",
    ]),

    # 13. Masas e insumos producidos (batidos, bases de producción)
    ("MASAS", [
        " masa ", "base cheesecake", "base 3 leches",
        " bizcocho", " brownie",
        " batidor ",
    ]),

    # 14. Pan (productos de panadería)
    ("PAN", [
        " pan ", " brioche", " croissant", "rol de", " rosca",
        "dona lev", " dona ",
    ]),

    # 15. Empaques (cajas, domos, charolas, moldes, bolsas de producto)
    ("EMPAQUES", [
        " domo ", "caja carton", "caja pastel", "caja cheesecake",
        "caja 12 bollos", " aluminio pay", "alumino pay",
        "rebanada pay", "bisagra bollo", "bisagra para bollo",
        " molde ", " charola", " porta vasos",
        "tapa vasos", "1pzcupcake", "1pz cupcake", "2pz cupcake",
        "4pzcupcake", "6pz cupcake", "12pz cupcake",
        "bolsa camiseta", "base aluminio", "base de carton",
        "base giratoria", " fajita ", "faja san valentin",
        "papel encerado", "rollo plastico", "pelicula pvc",
        " fajita", "bolsa celofan", "bolsa de papel kraft",
        "bolsa de brillo", "bolsa jumbo", "bolsa papel kraft",
        "caja rosa", "impresion caja", "impresion m20",
        "impresion 2 bollos", " bisagra", " manga ",
        "domo 3 leches", "charola 3 leches", "molde 3 leches",
        " cupcake",
    ]),

    # 16. Desechables (vasos, cucharas, platos)
    ("DESACHABLES", []),  # placeholder — cubierto por subcaso específico

    # 17. Desechables reales
    ("DESECHABLES", [
        " vaso", " cuchara", " plato ", "desechable", " cubierto",
        "cinta adhesiva", "liga no", "liga payaso",
        "8oz", "9oz", "12oz", "16oz", "20oz",
        "bolsa camiseta reb", "portavasos", "porta vasos",
        "vaso litro", " servilleta",
    ]),

    # 18. Etiquetas y papelería
    ("ETIQUETAS", [
        " etiqueta", "papelcliche", "papel cliche", " sticker",
    ]),

    # 19. Útiles de cocina y equipamiento (no comestibles)
    ("UTILES", [
        "bascula", "espatula", " estan ", "mesa de trabajo",
        "set de cuchillos", "cartera acero", " red negra",
        "garrafon 19", "rollo anti",
    ]),
]

# Normalización de categorías existentes inconsistentes → clave canónica
CATEGORY_NORMALIZATION: dict[str, str] = {
    "Empaque": "EMPAQUES",
    "EMPAQUE": "EMPAQUES",
    "empaque": "EMPAQUES",
    "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)": "RELLENOS Y CREMAS",
    "Betun, Cremas, Rellenos (INSUMO PRODUCIDO)": "RELLENOS Y CREMAS",
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
    "desechables": "DESECHABLES",
    "DESECHABLE": "DESECHABLES",
    "ETIQUETA": "ETIQUETAS",
    "etiquetas": "ETIQUETAS",
}


def _match_category(nombre: str) -> str | None:
    k = _key(nombre)
    for category, keywords in RULES:
        if keywords and _match(k, keywords):
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
            help="Normaliza categorías existentes a forma canónica.",
        )
        parser.add_argument(
            "--tipo",
            default="",
            help="Filtrar por tipo_item: MATERIA_PRIMA, INSUMO_INTERNO, EMPAQUE.",
        )

    def handle(self, *args, **options):
        apply_changes = options["apply"]
        normalizar = options["normalizar"]
        tipo_filter = (options.get("tipo") or "").strip().upper()
        dry = not apply_changes

        if dry:
            self.stdout.write(self.style.WARNING(
                "── DRY-RUN: ningún cambio se guardará. Usa --apply para confirmar.\n"
            ))

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
                        f"  NORMALIZAR  {insumo.nombre!r:50s}  "
                        f"{insumo.categoria!r} → {canon!r}"
                    )
                    if not dry:
                        insumo.categoria = canon
                        insumo.save(update_fields=["categoria"])
                    cambios_norm += 1
            self.stdout.write(
                self.style.SUCCESS(f"\n  Normalizaciones: {cambios_norm}\n")
                if cambios_norm else self.style.SUCCESS("  Categorías existentes ya son canónicas.\n")
            )

        # ── Fase 2: asignar categorías a los que no tienen ────────────────
        sin_cat = qs.filter(categoria="")
        total = sin_cat.count()
        self.stdout.write(f"Insumos activos sin categoría: {total}\n")

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
        total_assigned = sum(len(v) for v in assigned.values())

        self.stdout.write("\n── Asignaciones por categoría:\n")
        for cat, names in sorted(assigned.items()):
            self.stdout.write(self.style.SUCCESS(f"  {cat} ({len(names)}):"))
            for n in names:
                self.stdout.write(f"      · {n}")

        self.stdout.write(f"\n── Sin match ({len(skipped)}):")
        for n in skipped:
            self.stdout.write(self.style.WARNING(f"  ? {n}"))

        pct = (100 * total_assigned // total) if total else 0
        self.stdout.write(
            f"\nResumen: {total_assigned}/{total} insumos asignados ({pct}% cobertura).\n"
        )
        if skipped:
            self.stdout.write(
                "Agrega keywords a RULES en categorizar_insumos.py para cubrir los restantes.\n"
            )
        if dry and total_assigned:
            self.stdout.write(
                self.style.WARNING("\nEjecuta con --apply --normalizar para guardar los cambios.\n")
            )
