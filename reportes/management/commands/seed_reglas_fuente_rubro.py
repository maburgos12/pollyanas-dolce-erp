"""Siembra las reglas rubro→fuente para consolidar el gasto/ingreso real.

Idempotente: solo administra reglas con origen=SEED; nunca toca las de
origen=ADMIN (si un rubro tiene regla ADMIN, el seed lo respeta y lo omite).
El mapeo vive en ``reportes/data/mapeo_rubros_fuentes.csv``.

Los ingresos de Ventas se asignan a nombres POS reales con matching difuso
(rapidfuzz, el mismo enfoque que usa el ERP para insumos): la asignación
queda EXPLÍCITA en filtros (categoria_pos / productos_pos con nombres POS
exactos) y el comando imprime cada asignación con su score para auditoría.
Dos rubros no pueden reclamar el mismo producto POS (se reporta conflicto).
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import CategoriaGasto, ReglaFuenteRubro, RubroPresupuesto
from reportes.services_presupuesto_maestro import normalize_header_text

CSV_DEFAULT = Path(__file__).resolve().parents[2] / "data" / "mapeo_rubros_fuentes.csv"
TIPOS_VALIDOS = {choice[0] for choice in ReglaFuenteRubro.FUENTE_CHOICES}

# Umbrales del matching difuso rubro→POS.
SCORE_PRODUCTO = 90
SCORE_CATEGORIA = 95

# Abreviaturas usadas en el presupuesto/POS ("CHEESECAKE · TORTUGA R").
ABREVIATURAS = {
    "r": "rebanada",
    "gde": "grande",
    "ind": "individual",
    "pz": "piezas",
    "10pz": "10 piezas",
}
STOPWORDS = {"de", "del", "la", "el", "los", "las", "y", "sabor", "pastel"}


def canon_pos(texto: object) -> str:
    """Canónico para comparar nombres de presupuesto vs POS: minúsculas, sin
    acentos ni signos, abreviaturas expandidas, sin palabras vacías, tokens
    ordenados. "SNICKER'S" y "Snickers" quedan iguales."""
    base = normalize_header_text(texto)
    base = re.sub(r"[^a-z0-9 ]+", " ", base)
    tokens: set[str] = set()
    for tok in base.split():
        tok = ABREVIATURAS.get(tok, tok)
        for parte in tok.split():
            if parte not in STOPWORDS:
                tokens.add(parte)
    return " ".join(sorted(tokens))


def _score(a: str, b: str) -> float:
    """Score de producto: token_sort penaliza tokens extra.

    token_set_ratio daba 100 a cualquier superconjunto ("chico crunch" vs
    "TOPPING CRUNCH C") y regalaba productos ajenos al rubro (hallazgo de
    auditoría con doble conteo real). token_sort exige tamaños comparables.
    """
    from rapidfuzz import fuzz

    return fuzz.token_sort_ratio(a, b)


def _score_categoria(a: str, b: str) -> float:
    """Score de categoría completa: ratio simple, sin semántica de subconjunto
    ("fresa rebanada" vs "rebanada" NO debe dar 100)."""
    from rapidfuzz import fuzz

    return fuzz.ratio(a, b)


class Command(BaseCommand):
    help = "Crea/actualiza reglas de fuente por rubro desde el CSV de mapeo."

    def add_arguments(self, parser):
        parser.add_argument("--csv", default=str(CSV_DEFAULT))
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--sin-ventas",
            action="store_true",
            help="No generar reglas VENTA_POS para los rubros de ingresos de Ventas.",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv"])
        if not csv_path.exists():
            raise CommandError(f"No existe el CSV de mapeo: {csv_path}")
        dry_run = options["dry_run"]

        planes: dict[int, list[dict]] = {}  # rubro_id -> [kwargs de regla]
        avisos: list[str] = []

        # --- filas del CSV -------------------------------------------------
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for idx, row in enumerate(csv.DictReader(fh), start=2):
                area = (row.get("area") or "").strip()
                concepto = normalize_header_text(row.get("concepto") or "")
                tipo = (row.get("tipo_fuente") or "").strip().upper()
                if tipo not in TIPOS_VALIDOS:
                    raise CommandError(f"fila {idx}: tipo_fuente inválido '{tipo}'")
                categoria = None
                if (row.get("categoria_gasto") or "").strip():
                    codigo = row["categoria_gasto"].strip()
                    categoria = CategoriaGasto.objects.filter(codigo=codigo).first()
                    if categoria is None:
                        # Abortar ANTES de escribir: si esta fila se omitiera, la
                        # reconciliación borraría la regla SEED previa del rubro
                        # como si se hubiera retirado del mapeo (pérdida de config).
                        raise CommandError(
                            f"fila {idx}: categoria_gasto '{codigo}' no existe en CategoriaGasto. "
                            "Corrige el CSV o crea la categoría; no se escribió nada."
                        )
                try:
                    filtros = json.loads(row["filtros"]) if (row.get("filtros") or "").strip() else {}
                except json.JSONDecodeError as exc:
                    raise CommandError(f"fila {idx}: filtros JSON inválido: {exc}")

                rubros = [
                    r
                    for r in RubroPresupuesto.objects.filter(area__codigo=area, activo=True)
                    if normalize_header_text(r.concepto) == concepto
                ]
                if not rubros:
                    avisos.append(f"fila {idx}: sin rubros para area={area} concepto='{row.get('concepto')}'")
                    continue
                for rubro in rubros:
                    planes.setdefault(rubro.id, []).append(
                        {
                            "tipo_fuente": tipo,
                            "categoria_gasto": categoria,
                            "filtros": filtros,
                            "notas": (row.get("notas") or "").strip()[:200],
                        }
                    )

        # --- ingresos de Ventas: matching difuso contra nombres POS reales ---
        asignaciones_ventas: list[str] = []
        if not options["sin_ventas"]:
            for rubro, filtros, nota in self._asignaciones_ventas(asignaciones_ventas, avisos):
                planes.setdefault(rubro.id, []).append(
                    {
                        "tipo_fuente": ReglaFuenteRubro.FUENTE_VENTA_POS,
                        "categoria_gasto": None,
                        "filtros": filtros,
                        "notas": nota[:200],
                    }
                )

        # --- aplicar --------------------------------------------------------
        creadas = 0
        omitidos_admin = 0
        rubros_con_admin = set(
            ReglaFuenteRubro.objects.filter(
                rubro_id__in=planes.keys(), origen=ReglaFuenteRubro.ORIGEN_ADMIN
            ).values_list("rubro_id", flat=True)
        )
        with transaction.atomic():
            for rubro_id, reglas in planes.items():
                if rubro_id in rubros_con_admin:
                    omitidos_admin += 1
                    continue
                if not dry_run:
                    ReglaFuenteRubro.objects.filter(
                        rubro_id=rubro_id, origen=ReglaFuenteRubro.ORIGEN_SEED
                    ).delete()
                    for kwargs in reglas:
                        ReglaFuenteRubro.objects.create(
                            rubro_id=rubro_id, origen=ReglaFuenteRubro.ORIGEN_SEED, **kwargs
                        )
                creadas += len(reglas)

            # Reconciliación: reglas SEED de rubros que salieron del mapeo se
            # eliminan para que la base converja al estado declarado en el CSV.
            # No toca reglas ADMIN ni rubros con regla ADMIN (ahí manda admin).
            obsoletas_qs = ReglaFuenteRubro.objects.filter(
                origen=ReglaFuenteRubro.ORIGEN_SEED
            ).exclude(rubro_id__in=planes.keys()).exclude(rubro_id__in=rubros_con_admin)
            if options["sin_ventas"]:
                # Corrida parcial: las reglas de Ventas las administra la corrida completa.
                obsoletas_qs = obsoletas_qs.exclude(rubro__area__codigo="ventas")
            obsoletas = obsoletas_qs.count()
            if not dry_run and obsoletas:
                obsoletas_qs.delete()
            if dry_run:
                transaction.set_rollback(True)

        # --- reporte de cobertura -------------------------------------------
        total = RubroPresupuesto.objects.filter(activo=True).count()
        con_regla = len(planes) - omitidos_admin + len(rubros_con_admin)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(f"[{modo}] reglas: {creadas} en {len(planes)} rubros "
                          f"(admin respetados: {omitidos_admin}, seed obsoletas eliminadas: {obsoletas})")
        self.stdout.write(f"Cobertura: {con_regla}/{total} rubros activos con regla")
        for area_codigo in (
            RubroPresupuesto.objects.filter(activo=True)
            .order_by()  # el ordering del modelo rompe DISTINCT sobre values_list
            .values_list("area__codigo", flat=True)
            .distinct()
        ):
            area_ids = set(
                RubroPresupuesto.objects.filter(activo=True, area__codigo=area_codigo).values_list(
                    "id", flat=True
                )
            )
            self.stdout.write(f"  {area_codigo}: {len(area_ids & set(planes))}/{len(area_ids)}")
        if asignaciones_ventas:
            self.stdout.write("Asignaciones ventas POS (auditar):")
            for linea in asignaciones_ventas:
                self.stdout.write(f"  {linea}")
        for aviso in avisos:
            self.stdout.write(self.style.WARNING(f"AVISO {aviso}"))

    # ------------------------------------------------------------------ #
    # Matching difuso rubros de Ventas → nombres POS                      #
    # ------------------------------------------------------------------ #

    def _asignaciones_ventas(self, asignaciones: list[str], avisos: list[str]):
        """Asigna cada rubro de Ventas a nombres POS reales.

        Devuelve tuplas (rubro, filtros, nota). La asignación se decide por
        score difuso; los productos ganados por un rubro no pueden repetirse
        en otro (el de mayor score gana y el conflicto se reporta).
        """
        from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact

        pares_pos = list(
            PointSalesDailyProductFact.objects.order_by()
            .values_list("categoria", "producto_nombre_historico")
            .distinct()
        )
        categorias_pos = sorted({cat for cat, _ in pares_pos})
        canon_pares = [(cat, prod, canon_pos(f"{cat} {prod}")) for cat, prod in pares_pos]

        ventas = RubroPresupuesto.objects.filter(
            area__codigo="ventas", tipo=RubroPresupuesto.TIPO_INGRESO, activo=True
        ).select_related("sucursal")

        producto_a_categorias: dict[str, set[str]] = {}
        for cat, prod, _canon in canon_pares:
            producto_a_categorias.setdefault(prod, set()).add(cat)

        propuestas = []  # [rubro, score, categoria_pos, productos, nota] (mutable)
        for rubro in ventas:
            partes = [p.strip() for p in rubro.concepto.split("·")]
            cat_r = partes[0]
            prod_r = partes[1] if len(partes) > 1 else ""
            objetivo = canon_pos(f"{cat_r} {prod_r}")

            candidatos = [
                (cat, prod, _score(objetivo, canon))
                for cat, prod, canon in canon_pares
            ]
            con_score = [c for c in candidatos if c[2] >= SCORE_PRODUCTO]
            if con_score:
                mejor = max(con_score, key=lambda c: c[2])
                productos = sorted({prod for _, prod, _ in con_score})
                propuestas.append([rubro, mejor[2], "", productos, f"POS: {', '.join(productos)}"])
                continue

            # Sin producto: ¿el "producto" del rubro es una categoría POS
            # completa? (BEBIDAS/OTROS · TE → categoría TE)
            canon_prod = canon_pos(prod_r or cat_r)
            cat_scores = [(cat, _score_categoria(canon_prod, canon_pos(cat))) for cat in categorias_pos]
            mejor_cat = max(cat_scores, key=lambda c: c[1], default=None)
            if mejor_cat and mejor_cat[1] >= SCORE_CATEGORIA:
                propuestas.append(
                    [rubro, mejor_cat[1], mejor_cat[0], [], f"POS categoría completa: {mejor_cat[0]}"]
                )
                continue

            propuestas.append([rubro, 0, "", [], "SIN MATCH POS — asignar en admin"])

        # Un producto POS no puede pertenecer a dos rubros: gana el mayor score.
        dueno_por_producto: dict[str, list] = {}
        for propuesta in sorted(propuestas, key=lambda p: -p[1]):
            rubro, score, _cat, productos, _nota = propuesta
            ganados = []
            for prod in productos:
                previo = dueno_por_producto.get(prod)
                if previo is None:
                    dueno_por_producto[prod] = propuesta
                    ganados.append(prod)
                else:
                    avisos.append(
                        f"conflicto POS: '{prod}' lo reclama '{rubro.concepto}' "
                        f"(score {score:.0f}) pero ya es de '{previo[0].concepto}' (score {previo[1]:.0f})"
                    )
            if productos:
                # Conserva solo los productos realmente ganados por este rubro.
                propuesta[3][:] = ganados

        # Exclusividad categoría/producto (hallazgo de auditoría con doble
        # conteo real): una regla de categoría COMPLETA no puede convivir con
        # productos de esa misma categoría asignados a otros rubros — sumaría
        # dos veces la misma venta. La categoría-completa se anula y se avisa.
        categorias_de_asignados: set[str] = set()
        for prod in dueno_por_producto:
            categorias_de_asignados |= producto_a_categorias.get(prod, set())
        for propuesta in propuestas:
            if propuesta[2] and propuesta[2] in categorias_de_asignados:
                avisos.append(
                    f"conflicto POS: la categoría completa '{propuesta[2]}' de "
                    f"'{propuesta[0].concepto}' se solapa con productos ya asignados "
                    "a otros rubros; se anula para no duplicar dinero — asignar en admin"
                )
                propuesta[2] = ""
                propuesta[4] = "SIN MATCH POS (categoría solapada) — asignar en admin"
                propuesta[1] = 0

        for rubro, score, categoria_pos, productos, nota in propuestas:
            filtros: dict[str, object] = {"campo_monto": "total_venta"}
            if productos:
                filtros["productos_pos"] = productos
            if categoria_pos:
                filtros["categoria_pos"] = categoria_pos
            etiqueta = "SIN MATCH" if not productos and not categoria_pos else f"score {score:.0f}"
            asignaciones.append(f"{rubro.concepto} -> {nota} [{etiqueta}]")
            yield rubro, filtros, nota
