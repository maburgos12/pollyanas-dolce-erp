from __future__ import annotations

from dataclasses import dataclass

from django.core.management.base import BaseCommand
from django.db.models import Q

from operacion.bitacoras_config import BITACORA_CONFIG, SIZE_LABELS
from operacion.models import BitacoraOperativaLinea
from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
from recetas.utils.normalizacion import normalizar_nombre

STOP_TOKENS = {"grande", "mediano", "chico", "mini", "individual", "pieza", "pastel"}


@dataclass(frozen=True)
class Suggestion:
    alias: str
    receta: Receta | None
    confidence: str
    reason: str
    choices: int = 0


def _alias_codigo(alias: str) -> str:
    return f"BITACORA-{(normalizar_codigo_point(alias)[:60] or 'alias')}"


def _product_aliases():
    aliases = {}
    for config in BITACORA_CONFIG.values():
        if config["familia"] not in {"daily_product", "weekly_matrix"}:
            continue
        for item in config["items"]:
            if isinstance(item, dict):
                alias = f"{item['producto']} {SIZE_LABELS.get(item['tamano'], item['tamano'])}".strip()
                aliases.setdefault(alias, set()).update(item.get("aliases") or [])
                aliases[alias].add(alias)
                aliases[alias].add(item["producto"])
            else:
                alias = str(item)
                aliases.setdefault(alias, set()).add(alias)
    for alias in _pending_capture_aliases():
        aliases.setdefault(alias, set()).add(alias)
    return {alias: sorted(values) for alias, values in sorted(aliases.items())}


def _pending_capture_aliases():
    rows = (
        BitacoraOperativaLinea.objects.filter(receta__isnull=True)
        .exclude(datos__alias_captura="")
        .exclude(datos__alias_captura__isnull=True)
        .values_list("datos__alias_captura", flat=True)
        .distinct()
    )
    return [row for row in rows if row]


def _match(alias: str, candidates: list[str]) -> Suggestion:
    for candidate in candidates:
        point_alias = (
            RecetaCodigoPointAlias.objects.filter(activo=True, nombre_point__iexact=candidate)
            .select_related("receta")
            .first()
        )
        if point_alias:
            return Suggestion(alias, point_alias.receta, "alta", f"alias Point exacto: {candidate}")

    for candidate in candidates:
        receta = Receta.objects.filter(
            nombre_normalizado=normalizar_nombre(candidate),
            pasa_modulo_produccion=True,
        ).first()
        if receta:
            return Suggestion(alias, receta, "alta", f"nombre ERP exacto: {candidate}")

    tokens = [
        token
        for token in normalizar_nombre(alias).split()
        if len(token) > 2 and token not in STOP_TOKENS
    ]
    if not tokens:
        return Suggestion(alias, None, "baja", "sin tokens suficientes")

    qs = Receta.objects.filter(pasa_modulo_produccion=True)
    for token in tokens[:3]:
        qs = qs.filter(nombre_normalizado__contains=token)
    matches = list(qs.order_by("nombre")[:4])
    if len(matches) == 1 and len(tokens) >= 2:
        return Suggestion(alias, matches[0], "alta", "match unico por tokens", 1)
    if matches:
        return Suggestion(alias, matches[0], "media", "requiere revision por tokens", len(matches))

    any_token = Q()
    for token in tokens[:3]:
        any_token |= Q(nombre_normalizado__contains=token)
    matches = list(Receta.objects.filter(any_token, pasa_modulo_produccion=True).order_by("nombre")[:4])
    if len(matches) == 1 and len(tokens) >= 2:
        return Suggestion(alias, matches[0], "media", "match parcial unico", 1)
    return Suggestion(alias, matches[0] if matches else None, "baja", "sin match claro", len(matches))


def _apply(suggestion: Suggestion) -> bool:
    if suggestion.confidence != "alta" or suggestion.receta is None:
        return False
    code = _alias_codigo(suggestion.alias)
    RecetaCodigoPointAlias.objects.update_or_create(
        codigo_point_normalizado=normalizar_codigo_point(code),
        defaults={
            "receta": suggestion.receta,
            "codigo_point": code,
            "nombre_point": suggestion.alias,
            "activo": True,
        },
    )
    BitacoraOperativaLinea.objects.filter(receta__isnull=True, datos__alias_captura=suggestion.alias).update(
        receta=suggestion.receta
    )
    return True


class Command(BaseCommand):
    help = "Sugiere y opcionalmente aplica aliases de productos capturados en bitacoras operativas."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply-high-confidence",
            action="store_true",
            help="Crea aliases y actualiza lineas pendientes solo para matches de confianza alta.",
        )

    def handle(self, *args, **options):
        apply_high = options["apply_high_confidence"]
        suggestions = [_match(alias, candidates) for alias, candidates in _product_aliases().items()]
        applied = 0

        self.stdout.write("alias|confianza|receta|motivo")
        for suggestion in suggestions:
            if apply_high and _apply(suggestion):
                applied += 1
            receta = suggestion.receta.nombre if suggestion.receta else ""
            self.stdout.write(f"{suggestion.alias}|{suggestion.confidence}|{receta}|{suggestion.reason}")

        self.stdout.write(self.style.SUCCESS(f"Total: {len(suggestions)} sugerencias; aplicadas: {applied}"))
