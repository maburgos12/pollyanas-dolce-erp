from typing import Optional, Tuple
from rapidfuzz import fuzz, process
from maestros.models import Insumo
from .normalizacion import normalizar_nombre

def match_insumo(nombre_origen: str, score_threshold: float = 75.0) -> Tuple[Optional[Insumo], float, str]:
    """Matching en 3 pasos: EXACT, CONTAINS, FUZZY."""
    nombre_norm = normalizar_nombre(nombre_origen)

    if not nombre_norm:
        return (None, 0.0, "NO_MATCH")

    # 1) Exact
    insumo = Insumo.objects.filter(nombre_normalizado=nombre_norm).first()
    if insumo:
        return (insumo, 100.0, "EXACT")

    # 2) Contains (cuando el origen es más corto)
    insumo = Insumo.objects.filter(nombre_normalizado__icontains=nombre_norm).first()
    if insumo:
        return (insumo, 95.0, "CONTAINS")

    # 3) Fuzzy
    nombres = list(Insumo.objects.values_list("nombre_normalizado", flat=True))
    if not nombres:
        return (None, 0.0, "NO_MATCH")

    best = process.extractOne(nombre_norm, nombres, scorer=fuzz.ratio)
    if not best:
        return (None, 0.0, "NO_MATCH")

    best_name, score, _ = best
    if score >= score_threshold:
        insumo = Insumo.objects.filter(nombre_normalizado=best_name).first()
        return (insumo, float(score), "FUZZY")

    return (None, float(score), "NO_MATCH")

def clasificar_match(score: float) -> str:
    if score >= 90:
        return "AUTO_APPROVED"
    if score >= 75:
        return "NEEDS_REVIEW"
    return "REJECTED"
