from __future__ import annotations

from typing import Iterable

from recetas.utils.normalizacion import normalizar_nombre


_STAGE_ALIASES = {
    "dream whip": "betun",
    "fresa fresca": "fresa",
    "fresa fresa": "fresa",
    "mermelada de fresa": "mermelada",
}

_EXCLUDED_MAIN_COMPONENTS = {
    "armado",
    "presentacion",
    "presentación",
}


def _token_overlap_score(a: str, b: str) -> float:
    ta = set(a.split())
    tb = set(b.split())
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


def find_parent_cost_for_stage(stage: str, main_costs: Iterable[tuple[str, float]]) -> float | None:
    stage_norm = normalizar_nombre(stage)
    if not stage_norm:
        return None

    alias_target = None
    for alias, target in _STAGE_ALIASES.items():
        if alias in stage_norm:
            alias_target = target
            break

    best_cost: float | None = None
    best_score = -1.0

    for name, cost in main_costs:
        name_norm = normalizar_nombre(name)
        if not name_norm or name_norm in _EXCLUDED_MAIN_COMPONENTS:
            continue

        score = _token_overlap_score(stage_norm, name_norm)
        if name_norm in stage_norm or stage_norm in name_norm:
            score += 1.0
        if alias_target and alias_target in name_norm:
            score += 2.0

        if score > best_score:
            best_score = score
            best_cost = cost

    return best_cost if best_score > 0 else None
