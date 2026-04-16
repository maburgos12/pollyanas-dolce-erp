from __future__ import annotations

from dataclasses import dataclass

from core.models import Sucursal, sucursales_operativas

from horarios_especiales.models import SucursalAlias, normalize_text


@dataclass
class BranchResolution:
    input_token: str
    branch: Sucursal
    matched_by: str
    score: int


def split_branch_tokens(raw_segment: str) -> list[str]:
    cleaned = raw_segment.replace(";", ",")
    chunks: list[str] = []
    for piece in cleaned.split(","):
        for nested in piece.split(" y "):
            token = nested.strip()
            if token:
                chunks.append(token)
    return list(dict.fromkeys(chunks))


def resolve_branch_token(token: str, *, reference_date=None) -> tuple[list[BranchResolution], list[str]]:
    token_norm = normalize_text(token)
    if not token_norm:
        return [], ["No se recibió un nombre de sucursal válido."]

    branches = list(sucursales_operativas(reference_date=reference_date))
    alias_rows = list(
        SucursalAlias.objects.filter(is_active=True, sucursal__in=branches).select_related("sucursal")
    )
    candidates: dict[int, BranchResolution] = {}

    for branch in branches:
        code_norm = normalize_text(branch.codigo)
        name_norm = normalize_text(branch.nombre)
        if token_norm == code_norm:
            candidates[branch.id] = BranchResolution(token, branch, "codigo_exacto", 100)
        elif token_norm == name_norm:
            candidates[branch.id] = BranchResolution(token, branch, "nombre_exacto", 95)
        elif token_norm in {code_norm, name_norm}:
            candidates[branch.id] = BranchResolution(token, branch, "directo", 90)
        elif token_norm in code_norm or token_norm in name_norm:
            previous = candidates.get(branch.id)
            if previous is None or previous.score < 60:
                candidates[branch.id] = BranchResolution(token, branch, "contiene", 60)

    for alias in alias_rows:
        alias_norm = normalize_text(alias.alias)
        if token_norm == alias_norm:
            previous = candidates.get(alias.sucursal_id)
            if previous is None or previous.score < 98:
                candidates[alias.sucursal_id] = BranchResolution(token, alias.sucursal, "alias_exacto", 98)
        elif token_norm in alias_norm:
            previous = candidates.get(alias.sucursal_id)
            if previous is None or previous.score < 70:
                candidates[alias.sucursal_id] = BranchResolution(token, alias.sucursal, "alias_parcial", 70)

    ordered = sorted(candidates.values(), key=lambda item: (-item.score, item.branch.codigo, item.branch.nombre))
    if not ordered:
        return [], [f"No se encontró una sucursal operativa para '{token}'."]
    top_score = ordered[0].score
    top_matches = [row for row in ordered if row.score == top_score]
    if len(top_matches) > 1:
        return ordered, [
            f"'{token}' es ambiguo entre: " + ", ".join(match.branch.codigo for match in top_matches)
        ]
    return ordered[:1], []

