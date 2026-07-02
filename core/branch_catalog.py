from __future__ import annotations

from core.models import Sucursal, sucursales_operativas_q

EXCLUDED_BRANCH_CODES: tuple[str, ...] = ("TMP1", "MATRIZDBG")

POINT_NETWORK_BRANCH_CODES = (
    "COLOSIO",
    "CRUCERO",
    "EL_TUNEL",
    "GUAMUCHIL",
    "LAS_GLORIAS",
    "LEYVA",
    "MATRIZ",
    "PAYAN",
    "PLAZA_NIO",
)

POINT_MATURE_BRANCH_CODES = (
    "COLOSIO",
    "CRUCERO",
    "EL_TUNEL",
    "LAS_GLORIAS",
    "LEYVA",
    "MATRIZ",
    "PAYAN",
    "PLAZA_NIO",
)

POINT_BRANCH_CODE_ALIASES = {
    "GLORIAS": "LAS_GLORIAS",
    "NIO": "PLAZA_NIO",
    "TUNEL": "EL_TUNEL",
}


def display_branch_name(name: str | None) -> str:
    value = (name or "").strip()
    if not value or value.upper() == "CEDIS" or value.lower().startswith("sucursal "):
        return value
    return f"Sucursal {value}"


def display_branch(branch: Sucursal | None) -> str:
    return display_branch_name(branch.nombre if branch else "")


def canonical_point_active_branch_qs():
    return eligible_operational_branch_qs().filter(codigo__in=POINT_MATURE_BRANCH_CODES).order_by("codigo")


def canonical_point_network_branch_qs(reference_date=None):
    return eligible_operational_branch_qs(reference_date).filter(codigo__in=POINT_NETWORK_BRANCH_CODES).order_by("codigo")


def canonical_point_branch_code(code: str | None) -> str:
    normalized = (code or "").strip().upper()
    return POINT_BRANCH_CODE_ALIASES.get(normalized, normalized)


def eligible_operational_branch_qs(reference_date=None):
    return (
        Sucursal.objects.filter(sucursales_operativas_q(reference_date))
        .exclude(codigo__in=EXCLUDED_BRANCH_CODES)
        .order_by("codigo")
    )
