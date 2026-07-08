from __future__ import annotations

from unidecode import unidecode

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

POINT_BRANCH_CANONICAL_NAMES = {
    "COLOSIO": "Sucursal Colosio",
    "CRUCERO": "Sucursal Crucero",
    "EL_TUNEL": "Sucursal El Túnel",
    "GUAMUCHIL": "Sucursal Guamuchil",
    "LAS_GLORIAS": "Sucursal Las Glorias",
    "LEYVA": "Sucursal Leyva",
    "MATRIZ": "Sucursal Matriz",
    "PAYAN": "Sucursal Payan",
    "PLAZA_NIO": "Sucursal Plaza Nío",
}


def display_branch_name(name: str | None) -> str:
    value = (name or "").strip()
    if not value or value.upper() == "CEDIS" or value.lower().startswith("sucursal "):
        return value
    return f"Sucursal {value}"


def display_branch(branch: Sucursal | None) -> str:
    return display_branch_name(branch.nombre if branch else "")


def canonical_branch_catalog_name(code: str | None, name: str | None) -> str:
    normalized_code = canonical_point_branch_code(code)
    explicit_name = POINT_BRANCH_CANONICAL_NAMES.get(normalized_code)
    if explicit_name:
        return explicit_name
    return display_branch_name(name)


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


def _normalizar_texto_sucursal(texto: str | None) -> str:
    if not texto:
        return ""
    return " ".join(unidecode(str(texto)).lower().split())


def indice_sucursales_por_texto(sucursal_qs=None) -> dict[str, "Sucursal"]:
    """Índice de sucursales activas por nombre/código normalizado.

    Fuente única para resolver texto libre (p.ej. `Empleado.sucursal`) a una
    Sucursal, sin depender de igualdad exacta: tolerante al prefijo 'Sucursal ',
    a los acentos y a los separadores del código. `sucursal_qs` permite pasar el
    modelo histórico desde una migración de datos.
    """
    qs = sucursal_qs if sucursal_qs is not None else Sucursal.objects.filter(activa=True)
    indice: dict[str, Sucursal] = {}
    for sucursal in qs:
        nombre_norm = _normalizar_texto_sucursal(sucursal.nombre)
        claves = {
            nombre_norm,
            _normalizar_texto_sucursal(sucursal.codigo),
            _normalizar_texto_sucursal((sucursal.codigo or "").replace("_", " ")),
        }
        if nombre_norm.startswith("sucursal "):
            claves.add(nombre_norm[len("sucursal "):])
        for clave in claves:
            if clave:
                indice.setdefault(clave, sucursal)
    return indice


def resolver_sucursal_por_texto(texto: str | None, indice: dict | None = None, sucursal_qs=None) -> "Sucursal | None":
    """Resuelve texto libre de sucursal a una Sucursal por nombre/código normalizado."""
    objetivo = _normalizar_texto_sucursal(texto)
    if not objetivo:
        return None
    if indice is None:
        indice = indice_sucursales_por_texto(sucursal_qs)
    if objetivo in indice:
        return indice[objetivo]
    if objetivo.startswith("sucursal "):
        return indice.get(objetivo[len("sucursal "):])
    return None
