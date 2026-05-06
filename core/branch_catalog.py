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


def canonical_point_active_branch_qs():
    return eligible_operational_branch_qs().filter(codigo__in=POINT_MATURE_BRANCH_CODES).order_by("codigo")


def eligible_operational_branch_qs(reference_date=None):
    return (
        Sucursal.objects.filter(sucursales_operativas_q(reference_date))
        .exclude(codigo__in=EXCLUDED_BRANCH_CODES)
        .order_by("codigo")
    )
