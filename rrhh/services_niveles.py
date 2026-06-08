from __future__ import annotations

from django.db.models import Q

from .models import Empleado

NIVELES_JEFATURA = frozenset({Empleado.NIVEL_JEFATURA, Empleado.NIVEL_DIRECCION})
NIVELES_LIDERAZGO = frozenset(
    {
        Empleado.NIVEL_ENCARGADA,
        Empleado.NIVEL_SUPERVISION,
        Empleado.NIVEL_JEFATURA,
        Empleado.NIVEL_DIRECCION,
    }
)


def jefatura_q() -> Q:
    return Q(nivel_organizacional__in=NIVELES_JEFATURA) | Q(puesto_operativo="JEFATURA") | Q(puesto__icontains="jefe")


def liderazgo_q() -> Q:
    return (
        Q(nivel_organizacional__in=NIVELES_LIDERAZGO)
        | Q(puesto_operativo="JEFATURA")
        | Q(puesto__icontains="jefe")
        | Q(puesto__icontains="encarg")
    )


def empleado_es_liderazgo(empleado: Empleado | None, *, departamento: str = "") -> bool:
    if not empleado:
        return False
    if departamento and (empleado.departamento or "").strip().upper() != departamento:
        return False
    if empleado.nivel_organizacional in NIVELES_LIDERAZGO:
        return True
    puesto = f"{empleado.puesto_operativo or ''} {empleado.puesto or ''}".upper()
    return "JEFATURA" in puesto or "JEFE" in puesto or "ENCARG" in puesto
