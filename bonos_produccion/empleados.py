from __future__ import annotations

from django.db.models import Q, QuerySet

from rrhh.models import Empleado


def empleados_elegibles_bonos_produccion() -> QuerySet[Empleado]:
    """
    Fuente unica para quien cobra bono de produccion.
    La jerarquia de permisos vive en jefe_directo; no habilita pago de bono.
    """
    return (
        Empleado.objects.filter(activo=True)
        .filter(
            Q(participa_bonos_produccion=True)
            | Q(bonos_esquemas__codigo="PRODUCCION", bonos_esquemas__activo=True)
        )
        .distinct()
        .order_by("nombre")
    )


def bonos_produccion_elegibles_queryset(qs):
    """
    Filtra filas abiertas de bono usando RRHH como fuente de elegibilidad.
    Los bonos cerrados/pagados se conservan como historial.
    """
    empleados_ids = empleados_elegibles_bonos_produccion().values_list("id", flat=True)
    return qs.filter(
        Q(estatus__in=["CERRADO", "PAGADO"])
        | Q(empleado_id__in=empleados_ids)
    )


def inicializar_bonos_desde_rrhh(periodo):
    """Materializa el personal elegible del expediente RRHH sin alterar bonos existentes."""
    from .models import (
        AREA_PRODUCCION, AREAS_PRODUCCION, BonoProduccionEmpleado,
        area_bono_produccion_empleado,
    )

    areas_validas = {codigo for codigo, _ in AREAS_PRODUCCION}
    creados = 0
    considerados = 0
    for empleado in empleados_elegibles_bonos_produccion():
        area = area_bono_produccion_empleado(empleado)
        if area not in areas_validas:
            area = AREA_PRODUCCION
        considerados += 1
        _, created = BonoProduccionEmpleado.objects.get_or_create(
            periodo=periodo, empleado=empleado, defaults={"area": area},
        )
        creados += int(created)
    return {"creados": creados, "total": considerados}
