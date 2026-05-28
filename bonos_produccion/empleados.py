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
