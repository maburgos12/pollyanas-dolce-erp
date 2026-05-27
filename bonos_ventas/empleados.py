from __future__ import annotations

from django.db.models import Q, QuerySet

from rrhh.models import Empleado


AREAS_BONOS_VENTAS = ("VENTAS", "REPARTIDOR", "CAJAS", "AUXILIAR CAJAS", "CALL CENTER")


def empleados_elegibles_bonos_ventas() -> QuerySet[Empleado]:
    return Empleado.objects.filter(
        Q(participa_bonos_ventas=True) | Q(area__in=AREAS_BONOS_VENTAS),
        activo=True,
    ).order_by("nombre")
