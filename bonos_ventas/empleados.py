from __future__ import annotations

from django.db.models import QuerySet

from rrhh.models import Empleado


AREAS_BONOS_VENTAS = ("VENTAS", "REPARTIDOR")


def empleados_elegibles_bonos_ventas() -> QuerySet[Empleado]:
    return Empleado.objects.filter(activo=True, area__in=AREAS_BONOS_VENTAS).order_by("nombre")
