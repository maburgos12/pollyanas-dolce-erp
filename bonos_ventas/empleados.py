from __future__ import annotations

from django.db.models import Q, QuerySet

from rrhh.models import Empleado


# Valores exactos guardados en Empleado.area por AREA_DIVISION_CHOICES de rrhh/views.py.
# "VENTAS" se mantiene como red de seguridad para datos legacy (pre-catálogo).
AREAS_BONOS_VENTAS = ("VENTAS", "REPARTIDORES", "CAJAS", "AUXILIAR CAJAS", "CALL CENTER")


def empleados_elegibles_bonos_ventas() -> QuerySet[Empleado]:
    return Empleado.objects.filter(
        Q(participa_bonos_ventas=True) | Q(area__in=AREAS_BONOS_VENTAS),
        activo=True,
    ).order_by("nombre")
