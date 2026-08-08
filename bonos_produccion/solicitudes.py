from django.db.models import Q

from rrhh.models import Empleado


def empleados_operables_solicitudes_produccion():
    """Personal al que una operadora acotada puede capturar tramites."""
    return (
        Empleado.objects.filter(
            Q(departamento=Empleado.DEP_PRODUCCION) | Q(departamento_origen=Empleado.DEP_PRODUCCION),
            activo=True,
            jefe_directo__activo=True,
            jefe_directo__usuario_erp__is_active=True,
        )
        .select_related("jefe_directo__usuario_erp", "sucursal_ref")
        .distinct()
    )
