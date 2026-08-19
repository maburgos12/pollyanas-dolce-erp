from dataclasses import dataclass
from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db.models import Count, Sum
from django.utils import timezone

from .models import ReparacionUnidad, ServicioRealizadoUnidad


@dataclass(frozen=True)
class ResumenAnualUnidad:
    servicios_cantidad: int
    servicios_total: Decimal
    reparaciones_cantidad: int
    reparaciones_total: Decimal

    @property
    def gasto_total(self) -> Decimal:
        return self.servicios_total + self.reparaciones_total


def servicios_realizados_validos(unidad, *, today):
    return (
        ServicioRealizadoUnidad.objects.vigentes()
        .filter(unidad=unidad, fecha_servicio__lte=today)
        .exclude(tipo_servicio__nombre__iexact="Registro inicial de kilometraje")
    )


def validar_fecha_servicio_realizado(fecha, *, today=None):
    limite = today or timezone.localdate()
    if fecha > limite:
        raise ValidationError("La fecha de un servicio realizado no puede estar en el futuro.")


def resumen_anual_unidad(unidad, *, year, today):
    servicios = servicios_realizados_validos(unidad, today=today).filter(fecha_servicio__year=year)
    reparaciones = ReparacionUnidad.objects.filter(unidad=unidad, fecha_ingreso__year=year)
    resumen_servicios = servicios.aggregate(cantidad=Count("id"), total=Sum("costo"))
    resumen_reparaciones = reparaciones.aggregate(cantidad=Count("id"), total=Sum("costo_total"))
    return ResumenAnualUnidad(
        servicios_cantidad=resumen_servicios["cantidad"],
        servicios_total=resumen_servicios["total"] or Decimal("0"),
        reparaciones_cantidad=resumen_reparaciones["cantidad"],
        reparaciones_total=resumen_reparaciones["total"] or Decimal("0"),
    )
