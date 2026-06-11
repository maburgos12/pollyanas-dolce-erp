from __future__ import annotations

from .models import BonoVentasEmpleado


def recalcular_desde_registros(bono: BonoVentasEmpleado) -> None:
    registros = bono.registros.all()
    asistencias = registros.filter(tiene_asistencia=True)
    bono.dias_trabajados = asistencias.count()
    bono.dias_asistencia = bono.dias_trabajados
    bono.dias_uniforme = asistencias.filter(tiene_uniforme=True).count()
    bono.dias_puntualidad = asistencias.filter(tiene_puntualidad=True).count()
    bono.recalcular()
    bono.save()
