from __future__ import annotations

from .models import BonoProduccionEmpleado


def recalcular_desde_registros(bono: BonoProduccionEmpleado) -> None:
    registros = bono.registros.all()
    asistencias = registros.filter(tiene_asistencia=True)
    bono.dias_trabajados = asistencias.count()
    bono.dias_uniforme = asistencias.filter(tiene_uniforme=True).count()
    bono.dias_puntualidad = asistencias.filter(tiene_puntualidad=True).count()
    bono.dias_asistencia = bono.dias_trabajados
    bono.dias_produccion = asistencias.filter(tiene_produccion=True).count()
    bono.total_embetunados = sum(r.cantidad_embetunados for r in asistencias)
    bono.recalcular()
    bono.save()
