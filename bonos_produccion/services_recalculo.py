from __future__ import annotations

from datetime import timedelta

from django.utils import timezone

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
    inicio, fin = bono.periodo.rango_fechas()
    if inicio <= timezone.localdate() <= fin:
        fin = timezone.localdate()
    fechas_esperadas = set()
    fecha = inicio
    while fecha <= fin:
        if not bono.empleado.fecha_ingreso or fecha >= bono.empleado.fecha_ingreso:
            fechas_esperadas.add(fecha)
        fecha += timedelta(days=1)
    registros_contexto = list(registros.exclude(fecha__isnull=True))
    fechas_contexto = {registro.fecha for registro in registros_contexto}
    if fechas_esperadas.issubset(fechas_contexto) and all(
        registro.falta_penalizable is not None for registro in registros_contexto
    ):
        bono.faltas_rrhh = sum(bool(registro.falta_penalizable) for registro in registros_contexto)
    else:
        bono.faltas_rrhh = None
    bono.recalcular()
    bono.save()
