"""Reparación acotada de alertas; no evalúa checadas, horas ni otros conceptos."""
from datetime import timedelta
from django.db import transaction
from django.db.models import Max
from .models import IncidenciaAsistencia, IncidenciaAsistenciaBitacora
from .services_extra_bloqueos import bloquear_jornadas_extra


@transaction.atomic
def reparar_alertas_faltas(empleado, inicio, fin, *, actor=None, comentario='Reevaluación de justificación de ausencia.'):
    from .services_asistencia_reglas import _faltas_vigentes, _emitidas_en_ventana, _upsert_incidencia
    ultima = IncidenciaAsistencia.objects.filter(empleado=empleado,fecha__lte=fin).aggregate(fecha=Max('fecha'))['fecha']
    if ultima is None:
        return
    fin = min(fin,ultima)
    if fin < inicio:
        return
    bloquear_jornadas_extra([(empleado.pk,inicio+timedelta(days=n)) for n in range((fin-inicio).days+1)])
    fecha = inicio
    while fecha <= fin:
        conteo = _faltas_vigentes(empleado,fecha)
        for tipo,umbral,severidad in (('aviso_baja_faltas',3,'alta'),('baja_faltas',4,'critica')):
            anterior = IncidenciaAsistencia.objects.select_for_update().filter(empleado=empleado,fecha=fecha,tipo=tipo).first()
            if anterior and anterior.editado_manual:
                continue
            merece = conteo>=umbral and not _emitidas_en_ventana(empleado,tipo,fecha-timedelta(days=29),fecha)
            estado_previo = anterior.estado if anterior else ''
            if merece:
                actual,creada,_ = _upsert_incidencia(empleado=empleado,fecha=fecha,tipo=tipo,estado='pendiente',severidad=severidad,ventana_inicio=fecha-timedelta(days=29),ventana_fin=fecha,conteo_faltas_30d=conteo,detalle='Escalamiento de faltas tras revisión de justificación.',metadata={'faltas_30d':conteo})
            elif anterior and anterior.estado!='resuelto':
                actual = anterior
                actual.estado, actual.severidad = 'resuelto','info'
                actual.save(update_fields=['estado','severidad','actualizado_en'])
            else:
                continue
            if actual.estado != estado_previo:
                IncidenciaAsistenciaBitacora.objects.create(incidencia=actual,usuario=actor,campo='estado',valor_anterior=estado_previo,valor_nuevo=actual.estado,comentario=comentario)
        fecha += timedelta(days=1)
