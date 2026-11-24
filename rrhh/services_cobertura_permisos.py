"""Fuentes de permiso sin efectos secundarios. Nunca convierte permiso en trabajo."""
from datetime import datetime, time
from django.db.models import Q
from django.utils import timezone
from .models import BeneficiarioContingencia, PermisoSalida


def contingencia_en_fecha(empleado, fecha):
    return BeneficiarioContingencia.objects.select_related('permiso').filter(
        empleado_id=empleado.pk, permiso__estado='aprobado',
        fecha_inicio__lte=fecha, fecha_fin__gte=fecha,
    ).order_by('permiso_id').first()


def permiso_personal_dia_completo(empleado, fecha):
    inicio = timezone.make_aware(datetime.combine(fecha, time.min))
    fin = timezone.make_aware(datetime.combine(fecha, time.max))
    return PermisoSalida.objects.filter(
        empleado=empleado, estado=PermisoSalida.ESTADO_APROBADO,
        fechas_laborables=[], fecha_inicio__lte=fin,
    ).filter(
        Q(tipo=PermisoSalida.TIPO_PERMISO_DIA, fecha_inicio__date__lte=fecha, fecha_fin__date__gte=fecha)
        | Q(tipo=PermisoSalida.TIPO_PERMISO_DIA, fecha_inicio__date=fecha, fecha_fin__isnull=True)
        | Q(fecha_inicio__lte=inicio, fecha_fin__gte=fin)
    ).order_by('pk').first()


def cobertura_permiso_dia(empleado, fecha):
    """Fuente de no penalización diaria; permisos parciales quedan en su flujo."""
    return contingencia_en_fecha(empleado, fecha) or permiso_personal_dia_completo(empleado, fecha)


def falta_justificada(incidencia):
    if incidencia.tipo not in {'falta', 'falta_retardos'}:
        return False
    if incidencia.tipo == 'falta' and contingencia_en_fecha(incidencia.empleado, incidencia.fecha) and not actividad_real_en_fecha(incidencia.empleado,incidencia.fecha):
        return True
    # Conserva faltas reales validadas: conciliado por sí solo no es permiso.
    if incidencia.estado == 'conciliado':
        if incidencia.solicitud_vacaciones_id and incidencia.solicitud_vacaciones.estado in {'solicitada','preautorizada','aprobada'}:
            return True
        if incidencia.permiso_id and permiso_personal_dia_completo(incidencia.empleado, incidencia.fecha):
            return True
    return False


def contingencias_en_rango(empleado_ids, inicio, fin):
    """Un query para proyectar snapshots aprobados, sin duplicar identidades."""
    from collections import defaultdict
    from datetime import timedelta
    resultado = defaultdict(list)
    for item in BeneficiarioContingencia.objects.filter(
        empleado_id__in=empleado_ids, permiso__estado='aprobado',
        fecha_inicio__lte=fin, fecha_fin__gte=inicio,
    ).select_related('permiso').order_by('empleado_id', 'permiso_id'):
        dia = max(inicio, item.fecha_inicio)
        while dia <= min(fin, item.fecha_fin):
            resultado[(item.empleado_id, dia)].append(item)
            dia += timedelta(days=1)
    return dict(resultado)


def dias_neutrales_bono(empleado, inicio, fin):
    """Cuenta la unión de días sin trabajo, sin sumar cantidades ganadas."""
    from datetime import timedelta
    from .services_asistencia_contexto import cargar_contexto_asistencia, CODIGO_CONTINGENCIA
    fin = min(fin, timezone.localdate())
    if fin < inicio:
        return 0
    lote = cargar_contexto_asistencia(empleados=[empleado], fecha_inicio=inicio, fecha_fin=fin)
    return sum(lote.clasificar(empleado, inicio+timedelta(days=n)).codigo == CODIGO_CONTINGENCIA for n in range((fin-inicio).days+1))


def tiene_actividad_real(asistencia):
    return bool(asistencia and any(getattr(asistencia,campo,None) for campo in ('entrada','salida_comida','regreso_comida','salida')))


def actividad_real_en_fecha(empleado,fecha):
    from .models import AsistenciaEmpleado
    return tiene_actividad_real(AsistenciaEmpleado.objects.filter(empleado_id=empleado.pk,fecha=fecha).first())


def fecha_en_intervalo_laboral(empleado, fecha):
    """Vigencia actual acreditada, incluso tras una corrección retroactiva."""
    if empleado.fecha_ingreso and fecha < empleado.fecha_ingreso:
        return False
    if empleado.activo:
        return True
    baja = empleado.bajas_rrhh.order_by('-fecha_baja','-pk').first()
    return bool(baja and fecha <= baja.fecha_baja)
