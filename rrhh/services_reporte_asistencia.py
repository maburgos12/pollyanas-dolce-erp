"""Reportes completos de asistencia. Solo lecturas; reglas de extra compartidas."""
from collections import defaultdict
from datetime import date, datetime, time, timedelta

from django.core.exceptions import PermissionDenied, ValidationError
from django.db.models import Q
from django.utils import timezone

from core.access import can_view_rrhh
from .models import AsistenciaEmpleado, Empleado, EmpleadoBaja, HoraExtra, IncidenciaAsistencia, PermisoSalida, SolicitudVacaciones
from .services_extra_conciliacion import conciliar_extra_diario, formato_minutos

EXTRA_KEYS = ('detectado', 'autorizado', 'pendiente', 'rechazado', 'solicitado')
PERMISSION_KEYS = ('minutos_cg', 'minutos_sg', 'dias_cg', 'dias_sg', 'conflictos', 'duracion_desconocida')
SUMMARY_KEYS = ('faltas', 'faltas_conciliadas', 'falta_retardos', 'retardos', 'comida_excedida', 'jornada_incompleta', 'hora_extra', 'avisos_baja', 'avisos_baja_30d', 'permisos', 'permisos_aplicables', 'vacaciones', 'suspensiones', 'faltas_30d') + PERMISSION_KEYS


def _midnight(day):
    return timezone.make_aware(datetime.combine(day, time.min))


def _union(intervals):
    merged = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def _extra_summary(rows):
    applicable = [row for row in rows if row.get('aplicable_laboral', row['estado_laboral_label'] != 'No aplica')]
    calculable = [row for row in applicable if row['extra']['detectado_minutos'] is not None]
    result = {f'{key}_minutos': sum(row['extra'][f'{key}_minutos'] or 0 for row in applicable) for key in EXTRA_KEYS}
    if not calculable:
        result['detectado_minutos'] = result['pendiente_minutos'] = None
    result.update({key: formato_minutos(result[f'{key}_minutos']) for key in EXTRA_KEYS})
    result.update(dias_calculables=len(calculable), dias_no_calculables=len(applicable)-len(calculable),
                  dias_sin_registro=sum(row['asistencia'] is None for row in applicable),
                  dias_no_aplica=len(rows)-len(applicable), parcial=bool(calculable) and len(calculable)<len(applicable))
    return result


def _incidence(item):
    return {'id': item.id, 'fecha': item.fecha, 'tipo': item.get_tipo_display(), 'tipo_codigo': item.tipo,
            'estado': item.get_estado_display(), 'estado_codigo': item.estado, 'minutos': item.minutos, 'detalle': item.detalle}


def build_reporte_departamento(fecha_inicio, fecha_fin, *, departamento='', area='', empleado_id='', sucursal='', user=None):
    """Materializa un contrato compartido. El consumidor controla la instantánea SQL."""
    if user is not None and not can_view_rrhh(user):
        raise PermissionDenied
    if not isinstance(fecha_inicio, date) or not isinstance(fecha_fin, date) or isinstance(fecha_inicio, datetime) or isinstance(fecha_fin, datetime):
        raise ValidationError('Fechas inválidas.')
    if not 0 <= (fecha_fin-fecha_inicio).days <= 30:
        raise ValidationError('El periodo debe contener entre 1 y 31 días.')
    if departamento and departamento not in dict(Empleado.DEP_CHOICES):
        raise ValidationError('Departamento inválido.')
    if empleado_id != '' and empleado_id is not None:
        raw_id = str(empleado_id)
        if not raw_id.isascii() or not raw_id.isdigit() or len(raw_id) > 19:
            raise ValidationError('Empleado inválido.')
        empleado_id = int(raw_id)
        if not 1 <= empleado_id <= 2**63-1 or not Empleado.objects.filter(pk=empleado_id).exists():
            raise ValidationError('Empleado inválido.')
    for key, value in (('area', area), ('sucursal', sucursal)):
        if value and not Empleado.objects.filter(**{key: value}).exists():
            raise ValidationError(f'{key.capitalize()} inválida.')
    empleados_qs = Empleado.objects.select_related('sucursal_ref').order_by('departamento', 'nombre', 'id')
    for key, value in (('departamento', departamento), ('area', area), ('pk', empleado_id), ('sucursal', sucursal)):
        if value:
            empleados_qs = empleados_qs.filter(**{key: value})
    candidates = list(empleados_qs)
    ids = [employee.id for employee in candidates]
    window = fecha_fin-timedelta(days=29)
    start_dt, end_dt = _midnight(fecha_inicio), _midnight(fecha_fin+timedelta(days=1))
    attendance = {(a.empleado_id,a.fecha): a for a in AsistenciaEmpleado.objects.filter(empleado_id__in=ids,fecha__range=(fecha_inicio,fecha_fin)).select_related('turno','sucursal')}
    extras = defaultdict(list)
    for item in HoraExtra.objects.filter(empleado_id__in=ids,fecha__range=(fecha_inicio,fecha_fin)):
        extras[(item.empleado_id,item.fecha)].append(item)
    incidences = defaultdict(list)
    for item in IncidenciaAsistencia.objects.filter(empleado_id__in=ids,fecha__range=(min(window,fecha_inicio),fecha_fin)).order_by('fecha','tipo'):
        incidences[item.empleado_id].append(item)
    permissions = defaultdict(list)
    permission_qs = PermisoSalida.objects.filter(empleado_id__in=ids,estado='aprobado',fecha_inicio__lt=end_dt).filter(Q(fecha_fin__gte=start_dt)|Q(fecha_fin__isnull=True,fecha_inicio__gte=start_dt))
    for item in permission_qs.order_by('fecha_inicio','id'):
        permissions[item.empleado_id].append(item)
    vacations = defaultdict(list)
    for item in SolicitudVacaciones.objects.filter(empleado_id__in=ids, estado=SolicitudVacaciones.ESTADO_APROBADA, fecha_inicio__lte=fecha_fin, fecha_fin__gte=fecha_inicio):
        vacations[item.empleado_id].append(item)
    bajas = {}
    for item in EmpleadoBaja.objects.filter(empleado_id__in=ids).order_by('fecha_baja','id'):
        bajas[item.empleado_id] = item.fecha_baja
    event_employee_ids = {key[0] for key in attendance} | {key[0] for key in extras}
    event_employee_ids.update(eid for eid, items in incidences.items() if any(fecha_inicio <= i.fecha <= fecha_fin for i in items))
    event_employee_ids.update(eid for eid, items in permissions.items() if items)
    event_employee_ids.update(eid for eid, items in vacations.items() if items)
    reports = []
    dates = [fecha_inicio+timedelta(days=n) for n in range((fecha_fin-fecha_inicio).days+1)]
    for employee in candidates:
        eid = employee.id
        baja = bajas.get(eid) if not employee.activo else None
        employee.fecha_baja_reporte = baja
        active_events = eid in event_employee_ids
        if not employee.activo and not active_events:
            continue
        summary = dict.fromkeys(SUMMARY_KEYS,0)
        valid_inc = [i for i in incidences[eid] if i.fecha>=employee.fecha_ingreso and (baja is None or i.fecha<=baja)]
        summary['faltas_30d'] = sum(i.tipo=='falta' and i.estado=='pendiente' and window<=i.fecha<=fecha_fin for i in valid_inc)
        avisos = [_incidence(i) for i in valid_inc if i.tipo in ('aviso_baja_faltas','baja_faltas') and window<=i.fecha<=fecha_fin and i.estado!='resuelto']
        summary['avisos_baja_30d'] = len(avisos)
        by_day = defaultdict(list)
        for item in incidences[eid]:
            if fecha_inicio<=item.fecha<=fecha_fin:
                if item.fecha < employee.fecha_ingreso and item.estado == 'resuelto':
                    continue
                by_day[item.fecha].append(_incidence(item))
                if item.fecha < employee.fecha_ingreso or (baja and item.fecha > baja) or item.estado=='resuelto':
                    continue
                pending = item.estado=='pendiente'
                key = {'falta': 'faltas' if pending else 'faltas_conciliadas','falta_retardos':'falta_retardos','retardo':'retardos','retardo_tolerancia':'retardos','comida_excedida':'comida_excedida','jornada_incompleta':'jornada_incompleta','hora_extra_pendiente':'hora_extra','aviso_baja_faltas':'avisos_baja','baja_faltas':'avisos_baja','suspension':'suspensiones'}.get(item.tipo)
                if key and (pending or key in ('faltas_conciliadas','hora_extra','suspensiones')):
                    summary[key]+=1
        vacation_days = defaultdict(list)
        for vacation in vacations[eid]:
            for day in dates:
                if vacation.fecha_inicio <= day <= vacation.fecha_fin:
                    vacation_days[day].append({'id':vacation.id, 'fecha_inicio':vacation.fecha_inicio, 'fecha_fin':vacation.fecha_fin})
            lo = max(vacation.fecha_inicio, fecha_inicio, employee.fecha_ingreso)
            hi = min(vacation.fecha_fin, fecha_fin, baja or fecha_fin)
            if hi >= lo:
                summary['vacaciones'] += (hi-lo).days+1
        permiso_days = defaultdict(list)
        intervals = defaultdict(lambda: {True: [], False: []})
        full_days = defaultdict(set)
        permiso_report = []
        for permiso in permissions[eid]:
            first = timezone.localtime(permiso.fecha_inicio).date()
            final = timezone.localtime(permiso.fecha_fin).date() if permiso.fecha_fin else first
            if permiso.fecha_fin and final > first and timezone.localtime(permiso.fecha_fin).time() == time.min:
                final -= timedelta(days=1)
            observations = []
            invalid = permiso.fecha_fin is not None and permiso.fecha_fin < permiso.fecha_inicio
            if invalid:
                observations.append('Intervalo de permiso inválido; duración N/D')
            minutes, days_count = (None if invalid or permiso.fecha_fin is None else 0), 0
            matched_days = []
            for day in dates:
                if permiso.tipo=='permiso_dia':
                    matches = first<=day<=final if not invalid else day==first
                    if matches and not invalid:
                        full_days[day].add(permiso.goce_sueldo)
                        days_count+=1
                elif permiso.fecha_fin and not invalid:
                    lo, hi = max(permiso.fecha_inicio,_midnight(day)), min(permiso.fecha_fin,_midnight(day+timedelta(days=1)))
                    matches = hi>lo
                    if matches:
                        intervals[day][permiso.goce_sueldo].append((lo,hi))
                        minutes+=(hi-lo).total_seconds()/60
                else:
                    matches = day==first
                if matches:
                    permiso_days[day].append(permiso)
                    matched_days.append(day)
            if not any(permiso in values for values in permiso_days.values()):
                continue
            applicable_permission = any(day >= employee.fecha_ingreso and (baja is None or day <= baja) for day in matched_days)
            if any(day < employee.fecha_ingreso or (baja and day > baja) for day in matched_days):
                observations.append('Evidencia fuera del periodo laboral; no incluida en totales de tiempo')
            if permiso.tipo=='permiso_dia':
                minutes=None
            elif minutes is None:
                summary['duracion_desconocida']+=int(applicable_permission)
                observations.append('Permiso por horas con duración N/D')
            permiso_report.append({'id':permiso.id,'folio':permiso.folio,'tipo':permiso.get_tipo_display(),'tipo_codigo':permiso.tipo,'estado':permiso.get_estado_display(),'goce_sueldo':permiso.goce_sueldo,'fecha_inicio':permiso.fecha_inicio,'fecha_fin':permiso.fecha_fin,'minutos':int(minutes) if minutes is not None else None,'dias':days_count,'observaciones':observations,'motivo':permiso.motivo,'aplicable_laboral':applicable_permission})
        summary['permisos']=len(permiso_report)
        summary['permisos_aplicables']=sum(p['aplicable_laboral'] for p in permiso_report)
        conflict_days = set()
        for day in dates:
            cg, sg = _union(intervals[day][True]), _union(intervals[day][False])
            applicable_day = day >= employee.fecha_ingreso and (baja is None or day <= baja)
            for goce, merged in ((True,cg),(False,sg)):
                if not applicable_day:
                    continue
                summary['minutos_cg' if goce else 'minutos_sg']+=int(sum((end-start).total_seconds() for start,end in merged)//60)
                summary['dias_cg' if goce else 'dias_sg']+=int(goce in full_days[day])
            if (True in full_days[day] and (False in full_days[day] or sg)) or (False in full_days[day] and cg) or any(max(a,c)<min(b,d) for a,b in cg for c,d in sg):
                conflict_days.add(day)
        summary['conflictos']=sum(day >= employee.fecha_ingreso and (baja is None or day <= baja) for day in conflict_days)
        rows=[]
        for day in dates:
            a=attendance.get((eid,day))
            records=extras[(eid,day)]
            events=bool(a or records or by_day[day] or permiso_days[day] or vacation_days[day])
            pre=day<employee.fecha_ingreso
            post=bool(baja and day>baja)
            observations=[]
            if not a:
                observations.append('Sin checada' if events else 'Sin registro; no se infiere falta ni descanso')
            if pre and events:
                observations.append('Actividad anterior al ingreso; revisar')
            if post and events:
                observations.append('Actividad posterior a la baja registrada; revisar')
            if (pre or post) and events:
                observations.append('Evidencia fuera del periodo laboral; no incluida en KPIs')
            if day in conflict_days:
                observations.append('Conflicto de permisos con/sin goce; revisar sin decidir pago')
            label='No aplica' if pre or (post and not events) else ('Baja registrada' if post else ('Activo' if employee.activo else 'Inactivo'))
            extra=conciliar_extra_diario(None if pre else a, records)
            if label=='No aplica':
                extra['estado']='No aplica'
            late=None
            if a and a.turno_id and a.entrada and not pre:
                scheduled=timezone.make_aware(datetime.combine(day,a.turno.hora_entrada))
                late=max(0,int((a.entrada-scheduled).total_seconds()//60))
            permission_parts = ['Vacaciones aprobadas'] if vacation_days[day] else []
            for goce in (True, False):
                suffix = 'CG' if goce else 'SG'
                if goce in full_days[day]:
                    permission_parts.append(f'1 día {suffix}')
                merged = _union(intervals[day][goce])
                if merged:
                    minutes = int(sum((end-start).total_seconds() for start,end in merged)//60)
                    permission_parts.append(f'{formato_minutos(minutes)} {suffix}')
                if any(p.goce_sueldo == goce and p.tipo != 'permiso_dia' and (p.fecha_fin is None or p.fecha_fin < p.fecha_inicio) for p in permiso_days[day]):
                    permission_parts.append(f'N/D {suffix}')
            rows.append({'fecha':day,'asistencia':a,'vacaciones':vacation_days[day],'incidencias':by_day[day],'extra':extra,'tarde_minutos':late,'retardos_registrados':0 if pre or post else sum(i['tipo_codigo'] in ('retardo','retardo_tolerancia') and i['estado_codigo']=='pendiente' for i in by_day[day]),'permiso_texto':'; '.join(permission_parts),'permiso_folios':[p.folio for p in permiso_days[day]],'estado_laboral':'pre_ingreso' if pre else ('post_baja' if post else 'activo' if employee.activo else 'inactivo'),'estado_laboral_label':label,'aplicable_laboral':not (pre or post),'observaciones':observations})
        reports.append({'empleado':employee,'datos':{'id':eid,'nombre':employee.nombre,'codigo':employee.codigo,'puesto':employee.puesto,'sucursal':employee.sucursal_display,'departamento':employee.get_departamento_display(),'departamento_codigo':employee.departamento,'area':employee.area},'resumen':summary,'extra_resumen':_extra_summary(rows),'filas':rows,'permisos':permiso_report,'permisos_resumen':{key:summary[key] for key in PERMISSION_KEYS},'faltas_30d':summary['faltas_30d'],'avisos_30d':avisos,'observaciones_resumen':['Evidencia fuera del periodo laboral no incluida en KPIs de tiempo, extra o incidencias; permisos cuenta folios, permisos_aplicables cuenta los que intersectan el periodo laboral.'] if any(not row['aplicable_laboral'] and (row['asistencia'] or row['incidencias'] or row['extra']['registros'] or row['permiso_folios'] or row['vacaciones']) for row in rows) else []})
    global_summary={key:sum(r['resumen'][key] for r in reports) for key in SUMMARY_KEYS}
    global_summary['empleados']=len(reports)
    global_summary['extra']=_extra_summary([row for report in reports for row in report['filas']])
    global_summary['permisos']={key:sum(r['resumen'][key] for r in reports) for key in PERMISSION_KEYS}
    return {'reportes':reports,'resumen':global_summary,'fecha_inicio':fecha_inicio,'fecha_fin':fecha_fin,'ventana_inicio':window,'consultado_en':timezone.now()}
