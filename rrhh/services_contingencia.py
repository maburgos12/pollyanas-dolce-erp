"""Autorización auditable y acotada; preview estricto sin escritura por defecto."""
import hashlib
import json
from datetime import date, timedelta
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction, connection
from django.db.models import Q
from django.utils import timezone
from .models import (Empleado, EmpleadoBaja, PermisoContingencia, BeneficiarioContingencia,
                     EventoContingencia, IncidenciaAsistencia, IncidenciaAsistenciaBitacora,
                     AsistenciaEmpleado, PermisoSalida, PrenominaCorte, PrenominaMovimiento,
                     PrenominaEmpleadoResumen, NominaPeriodo, NominaLinea)
from .services_permisos import can_authorize_direccion
from .services_extra_bloqueos import bloquear_jornadas_extra

ALERTAS = {'aviso_baja_faltas', 'baja_faltas'}
PENALIDADES = {'falta'}


def _actor(actor):
    from django.contrib.auth import get_user_model
    vigente = get_user_model().objects.filter(pk=getattr(actor, 'pk', None), is_active=True).first()
    if not vigente or not can_authorize_direccion(vigente):
        raise PermissionDenied('Solo Dirección General activa puede autorizar contingencias.')


def _normalizar(datos):
    try:
        inicio, fin = date.fromisoformat(datos['fecha_inicio']), date.fromisoformat(datos['fecha_fin'])
        if not 0 <= (fin-inicio).days <= 30:
            raise ValueError
        result = {k: datos[k] for k in ('clave', 'motivo', 'descripcion', 'evidencia', 'alcance', 'tratamiento_salarial')}
        if not isinstance(result['clave'], str) or not 1 <= len(result['clave'].strip()) <= 80:
            raise ValueError
        if result['motivo'] not in {'seguridad', 'clima', 'salud', 'otro'} or result['alcance'] not in {'empresa', 'empleados'}:
            raise ValueError
        if result['tratamiento_salarial'] not in {'conservar', 'con_goce', 'sin_goce'}:
            raise ValueError
        if not isinstance(result['descripcion'], str) or not result['descripcion'].strip():
            raise ValueError
        if not isinstance(result['evidencia'], list) or not result['evidencia'] or not all(isinstance(x, str) and x.strip() for x in result['evidencia']):
            raise ValueError
        ids = datos.get('empleados', [])
        if not isinstance(ids, list) or any(type(x) is not int or x <= 0 for x in ids):
            raise ValueError
        if result['alcance'] == 'empleados' and not ids or result['alcance'] == 'empresa' and ids:
            raise ValueError
        result.update(fecha_inicio=inicio.isoformat(), fecha_fin=fin.isoformat(), empleados=sorted(set(ids)))
        return result
    except (KeyError, TypeError, ValueError):
        raise ValidationError('Contingencia inválida: días completos, 1–31 días, evidencia y alcance explícitos requeridos.')


def _json(value):
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _rows(query, lock=False, fields=None):
    if lock:
        list(query.select_for_update(of=('self',)).order_by('pk'))
    return _json(list(query.order_by('pk').values(*(fields or ()))))


def _poblacion(datos, permiso=None):
    inicio, fin = date.fromisoformat(datos['fecha_inicio']), date.fromisoformat(datos['fecha_fin'])
    if permiso and permiso.estado != 'borrador':
        return list(permiso.beneficiarios.values('empleado_id', 'fecha_inicio', 'fecha_fin', 'snapshot')), []
    qs = Empleado.objects.filter(tipo_personal=Empleado.TIPO_POLLYANA, fecha_ingreso__lte=fin)
    if datos['alcance'] == 'empleados':
        qs = qs.filter(pk__in=datos['empleados'])
        if qs.count() != len(datos['empleados']):
            raise ValidationError('IDs inexistentes, externos o fuera de la fecha de ingreso; no se infieren identidades.')
    bajas = {b.empleado_id: b for b in EmpleadoBaja.objects.order_by('fecha_baja', 'pk')}
    poblacion, excluidos = [], []
    for e in qs.order_by('pk'):
        baja = bajas.get(e.pk) if not e.activo else None
        if not e.activo and baja is None:
            excluidos.append({'empleado_id': e.pk, 'motivo': 'inactivo_sin_fecha_baja'})
            continue
        desde, hasta = max(inicio, e.fecha_ingreso), min(fin, baja.fecha_baja if baja else fin)
        if desde > hasta:
            continue
        poblacion.append(dict(empleado_id=e.pk, fecha_inicio=desde, fecha_fin=hasta, snapshot=_json({'fecha_ingreso': e.fecha_ingreso, 'activo': e.activo, 'fecha_baja': baja.fecha_baja if baja else None, 'sucursal_ref_id': e.sucursal_ref_id})))
    if datos['alcance'] == 'empleados' and {r['empleado_id'] for r in poblacion} != set(datos['empleados']):
        raise ValidationError('Empleados explícitos sin intervalo laboral acreditado.')
    return poblacion, excluidos


def _plan_alertas(poblacion, filas, inicio, hasta, cubiertas):
    """Proyección pura de la emisión/deduplicación de alertas de faltas."""
    virtual = {(r['empleado_id'],r['fecha'],r['tipo']):dict(r) for r in filas if r['tipo'] in ALERTAS}
    faltas = [r for r in filas if r['tipo']=='falta' and r['estado']=='pendiente'
              and (r['empleado_id'],r['fecha']) not in cubiertas]
    acciones = []
    for b in poblacion:
        fecha = date.fromisoformat(str(b['fecha_inicio']))
        evidencias = [date.fromisoformat(r['fecha']) for r in filas if r['empleado_id']==b['empleado_id']]
        limite = min(hasta,max(evidencias,default=fecha-timedelta(days=1)))
        while fecha <= limite:
            desde = (fecha-timedelta(days=29)).isoformat()
            iso = fecha.isoformat()
            from .services_cobertura_permisos import fecha_en_intervalo_laboral
            empleado_actual = Empleado.objects.get(pk=b['empleado_id'])
            elegible = fecha_en_intervalo_laboral(empleado_actual,fecha)
            cuenta = sum(r['empleado_id']==b['empleado_id'] and desde<=r['fecha']<=iso for r in faltas)
            for tipo, umbral in (('aviso_baja_faltas',3),('baja_faltas',4)):
                key = (b['empleado_id'],iso,tipo)
                previa = virtual.get(key)
                emitida = any(eid==b['empleado_id'] and t==tipo and desde<=dia<iso and r['estado']!='resuelto' for (eid,dia,t),r in virtual.items())
                nuevo = 'pendiente' if elegible and cuenta>=umbral and not emitida else 'resuelto'
                if previa and previa['editado_manual']:
                    continue
                if not previa and nuevo=='resuelto':
                    continue
                if not previa or previa['estado']!=nuevo or nuevo!='resuelto' and previa.get('conteo_faltas_30d')!=cuenta:
                    acciones.append(dict(id=previa['id'] if previa else None,empleado_id=b['empleado_id'],fecha=iso,tipo=tipo,antes=previa['estado'] if previa else None,despues=nuevo,conteo=cuenta))
                virtual[key] = dict(estado=nuevo,editado_manual=False,conteo_faltas_30d=cuenta,id=previa['id'] if previa else None)
            fecha += timedelta(days=1)
    return acciones


def previsualizar_contingencia(datos, actor, *, accion='aprobar', _lock=False):
    _actor(actor)
    datos = _normalizar(datos)
    if accion not in {'borrador', 'aprobar', 'revocar'}:
        raise ValidationError('Acción inválida.')
    permiso = PermisoContingencia.objects.filter(clave=datos['clave']).first()
    if permiso and permiso.datos != datos:
        raise ValidationError('La clave ya tiene otro contenido; no se modifica historia.')
    if accion == 'revocar' and not permiso:
        raise ValidationError('No existe autorización para revocar.')
    if permiso and permiso.estado == 'aprobado' and accion == 'borrador':
        raise ValidationError('Una autorización aprobada solo admite revocación; no vuelve a borrador.')
    if permiso and permiso.estado == 'borrador' and accion == 'revocar':
        raise ValidationError('Solo se revoca una autorización aprobada.')
    if permiso and permiso.estado == 'revocado' and accion != 'revocar':
        raise ValidationError('Una autorización revocada no puede reabrirse; use otra clave.')
    poblacion, excluidos = _poblacion(datos, permiso)
    ids = [r['empleado_id'] for r in poblacion]
    inicio, fin = date.fromisoformat(datos['fecha_inicio']), date.fromisoformat(datos['fecha_fin'])
    hasta = min(fin+timedelta(days=29), timezone.localdate())
    incidencias = IncidenciaAsistencia.objects.filter(empleado_id__in=ids, fecha__range=(inicio-timedelta(days=29), hasta))
    cortes = PrenominaCorte.objects.filter(fecha_inicio__lte=hasta, fecha_fin__gte=inicio)
    movimientos = PrenominaMovimiento.objects.filter(empleado_id__in=ids, fecha__range=(inicio, hasta))
    from bonos_ventas.models import BonoVentasEmpleado
    from bonos_produccion.models import BonoProduccionEmpleado
    bonos = []
    rangos_bonos = []
    conflictos = []
    meses = {(inicio+timedelta(days=n)).timetuple()[:2] for n in range((fin-inicio).days+1)}
    legacy = Q(pk__in=[])
    for anio,mes in sorted(meses):
        legacy |= Q(periodo__anio=anio,periodo__mes=mes)
    legacy &= Q(periodo__fecha_inicio__isnull=True)|Q(periodo__fecha_fin__isnull=True)
    for model in (BonoVentasEmpleado, BonoProduccionEmpleado):
        # Los periodos legacy se delimitan por mes/año cuando no hay rango.
        query = model.objects.filter(empleado_id__in=ids).filter(Q(periodo__fecha_inicio__lte=fin, periodo__fecha_fin__gte=inicio) | legacy)
        rows = _rows(query, _lock)
        periodo_modelo = model._meta.get_field('periodo').related_model
        rangos_bonos.extend(p.rango_fechas() for p in periodo_modelo.objects.filter(pk__in=[r['periodo_id'] for r in rows]))
        registro_modelo = model._meta.get_field('registros').related_model
        bonos.append({'modelo': model._meta.label, 'filas': rows,
                      'periodos': _rows(model._meta.get_field('periodo').related_model.objects.filter(pk__in=[r['periodo_id'] for r in rows]), _lock),
                      'registros': _rows(registro_modelo.objects.filter(bono_id__in=[r['id'] for r in rows]), _lock)})
        conflictos.extend({'tipo': 'bono_cerrado', 'modelo': model._meta.label, 'id': r['id']} for r in rows if r['estatus'] != 'BORRADOR')
    # Orden compartido con recalcular_corte_prenomina: padre antes de hijos.
    cortes_rows = _rows(cortes,_lock)
    nominas = NominaPeriodo.objects.filter(fecha_inicio__lte=hasta, fecha_fin__gte=inicio, lineas__empleado_id__in=ids).distinct()
    nomina_rows = _rows(nominas)
    if _lock:
        nomina_rows = _rows(NominaPeriodo.objects.filter(pk__in=[r['id'] for r in nomina_rows]), True)
    conflictos.extend({'tipo':'nomina_cerrada', 'id':r['id']} for r in nomina_rows if r['estatus'] != 'BORRADOR')
    inicio_fuentes = min([inicio-timedelta(days=29)]+[r[0] for r in rangos_bonos])
    fin_fuentes = max([hasta]+[r[1] for r in rangos_bonos])
    source = dict(nominas=nomina_rows,
                  lineas_nomina=_rows(NominaLinea.objects.filter(periodo_id__in=[r['id'] for r in nomina_rows],empleado_id__in=ids),_lock),
                  resumenes=_rows(PrenominaEmpleadoResumen.objects.filter(corte__in=cortes, empleado_id__in=ids),_lock),
                  empleados=_rows(Empleado.objects.filter(pk__in=ids), _lock, fields=['id','fecha_ingreso','activo','tipo_personal','sucursal_ref_id','exento_checador','modalidad_marcaje','salario_diario']), bajas=_rows(EmpleadoBaja.objects.filter(empleado_id__in=ids), _lock),
                  incidencias=_rows(incidencias, _lock), asistencias=_rows(AsistenciaEmpleado.objects.filter(empleado_id__in=ids, fecha__range=(inicio_fuentes,fin_fuentes)), _lock),
                  permisos=_rows(PermisoSalida.objects.filter(empleado_id__in=ids), _lock), cortes=cortes_rows, movimientos=_rows(movimientos, _lock), bonos=bonos)
    from .models import (SolicitudVacaciones,IncapacidadEmpleado,SuspensionEmpleado,
                         AsignacionJornadaEmpleado,AsignacionTurnoEmpleado,JornadaSemanalDia,Turno)
    for modelo in (SolicitudVacaciones,IncapacidadEmpleado,SuspensionEmpleado,AsignacionJornadaEmpleado,AsignacionTurnoEmpleado):
        source[modelo._meta.label] = _rows(modelo.objects.filter(empleado_id__in=ids,fecha_inicio__lte=fin_fuentes).filter(Q(fecha_fin__isnull=True)|Q(fecha_fin__gte=inicio_fuentes)),_lock)
    source['dias_jornada'] = _rows(JornadaSemanalDia.objects.filter(jornada_id__in=[r['jornada_id'] for r in source['rrhh.AsignacionJornadaEmpleado']]),_lock)
    turnos = {r['turno_id'] for r in source['dias_jornada']} | {r['turno_id'] for r in source['rrhh.AsignacionTurnoEmpleado']}
    source['turnos'] = _rows(Turno.objects.filter(pk__in=turnos),_lock)
    source['incidencias_contexto_bonos'] = _rows(IncidenciaAsistencia.objects.filter(empleado_id__in=ids,fecha__range=(inicio_fuentes,fin_fuentes)),_lock)
    from bonos_produccion.models import ConfigBonoArea
    from bonos_ventas.models import VentaCategoriaSucursal
    source['reglas_bonos'] = _rows(ConfigBonoArea.objects.filter(periodo_id__in=[r['periodo_id'] for g in bonos if g['modelo']=='bonos_produccion.BonoProduccionEmpleado' for r in g['filas']]),_lock)
    source['categorias_ventas'] = _rows(VentaCategoriaSucursal.objects.filter(periodo_id__in=[r['periodo_id'] for g in bonos if g['modelo']=='bonos_ventas.BonoVentasEmpleado' for r in g['filas']]),_lock)
    for r in source['incidencias']:
        cubierto = any(b['empleado_id'] == r['empleado_id'] and str(b['fecha_inicio']) <= r['fecha'] <= str(b['fecha_fin']) for b in poblacion)
        if r['editado_manual'] and (r['tipo'] in ALERTAS and r['fecha'] >= inicio.isoformat() or cubierto and r['tipo'] in PENALIDADES):
            conflictos.append({'tipo': 'incidencia_manual', 'id': r['id']})
    conflictos.extend({'tipo': 'prenomina_cerrada', 'id': r['id']} for r in source['cortes'] if r['estado'] in {'CERRADO', 'EXPORTADO'})
    conflictos.extend({'tipo': 'movimiento_exportado', 'id': r['id']} for r in source['movimientos'] if r['estado'] == 'EXPORTADO')
    # El desplazamiento de una emisión puede cambiar la deduplicación más allá
    # de 29 días. Bloquear historia posterior en vez de corregirla parcialmente.
    cola = _rows(IncidenciaAsistencia.objects.filter(empleado_id__in=ids, fecha__gt=hasta, tipo__in=ALERTAS).exclude(estado='resuelto'), _lock)
    source['alertas_fuera_ventana'] = cola
    source['autorizaciones'] = _rows(PermisoContingencia.objects.filter(beneficiarios__empleado_id__in=ids).distinct())
    source['beneficiarios'] = _rows(BeneficiarioContingencia.objects.filter(empleado_id__in=ids),_lock)
    conflictos.extend({'tipo':'alerta_fuera_ventana', 'id':r['id']} for r in cola)
    from .services_cobertura_permisos import contingencias_en_rango
    existentes = contingencias_en_rango(ids,inicio-timedelta(days=29),hasta)
    actividad = {(r['empleado_id'],r['fecha']) for r in source['asistencias'] if any(r[c] for c in ('entrada','salida_comida','regreso_comida','salida'))}
    cubiertas = {(eid,fecha.isoformat()) for (eid,fecha),items in existentes.items()
                 if any(accion!='revocar' or x.permiso_id!=getattr(permiso,'pk',None) for x in items)
                 and (eid,fecha.isoformat()) not in actividad}
    if accion=='aprobar':
        cubiertas |= {(b['empleado_id'],(date.fromisoformat(str(b['fecha_inicio']))+timedelta(days=n)).isoformat())
                     for b in poblacion for n in range((date.fromisoformat(str(b['fecha_fin']))-date.fromisoformat(str(b['fecha_inicio']))).days+1)
                     if (b['empleado_id'],(date.fromisoformat(str(b['fecha_inicio']))+timedelta(days=n)).isoformat()) not in actividad}
    plan_faltas = []
    filas_proyectadas = [dict(r) for r in source['incidencias']]
    empleado_por_id = {e.pk:e for e in Empleado.objects.filter(pk__in=ids)}
    for b in poblacion:
        fecha = date.fromisoformat(str(b['fecha_inicio']))
        while fecha <= min(date.fromisoformat(str(b['fecha_fin'])),timezone.localdate()):
            eid,iso = b['empleado_id'],fecha.isoformat()
            fila = next((r for r in filas_proyectadas if r['empleado_id']==eid and r['fecha']==iso and r['tipo']=='falta'),None)
            nuevo = None
            if accion=='aprobar' and (eid,iso) not in actividad and fila and not fila['permiso_id'] and not fila['solicitud_vacaciones_id'] and not fila['editado_manual']:
                nuevo = 'resuelto'
            if accion=='revocar' and (eid,iso) not in cubiertas:
                nuevo = _estado_falta_revocacion(empleado_por_id[eid],fecha)
            if nuevo is not None and (not fila or fila['estado']!=nuevo):
                plan_faltas.append(dict(id=fila['id'] if fila else None,empleado_id=eid,fecha=iso,antes=fila['estado'] if fila else None,despues=nuevo))
                if fila:
                    fila['estado'] = nuevo
                else:
                    filas_proyectadas.append(dict(id=None,empleado_id=eid,fecha=iso,tipo='falta',estado=nuevo))
            fecha += timedelta(days=1)
    plan_alertas = _plan_alertas(poblacion,filas_proyectadas,inicio,hasta,cubiertas) if accion!='borrador' else []
    afectadas = [r for r in plan_faltas if r['despues']=='resuelto']
    destino = {'aprobar':'aprobado','borrador':'borrador','revocar':'revocado'}[accion]
    idempotente = bool(permiso and permiso.estado==destino)
    if idempotente:
        plan_faltas,plan_alertas,afectadas = [],[],[]
    if not poblacion and accion!='borrador':
        conflictos.append({'tipo':'poblacion_vacia'})
    from .services_contingencia_bonos import proyectar_bonos
    bonos_plan = proyectar_bonos(datos,poblacion,permiso,accion,bonos,plan_faltas,plan_alertas) if not idempotente else []
    conflictos.extend({'tipo':'registros_ambiguos_produccion','id':b['id']} for b in bonos_plan if b['modelo']=='bonos_produccion.BonoProduccionEmpleado' and b.get('omitido'))
    revision = permiso.revision if permiso else 0
    payload = dict(bonos_plan=bonos_plan,faltas_plan=plan_faltas,alertas_plan=plan_alertas,datos=datos, accion=accion, revision=revision, poblacion=_json(poblacion), excluidos=excluidos, fuentes=source, fecha_evaluacion=str(timezone.localdate()))
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return dict(digest=digest, accion=accion, datos=datos, beneficiarios=_json(poblacion), excluidos=excluidos, conflictos=conflictos,
                movimientos_previos=source['movimientos'],
                bonos_plan=bonos_plan,
                alertas_plan=plan_alertas,
                alertas_antes=sum(r['tipo'] in ALERTAS and r['estado']!='resuelto' and r['fecha']>=inicio.isoformat() for r in source['incidencias']),
                alertas_despues=sum(r['tipo'] in ALERTAS and r['estado']!='resuelto' and r['fecha']>=inicio.isoformat() for r in source['incidencias']) + sum(a['despues']!='resuelto' for a in plan_alertas if not a['antes'] or a['antes']=='resuelto') - sum(a['despues']=='resuelto' and a['antes']!='resuelto' for a in plan_alertas),
                incidencias_a_resolver=[r['id'] for r in afectadas],
                incidencias_a_revalidar=[r for r in plan_faltas if r['despues']!='resuelto'],
                faltas_plan=plan_faltas,
                conteos={'beneficiarios':len(ids), 'faltas_generadas':len(afectadas), 'alertas_ventana':sum(r['tipo'] in ALERTAS and r['fecha']>=inicio.isoformat() for r in source['incidencias']), 'asistencias':len(source['asistencias']), 'bonos_borrador':sum(r['estatus']=='BORRADOR' for b in bonos for r in b['filas'])},
                ventana_fin=str(hasta), fuentes_inicio=str(inicio_fuentes), fuentes_fin=str(fin_fuentes), revision=revision,idempotente=idempotente)


def _estado_falta_revocacion(empleado,fecha):
    from .services_cobertura_permisos import actividad_real_en_fecha, fecha_en_intervalo_laboral
    from .services_turnos import horario_programado_para_fecha, ESTADO_DESCANSO
    from .services_vacaciones import es_dia_laborable
    from .services_asistencia_reglas import (_incapacidad_activa_en_fecha,_suspension_activa_en_fecha,
        _vacaciones_vigentes_en_fecha,_permiso_aprobado_en_rango,_cobertura_repetida,_aware_datetime,_minutos_entre)
    if not fecha_en_intervalo_laboral(empleado,fecha) or empleado.exento_checador or actividad_real_en_fecha(empleado,fecha) or not es_dia_laborable(fecha):
        return None
    horario = horario_programado_para_fecha(empleado,fecha)
    if horario.estado==ESTADO_DESCANSO or _incapacidad_activa_en_fecha(empleado,fecha) or _suspension_activa_en_fecha(empleado,fecha):
        return None
    if _vacaciones_vigentes_en_fecha(empleado,fecha) or _permiso_aprobado_en_rango(empleado,fecha):
        return 'conciliado'
    if horario.turno:
        inicio = _aware_datetime(fecha,horario.turno.hora_entrada)
        fin = _aware_datetime(fecha,horario.turno.hora_salida)
        if fin<=inicio:
            fin += timedelta(days=1)
        total = _minutos_entre(inicio,fin)
        _,cubiertos = _cobertura_repetida(empleado,fecha,inicio,fin,total)
        if total and cubiertos>=total:
            return 'conciliado'
    return 'pendiente'


def _auditar(incidencia, actor, permiso, estado_anterior):
    IncidenciaAsistenciaBitacora.objects.create(incidencia=incidencia, usuario=actor, campo='estado', valor_anterior=estado_anterior, valor_nuevo=incidencia.estado, comentario=f'Contingencia {permiso.clave}, revisión {permiso.revision}; fuentes preservadas.')


def _reparar(permiso, actor, preview, *, revocar=False):
    from .services_asistencia_reglas import _evaluar_falta_sin_registro, _upsert_incidencia
    empleados = {e.pk:e for e in Empleado.objects.filter(pk__in=permiso.beneficiarios.values_list('empleado_id',flat=True))}
    for cambio in preview['faltas_plan']:
        empleado = empleados[cambio['empleado_id']]
        fecha = date.fromisoformat(cambio['fecha'])
        if revocar:
            _evaluar_falta_sin_registro(empleado,fecha,set())
            incidencia = IncidenciaAsistencia.objects.get(empleado=empleado,fecha=fecha,tipo='falta')
        else:
            incidencia = IncidenciaAsistencia.objects.get(pk=cambio['id'])
            incidencia.estado,incidencia.severidad = 'resuelto','info'
            incidencia.metadata = {**incidencia.metadata,'contingencia_id':permiso.pk,'revision_contingencia':permiso.revision}
            incidencia.save(update_fields=['estado','severidad','metadata','actualizado_en'])
        _auditar(incidencia,actor,permiso,cambio['antes'] or '')
    for cambio in preview['alertas_plan']:
        if cambio['despues']=='resuelto':
            incidencia = IncidenciaAsistencia.objects.get(pk=cambio['id'])
            incidencia.estado,incidencia.severidad = 'resuelto','info'
            incidencia.save(update_fields=['estado','severidad','actualizado_en'])
        else:
            fecha = date.fromisoformat(cambio['fecha'])
            incidencia,_,_ = _upsert_incidencia(empleado=empleados[cambio['empleado_id']],fecha=fecha,
                tipo=cambio['tipo'],estado='pendiente',severidad='alta' if cambio['tipo']=='aviso_baja_faltas' else 'critica',
                ventana_inicio=fecha-timedelta(days=29),ventana_fin=fecha,conteo_faltas_30d=cambio['conteo'],
                detalle='Escalamiento de faltas tras revisión de contingencia.',metadata={'faltas_30d':cambio['conteo']})
        _auditar(incidencia,actor,permiso,cambio['antes'] or '')
    # No recalcular cortes completos ni tocar campos financieros/manuales.
    ids = list(permiso.beneficiarios.values_list('empleado_id',flat=True))
    # Retirar únicamente movimientos automáticos de faltas justificadas, nunca
    # capturas ni exportaciones; el evento retiene su instantánea anterior.
    from .services_cobertura_permisos import falta_justificada
    justificadas = [str(i.pk) for i in IncidenciaAsistencia.objects.filter(empleado_id__in=ids, fecha__range=(permiso.fecha_inicio, permiso.fecha_fin), tipo='falta') if falta_justificada(i)]
    PrenominaMovimiento.objects.filter(empleado_id__in=ids, fuente_modelo='rrhh.IncidenciaAsistencia', fuente_id__in=justificadas, tipo_movimiento_erp='FALTA').exclude(estado='EXPORTADO').delete()
    _actualizar_prenomina_afectada(permiso, ids)
    for modulo in ('bonos_ventas.services_checador','bonos_produccion.services_checador'):
        from importlib import import_module
        sync = import_module(modulo).sincronizar_empleado_dia_desde_checador
        for b in permiso.beneficiarios.all():
            fecha = b.fecha_inicio
            while fecha <= min(b.fecha_fin,timezone.localdate()):
                from .services_cobertura_permisos import fecha_en_intervalo_laboral
                if fecha_en_intervalo_laboral(empleados[b.empleado_id],fecha):
                    sync(b.empleado_id,fecha)
                fecha += timedelta(days=1)


def _actualizar_prenomina_afectada(permiso, empleado_ids):
    """Recalcula solo colaboradores ya presentes, sin reconstruir el corte."""
    from . import services_prenomina as p
    hasta = min(permiso.fecha_fin+timedelta(days=29), timezone.localdate())
    cortes = PrenominaCorte.objects.filter(fecha_inicio__lte=hasta,fecha_fin__gte=permiso.fecha_inicio).exclude(estado__in=['CERRADO','EXPORTADO'])
    for corte in cortes.order_by('pk'):
        resumenes = list(corte.resumenes.filter(empleado_id__in=empleado_ids).select_related('empleado'))
        if not resumenes:
            continue
        ids = [r.empleado_id for r in resumenes]
        incidencias = p._incidencias_por_empleado(corte,ids)
        incapacidades = p._incapacidades_por_empleado(corte,ids)
        from .services_cobertura_permisos import falta_justificada
        for resumen in resumenes:
            vigentes = [i for i in incidencias.get(resumen.empleado_id,[]) if not falta_justificada(i)
                and i.fecha >= resumen.empleado.fecha_ingreso
                and not any(inc.fecha_inicio <= i.fecha <= inc.fecha_fin for inc in incapacidades.get(resumen.empleado_id,[]))]
            resumen.faltas = sum(i.tipo in {'falta','falta_retardos'} for i in vigentes)
            resumen.alertas_bloqueantes = sum(p._incidencia_bloquea(i) for i in vigentes)
            resumen.estado = ('BLOQUEADO' if resumen.alertas_bloqueantes else 'REVISAR' if resumen.ajustes_pendientes else 'LISTO')
            mensajes = [{'fecha':i.fecha.isoformat(),'tipo':i.tipo,'estado':i.estado,'severidad':i.severidad,'detalle':i.detalle}
                        for i in vigentes if i.tipo in p.TIPOS_ALERTA_OPERATIVA or i.severidad in {'alta','critica'}]
            resumen.observaciones = p._observaciones(mensajes,resumen.ajustes_pendientes)
            resumen.snapshot = {**resumen.snapshot,'incidencias':mensajes}
            resumen.save(update_fields=['faltas','alertas_bloqueantes','estado','observaciones','snapshot'])
        corte.resumen = p._resumen_corte(corte)
        # Exige revisión explícita del corte abierto, nunca reabre uno cerrado.
        corte.estado = PrenominaCorte.ESTADO_EN_REVISION
        corte.save(update_fields=['resumen','estado','actualizado_en'])


@transaction.atomic
def aplicar_contingencia(datos, actor, *, digest, accion='aprobar'):
    _actor(actor)
    datos = _normalizar(datos)
    # Serializa la misma autorización incluso cuando aún no existe encabezado.
    key = int.from_bytes(hashlib.blake2b(('rrhh:contingencia:'+datos['clave']).encode(),digest_size=8).digest(),'big',signed=True)
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_advisory_xact_lock(%s)', [key])
    preliminar = previsualizar_contingencia(datos,actor,accion=accion)
    inicio_bloqueo,fin_bloqueo = date.fromisoformat(preliminar['fuentes_inicio']),date.fromisoformat(preliminar['fuentes_fin'])
    jornadas = [(b['empleado_id'],inicio_bloqueo+timedelta(days=n)) for b in preliminar['beneficiarios'] for n in range(max(0,(fin_bloqueo-inicio_bloqueo).days+1))]
    bloquear_jornadas_extra(jornadas)
    permiso = PermisoContingencia.objects.select_for_update().filter(clave=datos['clave']).first()
    preview = previsualizar_contingencia(datos,actor,accion=accion,_lock=True)
    if preview['digest'] != digest:
        raise ValidationError('La población o las fuentes cambiaron; genere una nueva simulación.')
    if preview['conflictos']:
        raise ValidationError('Conflictos manuales o dependencias cerradas/exportadas; requieren revisión separada.')
    estado = {'borrador':'borrador','aprobar':'aprobado','revocar':'revocado'}[accion]
    if permiso and permiso.estado == estado:
        return permiso
    if permiso is None:
        campos = {k:v for k,v in datos.items() if k != 'empleados'}
        permiso = PermisoContingencia.objects.create(**campos, datos=datos, creado_por=actor, estado=estado)
    else:
        permiso.estado = estado
        permiso.revision += 1
        permiso.save(update_fields=['estado','revision'])
    permiso.refresh_from_db()
    if accion == 'aprobar' and not permiso.beneficiarios.exists():
        BeneficiarioContingencia.objects.bulk_create([BeneficiarioContingencia(permiso=permiso, **b) for b in preview['beneficiarios']])
    EventoContingencia.objects.create(permiso=permiso, revision=permiso.revision, accion=accion, actor=actor,digest=digest,snapshot=preview)
    if accion != 'borrador':
        _reparar(permiso,actor,preview,revocar=accion=='revocar')
    return permiso
