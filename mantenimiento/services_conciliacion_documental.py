"""Lectura del índice técnico vigente; los documentos nunca se suman al costo."""
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal

from django.core.exceptions import PermissionDenied, ValidationError
from django.db.models import Q
from django.utils import timezone

from core.models import Sucursal
from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
from .services_access import authorized_branch_ids, can_view_costs
from .services_documentos_financieros import documentos_visibles_lote, trabajos_autorizados
from .services_vinculos_proveedores import actor_actual

ESTADOS = (('', 'Todos'), ('concluidos', 'Concluidos'), ('abiertos', 'Abiertos'), ('cancelados', 'Cancelados'))


def leer_conciliacion_documental(user, *, anio=None, mes='', sucursal='', estado=''):
    actor = actor_actual(user)
    ordenes = trabajos_autorizados(actor, 'orden')
    reportes = trabajos_autorizados(actor, 'falla')
    try:
        year = int(anio or timezone.localdate().year)
        month = int(mes) if mes else None
        if not 1900 <= year <= 2100 or (month is not None and not 1 <= month <= 12):
            raise ValueError
        inicio = date(year, month or 1, 1)
        siguiente = date(year + 1, 1, 1) if not month or month == 12 else date(year, month + 1, 1)
    except (TypeError, ValueError):
        raise ValidationError('Selecciona un año entre 1900 y 2100 y un mes válido.')
    if estado not in dict(ESTADOS):
        raise ValidationError('Selecciona un estado válido.')
    ambito = authorized_branch_ids(actor)
    ids = set(ordenes.values_list('activo_ref__sucursal_id', flat=True)) | set(reportes.values_list('sucursal_id', flat=True))
    sucursales = list(Sucursal.objects.filter(pk__in=ids).order_by('nombre').values('id', 'nombre'))
    if sucursal:
        try:
            branch = int(sucursal)
        except (TypeError, ValueError):
            raise ValidationError('Selecciona una sucursal válida.')
        if branch not in ids:
            raise PermissionDenied('La sucursal no está disponible en tu consulta.')
        ordenes = ordenes.filter(activo_ref__sucursal_id=branch)
        reportes = reportes.filter(sucursal_id=branch)
    inicio_dt = timezone.make_aware(datetime.combine(inicio, time.min))
    siguiente_dt = timezone.make_aware(datetime.combine(siguiente, time.min))
    ordenes = ordenes.filter(Q(fecha_cierre__gte=inicio, fecha_cierre__lt=siguiente) |
        Q(fecha_cierre__isnull=True, fecha_programada__gte=inicio, fecha_programada__lt=siguiente))
    reportes = reportes.filter(Q(fecha_cierre__gte=inicio_dt, fecha_cierre__lt=siguiente_dt) |
        Q(fecha_cierre__isnull=True, fecha_resolucion__gte=inicio_dt, fecha_resolucion__lt=siguiente_dt) |
        Q(fecha_cierre__isnull=True, fecha_resolucion__isnull=True, fecha_reporte__gte=inicio_dt, fecha_reporte__lt=siguiente_dt))
    finales = ('cerrado', 'resuelto')
    if estado == 'concluidos':
        ordenes, reportes = ordenes.filter(estatus='CERRADA'), reportes.filter(estatus__in=finales)
    elif estado == 'abiertos':
        ordenes = ordenes.exclude(estatus__in=('CERRADA', 'CANCELADA'))
        reportes = reportes.exclude(estatus__in=(*finales, 'cancelado'))
    elif estado == 'cancelados':
        ordenes, reportes = ordenes.filter(estatus='CANCELADA'), reportes.filter(estatus='cancelado')
    ordenes = list(ordenes.select_related('activo_ref__sucursal').order_by('pk'))
    reportes = list(reportes.select_related('sucursal', 'activo_relacionado').order_by('pk'))
    documentos = defaultdict(list)
    for doc in documentos_visibles_lote(actor, [o.pk for o in ordenes], [r.pk for r in reportes]):
        documentos[(doc.tipo_trabajo, doc.trabajo_original_id)].append(doc.tipo_documento)
    costos = can_view_costs(actor)
    grupos = defaultdict(lambda: Decimal('0'))
    filas = []
    for tipo, trabajos in (('orden', ordenes), ('falla', reportes)):
        for trabajo in trabajos:
            equipo = trabajo.activo_ref if tipo == 'orden' else trabajo.activo_relacionado
            centro = equipo.sucursal if tipo == 'orden' else trabajo.sucursal
            cierre = trabajo.fecha_cierre if tipo == 'orden' else (trabajo.fecha_cierre or trabajo.fecha_resolucion)
            fecha = cierre or (trabajo.fecha_programada if tipo == 'orden' else trabajo.fecha_reporte)
            fecha = timezone.localtime(fecha).date() if isinstance(fecha, datetime) else fecha
            terminada = trabajo.estatus == 'CERRADA' if tipo == 'orden' else trabajo.estatus in finales
            soportes = documentos[(tipo, trabajo.pk)]
            fila = {'tipo': tipo, 'id': trabajo.pk, 'titulo': trabajo.folio if tipo == 'orden' else trabajo.titulo,
                'equipo': equipo.nombre if equipo else trabajo.area_instalacion or 'Instalación',
                'sucursal': centro.nombre if centro else 'Sin sucursal',
                'codigo_sucursal': centro.codigo if centro else '', 'ubicacion': equipo.ubicacion if equipo else '',
                'fecha': fecha.isoformat(), 'estado': trabajo.get_estatus_display(),
                'origen_fecha': 'Cierre' if trabajo.fecha_cierre else ('Programación' if tipo == 'orden' else ('Resolución' if trabajo.fecha_resolucion else 'Reporte')),
                'documentos': len(soportes), 'tipos_documentales': sorted(set(soportes)),
                'soporte': 'Documentos consultables' if soportes else 'Soporte pendiente de consulta o confirmación',
                'duplicado_operativo': bool(tipo == 'falla' and trabajo.duplicado_de_id)}
            if costos:
                if tipo == 'orden':
                    importe = trabajo.costo_total
                    clase = 'Componentes capturados' if importe != 0 else 'Cero sin confirmación'
                    fila['componentes'] = [str(trabajo.costo_repuestos), str(trabajo.costo_mano_obra), str(trabajo.costo_otros)]
                    if trabajo.factura_archivo or trabajo.numero_factura:
                        fila['soporte'] += ' · archivo o referencia registrados; validez fiscal pendiente'
                else:
                    importe = trabajo.costo_real if trabajo.costo_real is not None else trabajo.costo_estimado
                    clase = 'Capturado real' if trabajo.costo_real is not None else ('Estimado histórico' if importe is not None else 'No capturado')
                incluido = bool(terminada and cierre and importe is not None and importe > 0)
                fila.update(importe_fuente=format(importe, '.2f') if importe is not None else None,
                    clasificacion=clase, incluido_vigente=incluido,
                    motivo_indice='Incluido en el cálculo vigente' if incluido else (
                        'No está concluido' if not terminada else 'Sin fecha de cierre o resolución' if not cierre else
                        'Importe no capturado' if importe is None else 'Importe cero o negativo: no suma al índice vigente'))
                if incluido:
                    grupos[(fecha.strftime('%Y-%m'), fila['codigo_sucursal'], fila['ubicacion'])] += importe
            filas.append(fila)
    resumen = [{'periodo': k[0], 'sucursal': k[1], 'ubicacion': k[2], 'importe_vigente': format(v, '.2f')} for k, v in sorted(grupos.items())]
    diferencia = None
    if costos and ambito is None and not sucursal and estado in ('', 'concluidos'):
        vigente = {}
        for m in ([month] if month else range(1, 13)):
            for grupo in PresupuestoRealConsolidacionService._build_mant_equipo_index(date(year, m, 1)):
                vigente[(f'{year}-{m:02d}', grupo['activo_ref__sucursal__codigo'], grupo['activo_ref__ubicacion'])] = grupo['monto']
        # La diferencia absoluta por grupo evita que dos errores se compensen.
        diferencia = format(sum((abs(grupos[k] - vigente.get(k, Decimal('0'))) for k in set(grupos) | set(vigente)), Decimal('0')), '.2f')
    return {'anio': year, 'mes': str(month or ''), 'sucursal': str(sucursal), 'estado': estado,
        'estados': ESTADOS, 'meses': list(range(1, 13)), 'sucursales': sucursales,
        'filas': sorted(filas, key=lambda r: (r['fecha'], r['tipo'], r['id']), reverse=True),
        'resumen': resumen, 'puede_ver_costos': costos,
        'total_vigente': format(sum(grupos.values(), Decimal('0')), '.2f') if costos else None,
        'diferencia_reproduccion': diferencia, 'total_conciliado': None,
        'alcance': 'Global' if ambito is None else 'Sucursales autorizadas'}
