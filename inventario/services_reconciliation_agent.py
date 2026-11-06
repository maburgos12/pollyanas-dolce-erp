"""Investigación reproducible y solicitudes internas; nunca corrige fuentes operativas."""
from collections import Counter, defaultdict
from datetime import timedelta
from decimal import Decimal
import hashlib
import json

from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.db.models import Max, Q
from django.utils import timezone

from maestros.models import CostoInsumo, Insumo
from pos_bridge.models import PointTransferLine, PointSyncJob
from pos_bridge.services.inventory_baseline import point_transfer_origin_exit_hash, latest_point_almacen_baseline_run
from recetas.utils.costeo_snapshot import POINT_UNIT_ALIASES
from recetas.models import normalizar_codigo_point
from reportes.models import ProductInventoryAuditCase
from reportes.services_inventory_audit_agent import InventoryAuditAgent
from seguimiento.models import SeguimientoItem
from .models import InsumoAuditSnapshot, InsumoReconciliationCase, InsumoReconciliationEvent, MovimientoInventario
from .services_documentary_audit import SOURCE_PURCHASE, bounds, convert, decimal, digest, evidence, normalized

ORIGIN = 'CONCILIADOR_INSUMOS'
TOLERANCE = Decimal('0.0005')  # Precisión del kardex, no tolerancia de pérdida física.
AREAS = ProductInventoryAuditCase.ResponsibleArea


def documentary_quantity(quantity, source, target):
    value = decimal(quantity)
    if value is None or value < 0 or not source or not target:
        return None, 'Cantidad inválida o unidad ausente.'
    source_code = POINT_UNIT_ALIASES.get(normalized(source), normalized(source))
    target_code = POINT_UNIT_ALIASES.get(normalized(target), normalized(target))
    if normalized(source_code) == normalized(target_code):
        return value, f'Misma unidad documental: {source} = {target}; no se convirtió la presentación.'
    converted = convert(value, source_code, target_code)
    if converted is not None:
        return converted, f'Equivalencia dimensional existente: {source} → {target}.'
    return None, f'Presentación sin equivalencia acreditada: {source} → {target}.'


class InsumoReconciliationAgent:
    def __init__(self):
        self.assignment = InventoryAuditAgent()

    def run(self, period, *, dry_run=False):
        with transaction.atomic():
            with connection.cursor() as cursor:
                if not connection.savepoint_ids:
                    cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                cursor.execute('SET LOCAL statement_timeout = 60000')
                cursor.execute('SELECT pg_try_advisory_xact_lock(%s, %s)', [731108, period.year*100+period.month])
                if not cursor.fetchone()[0]:
                    return {'period': str(period), 'status': 'busy'}
            self.period, self.start, self.end = period, *bounds(period)
            self.snapshot = InsumoAuditSnapshot.objects.filter(periodo=period).first()
            self.results = []
            self.purchases()
            self.transfers()
            self.coverage()
            self.consumption()
            seen={r['key'] for r in self.results}
            for old in InsumoReconciliationCase.objects.filter(periodo=period,kind__in=['COMPRA','TRANSFERENCIA']).exclude(key__in=seen):
                self.add(old.key.split(':',1)[1],old.kind,old.titulo,
                    'La fuente del expediente anterior ya no aparece en el corte vigente; no se interpreta su ausencia como conciliación.',
                    {'expediente_anterior':old.pk,'clave':old.key},
                    pending='Identificar la cancelación, revisión o cambio de fecha del documento original y su representación vigente.',
                    insumo_id=old.insumo_id,location=old.ubicacion)
            counters = Counter()
            for result in self.results:
                if not dry_run:
                    result = self.persist(result, counters)
                counters[result['estado']] += 1
            return {'period': str(period), 'status': 'investigated', 'counts': dict(counters),
                    'cases': self.results if dry_run else len(self.results)}

    def add(self, key, kind, title, conclusion, data, *, pending='', area=AREAS.ADMINISTRATION,
            insumo_id=None, location='', explained=False):
        self.results.append({'key': f'{self.period:%Y-%m}:{key}', 'periodo': self.period,
            'kind': kind, 'titulo': title[:220], 'insumo_id': insumo_id, 'ubicacion': location,
            'estado': 'ACLARACION' if pending else ('EXPLICADO' if explained else 'CONCILIADO'),
            'conclusion': conclusion, 'evidencia': {'investigacion': data, 'solicitud': pending, 'area': area}})

    def purchases(self):
        costs = list(CostoInsumo.objects.filter(raw__source=SOURCE_PURCHASE,
            fecha__gte=self.period, fecha__lt=self.end.date()).select_related('insumo__unidad_base').order_by('pk'))
        duplicates = Counter((str(c.raw.get('purchase_id')), c.insumo_id) for c in costs)
        hashes = {c.pk: hashlib.sha256(f'point_purchase|{c.source_hash}'.encode()).hexdigest() for c in costs}
        moves = {m.source_hash: m for m in MovimientoInventario.objects.filter(source_hash__in=hashes.values())}
        invoice_candidates = defaultdict(list)
        if self.snapshot:
            for row in self.snapshot.payload['rows']:
                for entry in row['evidence']:
                    if entry['model'] == 'maestros.CostoInsumo':
                        invoice_candidates[entry['id']] = entry.get('invoice_candidates', [])
        for cost in costs:
            raw, item = cost.raw or {}, cost.insumo
            unit = item.unidad_base.codigo if item.unidad_base_id else ''
            expected, reason = documentary_quantity(raw.get('quantity'), raw.get('unit'), unit)
            movement = moves.get(hashes[cost.pk])
            problems = []
            data = {'compra': evidence(cost, fecha=cost.fecha, insumo_id=item.pk,
                purchase_id=raw.get('purchase_id'), folio=raw.get('folio'), proveedor=raw.get('supplier'),
                articulo=raw.get('article_name'), cantidad=raw.get('quantity'), unidad=raw.get('unit')),
                'unidad_base': unit, 'conversion': reason, 'cantidad_esperada': expected,
                'facturas_candidatas': invoice_candidates[cost.pk],
                'limite': 'Conciliación de representación documental; no acredita recepción física.'}
            if expected is None:
                problems.append(f'Confirmar cuántas {unit or "unidades base"} contiene cada {raw.get("unit") or "presentación"} del artículo y adjuntar factura o ficha de empaque. No se aplicará la equivalencia sin aprobación.')
            if not raw.get('purchase_id') or duplicates[(str(raw.get('purchase_id')), item.pk)] > 1:
                problems.append('Identificar la línea original y su revisión; hay identidad incompleta o varias líneas del mismo documento e insumo.')
            if normalized(raw.get('branch')) != 'almacen':
                problems.append('Confirmar el almacén receptor del documento.')
            if movement is None:
                problems.append('Localizar la recepción/entrada del documento: se buscó su hash en todo el kardex, no solo en este mes.')
            else:
                data['movimiento'] = evidence(movement, insumo_id=movement.insumo_id, cantidad=movement.cantidad,
                    tipo=movement.tipo, almacen=movement.almacen, fecha=movement.fecha, trazabilidad=movement.trazabilidad)
                if (movement.tipo != 'ENTRADA' or movement.insumo_id != item.pk or
                    movement.almacen != 'ALMACEN_1' or movement.trazabilidad.get('costo_insumo_id') != cost.pk):
                    problems.append('Aclarar la identidad, ubicación o referencia contradictoria entre compra y entrada.')
                elif expected is not None and abs(expected-movement.cantidad) > TOLERANCE:
                    problems.append(f'Explicar la diferencia: documento {expected} {unit}, entrada {movement.cantidad} {unit}. Adjuntar recepción o corrección autorizada.')
                if not self.start <= movement.fecha < self.end:
                    problems.append('Compra y entrada pertenecen a cortes distintos; confirmar fecha efectiva de recepción antes de cerrar el periodo.')
            conclusion = ('; '.join(problems) if problems else
                f'Compra y entrada coinciden en identidad, ubicación, referencia y cantidad ({expected} {unit}). {reason}')
            self.add(f'purchase:{cost.pk}', 'COMPRA', f'Compra {raw.get("folio") or cost.pk} · {item.nombre}',
                conclusion, data, pending='\n'.join(problems), insumo_id=item.pk, location='ALMACEN')
        self.purchase_count = len(costs)
        self.purchase_dates = sorted({str(c.fecha) for c in costs})

    def transfers(self):
        baseline = latest_point_almacen_baseline_run()
        cutover = getattr(baseline, 'finished_at', None)
        period_query = Q(sent_at__gte=self.start, sent_at__lt=self.end) | Q(received_at__gte=self.start, received_at__lt=self.end)
        lines = list(PointTransferLine.objects.filter(period_query, is_insumo=True, is_current_snapshot=True)
            .filter(Q(origin_branch__normalized_name__in=['almacen','cedis']) | Q(destination_branch__normalized_name__in=['almacen','cedis']))
            .select_related('insumo__unidad_base','origin_branch','destination_branch').order_by('pk'))
        keys = [k for line in lines for k in [line.source_hash, point_transfer_origin_exit_hash(line.source_hash)]]
        moves = {m.source_hash: m for m in MovimientoInventario.objects.filter(source_hash__in=keys)}
        documents = defaultdict(list)
        for line in lines:
            documents[line.transfer_external_id].append(line)
        for document, rows in documents.items():
            problems, records, limits = [], [], []
            for line in rows:
                item = line.insumo
                unit = item.unidad_base.codigo if item and item.unidad_base_id else ''
                sent, rule = documentary_quantity(line.sent_quantity, line.unit, unit)
                received, _ = documentary_quantity(line.received_quantity, line.unit, unit)
                problems_line = []
                if not item:
                    problems_line.append('falta identidad de insumo')
                elif normalized(line.item_name) not in {normalized(item.nombre),normalized(item.nombre_point)}:
                    problems_line.append('nombre histórico distinto del maestro; verificar identidad original')
                if item and normalizar_codigo_point(line.item_code) != normalizar_codigo_point(item.codigo_point):
                    problems_line.append('código histórico distinto del maestro; verificar identidad original')
                if line.is_cancelled:
                    problems_line.append('documento cancelado; verificar compensación sin inferir devolución física')
                if sent is None or received is None:
                    problems_line.append(rule)
                if not line.is_received or line.received_at is None:
                    problems_line.append('falta recepción documentada')
                elif sent is not None and received is not None and sent != received:
                    problems_line.append(f'enviado {sent}, recibido {received} {unit}')
                representations = []
                for branch, key, qty, kind, stamp in [
                    (line.origin_branch, point_transfer_origin_exit_hash(line.source_hash),sent,'SALIDA',line.sent_at),
                    (line.destination_branch,line.source_hash,received,'ENTRADA',line.received_at)]:
                    location = {'almacen':'ALMACEN_1','cedis':'CUARTO_FRIO'}.get(branch.normalized_name)
                    if not location:
                        continue
                    move = moves.get(key)
                    # El importador solo materializa salidas Almacén→CEDIS posteriores al baseline.
                    sent_stamp = line.sent_at or line.registered_at
                    origin_expected = (cutover and sent_stamp and sent_stamp > cutover
                        and line.origin_branch.normalized_name == 'almacen'
                        and line.destination_branch.normalized_name == 'cedis')
                    if kind == 'SALIDA' and move is None and not origin_expected:
                        limit = f'Detalle {line.detail_external_id}: la salida no está contemplada por la integración vigente (solo Almacén→CEDIS posterior al corte inicial). No es prueba de faltante.'
                        limits.append(limit)
                        representations.append({'hash':key,'ausente':True,'limite_integracion':limit})
                        continue
                    representations.append(evidence(move, cantidad=move.cantidad, fecha=move.fecha) if move else {'hash':key,'ausente':True})
                    if (move is None or not item or move.insumo_id != item.pk or move.tipo != kind or
                            move.almacen != location or qty is None or abs(move.cantidad-qty)>TOLERANCE):
                        problems_line.append(f'representación {kind} sin conciliar en {branch.name}')
                    elif stamp and timezone.localdate(stamp) != timezone.localdate(move.fecha):
                        problems_line.append(f'fecha de {kind} distinta del documento')
                if problems_line:
                    problems.append(f'Línea {line.detail_external_id} {line.item_name}: '+ '; '.join(problems_line))
                records.append(evidence(line, detalle=line.detail_external_id, articulo=line.item_name,
                    insumo_id=line.insumo_id, enviado=line.sent_quantity, recibido=line.received_quantity,
                    unidad=line.unit, regla=rule, envio=line.sent_at, recepcion=line.received_at,
                    movimientos=representations))
            crossed = any(l.sent_at and l.received_at and
                (timezone.localdate(l.sent_at).replace(day=1) != timezone.localdate(l.received_at).replace(day=1)) for l in rows)
            self.add('transfer:'+hashlib.sha256(str(document).encode()).hexdigest()[:32], 'TRANSFERENCIA',
                f'Transferencia {document}', '; '.join(problems) if problems else
                ('Envío y recepción documentales coinciden. '+ ' '.join(limits) if limits else
                 'Envío y recepción conciliados con sus movimientos. Cruza cortes: se conserva cada fecha sin compensar meses.' if crossed else
                 'Envío, recepción y sus movimientos coinciden por identidad, fecha, ubicación y cantidad.'),
                {'documento':str(document),'lineas':records,'cruza_cortes':crossed,
                 'limites_integracion':limits,'corte_inicial':cutover,'baseline_id':baseline.pk if baseline else None},
                pending=('Adjuntar remisión y recepción o aclaración por línea:\n'+'\n'.join(problems)) if problems else '',
                area=AREAS.LOGISTICS, explained=crossed or bool(limits))

    def coverage(self):
        latest = CostoInsumo.objects.filter(raw__source=SOURCE_PURCHASE).aggregate(ultima=Max('fecha'))['ultima']
        jobs = list(PointSyncJob.objects.filter(job_type__icontains='purchase').order_by('-pk').values('id','status','parameters','result_summary')[:5])
        pending = '' if self.purchase_count else (
            f'Adjuntar exportación de compras Point de {self.period:%Y-%m} para Almacén, con folio, fecha, proveedor, artículo, cantidad y unidad; '
            'o documentar que no hubo compras con el reporte de origen. Revisar la importación existente sin crear entradas manuales duplicadas.')
        self.add('coverage:purchases','COBERTURA','Cobertura de compras del periodo',
            f'{self.purchase_count} líneas importadas. Última compra importada: {latest or "sin registros"}. '
            + ('No hay ciclos de compra registrados; la importación existente recibe archivos. La ausencia de líneas no prueba ausencia de compras.' if not jobs else
               'Se adjuntan los ciclos registrados; su existencia no acredita cobertura externa completa.'),
            {'lineas':self.purchase_count,'fechas':self.purchase_dates,'ultima_fecha_global':latest,'ciclos':jobs,
             'lector':'PointPurchaseCostImportService.import_from_browser_exports'}, pending=pending, explained=bool(self.purchase_count))
        if not self.snapshot:
            self.add('coverage:snapshot','COBERTURA','Falta revisión documental del periodo',
                'No existe revisión documental para investigar cobertura de capturas y consumo.',{},
                pending='Ejecutar la revisión documental existente del periodo y revisar su resultado.',area=AREAS.ADMINISTRATION)
            return
        groups = defaultdict(list)
        for row in self.snapshot.payload['rows']:
            state = row.get('inventory_now',{}).get('state','MISSING')
            active = any(decimal(v) not in (None,Decimal('0')) for v in row['totals'].values())
            if state != 'FRESH':
                reason = 'CON_MOVIMIENTOS' if active else 'SIN_MOVIMIENTOS_OBSERVADOS'
                groups[(row['location'],reason)].append({'id':row['insumo_id'],'nombre':row['name'],
                    'codigo':row['point_code'],'unidad':row['unit'],'captura':row.get('inventory_now'),
                    'sin_codigo':not bool(row['point_code'])})
        for (location, reason), rows in groups.items():
            pending = '' if reason == 'SIN_MOVIMIENTOS_OBSERVADOS' else (
                'Aportar captura o exportación Point de los insumos enumerados, con código, unidad, ubicación y fecha. '
                'Revisar errores de sincronización e identidad; no sustituir datos faltantes por cero.')
            self.add(f'inventory:{location}:{reason}','CAPTURA',f'Capturas Point · {location} · {reason.lower().replace("_"," ")}',
                f'{len(rows)} maestros sin captura vigente en la revisión {self.snapshot.pk}. '
                + ('Tienen movimientos: requieren aclaración específica de captura.' if pending else
                   'No tienen movimientos observados en el corte. No representan por sí mismos un faltante; se conserva la limitación de cobertura.'),
                {'revision':self.snapshot.pk,'fecha_revision':self.snapshot.creado_en,'insumos':rows},
                pending=pending, location=location, explained=not pending)
        for location in ('ALMACEN','CEDIS'):
            counts = self.snapshot.payload['coverage']
            self.add(f'physical:{location}','FISICO',f'Cortes y medición física · {location}',
                'El consumo calculado no acredita consumo físico. Se localizaron los siguientes candidatos existentes antes de solicitar documentación.',
                {'revision':self.snapshot.pk,'conteos_mensuales':counts.get('physical_counts_candidates',[]),
                 'conteos_sucursal':counts.get('branch_counts_candidates',[]),'lotes':counts.get('production_lots_candidates',[]),
                 'mermas_erp':counts.get('erp_waste',[])},
                pending=f'Identificar los conteos aceptados de apertura y cierre de {self.period:%Y-%m} en {location}, o confirmar su ausencia; enlazar cantidades medidas, mermas y soportes existentes. Una respuesta no ajusta saldos.',
                area=AREAS.PRODUCTION if location=='CEDIS' else AREAS.ADMINISTRATION,location=location)

    def consumption(self):
        if not self.snapshot:
            return
        for row in self.snapshot.payload['rows']:
            totals=row['totals']
            if not any(decimal(totals.get(k)) not in (None,Decimal('0')) for k in ['calculated','calculated_sale','unverified']):
                continue
            sources=[e for e in row['evidence'] if e.get('category') in {'calculated','calculated_sale','unverified'}]
            pending=('Identificar el documento o medición de origen de los movimientos enumerados sin procedencia acreditada; '
                     'aportar folio, fecha, insumo, unidad y cantidad. No usar la receta actual como prueba de consumo físico.') if decimal(totals.get('unverified')) not in (None,Decimal('0')) else ''
            self.add(f'consumption:{row["key"]}','CONSUMO',f'Procedencia del consumo · {row["name"]} · {row["location"]}',
                f'BOM registrado: {totals.get("calculated","0")}; venta: {totals.get("calculated_sale","0")}; '
                f'origen sin acreditar: {totals.get("unverified","0")} {row["unit"]}. '
                'Se trazan los movimientos a sus documentos calculados. No se certifica uso físico.',
                {'revision':self.snapshot.pk,'fecha_revision':self.snapshot.creado_en,'movimientos':sources,
                 'subtotal_receta_actual':row.get('theoretical_current'),'componentes_bloqueados':row.get('theoretical_blocked')},
                pending=pending,area=AREAS.PRODUCTION,insumo_id=row['insumo_id'],location=row['location'],explained=not pending)

    def persist(self, result, counters):
        case = InsumoReconciliationCase.objects.select_for_update().filter(key=result['key']).first()
        request = case.solicitud if case and case.solicitud_id else None
        response = {'comentarios':[], 'evidencias':[], 'estado_solicitud':'PENDIENTE' if result['evidencia']['solicitud'] else None}
        if request:
            response = {'comentarios':list(request.comentarios.order_by('pk').values('id','comentario','created_at','tipo')),
                'evidencias':list(request.evidencias.order_by('pk').values('id','nombre_original','comentario','estatus','created_at')),
                'estado_solicitud':request.estatus}
            if result['estado']=='ACLARACION' and (response['comentarios'] or response['evidencias']):
                result['estado']='RESPUESTA'
                result['conclusion'] += ' Hay respuesta o evidencia adjunta; no acredita por sí sola la equivalencia ni corrige fuentes. Verificar el soporte indicado.'
        if request and result['estado']=='CONCILIADO' and not request.esta_cerrado and request.estatus != 'EN_REVISION':
            request.estatus='EN_REVISION'
            request.save(update_fields=['estatus','updated_at'])
            response['estado_solicitud']='EN_REVISION'
        result['evidencia']['respuesta'] = response
        assignee, assignment_reason = self.assignment._resolve_assignee(result['evidencia']['area'], [])
        result['evidencia']['asignacion'] = {'user_id':assignee,'motivo':assignment_reason}
        # La huella excluye la hora de ejecución; una corrida idéntica no crea eventos ni solicitudes.
        fingerprint = digest(result)
        now = timezone.now()
        if case and case.fingerprint == fingerprint:
            case.comprobado_en = now
            case.save(update_fields=['comprobado_en'])
            return result
        if case is None:
            case = InsumoReconciliationCase.objects.create(**result, fingerprint=fingerprint)
            counters['created'] += 1
        else:
            for field, value in result.items():
                setattr(case,field,value)
            case.fingerprint=fingerprint
            case.actualizado_en=now
            case.comprobado_en=now
            case.save()
            counters['updated'] += 1
        data=result['evidencia']['investigacion']
        context=[f'Periodo: {case.periodo:%Y-%m}. Ubicación: {case.ubicacion or "según documento"}.']
        if 'compra' in data:
            purchase=data['compra']
            context.append(f'Compra {purchase.get("folio")} (referencia {purchase.get("purchase_id")}), fecha {purchase.get("fecha")}, proveedor {purchase.get("proveedor") or "no informado"}: {purchase.get("articulo")}, {purchase.get("cantidad")} {purchase.get("unidad")}.')
            movement=data.get('movimiento')
            if movement:
                context.append(f'Entrada #{movement["id"]}: {movement["cantidad"]} {data["unidad_base"]}, fecha {movement["fecha"]}, almacén {movement["almacen"]}.')
        for line in data.get('lineas',[]) if case.kind == 'TRANSFERENCIA' else []:
            context.append(f'Detalle {line["detalle"]}: {line["articulo"]}; enviado {line["enviado"]}, recibido {line["recibido"]} {line["unidad"]}; envío {line["envio"]}, recepción {line["recepcion"]}.')
        for item in data.get('insumos',[]):
            context.append(f'Insumo #{item["id"]}: {item["nombre"]}; código {item["codigo"] or "ausente"}; unidad {item["unidad"]}; captura {(item.get("captura") or {}).get("state","ausente")}.')
        for move in data.get('movimientos',[]):
            context.append(f'Movimiento #{move["id"]}: cantidad {move.get("quantity",move.get("cantidad","no informada"))}; fecha {move.get("date",move.get("fecha","consultar expediente"))}.')
        description='\n'.join(context)+'\n\n'+case.conclusion+'\n\nExpediente: /reportes/conciliacion-insumos/?caso='+str(case.pk)
        deliverable=result['evidencia']['solicitud'] or 'Las fuentes quedaron conciliadas documentalmente. Revisar evidencia y cerrar la solicitud sin ajustar inventario.'
        if result['evidencia']['solicitud'] and request is None:
            request = SeguimientoItem.objects.create(tipo=SeguimientoItem.TIPO_COMPROMISO,
                titulo=('Aclaración: '+case.titulo)[:220],
                descripcion=description,
                entregable_esperado=deliverable, responsable_user_id=assignee,
                area=result['evidencia']['area'], requiere_aprobacion=True, origen=ORIGIN,
                referencia_externa=case.key, metadata={'source':ORIGIN,'caso_id':case.pk,'periodo':str(case.periodo),
                    'agent_description':description,'agent_deliverable':deliverable,
                    'evidencia':json.loads(json.dumps(result['evidencia'],cls=DjangoJSONEncoder))})
            case.solicitud=request
            case.save(update_fields=['solicitud'])
            counters['requests_created'] += 1
        elif request:
            # Nunca pisa la respuesta, asignación ni aprobación humana; la investigación vive en metadata.
            fields=['metadata','updated_at']
            for field,key,value in [('descripcion','agent_description',description),('entregable_esperado','agent_deliverable',deliverable)]:
                if getattr(request,field)==request.metadata.get(key):
                    setattr(request,field,value)
                    request.metadata[key]=value
                    fields.append(field)
            request.metadata={**request.metadata,'investigacion_actual':json.loads(json.dumps(result,cls=DjangoJSONEncoder))}
            request.save(update_fields=fields)
        InsumoReconciliationEvent.objects.create(caso=case,fingerprint=fingerprint,resultado=result)
        return result
