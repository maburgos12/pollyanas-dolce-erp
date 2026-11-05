"""Conciliación documental de insumos. Los únicos escritores son de expedientes."""
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from lxml.etree import XMLSyntaxError

from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.db.models import Q
from django.utils import timezone
from unidecode import unidecode

from maestros.models import CostoInsumo, Insumo
from pos_bridge.models import PointProductionLine, PointTransferLine, PointWasteLine
from pos_bridge.services.inventory_baseline import point_transfer_origin_exit_hash
from recetas.models import LineaReceta, normalizar_codigo_point
from .canonical_point_inventory import CanonicalPointInventoryService
from .models import InsumoAuditSnapshot, MovimientoInventario
from .services_auditoria_insumos import ConsumoInsumoAuditService

VERSION = 1
ZERO = Decimal('0')
LOCATIONS = {'ALMACEN_1': 'ALMACEN', 'CUARTO_FRIO': 'CEDIS'}
SOURCE_PURCHASE = 'POINT_COMPRAS_HISTORICAS'
MEASURES = (
    ('purchase', 'Compras representadas en kardex'),
    ('transfer_in', 'Transferencias recibidas'),
    ('transfer_out', 'Transferencias enviadas'),
    ('calculated', 'Consumo calculado por BOM'),
    ('calculated_sale', 'Consumo calculado por venta'),
    ('unverified', 'Consumo sin medición acreditada'),
    ('adjustment', 'Ajustes registrados'),
    ('other_in', 'Otras entradas sin origen conciliado'),
    ('other_out', 'Otras salidas sin origen conciliado'),
)


def month(value):
    if not re.fullmatch(r'\d{4}-\d{2}', str(value)):
        raise ValueError('Indica un mes válido: AAAA-MM.')
    result = date.fromisoformat(f'{value}-01')
    if result.year < 2000 or result > timezone.localdate().replace(day=1):
        raise ValueError('El mes debe estar entre enero de 2000 y el mes actual.')
    return result


def bounds(period):
    end = (period.replace(day=28) + timedelta(days=4)).replace(day=1)
    return timezone.make_aware(datetime.combine(period, time.min)), timezone.make_aware(datetime.combine(end, time.min))


def digest(value):
    return hashlib.sha256(json.dumps(value, cls=DjangoJSONEncoder, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def decimal(value):
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def normalized(value):
    return ' '.join(unidecode(str(value or '')).lower().split())


def convert(quantity, source, target):
    quantity = decimal(quantity)
    if quantity is None:
        return None
    # Sólo equivalencias dimensionales existentes; jamás densidades o presentaciones inferidas.
    return ConsumoInsumoAuditService()._convert_quantity(quantity, source, target)


def evidence(obj, **values):
    return {'model': obj._meta.label, 'id': obj.pk, **values}


def _in_period(value, start, end):
    return value is not None and start <= value < end


class DocumentaryAudit:
    def __init__(self, period):
        self.period = period
        self.start, self.end = bounds(period)
        self.rows = {}
        self.issues = []
        self.used_movements = set()
        self.items = {i.pk: i for i in Insumo.objects.select_related('unidad_base').order_by('pk')}
        self.coverage = {}

    def row(self, insumo_id, location):
        key = f'{insumo_id}:{location}'
        if key not in self.rows:
            item = self.items[insumo_id]
            self.rows[key] = {
                'key': key, 'insumo_id': insumo_id, 'name': item.nombre,
                'point_code': item.codigo_point, 'item_type': item.tipo_item,
                'unit': item.unidad_base.codigo if item.unidad_base_id else '',
                'location': location,
                'responsible': 'Yesenia con Ernesto' if location == 'ALMACEN' else 'Carolina',
                'totals': {name: ZERO for name, _ in MEASURES}, 'evidence': [], 'issues': [],
                'theoretical_current': None, 'theoretical_productions': 0, 'theoretical_blocked': 0,
                'physical_consumption': None, 'opening': None, 'closing': None, 'difference': None,
            }
        return self.rows[key]

    def issue(self, code, detail, source=None, row=None):
        target = row['issues'] if row is not None else self.issues
        item = {'code': code, 'detail': detail}
        if source:
            item['source'] = source
        if item not in target:
            target.append(item)

    def build(self):
        movements = list(MovimientoInventario.objects.filter(fecha__gte=self.start, fecha__lt=self.end).order_by('pk'))
        by_hash = {m.source_hash: m for m in movements if m.source_hash}
        self.coverage['ledger_rows'] = len(movements)
        self.purchases(by_hash)
        self.transfers(by_hash)
        bom_hashes = self.production()
        sale_hashes = self.sale_calculations()
        self.source_manifest()
        for movement in movements:
            if movement.almacen not in LOCATIONS:
                self.issue('UBICACION_FUERA_ALCANCE', 'Movimiento fuera de Almacén/CEDIS; no se suma.', evidence(movement, location=movement.almacen, quantity=str(movement.cantidad)))
                continue
            row = self.row(movement.insumo_id, LOCATIONS[movement.almacen])
            if movement.pk in self.used_movements:
                continue
            source = evidence(movement, date=movement.fecha.isoformat(), quantity=str(movement.cantidad), reference=movement.referencia, hash=movement.source_hash)
            category = {'ENTRADA': 'other_in', 'SALIDA': 'other_out', 'AJUSTE': 'adjustment'}.get(movement.tipo)
            if movement.tipo == 'CONSUMO':
                calculation = bom_hashes.get(movement.source_hash)
                category = 'calculated' if calculation and calculation['insumo_id'] == movement.insumo_id else 'unverified'
                source['kind'] = 'BOM' if category == 'calculated' else 'ORIGEN_NO_ACREDITADO'
                if category == 'calculated':
                    source['production'] = bom_hashes[movement.source_hash]
                # Un FK de lote no acredita por sí solo que la cantidad haya sido medida.
                if movement.source_hash in sale_hashes and movement.insumo_id == sale_hashes[movement.source_hash]['insumo_id']:
                    category = 'calculated_sale'
                    source.update(kind='CALCULO_VENTA', calculation=sale_hashes[movement.source_hash])
                source['lote_id'] = movement.lote_id
                source['linea_bitacora_id'] = movement.linea_bitacora_id
            if category:
                row['totals'][category] += movement.cantidad
            source['category'] = category or 'UNKNOWN'
            row['evidence'].append(source)
            if category in {'other_in', 'other_out'} or category is None:
                self.issue('ORIGEN_MOVIMIENTO_PENDIENTE', 'No se identificó un evento fuente único para este movimiento.', source, row)
            if category == 'adjustment':
                self.issue('AJUSTE_NO_ES_CONSUMO', 'Ajuste conservado separado; requiere motivo y soporte, no demuestra uso físico.', source, row)
            if movement.cantidad < 0 and movement.tipo != 'AJUSTE':
                self.issue('SIGNO_REQUIERE_REVISION', 'Cantidad negativa fuera de ajuste; no se invirtió su significado.', source, row)
        self.waste()
        self.catalog_and_inventory()
        self.coverage['external_purchase_completeness'] = 'NO_DETERMINADA'
        self.coverage['physical_consumption_verified'] = 0
        self.coverage['historical_boundaries_verified'] = 0
        self.issue('SIN_FRONTERAS_INDEPENDIENTES', 'No se acreditan apertura/cierre físicos del periodo ni consumo medido. Las capturas actuales no cierran el mes.')
        self.issue('COBERTURA_EXTERNA_NO_ACREDITADA', 'Se concilian registros importados. Su presencia no prueba que Point haya entregado todos los documentos, revisiones o cancelaciones.')
        self.issue('RECETA_HISTORICA_NO_ACREDITADA', 'La relación con producción/BOM identifica cálculos; no acredita vigencia histórica de la receta ni consumo físico.')
        for row in self.rows.values():
            t = row['totals']
            row['ledger_net'] = t['purchase'] + t['transfer_in'] + t['other_in'] + t['adjustment'] - t['transfer_out'] - t['other_out'] - t['calculated'] - t['calculated_sale'] - t['unverified']
            row['measures'] = [{'key': key, 'label': label, 'quantity': str(t[key])} for key, label in MEASURES]
            row['status'] = 'INCONCLUSO'
            row['issue_count'] = len(row['issues'])
        return json.loads(json.dumps({
            'version': VERSION, 'period': self.period.isoformat(),
            'coverage': self.coverage, 'issues': self.issues,
            'rows': sorted(self.rows.values(), key=lambda r: (r['name'], r['location'], r['insumo_id'])),
        }, cls=DjangoJSONEncoder, ensure_ascii=False))

    def purchases(self, by_hash):
        costs = list(CostoInsumo.objects.filter(raw__source=SOURCE_PURCHASE, fecha__gte=self.period, fecha__lt=self.end.date()).order_by('pk'))
        self.coverage['purchase_lines'] = len(costs)
        self.coverage['purchase_documents'] = len({c.raw.get('purchase_id') for c in costs if c.raw.get('purchase_id')})
        matched = 0
        identities = defaultdict(list)
        for cost in costs:
            identities[(cost.raw.get('purchase_id'), cost.insumo_id)].append(cost.pk)
        cfdis = self.invoice_candidates(costs)
        for cost in costs:
            raw = cost.raw or {}
            row = self.row(cost.insumo_id, 'ALMACEN')
            source = evidence(cost, date=str(cost.fecha), purchase_id=raw.get('purchase_id'), folio=raw.get('folio'), supplier=raw.get('supplier'), quantity=raw.get('quantity'), unit=raw.get('unit'), total=raw.get('total_cost'), hash=cost.source_hash)
            source['invoice_candidates'] = cfdis.get(str(raw.get('folio') or ''), [])
            row['evidence'].append(source)
            if normalized(raw.get('branch')) != 'almacen':
                self.issue('COMPRA_UBICACION_AMBIGUA', 'El documento de compra no acredita Almacén como destino.', source, row)
            if not raw.get('purchase_id') or len(identities[(raw.get('purchase_id'), cost.insumo_id)]) > 1:
                self.issue('COMPRA_REVISION_O_DUPLICADO', 'Documento/insumo con identidad insuficiente o varias líneas. No fusionar ni descartar sin ID de línea y revisión.', source, row)
            names = {normalized(row['name']), normalized(self.items[cost.insumo_id].nombre_point)} - {''}
            if normalized(raw.get('article_name')) not in names:
                self.issue('COMPRA_IDENTIDAD_PENDIENTE', 'Nombre de compra distinto del maestro; validar vínculo histórico.', source, row)
            expected = convert(raw.get('quantity'), raw.get('unit'), row['unit'])
            if expected is None:
                self.issue('UNIDAD_NO_CONVERTIBLE', 'No existe conversión dimensional acreditada de compra a unidad base.', source, row)
            source_hash = hashlib.sha256(f'point_purchase|{cost.source_hash}'.encode()).hexdigest()
            movement = by_hash.get(source_hash)
            if movement is None:
                self.issue('COMPRA_SIN_ENTRADA_PERIODO', 'No hay entrada derivada en este periodo; revisar recepción y fecha de captura.', source, row)
            elif movement.tipo != 'ENTRADA' or movement.insumo_id != cost.insumo_id or movement.almacen != 'ALMACEN_1' or movement.trazabilidad.get('costo_insumo_id') != cost.pk:
                self.issue('COMPRA_ENTRADA_CONTRADICTORIA', 'La representación del documento tiene tipo, identidad, ubicación o referencia incompatibles.', evidence(movement, quantity=str(movement.cantidad)), row)
            else:
                self.used_movements.add(movement.pk)
                row['totals']['purchase'] += movement.cantidad
                row['evidence'].append(evidence(movement, category='purchase', quantity=str(movement.cantidad), date=movement.fecha.isoformat(), reference=movement.referencia, cost_id=cost.pk))
                if expected is not None and expected >= 0 and abs(expected - movement.cantidad) <= Decimal('0.0005'):
                    matched += 1
                else:
                    self.issue('COMPRA_CANTIDAD_NO_CONCILIADA', f'Entrada {movement.cantidad} {row["unit"]}; cantidad convertida: {expected if expected is not None else "desconocida"}.', source, row)
            self.issue('RECEPCION_FISICA_NO_ACREDITADA', 'Compra importada y entrada derivada no son evidencia independiente de recepción física.', source, row)
            self.issue('FACTURA_RECEPCION_PENDIENTE', 'Los CFDI candidatos no se suman como entrada ni validan presentación/recepción automáticamente.', source, row)
        self.coverage['purchase_quantity_matches'] = matched

    def invoice_candidates(self, costs):
        from sat_client.models import CfdiDescargado
        from sat_client.services.base import parse_xml
        folios = {str(c.raw.get('folio') or '') for c in costs} - {''}
        result = defaultdict(list)
        # Ventana documental explícita; ausencia fuera de ella permanece desconocida.
        qs = CfdiDescargado.objects.filter(fecha_emision__gte=self.start-timedelta(days=7), fecha_emision__lt=self.end+timedelta(days=7)).exclude(tipo_comprobante='N').only('id', 'uuid', 'xml_raw', 'total', 'estatus', 'fecha_emision', 'nombre_emisor')
        count = 0
        skipped = 0
        for cfdi in qs.iterator(chunk_size=100):
            count += 1
            if not cfdi.xml_raw or len(cfdi.xml_raw) > 2_000_000:
                skipped += 1
                continue
            try:
                root = parse_xml(cfdi.xml_raw)
                if root is None or root.get('Folio') not in folios or root.get('TipoDeComprobante') not in {'I', 'E'}:
                    continue
                concepts = [{k: node.get(k) for k in ('Cantidad', 'Unidad', 'ClaveUnidad', 'Descripcion', 'Importe')} for node in root.iter() if isinstance(node.tag, str) and node.tag.endswith('}Concepto')]
                result[root.get('Folio')].append(evidence(cfdi, uuid=str(cfdi.uuid), folio=root.get('Folio'), serie=root.get('Serie'), supplier=cfdi.nombre_emisor, date=cfdi.fecha_emision.isoformat(), total=str(cfdi.total), status=cfdi.estatus, concepts=concepts, match='SOLO_FOLIO_CANDIDATO'))
            except (ValueError, TypeError, XMLSyntaxError):
                skipped += 1
                continue
        self.coverage['invoice_rows_scanned_window'] = count
        self.coverage['invoice_xml_unreadable'] = skipped
        return result

    def transfers(self, by_hash):
        query = Q(sent_at__gte=self.start, sent_at__lt=self.end) | Q(received_at__gte=self.start, received_at__lt=self.end) | Q(registered_at__gte=self.start, registered_at__lt=self.end)
        lines = list(PointTransferLine.objects.filter(query, is_insumo=True).select_related('origin_branch', 'destination_branch').order_by('pk'))
        self.coverage['transfer_rows'] = len(lines)
        self.coverage['transfer_superseded'] = sum(not t.is_current_snapshot for t in lines)
        paired = 0
        for line in lines:
            origin = {'almacen': 'ALMACEN', 'cedis': 'CEDIS'}.get(line.origin_branch.normalized_name)
            destination = {'almacen': 'ALMACEN', 'cedis': 'CEDIS'}.get(line.destination_branch.normalized_name)
            if not origin and not destination:
                continue
            source = evidence(line, transfer=line.transfer_external_id, detail=line.detail_external_id, item_name=line.item_name, item_code=line.item_code, origin=line.origin_branch.name, destination=line.destination_branch.name, sent=str(line.sent_quantity), received=str(line.received_quantity), unit=line.unit, sent_at=line.sent_at.isoformat() if line.sent_at else None, received_at=line.received_at.isoformat() if line.received_at else None, current=line.is_current_snapshot, cancelled=line.is_cancelled, hash=line.source_hash)
            if not line.is_current_snapshot:
                continue
            if line.insumo_id not in self.items:
                self.issue('TRANSFERENCIA_SIN_IDENTIDAD', 'La transferencia no tiene insumo enlazado.', source)
                continue
            affected = [self.row(line.insumo_id, loc) for loc in dict.fromkeys([origin, destination]) if loc]
            for row in affected:
                row['evidence'].append(source)
                if normalized(line.item_name) not in {normalized(row['name']), normalized(self.items[line.insumo_id].nombre_point)} or normalizar_codigo_point(line.item_code) != normalizar_codigo_point(row['point_code']):
                    self.issue('IDENTIDAD_HISTORICA_AMBIGUA', 'Código/nombre del documento contradice el maestro actual; validar PK Point y vigencia.', source, row)
            if line.is_cancelled:
                for row in affected:
                    self.issue('TRANSFERENCIA_CANCELADA', 'Documento cancelado: comprobar movimientos compensatorios; no asumir devolución física.', source, row)
                continue
            unit = affected[0]['unit']
            sent = convert(line.sent_quantity, line.unit, unit)
            received = convert(line.received_quantity, line.unit, unit)
            if sent is None or received is None:
                for row in affected:
                    self.issue('UNIDAD_NO_CONVERTIBLE', 'La unidad de transferencia no tiene conversión dimensional acreditada.', source, row)
                continue
            if sent < 0 or received < 0:
                for row in affected:
                    self.issue('SIGNO_REQUIERE_REVISION', 'Transferencia negativa; representación sin conciliar.', source, row)
                continue
            checks = []
            for loc, stamp, qty, key, category, expected_type in (
                (origin, line.sent_at, sent, point_transfer_origin_exit_hash(line.source_hash), 'transfer_out', 'SALIDA'),
                (destination, line.received_at if line.is_received else None, received, line.source_hash, 'transfer_in', 'ENTRADA'),
            ):
                if not loc or not _in_period(stamp, self.start, self.end):
                    continue
                row = self.row(line.insumo_id, loc)
                m = by_hash.get(key)
                ok = m is not None and m.insumo_id == line.insumo_id and m.tipo == expected_type and LOCATIONS.get(m.almacen) == loc and abs(m.cantidad-qty) <= Decimal('0.0005')
                checks.append(ok)
                if ok:
                    self.used_movements.add(m.pk)
                    row['totals'][category] += m.cantidad
                    row['evidence'].append(evidence(m, category=category, quantity=str(m.cantidad), reference=m.referencia, transfer_line_id=line.pk))
                elif qty != ZERO or m is not None:
                    self.issue('TRANSFERENCIA_KARDEX_PENDIENTE', f'No coincide la representación {category} de {qty} {unit} en el periodo.', source, row)
            if line.is_received and line.received_at and sent == received and len(checks) == 2 and all(checks):
                paired += 1
            if not line.is_received or line.received_at is None:
                code, detail = 'TRANSFERENCIA_PENDIENTE_RECEPCION', 'Envío sin recepción acreditada; mantener tránsito separado.'
            elif sent != received:
                code, detail = 'DIFERENCIA_TRANSFERENCIA', f'Enviado menos recibido: {sent-received} {unit}. Requiere aclaración, no prueba uso indebido.'
            elif not (_in_period(line.sent_at, self.start, self.end) and _in_period(line.received_at, self.start, self.end)):
                code, detail = 'TRANSFERENCIA_CRUZA_CORTE', 'Envío y recepción pertenecen a cortes distintos; no compensarlos dentro de un solo periodo.'
            else:
                continue
            for row in affected:
                self.issue(code, detail, source, row)
        self.coverage['transfer_pairs_quantity_verified'] = paired

    def production(self):
        productions = list(PointProductionLine.objects.filter(production_date__gte=self.period, production_date__lt=self.end.date()).select_related('receta__rendimiento_unidad', 'branch').order_by('pk'))
        recipes = {p.receta_id for p in productions if p.receta_id}
        boms = defaultdict(list)
        for line in LineaReceta.objects.filter(receta_id__in=recipes).exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION).select_related('unidad', 'receta').order_by('pk'):
            boms[line.receta_id].append(line)
        hashes = {}
        self.coverage['production_lines'] = len(productions)
        self.coverage['production_without_recipe'] = sum(p.receta_id is None for p in productions)
        for p in productions:
            source = evidence(p, branch=p.branch.name, production=p.production_external_id, detail=p.detail_external_id, date=str(p.production_date), item_name=p.item_name, quantity=str(p.produced_quantity), unit=p.unit, receta_id=p.receta_id, is_insumo=p.is_insumo)
            location = {'almacen': 'ALMACEN', 'cedis': 'CEDIS'}.get(p.branch.normalized_name)
            if not location:
                self.issue('PRODUCCION_FUERA_ALCANCE', 'Producción fuera de Almacén/CEDIS; sin asignación inferida.', source)
                continue
            if p.receta_id is None or not boms[p.receta_id]:
                self.issue('PRODUCCION_SIN_RECETA_COMPLETA', 'Falta vínculo de receta o componentes; producción conservada fuera del teórico.', source)
            if p.insumo_id in self.items and p.is_insumo:
                self.row(p.insumo_id, location)['evidence'].append(source)
            for line in boms[p.receta_id]:
                if line.insumo_id not in self.items:
                    self.issue('BOM_SIN_IDENTIDAD', 'Componente de receta sin insumo; no se considera cero.', evidence(line, receta_id=line.receta_id, production_id=p.pk))
                    continue
                row = self.row(line.insumo_id, location)
                if p.produced_quantity > 0:
                    recipe = p.receta
                    yield_qty = recipe.rendimiento_cantidad
                    yield_unit = recipe.rendimiento_unidad.codigo if recipe.rendimiento_unidad_id else ''
                    output = convert(p.produced_quantity, p.unit, yield_unit)
                    component = convert(line.cantidad, line.unidad.codigo if line.unidad_id else line.unidad_texto, row['unit'])
                    if yield_qty and yield_qty > 0 and output is not None and component is not None and component >= 0 and line.match_status == LineaReceta.STATUS_AUTO:
                        theoretical = component * output / yield_qty
                        row['theoretical_current'] = (row['theoretical_current'] or ZERO) + theoretical
                        row['theoretical_productions'] += 1
                        row['evidence'].append(evidence(line, category='TEORICO_RECETA_ACTUAL', production_id=p.pk, receta_id=recipe.pk, component=str(line.cantidad), unit=line.unidad.codigo if line.unidad_id else line.unidad_texto, output=str(p.produced_quantity), output_unit=p.unit, yield_quantity=str(yield_qty), yield_unit=yield_unit, theoretical=str(theoretical)))
                    else:
                        row['theoretical_blocked'] += 1
                key = hashlib.sha256(f'BOM|{p.production_external_id}|{line.insumo_id}'.encode()).hexdigest()
                hashes.setdefault(key, {'insumo_id': line.insumo_id, 'productions': []})['productions'].append(source)
                if p.produced_quantity <= 0:
                    continue
                if line.insumo_id and self.items[line.insumo_id].tipo_item == Insumo.TIPO_INTERNO:
                    self.issue('PREPARACION_SIN_GENEALOGIA', 'Componente intermedio: no explotar otra vez sus materias primas como consumo físico del producto final.', evidence(line, receta_id=line.receta_id, production_id=p.pk), row)
                if self.items[line.insumo_id].tipo_item == Insumo.TIPO_HERRAMIENTA:
                    self.issue('CLASIFICACION_REQUIERE_REVISION', 'Artículo clasificado como herramienta usado en receta; revisar documentos y uso sin reclasificar.', evidence(line, receta_id=line.receta_id), row)
                if line.match_status != LineaReceta.STATUS_AUTO or line.cantidad is None or convert(line.cantidad, line.unidad.codigo if line.unidad_id else line.unidad_texto, row['unit']) is None:
                    self.issue('BOM_NO_COMPARABLE', 'Componente rechazado, sin cantidad o con unidad incompatible; no se acredita consumo teórico histórico.', evidence(line, receta_id=line.receta_id, quantity=str(line.cantidad), unit=line.unidad.codigo if line.unidad_id else line.unidad_texto), row)
        return hashes

    def sale_calculations(self):
        from .services_consumo_bom import ConsumoInsumoAutoService
        # Reutiliza exclusivamente la ruta dry-run: reproduce procedencia, no corrige el libro.
        summary = ConsumoInsumoAutoService().generar_consumos_produccion(
            self.period, self.end.date()-timedelta(days=1), dry_run=True,
        )
        return {i.source_hash: {'insumo_id': i.insumo.pk, 'quantity_current': str(i.cantidad),
                               'origin': i.origen, 'date': str(i.fecha), 'recipes': sorted(i.recetas)}
                for i in summary.items if i.origen in {'CONSUMO_VENTA', 'BOM_VENTA_SERVICIO'}}

    def source_manifest(self):
        from compras.models import RecepcionCompra, RecepcionItemDepartamental
        from mermas.models import MermaInsumo
        from pos_bridge.models import PointSyncJob
        from .models import ConteoFisicoMensual, LoteProduccion
        from .models_conteos import ConteoSucursal
        self.coverage['physical_counts_candidates'] = list(ConteoFisicoMensual.objects.filter(
            fecha_conteo__gte=self.period-timedelta(days=1), fecha_conteo__lte=self.end.date()
        ).values('id', 'periodo', 'fecha_conteo', 'estatus'))
        self.coverage['branch_counts_candidates'] = list(ConteoSucursal.objects.filter(
            fecha__gte=self.period-timedelta(days=1), fecha__lte=self.end.date()
        ).values('id', 'sucursal_id', 'fecha', 'estado'))
        self.coverage['production_lots_candidates'] = list(LoteProduccion.objects.filter(
            producido_en__gte=self.start, producido_en__lt=self.end
        ).values('id', 'insumo_id', 'receta_id', 'cantidad_inicial', 'unidad_id', 'linea_origen_id', 'estado'))
        self.coverage['receipts_classic'] = list(RecepcionCompra.objects.filter(
            fecha_recepcion__gte=self.period, fecha_recepcion__lt=self.end.date()
        ).values('id', 'folio', 'orden_id', 'fecha_recepcion', 'estatus'))
        self.coverage['receipts_departmental'] = list(RecepcionItemDepartamental.objects.filter(
            recibido_en__gte=self.start, recibido_en__lt=self.end
        ).values('id', 'linea_orden_id', 'cantidad_recibida', 'recibido_en'))
        self.coverage['erp_waste'] = list(MermaInsumo.objects.filter(
            creado_en__gte=self.start, creado_en__lt=self.end
        ).values('id', 'insumo_id', 'sucursal_id', 'cantidad_reportada', 'cantidad_aprobada',
                 'unidad_point', 'estatus', 'creado_en'))
        self.issue('RECEPCIONES_MERMAS_SIN_ENLACE', 'Recepciones ERP y solicitudes de merma se conservan en cobertura; sin enlace inequívoco no se suman a compras ni se acredita aplicación física.')
        self.coverage['unfinished_source_jobs'] = list(PointSyncJob.objects.filter(
            job_type__in=['inventory', 'production', 'transfers', 'waste', 'sales'],
            status__in=['PENDING', 'RUNNING'],
        ).order_by('pk').values('id', 'job_type', 'status', 'started_at'))
        if self.coverage['unfinished_source_jobs']:
            self.issue('JOBS_SIN_CIERRE', 'Hay jobs sin cierre, incluidos ciclos anteriores; un ciclo posterior no acredita su integridad histórica.')
        self.coverage['latest_source_jobs'] = []
        for kind in ('inventory', 'production', 'transfers', 'waste', 'sales'):
            job = PointSyncJob.objects.filter(job_type=kind).order_by('-started_at', '-pk').first()
            self.coverage['latest_source_jobs'].append(evidence(job, type=kind, status=job.status,
                started=job.started_at, finished=job.finished_at) if job else {'type': kind, 'status': 'MISSING'})
            if not job or job.status != 'SUCCESS' or job.finished_at is None:
                self.issue('FUENTE_NO_COMPLETA', f'Último job de {kind} ausente, incompleto o fallido. No acredita cobertura del periodo.')

    def waste(self):
        count = 0
        for w in PointWasteLine.objects.filter(movement_at__gte=self.start, movement_at__lt=self.end).select_related('branch').order_by('pk'):
            if not w.insumo_id:
                continue
            count += 1
            loc = {'almacen': 'ALMACEN', 'cedis': 'CEDIS'}.get(w.branch.normalized_name)
            source = evidence(w, quantity=str(w.quantity), unit=w.unit, date=w.movement_at.isoformat(), movement=w.movement_external_id)
            if loc:
                row = self.row(w.insumo_id, loc)
                row['evidence'].append(source)
                self.issue('MERMA_REPRESENTACION_PENDIENTE', 'Merma Point documentada; no sumar un segundo efecto sin identificar su representación en kardex.', source, row)
        self.coverage['point_waste_insumo_lines'] = count

    def catalog_and_inventory(self):
        active = [i for i in self.items.values() if i.activo]
        codes = defaultdict(list)
        for i in active:
            if i.codigo_point:
                codes[normalizar_codigo_point(i.codigo_point)].append(i.pk)
        recipe_usage = defaultdict(list)
        for line in LineaReceta.objects.filter(insumo__tipo_item=Insumo.TIPO_HERRAMIENTA).exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION).values('id', 'insumo_id', 'receta_id', 'cantidad'):
            recipe_usage[line['insumo_id']].append(line)
        self.coverage['active_insumos'] = len(active)
        self.coverage['current_inventory'] = {}
        for loc in ('ALMACEN', 'CEDIS'):
            readings = CanonicalPointInventoryService().read_many(active, location=loc)
            counts = Counter()
            canonical_seen = set()
            for item in active:
                row = self.row(item.pk, loc)
                reading = readings[item.pk]
                if reading.insumo_id not in canonical_seen:
                    counts[reading.freshness.value] += 1
                    canonical_seen.add(reading.insumo_id)
                row['inventory_now'] = {
                    'canonical_insumo_id': reading.insumo_id, 'quantity': str(reading.quantity_base) if reading.quantity_base is not None else None,
                    'unit': reading.base_unit, 'captured_at': reading.captured_at.isoformat() if reading.captured_at else None,
                    'sync_job_id': reading.sync_job_id, 'state': reading.freshness.value, 'error': reading.error,
                }
                if reading.freshness.value != 'FRESH':
                    self.issue('CAPTURA_POINT_NO_VIGENTE', reading.error or 'La captura Point no está vigente; no se sustituye por saldo interno.', row=row)
                if reading.quantity_base is not None and reading.quantity_base < 0:
                    self.issue('SALDO_NEGATIVO_PENDIENTE', f'Point registra {reading.quantity_base} {reading.base_unit}; no se ajustó ni compensó con otra ubicación.', row=row)
                if not item.codigo_point or len(codes.get(normalizar_codigo_point(item.codigo_point), [])) != 1:
                    self.issue('IDENTIDAD_NO_INEQUIVOCA', 'Código Point ausente o compartido por maestros activos. No consolidar historia por nombre.', row=row)
                if recipe_usage[item.pk]:
                    self.issue('CLASIFICACION_REQUIERE_REVISION', 'Herramienta con referencias de receta; validar uso y documentos.', {'lines': recipe_usage[item.pk]}, row)
            self.coverage['current_inventory'][loc] = dict(counts)


def build_report(period):
    nested = connection.in_atomic_block
    with transaction.atomic():
        if not nested:
            with connection.cursor() as cursor:
                cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                cursor.execute('SET LOCAL statement_timeout = 60000')
        return DocumentaryAudit(period).build()


def refresh_report(period):
    # Una lectura MVCC coherente por corrida; el advisory lock impide escritores paralelos del mismo mes.
    nested = connection.in_atomic_block
    with transaction.atomic():
        with connection.cursor() as cursor:
            if not nested:
                cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cursor.execute('SET LOCAL statement_timeout = 60000')
            cursor.execute('SELECT pg_try_advisory_xact_lock(%s, %s)', [731106, period.year*100+period.month])
            if not cursor.fetchone()[0]:
                return None
        payload = build_report(period)
        fingerprint = digest(payload)
        latest = InsumoAuditSnapshot.objects.filter(periodo=period).first()
        if latest and latest.fingerprint == fingerprint:
            return latest
        return InsumoAuditSnapshot.objects.create(periodo=period, fingerprint=fingerprint, payload=payload)
