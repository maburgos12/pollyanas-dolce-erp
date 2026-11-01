"""Aplica un plan revisado a costos históricos, sin alterar existencias ni cantidades fuente."""
import hashlib
import json
import sys
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from core.models import AuditLog
from inventario.models import ConsumoInsumoMensual, MovimientoInventario
from inventario.services_auditoria_insumos import ConsumoInsumoAuditService, _money, _q4, _pct, parse_period, period_bounds
from reportes.models import LineaPresupuestoMensual, ReglaFuenteRubro
from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService


def snapshot(obj):
    return json.loads(json.dumps({f.name: getattr(obj, f.attname) for f in obj._meta.fields}, default=str))


class Command(BaseCommand):
    help = 'Valida un plan JSON por stdin; --apply confirma la corrección auditada de costos.'

    def add_arguments(self, parser):
        parser.add_argument('--apply', action='store_true')

    @transaction.atomic
    def handle(self, *args, **options):
        plan = json.load(sys.stdin)
        fingerprint = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        if AuditLog.objects.filter(action='CORRECCION_COSTEO', object_id=fingerprint).exists():
            self.stdout.write(json.dumps({'estado': 'ya_aplicado', 'plan': fingerprint}))
            return
        if not plan.get('motivo') or not plan.get('autorizado_por'):
            raise CommandError('Falta motivo o autorización documentada.')
        periodo = parse_period(plan['periodo'])
        start, end = period_bounds(periodo)
        before = {'movimientos': [], 'consumos': [], 'presupuesto': []}
        after = {'movimientos': [], 'consumos': [], 'presupuesto': []}
        consolidacion = PresupuestoRealConsolidacionService()
        previous_indices = {'consumo': consolidacion._build_consumo_index(periodo)}
        exclusion_insumos = set()
        for item in plan['exclusiones']:
            mov = MovimientoInventario.objects.select_for_update().get(pk=item['movimiento_id'])
            if (mov.tipo != 'CONSUMO' or mov.source_hash != item['source_hash']
                    or mov.cantidad != Decimal(item['cantidad_fuente'])
                    or timezone.localdate(mov.fecha) < start or timezone.localdate(mov.fecha) > end
                    or 'exclusion_costeo' in mov.trazabilidad):
                raise CommandError(f'Fuente modificada o ya corregida: movimiento {mov.pk}')
            exclusion_insumos.add(mov.insumo_id)
            before['movimientos'].append(snapshot(mov))
            mov.trazabilidad = {**mov.trazabilidad, 'exclusion_costeo': {
                **item, 'motivo': plan['motivo'], 'autorizado_por': plan['autorizado_por'],
                'fecha': timezone.now().isoformat(), 'plan': fingerprint}}
            mov.save(update_fields=['trazabilidad'])
            after['movimientos'].append(snapshot(mov))
        ids = [r['id'] for r in plan['consumos']]
        if len(set(ids)) != len(ids):
            raise CommandError('Plan con consumos duplicados.')
        rows = {r.pk: r for r in ConsumoInsumoMensual.objects.select_for_update().filter(pk__in=ids, periodo=periodo)}
        if set(rows) != set(ids):
            raise CommandError('No coinciden los consumos y su periodo.')
        affected_insumos = {r.insumo_id for r in rows.values()}
        if not exclusion_insumos.issubset(affected_insumos):
            raise CommandError('La exclusión no tiene costo mensual en el plan.')
        service = ConsumoInsumoAuditService()
        actual = service._consumo_real_bulk(start, end, {r.insumo_id for r in rows.values()})
        for item in plan['consumos']:
            row = rows[item['id']]
            if (row.consumo_real != Decimal(item['consumo_anterior']) or row.costo_real != Decimal(item['costo_anterior'])
                    or row.metadata.get('costo_unitario_usado') != item['costo_unitario']):
                raise CommandError(f'Costo histórico modificado: {row.pk}')
            data = actual[row.insumo_id]
            nuevo = _q4(data['consumo_real'])
            if nuevo != Decimal(item['consumo_corregido']):
                raise CommandError(f'La fuente no confirma el plan para {row.pk}: {nuevo}')
            before['consumos'].append(snapshot(row))
            row.consumo_real = nuevo
            row.consumo_teorico = _q4(row.consumo_teorico - data['consumo_excluido'])
            if row.consumo_teorico < 0:
                raise CommandError(f'La receta cambió: consumo teórico insuficiente {row.pk}')
            costo = Decimal(item['costo_unitario'])
            row.costo_real = _money(nuevo * costo)
            row.costo_teorico = _money(row.consumo_teorico * costo)
            row.diferencia_unidades = _q4(nuevo - row.consumo_teorico)
            row.diferencia_costo = _money(row.diferencia_unidades * costo)
            row.diferencia_pct = _pct(row.diferencia_unidades / row.consumo_teorico * 100) if row.consumo_teorico else Decimal(0)
            row.alerta = service.clasificar_alerta(row.consumo_teorico, nuevo, row.diferencia_pct, data)
            row.metadata = {**row.metadata, 'correccion_costeo': fingerprint,
                'consumo_excluido': str(data['consumo_excluido']), 'ajuste_saldo_inicial': str(data['ajuste_saldo_inicial'])}
            row.save()
            after['consumos'].append(snapshot(row))
        indices = {}
        lineas = LineaPresupuestoMensual.objects.select_for_update(of=('self',)).filter(
            periodo=periodo, version='ORIGINAL', rubro__activo=True, fuente_real='AUTO:CONSUMO_MP').select_related('rubro').prefetch_related('rubro__reglas_fuente')
        for linea in lineas:
            reglas = list(linea.rubro.reglas_fuente.filter(activa=True))
            if not reglas or any(r.tipo_fuente != ReglaFuenteRubro.FUENTE_CONSUMO_MP for r in reglas):
                raise CommandError(f'Reglas inesperadas en presupuesto {linea.pk}')
            if not any((r.filtros or {}).get('total_empresa') or str((r.filtros or {}).get('insumo_id')) in {str(i) for i in affected_insumos} for r in reglas):
                continue
            anterior = sum((_money(consolidacion._monto_regla(r, periodo, previous_indices)[0] * r.signo) for r in reglas), Decimal(0))
            if anterior != linea.monto_real:
                raise CommandError(f'El presupuesto {linea.pk} ya difería de su fuente antes de corregir.')
            total = Decimal(0); breakdown = []
            for regla in reglas:
                monto, hay_datos = consolidacion._monto_regla(regla, periodo, indices)
                if not hay_datos:
                    raise CommandError(f'Sin datos para presupuesto {linea.pk}')
                aporte = _money(monto * regla.signo); total += aporte
                breakdown.append({'regla_id': regla.pk, 'tipo_fuente': regla.tipo_fuente, 'monto': str(aporte), 'filtros': regla.filtros or {}})
            if total == linea.monto_real:
                continue
            before['presupuesto'].append(snapshot(linea))
            linea.monto_real = total
            linea.metadata = {**linea.metadata, 'real_breakdown': breakdown, 'correccion_costeo': fingerprint}
            linea.save(update_fields=['monto_real', 'metadata', 'actualizado_en'])
            after['presupuesto'].append(snapshot(linea))
        payload = {'plan': plan, 'antes': before, 'despues': after}
        log_event(None, 'CORRECCION_COSTEO', 'inventario.ConsumoInsumoMensual', fingerprint, payload)
        if not options['apply']:
            transaction.set_rollback(True)
        self.stdout.write(json.dumps({'estado': 'aplicado' if options['apply'] else 'simulacion', 'id': fingerprint, **payload}, default=str))
