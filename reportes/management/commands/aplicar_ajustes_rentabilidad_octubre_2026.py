"""Reconocimiento acotado de fuentes ya conciliadas; no recalcula nóminas."""
import hashlib
import json
import os
import stat
from datetime import date
from decimal import Decimal as D
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.db.models import Q
from django.utils import timezone

from core.models import AuditLog
from core.cache_versions import bump_cache_scopes
from reportes.models import (EmpresaResultadoMensual, LineaPresupuestoMensual as L, ReglaFuenteRubro as R,
    GastoOperativoMensual, ObligacionGasto, PagoObligacionGasto, RubroPresupuesto,
    CategoriaGasto, CentroCosto, PresupuestoResumenMensual)
from reportes.services_budget_vs_actual import (BudgetVsActualSnapshotService, BUDGET_VS_ACTUAL_SOURCE,
    variance_pct, budget_tone)
from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
from reportes.services_rentabilidad_mensual import leer_costos_mensuales
from reportes.services_personal_registrado import destino_personal
from rentabilidad.services_resultado import registros_del_mes
from rrhh.models import NominaLinea, NominaConceptoLinea, NominaPeriodo, Empleado
from sat_client.models import CfdiDescargado

PERIODO = date(2026, 10, 1)
MARCA = 'ajustes_verificados_octubre_2026'
LINEAS = {20404: D('586.64'), 20532: D('1224.76'), 20522: D('250.36'),
          8826: D('95451.75'), 8874: D('1420.00')}
PUENTES = {16542: 20404, 16350: 20532, 16351: 20522}
GASTOS = [2130, 2131, 2419, 2420, 2421, 2422]


def huella(rows):
    return hashlib.sha256(json.dumps(list(rows), default=str, sort_keys=True).encode()).hexdigest()


def bloquear_fuentes():
    """Bloqueo granular: los padres también impiden insertar hijos durante el ajuste."""
    # Los padres y las filas se fijan antes de derivar identidades o leer conceptos.
    periodos = list(NominaPeriodo.objects.filter(pk__in=[22, 24]).order_by('pk').select_for_update().values_list('pk', flat=True))
    if periodos != [22, 24]:
        raise CommandError('Faltan los periodos de nómina conciliados.')
    pool = NominaLinea.objects.filter(periodo_id__in=periodos).order_by('pk')
    nominales = list(pool.select_for_update().values('pk', 'empleado_id'))
    empleados = sorted({n['empleado_id'] for n in nominales})
    bloqueados = list(Empleado.objects.filter(pk__in=empleados).order_by('pk').select_for_update().values_list('pk', flat=True))
    if bloqueados != empleados or nominales != list(pool.values('pk', 'empleado_id')):
        raise CommandError('Cambiaron las identidades de nómina durante el bloqueo.')
    gastos = GastoOperativoMensual.objects.filter(pk__in=GASTOS)
    gastos_fijos = list(gastos.order_by('pk').select_for_update().values('pk', 'cfdi_soporte_id', 'categoria_gasto_id', 'centro_costo_id'))
    cfdi_ids = [g['cfdi_soporte_id'] for g in gastos_fijos]
    if gastos.count() != 6 or set(cfdi_ids) != {794, 8884, 1993, 2002, 2047, 2207}:
        raise CommandError('Faltan los seis gastos originales o sus CFDI.')
    nominas = NominaLinea.objects.filter(periodo_id__in=[22, 24]).filter(
        Q(departamento_snapshot__in=['RRHH', 'ADMINISTRACION']) |
        Q(departamento_snapshot='', empleado__departamento__in=['RRHH', 'ADMINISTRACION']))
    personas = list(nominas.values_list('empleado_id', flat=True))
    conjuntos = [
        ('cfdi', CfdiDescargado.objects.filter(pk__in=cfdi_ids)),
        ('categorias', CategoriaGasto.objects.filter(pk__in=gastos.values('categoria_gasto_id'))),
        ('centros', CentroCosto.objects.filter(pk__in=gastos.values('centro_costo_id'))),
        ('periodos', NominaPeriodo.objects.filter(pk__in=[22, 24])),
        ('personas', Empleado.objects.filter(pk__in=personas)),
        ('rubros', RubroPresupuesto.objects.filter(pk__in=[738, 742, 1699, 1700, 1701])),
        ('reglas', R.objects.filter(pk__in=[15904, *PUENTES])),
        ('gastos', gastos),
        ('obligaciones', ObligacionGasto.objects.filter(gasto_operativo_id__in=GASTOS)),
        ('nominas', nominas),
        ('conceptos', NominaConceptoLinea.objects.filter(linea_id__in=nominas.values('pk'))),
        ('pagos', PagoObligacionGasto.objects.filter(obligacion_id__in=
            ObligacionGasto.objects.filter(gasto_operativo_id__in=GASTOS).values('pk'))),
    ]
    sources = {}
    for nombre, queryset in conjuntos:
        rows = list(queryset.order_by('pk').select_for_update().values())
        sources[nombre] = [r for r in rows if r['id'] not in PUENTES] if nombre == 'reglas' else rows
    return huella([sources])


def escribir_respaldo(ruta, contenido):
    ruta.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(ruta.parent.stat().st_mode) != 0o700:
        raise CommandError('El directorio de respaldo debe tener permisos0700.')
    esperado = json.loads(json.dumps(contenido, default=str, ensure_ascii=False))
    descriptor = os.open(ruta, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w', encoding='utf8') as archivo:
        json.dump(esperado, archivo, ensure_ascii=False, indent=2)
        archivo.flush()
        os.fsync(archivo.fileno())
    if json.loads(ruta.read_text()) != esperado:
        raise CommandError('El respaldo completo no coincide con los registros originales.')
    return hashlib.sha256(ruta.read_bytes()).hexdigest()


def ruta_respaldo(raw):
    original = Path(raw).expanduser().absolute()
    if any(p.is_symlink() for p in [original, *original.parents]):
        raise CommandError('No se aceptan enlaces simbólicos en la ruta del respaldo.')
    destino = original.resolve()
    raiz = Path.cwd().resolve()
    privado = raiz / 'storage/private/conciliaciones'
    if original.is_relative_to(raiz) or destino.is_relative_to(raiz):
        if not original.is_relative_to(privado) or not destino.is_relative_to(privado):
            raise CommandError('Usa un respaldo externo o el almacén privado de conciliaciones, sin escapes.')
    return destino


def estado_destinos(nuevas_reglas=()):
    snapshot = EmpresaResultadoMensual.objects.get(pk=10, periodo=PERIODO)
    presupuesto = PresupuestoResumenMensual.objects.get(pk=10, period=PERIODO,
        tipo=PresupuestoResumenMensual.TIPO_FUENTE, fuente_nombre=BUDGET_VS_ACTUAL_SOURCE)
    return dict(lineas=list(L.objects.filter(pk__in=LINEAS).order_by('pk').values()),
        reglas=list(R.objects.filter(pk__in=[*PUENTES, *nuevas_reglas]).order_by('pk').values()),
        snapshot={f.name: getattr(snapshot, f.name) for f in snapshot._meta.fields},
        presupuesto={f.name: getattr(presupuesto, f.name) for f in presupuesto._meta.fields})


def actualizar_resumen_consumidor(resumen, snapshot):
    actuals = BudgetVsActualSnapshotService()._actual_by_concept(snapshot)
    metadata = dict(resumen.metadata or {})
    filas = metadata.get('rows')
    if not isinstance(filas, list) or not any(f.get('concept') == 'utilidad_operativa' for f in filas):
        raise CommandError('El resumen consumidor no tiene los conceptos financieros esperados.')
    actualizadas = []
    for original in filas:
        row = dict(original)
        if row.get('concept') in actuals:
            monto = actuals[row['concept']]
            presupuesto = D(str(row['budget']))
            diferencia = monto - presupuesto
            row.update(actual=str(monto), variance=str(diferencia),
                variance_pct=str(variance_pct(diferencia, presupuesto)),
                tone=budget_tone(row['type'], diferencia))
        actualizadas.append(row)
    metadata.update(rows=actualizadas, ajustes_verificados_octubre_2026=True,
        alcance_resultado=snapshot.metadata['alcance_resultado'],
        fuente_corte=snapshot.metadata['fuente_corte'])
    resumen.total_actual = snapshot.utilidad_operativa_total
    resumen.total_variance = variance_pct(resumen.total_actual-resumen.total_budget, resumen.total_budget)
    resumen.metadata = metadata
    resumen.save(update_fields=['total_actual', 'total_variance', 'metadata', 'updated_at'])


def totales_actuales():
    lectura = leer_costos_mensuales(PERIODO)
    filas = registros_del_mes(PERIODO, lectura)
    if not filas:
        raise CommandError('No hay resultado mensual con fuentes para el periodo.')
    campos = ('ventas_netas', 'costo_materia_prima', 'costo_reventa', 'merma_registrada',
              'costo_variable_total', 'gasto_fijo_total', 'utilidad_operativa')
    totales = {campo: sum((getattr(fila, campo) or D(0) for fila in filas), D(0)) for campo in campos}
    totales['central'] = lectura['resultado']['central_registrado']
    totales['empresa'] = totales['utilidad_operativa'] - totales['central']
    return totales, lectura, len(filas)


def actualizar_snapshot(snapshot, totales, lectura, cantidad):
    snapshot.venta_total = totales['ventas_netas']
    snapshot.costo_materia_prima_total = totales['costo_materia_prima']
    snapshot.costo_reventa_total = totales['costo_reventa']
    snapshot.costo_fabricacion_total = (snapshot.costo_materia_prima_total + snapshot.mano_obra_prod_total
        + snapshot.indirecto_prod_total + snapshot.empaque_prod_total)
    snapshot.margen_bruto_total = totales['ventas_netas'] - totales['costo_variable_total']
    snapshot.gasto_comercial_total = totales['gasto_fijo_total']
    snapshot.contribucion_total = snapshot.margen_bruto_total - snapshot.gasto_comercial_total
    snapshot.gasto_corporativo_total = totales['central']
    snapshot.utilidad_operativa_total = totales['empresa']
    metadata = dict(snapshot.metadata or {})
    legacy_keys = ['venta_costeada_total', 'venta_no_receta_total', 'venta_sin_mapear_total',
        'sales_mapping_coverage_pct', 'venta_receta_sin_match_total', 'venta_total_calculada',
        'costo_materia_prima_calculado', 'costo_reventa_calculado', 'gasto_comercial_calculado',
        'costo_fabricacion_calculado', 'reventa_rows', 'reventa_rows_with_cost', 'reventa_rows_without_cost',
        'reventa_products_with_cost', 'venta_reventa_total']
    metadata['antecedentes_snapshot_20261105'] = {k: metadata.pop(k) for k in legacy_keys if k in metadata}
    metadata.update({MARCA: True, 'financial_totals_source': 'RENTABILIDAD_SUCURSAL',
        'alcance_resultado': 'PARCIAL_COSTOS_CENTRALES_SIN_CERTIFICAR',
        'merma_registrada': str(totales['merma_registrada']), 'rentabilidad_rows': cantidad,
        'rentabilidad_ventas_netas': str(totales['ventas_netas']),
        'central_lineas_pendientes': sum(bool(f['pendiente']) for f in lectura['resultado']['centrales']),
        'gastos_pendientes': len(lectura['pendientes']), 'fuente_corte': str(lectura['resultado']['fuente']),
        'ajustes_lineas': sorted(LINEAS), 'conciliado_en': timezone.now().isoformat()})
    snapshot.metadata = metadata
    snapshot.save(update_fields=['venta_total', 'costo_materia_prima_total', 'costo_reventa_total',
        'costo_fabricacion_total', 'margen_bruto_total', 'gasto_comercial_total', 'contribucion_total',
        'gasto_corporativo_total', 'utilidad_operativa_total', 'metadata', 'actualizado_en'])


class Command(BaseCommand):
    help = 'Comprueba y aplica exclusivamente los cinco destinos conciliados de octubre 2026.'

    def add_arguments(self, parser):
        parser.add_argument('--apply', action='store_true')
        parser.add_argument('--backup-file', help='JSON nuevo externo o en storage/private/conciliaciones; obligatorio al aplicar.')
        parser.add_argument('--restore-file', help='Respaldo anterior y recibo .after.json para reversión verificada.')

    @transaction.atomic
    def handle(self, *args, **options):
        aplicar = options['apply']
        if aplicar and not (options.get('backup_file') or options.get('restore_file')):
            raise CommandError('--apply exige --backup-file para el respaldo recuperable previo.')
        with connection.cursor() as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
            cursor.execute("SET LOCAL lock_timeout='2s'")
            cursor.execute("SET LOCAL statement_timeout='30s'")
        lineas = list(L.objects.select_for_update().filter(pk__in=LINEAS).select_related('rubro__area'))
        if len(lineas) != 5 or any(l.periodo != PERIODO or l.version != 'ORIGINAL' for l in lineas):
            raise CommandError('Los cinco destinos no corresponden al corte ORIGINAL aprobado.')
        snapshot = EmpresaResultadoMensual.objects.select_for_update().get(pk=10, periodo=PERIODO)
        presupuesto = PresupuestoResumenMensual.objects.select_for_update().get(pk=10, period=PERIODO,
            tipo=PresupuestoResumenMensual.TIPO_FUENTE, fuente_nombre=BUDGET_VS_ACTUAL_SOURCE)
        source_hash = bloquear_fuentes()
        if options.get('restore_file'):
            self.restaurar(ruta_respaldo(options['restore_file']), source_hash, aplicar)
            return
        actual, lectura, cantidad = totales_actuales()
        if (snapshot.metadata or {}).get(MARCA):
            audit = AuditLog.objects.filter(action='CONCILIAR', model='reportes.EmpresaResultadoMensual',
                object_id='10', payload__ajustes_verificados_octubre_2026=True).latest('pk')
            if (source_hash != audit.payload.get('fuentes_sha256')
                    or huella([estado_destinos(audit.payload['reglas_creadas'])]) != audit.payload.get('destinos_despues_sha256')):
                raise CommandError('Las fuentes, conceptos o reglas aplicadas cambiaron; revisar antes de reintentar.')
            if any(l.monto_real != LINEAS[l.pk] or (l.metadata or {}).get('sin_datos_fuente') for l in lineas):
                raise CommandError('El ajuste aplicado cambió; requiere una nueva conciliación.')
            if snapshot.utilidad_operativa_total != actual['empresa']:
                raise CommandError('El snapshot aplicado ya no coincide con sus fuentes; revisar antes de actualizar.')
            self.stdout.write(json.dumps(dict(estado='YA_APLICADO', totales=actual), default=str))
            return
        anteriores = {l.pk: l.monto_real for l in lineas}
        if anteriores != {20404: None, 20532: None, 20522: None, 8826: D('81848.84'), 8874: None}:
            raise CommandError('Cambió una línea desde el preview aprobado; no se aplicará automáticamente.')
        if actual['central'] != D('600079.66') or actual['empresa'] != D('864624.87'):
            raise CommandError('Cambió el resultado desde el preview único; conciliar la nueva revisión.')
        if snapshot.utilidad_operativa_total != D('1285478.39'):
            raise CommandError('Cambió el snapshot BI desde la revisión aprobada.')
        protegidas = L.objects.exclude(pk__in=LINEAS).order_by('pk').values()
        intactas = huella(protegidas)
        nomina = list(NominaLinea.objects.filter(pk__in=[1409, 1476]).order_by('pk').values())
        if len(nomina) != 2 or any(n['empleado_id'] != 33 or n['departamento_snapshot'] != 'RRHH' for n in nomina):
            raise CommandError('Cambió la identidad histórica de las fuentes RRHH.')
        nomina_hash = huella(nomina)
        conceptos_esperados = {
            1409: {'1': D('5519.88'), '14': D('550'), '15': D('550'), '4': D('272.36'), '32': D('710')},
            1476: {'1': D('5519.88'), '14': D('550'), '15': D('550'), '4': D('90.79'), '32': D('710')},
        }
        for linea in NominaLinea.objects.filter(pk__in=[1409, 1476]).select_related('empleado').prefetch_related('conceptos'):
            percepciones = [c for c in linea.conceptos.all() if c.tipo == 'PERCEPCION']
            conceptos = {c.codigo_concepto: c.importe for c in percepciones}
            if len(percepciones) != 5 or conceptos != conceptos_esperados[linea.pk]:
                raise CommandError('Cambió la composición de los cinco conceptos RRHH conciliados.')
            despensa = sum((c.importe for c in percepciones if c.codigo_concepto == '32'), D(0))
            if (sum((c.importe for c in percepciones), D(0)) - despensa != linea.total_percepciones
                    or despensa != D('710.00') or destino_personal(linea) != 'administracion'
                    or linea.periodo_id not in [22, 24]):
                raise CommandError('Los conceptos RRHH no concilian; no duplicar despensa ni premios.')
        reglas = list(R.objects.select_for_update().filter(pk__in=PUENTES).select_related('rubro__area'))
        if len(reglas) != 3 or any(not r.activa for r in reglas):
            raise CommandError('Faltan reglas activas de los tres puentes aprobados.')
        servicio = PresupuestoRealConsolidacionService()
        indices = {}
        for regla in reglas:
            if regla.rubro_id != next(l.rubro_id for l in lineas if l.pk == PUENTES[regla.pk]):
                raise CommandError('Cambió el destino de una regla aprobada.')
            regla.filtros = {**regla.filtros, 'cobertura_mensual_documentada': '2026-10'}
            # Cambiar el mecanismo no debe reiniciar la vigencia implícita de la regla.
            regla.filtros.setdefault('desde', regla.actualizado_en.date().replace(day=1).isoformat())
            monto, completo = servicio._monto_regla(regla, PERIODO, indices)
            if not completo or monto != LINEAS[PUENTES[regla.pk]]:
                raise CommandError('El puente ya no tiene el importe y cobertura aprobados.')
        propuestas = [R(rubro_id=738, tipo_fuente='NOMINA', filtros={
            'departamento': 'RRHH', 'campo_monto': 'total_percepciones', 'desde': '2026-10', 'hasta': '2026-10'}),
            R(rubro_id=742, tipo_fuente='NOMINA_CONCEPTO', filtros={
                'departamentos': ['RRHH'], 'tipo_concepto': 'PERCEPCION', 'codigos_concepto': ['32'],
                'desde': '2026-10', 'hasta': '2026-10'})]
        for regla, esperado in zip(propuestas, [D('13602.91'), D('1420.00')]):
            monto, completo = servicio._monto_regla(regla, PERIODO, indices)
            if not completo or monto != esperado:
                raise CommandError('Los componentes RRHH ya no coinciden con la conciliación.')
        salida = dict(estado='APLICADO' if aplicar else 'PREVIEW', delta_gasto='17084.67',
            lineas={str(k): str(v) for k, v in LINEAS.items()},
            central='617164.33', empresa_parcial='847540.20')
        if not aplicar:
            self.stdout.write(json.dumps(salida))
            return
        backup = dict(lineas=list(L.objects.filter(pk__in=LINEAS).order_by('pk').values()),
            reglas=list(R.objects.filter(pk__in=PUENTES).order_by('pk').values()),
            snapshot={f.name: getattr(snapshot, f.name) for f in snapshot._meta.fields},
            presupuesto={f.name: getattr(presupuesto, f.name) for f in presupuesto._meta.fields},
            otras_lineas_sha256=intactas, nomina_sha256=nomina_hash)
        ruta = ruta_respaldo(options['backup_file'])
        backup['fuentes_sha256'] = source_hash
        backup_sha = escribir_respaldo(ruta, backup)
        for regla in reglas:
            regla.full_clean()
            regla.save(update_fields=['filtros', 'actualizado_en'])
        for regla in propuestas:
            regla.notas = MARCA
            regla.full_clean()
            regla.save()
        summary = servicio.consolidar(periodo=PERIODO, rubro_ids=[l.rubro_id for l in lineas])
        if summary.errores or summary.conflictos_concurrencia or summary.actualizadas != 5:
            raise CommandError(f'Consolidación no íntegra: {summary.as_dict()}')
        for linea in L.objects.filter(pk__in=LINEAS):
            if linea.monto_real != LINEAS[linea.pk]:
                raise CommandError('El resultado no coincide con los cinco importes autorizados.')
        nuevo, lectura, cantidad = totales_actuales()
        if nuevo['central'] - actual['central'] != D('17084.67') or nuevo['empresa'] != D('847540.20'):
            raise CommandError('El puente financiero no coincide; revertir transacción completa.')
        if intactas != huella(protegidas.all()) or nomina_hash != huella(NominaLinea.objects.filter(pk__in=[1409, 1476]).order_by('pk').values()):
            raise CommandError('Se detectó una modificación ajena a los cinco destinos.')
        actualizar_snapshot(snapshot, nuevo, lectura, cantidad)
        actualizar_resumen_consumidor(presupuesto, snapshot)
        despues = estado_destinos([r.pk for r in propuestas])
        despues_sha = huella([despues])
        escribir_respaldo(ruta.with_suffix('.after.json'), dict(backup_sha256=backup_sha,
            fuentes_sha256=source_hash, reglas_creadas=[r.pk for r in propuestas], estado=despues))
        AuditLog.objects.create(action='CONCILIAR', model='reportes.EmpresaResultadoMensual', object_id='10',
            payload={MARCA: True, 'resultado': salida, 'backup_file': str(ruta),
                'otras_lineas_sha256': intactas, 'nomina_sha256': nomina_hash,
                'fuentes_sha256': source_hash, 'destinos_despues_sha256': despues_sha,
                'backup_sha256': backup_sha,
                'reglas_creadas': [r.pk for r in propuestas]})
        self.stdout.write(json.dumps(salida))
        transaction.on_commit(lambda: bump_cache_scopes('dashboard'))

    def restaurar(self, ruta, source_hash, aplicar):
        backup = json.loads(ruta.read_text())
        after = json.loads(ruta.with_suffix('.after.json').read_text())
        audit = AuditLog.objects.filter(action='CONCILIAR', model='reportes.EmpresaResultadoMensual',
            object_id='10', payload__backup_file=str(ruta),
            payload__ajustes_verificados_octubre_2026=True).order_by('-pk').first()
        if (audit is None or audit.payload.get('backup_sha256') != after['backup_sha256']
                or audit.payload.get('reglas_creadas') != after['reglas_creadas']
                or audit.payload.get('destinos_despues_sha256') != huella([after['estado']])):
            raise CommandError('El respaldo y recibo no coinciden con la auditoría de aplicación.')
        if hashlib.sha256(ruta.read_bytes()).hexdigest() != after['backup_sha256']:
            raise CommandError('El archivo de respaldo cambió después de la aplicación.')
        nuevas = after['reglas_creadas']
        if (len(nuevas) != 2 or len(set(nuevas)) != 2 or set(nuevas) & set(PUENTES)
                or {r['id'] for r in backup['lineas']} != set(LINEAS)
                or {r['id'] for r in backup['reglas']} != set(PUENTES)
                or backup['snapshot']['id'] != 10 or backup['snapshot']['periodo'] != str(PERIODO)
                or backup['presupuesto']['id'] != 10 or backup['presupuesto']['period'] != str(PERIODO)
                or backup['presupuesto']['fuente_nombre'] != BUDGET_VS_ACTUAL_SOURCE):
            raise CommandError('El respaldo contiene destinos fuera de la aplicación autorizada.')
        if source_hash != after['fuentes_sha256'] or source_hash != backup['fuentes_sha256']:
            raise CommandError('Las fuentes cambiaron; no restaurar una conciliación obsoleta.')
        list(R.objects.filter(pk__in=nuevas).order_by('pk').select_for_update())
        observado = json.loads(json.dumps(estado_destinos(nuevas), default=str))
        if observado != after['estado']:
            raise CommandError('Hay modificaciones posteriores; no sobrescribirlas al restaurar.')
        if not aplicar:
            self.stdout.write('RESTAURACION_VERIFICADA_SIN_APLICAR')
            return
        for modelo, key in [(L, 'lineas'), (R, 'reglas')]:
            for row in backup[key]:
                valores = {k: v for k, v in row.items() if k != 'id'}
                modelo.objects.filter(pk=row['id']).update(**valores)
        EmpresaResultadoMensual.objects.filter(pk=10).update(**{
            k: v for k, v in backup['snapshot'].items() if k != 'id'})
        PresupuestoResumenMensual.objects.filter(pk=10).update(**{
            k: v for k, v in backup['presupuesto'].items() if k != 'id'})
        count, detalle = R.objects.filter(pk__in=nuevas).delete()
        if count != 2 or detalle != {'reportes.ReglaFuenteRubro': 2}:
            raise CommandError('La reversión afectaría más que las dos reglas nuevas.')
        recuperado = json.loads(json.dumps(estado_destinos(), default=str))
        esperado = {k: backup[k] for k in ['lineas', 'reglas', 'snapshot', 'presupuesto']}
        if recuperado != esperado:
            raise CommandError('La restitución completa no coincide con el respaldo.')
        AuditLog.objects.create(action='RESTORE', model='reportes.EmpresaResultadoMensual', object_id='10',
            payload={MARCA: True, 'backup_file': str(ruta), 'reglas_retiradas': nuevas})
        self.stdout.write('RESTAURADO_Y_VERIFICADO')
        transaction.on_commit(lambda: bump_cache_scopes('dashboard'))
