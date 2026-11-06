"""Conciliaciones autorizadas: pago neto por fecha bancaria, fiscal sin modificar."""
from datetime import date
from decimal import Decimal, InvalidOperation

from core.models import AuditLog
from rrhh.models import NominaImportacion, NominaLinea

CLAVE = 'conciliaciones_pagos_reales'
ZERO = Decimal('0.00')


def ajustes_pagos_reales(start, end):
    """Devuelve únicamente deltas financieros; nunca otra nómina ni otro pago."""
    controles = []
    for imp in NominaImportacion.objects.filter(estatus='IMPORTADA', resumen__has_key=CLAVE):
        items = imp.resumen[CLAVE]
        if not isinstance(items, list):
            raise ValueError(f'Importación {imp.pk}: conciliaciones de pago inválidas.')
        controles.extend((imp, item) for item in items)
    result, seen = [], set()
    for imp, item in controles:
        try:
            if item['estado'] != 'APLICADO' or item['criterio'] != 'PAGO_REAL_CONFIRMADO_DG':
                raise ValueError('Conciliación sin autorización aplicada')
            line = NominaLinea.objects.select_related('periodo', 'empleado', 'sucursal_snapshot').get(
                pk=item['linea_id'], periodo_id=imp.periodo_id)
            if line.pk in seen:
                raise ValueError('Una línea tiene más de una conciliación de pago')
            seen.add(line.pk)
            audit = AuditLog.objects.get(pk=item['soporte_audit_id'], action='DOCUMENT',
                model='rrhh.NominaLinea', object_id=str(line.pk))
            source = audit.payload
            amount = Decimal(source['importe_bancario'])
            paid = date.fromisoformat(source['fecha_bancaria'])
            fiscal = line.periodo.fecha_fin.replace(day=1)
            if (not amount.is_finite() or amount <= 0 or amount != amount.quantize(Decimal('.01'))
                    or line.periodo.estatus not in ('CERRADA', 'PAGADA')
                    or source['empleado_id'] != line.empleado_id
                    or source['codigo'] != line.empleado.codigo
                    or not source.get('referencia_bancaria') or not source.get('fuentes')
                    or not item.get('autorizacion')
                    or Decimal(item['bruto_fiscal']) != line.total_percepciones
                    or Decimal(item['neto_fiscal']) != line.neto_calculado):
                raise ValueError('La conciliación no corresponde a sus fuentes vigentes')
            deltas = {}
            deltas[fiscal] = -line.total_percepciones
            month = paid.replace(day=1)
            deltas[month] = deltas.get(month, ZERO) + amount
            for period, delta in deltas.items():
                if start <= period < end:
                    result.append(dict(linea=line, mes=period, ajuste=delta,
                        pago_neto=amount, fecha_pago=paid, mes_fiscal=fiscal,
                        bruto_fiscal=line.total_percepciones, neto_fiscal=line.neto_calculado,
                        diferencia_neta=line.neto_calculado - amount,
                        soporte_audit_id=audit.pk, importacion_id=imp.pk))
        except (KeyError, TypeError, ValueError, InvalidOperation,
                NominaLinea.DoesNotExist, AuditLog.DoesNotExist) as exc:
            raise ValueError(f'Importación {imp.pk}: pago real sin conciliación válida ({exc}).') from exc
    return result


def detalle_pago_real(ajuste):
    return {key: value for key, value in ajuste.items() if key != 'linea'} | {
        'linea_id': ajuste['linea'].pk, 'codigo': ajuste['linea'].empleado.codigo}
