"""Control pagado de una cédula; el gasto excluye retenciones y cuentas por cobrar."""
from datetime import date
from decimal import Decimal, InvalidOperation
import re


def leer_pago_conciliado(expediente):
    data = (expediente.metadata or {}).get('pago_conciliado')
    if data is None:
        return None
    try:
        if data['version'] != 1 or expediente.estado != 'APLICADO':
            raise ValueError('El control no está aplicado.')
        amounts = {key: Decimal(data[key]) for key in (
            'importe_pagado', 'gasto_empresa', 'retenciones', 'cuentas_por_cobrar')}
        if any(not value.is_finite() or value < 0 or value != value.quantize(Decimal('.01'))
               for value in amounts.values()):
            raise ValueError('Importes inválidos.')
        if amounts['importe_pagado'] != sum(amounts[key] for key in (
                'gasto_empresa', 'retenciones', 'cuentas_por_cobrar')):
            raise ValueError('El pago no concilia con gasto, retenciones y cuentas por cobrar.')
        if amounts['gasto_empresa'] < expediente.total_patronal:
            raise ValueError('El gasto es menor que el control patronal.')
        fecha = date.fromisoformat(data['fecha_pago'])
        if fecha < expediente.periodo:
            raise ValueError('El pago precede a la obligación.')
        for key in ('soporte_pago_sha256', 'soporte_conciliacion_sha256', 'sua_sha256'):
            if not re.fullmatch('[0-9a-f]{64}', data[key]):
                raise ValueError('Falta la huella del soporte.')
        docs = [d for d in expediente.documentos.all() if d.clase in ('SUA_XLS', 'SUA_PDF')]
        if len(docs) != 1 or docs[0].sha256 != data['sua_sha256']:
            raise ValueError('El control no corresponde al SUA único.')
        return {**amounts, 'fecha_pago': fecha, 'soporte': data['soporte_pago_sha256']}
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f'Pago de cédula #{expediente.pk} sin conciliación válida: {exc}') from exc
