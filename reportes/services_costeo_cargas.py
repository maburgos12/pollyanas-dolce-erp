"""Cargas del costeo gerencial; conserva el periodo fiscal y no modifica expedientes."""
from collections import defaultdict
from datetime import date
from decimal import Decimal

from .models import ExpedienteCedulaIMSS, ExpedienteISN
from .services_rentabilidad_personal import _normalizar_registro_patronal, REGISTRO_PATRONAL_AUTORIZADO

ZERO = Decimal('0')
# Confirmación de Dirección en este análisis; no inferir pagos de otros meses.
PAGOS_CONFIRMADOS = {date(2026, 9, 1): date(2026, 8, 1)}


def clasificar_detalle(detalle, produccion):
    empleado = detalle.empleado
    if empleado is None:
        return 'sin_asignar', None
    if empleado.pk in produccion:
        return ('logistica' if produccion[empleado.pk] == 'Coordinación de despacho' else 'produccion'), None
    if empleado.puesto_operativo == 'REPARTIDOR':
        return 'logistica', None
    # El área y sucursal del documento preservan la distribución histórica.
    if detalle.area_codigo == 'gastos-venta' and detalle.sucursal_id:
        return 'gastos-venta', detalle.sucursal_id
    if detalle.area_codigo in {'produccion', 'administracion', 'logistica'}:
        return detalle.area_codigo, None
    return 'sin_asignar', detalle.sucursal_id


def leer_cargas_pagadas(periodo, personas):
    cobertura = PAGOS_CONFIRMADOS.get(periodo)
    resultado = {'confirmado': cobertura is not None, 'periodo_pago': periodo,
                 'cobertura': cobertura, 'filas': [], 'fuentes': [], 'pendientes': [], 'total': ZERO}
    if cobertura is None:
        return resultado
    produccion = {p['id']: p['componente'] for p in personas}
    expedientes = list(ExpedienteCedulaIMSS.objects.filter(periodo=cobertura, estado='APLICADO')
                       .prefetch_related('documentos__detalles__empleado', 'documentos__detalles__sucursal'))
    expedientes = [e for e in expedientes if _normalizar_registro_patronal(e.registro_patronal) == REGISTRO_PATRONAL_AUTORIZADO]
    fiscales = []
    for tipo in ('MENSUAL', 'BIMESTRAL'):
        candidatos = [e for e in expedientes if e.tipo == tipo]
        if len(candidatos) != 1:
            resultado['pendientes'].append(f'{tipo}: falta un expediente aplicado único del periodo confirmado.')
            continue
        e = candidatos[0]
        documentos = [d for d in e.documentos.all() if d.clase == 'SUA_XLS']
        if len(documentos) != 1:
            resultado['pendientes'].append(f'{tipo}: falta documento SUA único.')
            continue
        documento = documentos[0]
        detalles = list(documento.detalles.all())
        fiscales.append((e, 'IMSS' if tipo == 'MENSUAL' else 'RCV e Infonavit',
                         e.total_patronal, detalles, 'cuota_patronal', documento.archivo.name))
    isn = list(ExpedienteISN.objects.filter(periodo=cobertura, estado='APLICADO')
               .select_related('cfdi').prefetch_related('distribuciones__empleado', 'distribuciones__sucursal'))
    if len(isn) == 1 and (isn[0].cfdi.estatus or '').upper() == 'VIGENTE':
        e = isn[0]
        fiscales.append((e, 'ISN', e.importe_pagado, list(e.distribuciones.all()), 'monto_isn', e.uuid))
    else:
        resultado['pendientes'].append('ISN: falta un expediente aplicado único del periodo confirmado.')
    for expediente, concepto, total, detalles, campo, soporte in fiscales:
        # cuota_patronal ya incluye los componentes de RCV; sumar sólo esa columna.
        suma = sum((getattr(d, campo) for d in detalles), ZERO)
        if not detalles or suma != total or any(getattr(d, campo) < 0 for d in detalles):
            resultado['pendientes'].append(f'{concepto}: el detalle ({suma}) no concilia con el expediente ({total}).')
            continue
        importes = defaultdict(lambda: ZERO)
        sucursales = {d.sucursal_id: d.sucursal.nombre for d in detalles if d.sucursal_id}
        for detalle in detalles:
            importes[clasificar_detalle(detalle, produccion)] += getattr(detalle, campo)
        origen = 'ISN' if concepto == 'ISN' else 'CEDULA_IMSS'
        resultado['fuentes'].append({'concepto': concepto, 'origen': origen, 'expediente_id': expediente.pk,
            'periodo_pago': periodo, 'cobertura': cobertura, 'monto': total, 'soporte': soporte})
        for (area, sid), monto in sorted(importes.items(), key=lambda item: str(item[0])):
            resultado['filas'].append({'origen': origen, 'registro_id': expediente.pk,
                'clave': f'{origen}:{expediente.pk}:{area}:{sid}', 'area': area, 'sucursal_id': sid,
                'sucursal_nombre': sucursales.get(sid, 'No aplica'),
                'familia': 'cargas_patronales', 'concepto': concepto, 'monto_original': monto,
                'monto_mensual': monto, 'cobertura_inicio': cobertura, 'cobertura_fin': cobertura,
                'periodo_pago': periodo, 'regla_id': None, 'porcentaje': None,
                'estado': 'COMPLETO' if area != 'sin_asignar' else 'PENDIENTE', 'soporte': soporte,
                'detalle': 'Periodo fiscal agosto; pago en septiembre confirmado por Dirección. No es conciliación bancaria.'})
        resultado['total'] += total
    return resultado
