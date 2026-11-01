"""Reparto gerencial autorizado enero–agosto 2026; lectura, sin mutar cierres."""
from collections import defaultdict
from datetime import date
from decimal import Decimal

from django.db.models import Sum, Case, When, F, Value, IntegerField

from core.models import Sucursal
from pos_bridge.models import PointDailySale
from .services_costeo_logistica import repartir_centavos

ZERO = Decimal('0')
FONDOS = ('costos', 'produccion', 'administracion', 'logistica')
MESES = ('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto')


def ventas_con_identidad_historica(desde, hasta):
    """La relación actual de Point 2 no cambia las ventas de Crucero en 2026."""
    crucero = Sucursal.objects.filter(codigo='CRUCERO').values_list('pk', flat=True).first()
    return PointDailySale.objects.filter(sale_date__range=(desde, hasta)).annotate(
        sucursal_historica_id=Case(
            When(branch__external_id='2', sale_date__range=(date(2026, 1, 1), date(2026, 6, 30)),
                 then=Value(crucero)),
            default=F('branch__erp_branch_id'), output_field=IntegerField(),
        )
    )


def etapa_sucursal(codigo, mes):
    if codigo == 'BAMOA':
        if mes < 7:
            return 'Registro anterior a apertura'
        if mes == 7:
            return 'Apertura parcial desde 14/07'
    if codigo == 'GUAMUCHIL':
        if mes < 3:
            return 'Preapertura'
        if mes == 3:
            return 'Apertura parcial desde 24/03'
    if codigo == 'CRUCERO':
        return 'Últimas ventas observadas 21/06' if mes == 6 else 'Operación histórica'
    return 'Operación'


def reparto_historico(buckets, lineas):
    """Usa los mismos fondos y filtro de fuente que Estado de resultados."""
    sucursales = {s.pk: s for s in Sucursal.objects.all()}
    ventas = defaultdict(lambda: defaultdict(lambda: ZERO))
    gastos = defaultdict(lambda: defaultdict(lambda: ZERO))
    errores = []
    dias = (ventas_con_identidad_historica(date(2026, 1, 1), date(2026, 8, 31))
            .order_by().values('sale_date', 'sucursal_historica_id')
            .annotate(bruto=Sum('gross_amount'), descuento=Sum('discount_amount')))
    for dia in dias:
        fecha = dia['sale_date']
        sid = dia['sucursal_historica_id']
        sucursal = sucursales.get(sid)
        if sucursal is None:
            errores.append('Hay ventas sin sucursal histórica identificada.')
            continue
        if ((sucursal.codigo == 'BAMOA' and fecha < date(2026, 7, 14))
                or (sucursal.codigo == 'CRUCERO' and fecha > date(2026, 6, 30))):
            errores.append('Hay ventas fuera del período de Crucero o Bamoa; revisar identidad histórica.')
            continue
        ventas[fecha.month][sid] += dia['bruto'] - dia['descuento']
    for linea in lineas:
        if (linea.periodo.month <= 8 and linea.rubro.area.codigo == 'gastos-venta'
                and str(linea.fuente_real or '').strip()):
            monto = linea.monto_real or ZERO
            sid = linea.rubro.sucursal_id
            if monto and sid not in sucursales:
                errores.append('Hay gastos de venta sin sucursal identificada.')
                continue
            gastos[linea.periodo.month][sid] += monto
    detalle, totales = [], []
    for mes, nombre in enumerate(MESES, 1):
        claves = ('ingresos', 'gastos-venta', *FONDOS)
        if not all(buckets[k][mes]['con_real'] for k in claves):
            errores.append(f'{nombre}: falta consolidar alguno de los fondos.')
            continue
        bases = ventas[mes]
        if sum(bases.values(), ZERO) != buckets['ingresos'][mes]['real']:
            errores.append(f'{nombre}: las ventas por sucursal no concilian con Ingresos.')
            continue
        if sum(gastos[mes].values(), ZERO) != buckets['gastos-venta'][mes]['real']:
            errores.append(f'{nombre}: los gastos directos no concilian con Gastos de venta.')
            continue
        if not bases or sum(bases.values(), ZERO) <= 0 or any(v < 0 for v in bases.values()):
            errores.append(f'{nombre}: no hay una base positiva de ventas para repartir.')
            continue
        try:
            cuotas = {k: repartir_centavos(buckets[k][mes]['real'], bases) for k in FONDOS}
        except ValueError:
            errores.append(f'{nombre}: un fondo no admite reparto; revisar su importe.')
            continue
        resultado_mes = ZERO
        for sid in sorted(set(bases) | {i for i, v in gastos[mes].items() if v}):
            if sid not in sucursales:
                continue
            s = sucursales[sid]
            fila = {'mes': mes, 'mes_nombre': nombre, 'sucursal_id': sid, 'nombre': s.nombre,
                    'etapa': etapa_sucursal(s.codigo, mes), 'ventas': bases.get(sid, ZERO),
                    'gasto_directo': gastos[mes].get(sid, ZERO),
                    **{k: cuotas[k].get(sid, ZERO) for k in FONDOS}}
            fila['utilidad'] = fila['ventas'] - fila['gasto_directo'] - sum((fila[k] for k in FONDOS), ZERO)
            resultado_mes += fila['utilidad']
            detalle.append(fila)
        esperado = buckets['ingresos'][mes]['real'] - sum((buckets[k][mes]['real'] for k in ('gastos-venta', *FONDOS)), ZERO)
        if resultado_mes != esperado or any(sum(cuotas[k].values(), ZERO) != buckets[k][mes]['real'] for k in FONDOS):
            errores.append(f'{nombre}: el reparto no conserva el total de la empresa.')
        totales.append(resultado_mes)
    # No publicar un acumulado aparentemente completo cuando falta un mes.
    if errores:
        return {'errores': list(dict.fromkeys(errores)), 'filas': [], 'detalle': [], 'meses': MESES}
    resumen = {}
    for fila in detalle:
        sid = fila['sucursal_id']
        r = resumen.setdefault(sid, {'nombre': fila['nombre'], 'meses': [None] * 8,
                                    'utilidad': ZERO, 'ventas': ZERO, 'previo': ZERO})
        r['meses'][fila['mes'] - 1] = {'importe': fila['utilidad'], 'etapa': fila['etapa']}
        r['utilidad'] += fila['utilidad']
        r['ventas'] += fila['ventas']
        if fila['etapa'] in ('Registro anterior a apertura', 'Preapertura'):
            r['previo'] += fila['gasto_directo']
    for r in resumen.values():
        r['margen'] = r['utilidad'] / r['ventas'] * 100 if r['ventas'] else None
    return {'errores': [], 'filas': sorted(resumen.values(), key=lambda r: -r['utilidad']),
            'detalle': detalle, 'meses': MESES, 'totales': totales, 'total': sum(totales, ZERO)}


def exportar_reparto(reparto):
    import csv
    from django.http import HttpResponse

    if not reparto or reparto["errores"]:
        return HttpResponse("No hay un reparto conciliado para exportar.", status=409)
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="reparto-enero-agosto-2026.csv"'
    response.write("\ufeff")
    writer = csv.writer(response)
    writer.writerow(["Mes", "Sucursal", "Etapa", "Ventas", "Gasto directo", "Costos",
                     "Producción", "Administración", "Logística", "Utilidad operativa asignada"])
    for fila in reparto["detalle"]:
        nombre = fila["nombre"]
        if nombre.startswith(("=", "+", "-", "@", "\t", "\r")):
            nombre = "'" + nombre
        writer.writerow([fila["mes_nombre"], nombre, fila["etapa"],
                         *[fila[k] for k in ("ventas", "gasto_directo", "costos", "produccion",
                                            "administracion", "logistica", "utilidad")]])
    return response
