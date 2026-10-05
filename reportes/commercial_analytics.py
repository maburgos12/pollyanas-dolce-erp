"""Read-only revenue analytics. Historical branch identity is never remapped.

Revenue = persisted venta_total after discounts, including IVA and credit sales.
Never sum alternative sources or filter by today's branch activation status.
"""
from calendar import monthrange
from collections import defaultdict
from datetime import date
from decimal import Decimal

from django.conf import settings
from django.db.models import Count, Max, Min, Sum
from django.db.models.functions import TruncMonth
from django.utils import timezone

from core.cache_versions import get_or_set_versioned_cache
from reportes.dashboard_sales_dataset import MONTH_NAMES
from pos_bridge.models import PointMonthlySalesOfficial
from reportes.sales_confidence import selected_sales_facts

ZERO = Decimal('0')


def _percent(current, previous):
    return (current - previous) / previous * 100 if previous is not None and previous > 0 and current is not None else None


def _join_periods(previous, current, key):
    old, new = {r[key]: r for r in previous}, {r[key]: r for r in current}
    rows = []
    for identity in old.keys() | new.keys():
        a, b = old.get(identity), new.get(identity)
        row = dict(b or a)
        before, after = a['amount'] if a else None, b['amount'] if b else None
        row.update(previous=before, current=after, delta=(after or ZERO)-(before or ZERO) if old and new else None, growth=_percent(after, before))
        row['previous_quantity'] = a['quantity'] if a and 'quantity' in a else None
        row['current_quantity'] = b['quantity'] if b and 'quantity' in b else None
        row['previous_price'] = before / a['quantity'] if a and a.get('quantity', ZERO) > 0 else None
        row['current_price'] = after / b['quantity'] if b and b.get('quantity', ZERO) > 0 else None
        row['presence'] = 'Ambos periodos' if a and b else 'Solo periodo anterior' if a else 'Solo periodo actual'
        rows.append(row)
    return sorted(rows, key=lambda r: (-(r['current'] or ZERO), -(r['previous'] or ZERO), str(r[key])))


def decompose_products(rows):
    """Sequential exact bridge by SKU: q1*(p1-p0), p0*(q1-q0).

    No summed mixed-unit basket. Volume dollars are calculated within each SKU.
    No inference of list-price change or causal mix effect from a residual.
    Unmatched products/returns stay visible as a separate exact contribution.
    """
    price = volume = unmatched = ZERO
    matched = 0
    for r in rows:
        if r['previous_price'] is not None and r['current_price'] is not None:
            matched += 1
            price += r['current_quantity'] * (r['current_price']-r['previous_price'])
            volume += r['previous_price'] * (r['current_quantity']-r['previous_quantity'])
        else:
            unmatched += r['delta']
    effects = [dict(label='Precio medio realizado', amount=price), dict(label='Cantidad por producto', amount=volume)] if matched else []
    return effects + [dict(label='Productos sin comparación / ajustes', amount=unmatched)]


def _product_groups(facts):
    # Reuse persisted recipe links, but keep presentation/category distinct.
    # Unmapped keys stay source-specific; matching by name is forbidden.
    grouped = defaultdict(lambda: dict(amount=ZERO, quantity=ZERO))
    branch_grouped = defaultdict(lambda: dict(amount=ZERO, quantity=ZERO))
    for r in facts.values('sucursal_id', 'receta_id', 'source_kind', 'producto_clave', 'producto_nombre', 'categoria').annotate(
        amount=Sum('venta_total'), quantity=Sum('cantidad')):
        identity = ('recipe', r['receta_id'], r['categoria']) if r['receta_id'] else (r['source_kind'], r['producto_clave'], r['categoria'])
        item = grouped[identity]
        item.update(key=identity, name=r['producto_nombre'] or r['producto_clave'], category=r['categoria'] or 'Sin categoría')
        item['amount'] += r['amount'] or ZERO
        item['quantity'] += r['quantity'] or ZERO
        branch_item = branch_grouped[(r['sucursal_id'], identity)]
        branch_item.update(key=(r['sucursal_id'], identity), branch_id=r['sucursal_id'])
        branch_item['amount'] += r['amount'] or ZERO
        branch_item['quantity'] += r['quantity'] or ZERO
    return list(grouped.values()), list(branch_grouped.values())


def _build_panel(year, month, branch_id):
    today = timezone.localdate()
    previous_year = year-1
    annual = selected_sales_facts(start_date=date(previous_year, 1, 1), end_date=min(date(year, 12, 31), today))
    branch_options = list(annual.exclude(sucursal_id=None).values('sucursal_id', 'sucursal__nombre').distinct().order_by('sucursal__nombre'))
    if branch_id:
        annual = annual.filter(sucursal_id=branch_id)
    monthly_totals = list(annual.annotate(period=TruncMonth('fecha')).values('period').annotate(amount=Sum('venta_total'), latest=Max('fecha')))
    monthly_map = {r['period']: r['amount'] for r in monthly_totals}
    latest_current = max((r['latest'] for r in monthly_totals if r['period'].year == year), default=None)
    controls = dict(PointMonthlySalesOfficial.objects.filter(month_start__year=year).values_list('month_start', 'total_amount')) if not branch_id else {}
    monthly_rows = []
    for m in range(1, 13):
        before = monthly_map.get(date(previous_year, m, 1))
        after = monthly_map.get(date(year, m, 1))
        partial = year == today.year and m == today.month
        monthly_rows.append(dict(month=m, label=MONTH_NAMES[m] + (' · en curso' if partial else ''), previous=before, current=after,
            delta=after-before if before is not None and after is not None and not partial else None,
            growth=_percent(after, before) if not partial else None,
            control=controls.get(date(year,m,1)) if not partial else None,
            control_difference=after-controls[date(year,m,1)] if after is not None and date(year,m,1) in controls and not partial else None))
    end_month = month or 12
    start = date(year, month or 1, 1)
    requested_end = date(year, end_month, monthrange(year, end_month)[1])
    end = min(requested_end, today, latest_current if latest_current and latest_current >= start else today) if year == today.year and start <= today else requested_end
    prev_start = date(previous_year, month or 1, 1)
    # Match elapsed calendar dates for a current partial period; full historical years remain full.
    prev_end = date(previous_year, end.month, min(end.day, monthrange(previous_year, end.month)[1])) if end >= start else date(previous_year, end_month, monthrange(previous_year, end_month)[1])
    current = annual.filter(fecha__range=(start, end))
    previous = annual.filter(fecha__range=(prev_start, prev_end))
    def summary(q):
        return q.aggregate(amount=Sum('venta_total'), rows=Count('id'), updated=Max('actualizado_en'))
    now, before = summary(current), summary(previous)
    def branches(q):
        return list(q.values('sucursal_id', 'sucursal__nombre', 'sucursal__activa').annotate(amount=Sum('venta_total'), days=Count('fecha', distinct=True), first=Min('fecha'), last=Max('fecha')))
    branch_rows = _join_periods(branches(previous), branches(current), 'sucursal_id')
    for r in branch_rows:
        r['name'] = r['sucursal__nombre'] or 'Sin sucursal asignada'
        r['share'] = (r['current'] or ZERO)/now['amount']*100 if now['amount'] else None
    previous_products, previous_branch_products = _product_groups(previous)
    current_products, current_branch_products = _product_groups(current)
    products = _join_periods(previous_products, current_products, 'key')
    def categories(q):
        return list(q.values('categoria').annotate(amount=Sum('venta_total')))
    category_rows = _join_periods(categories(previous), categories(current), 'categoria')
    for r in category_rows:
        r['name'] = r['categoria'] or 'Sin categoría'
        r['share'] = (r['current'] or ZERO)/now['amount']*100 if now['amount'] else None
        r['previous_share'] = (r['previous'] or ZERO)/before['amount']*100 if before['amount'] else None
    components = []
    if now["amount"] is not None and before["amount"] is not None:
        previous_ids = {r['branch_id'] for r in previous_branch_products}
        current_ids = {r['branch_id'] for r in current_branch_products}
        same_branches = previous_ids & current_ids
        bridge_rows = _join_periods(previous_branch_products, current_branch_products, 'key')
        components = decompose_products([r for r in bridge_rows if r['branch_id'] in same_branches])
        components.append(dict(label='Sucursales con venta en un solo periodo',
            amount=sum((r['delta'] for r in bridge_rows if r['branch_id'] not in same_branches), ZERO)))
        for component in components:
            component['amount'] = component['amount'].quantize(Decimal('0.01'))
        rounding = now['amount']-before['amount']-sum((r['amount'] for r in components), ZERO)
        if rounding:
            components.append(dict(label='Redondeo', amount=rounding))
    annual_current = [r['current'] for r in monthly_rows if r['current'] is not None]
    annual_previous = [r['previous'] for r in monthly_rows if r['previous'] is not None]
    return dict(year=year, previous_year=previous_year, month=month, branch_id=branch_id,
        start=start, end=end, previous_start=prev_start, previous_end=prev_end,
        period_label=f'{MONTH_NAMES[month]} {year}' if month else f'Año {year}',
        current=now['amount'], previous=before['amount'], growth=_percent(now['amount'], before['amount']),
        delta=now['amount']-before['amount'] if now['amount'] is not None and before['amount'] is not None else None,
        annual_current=sum(annual_current, ZERO) if annual_current else None,
        annual_previous=sum(annual_previous, ZERO) if annual_previous else None,
        annual_current_months=len(annual_current), annual_previous_months=len(annual_previous),
        annual_reference_year=previous_year if year > 2025 else year,
        annual_reference=sum(annual_previous if year > 2025 else annual_current, ZERO) if (annual_previous if year > 2025 else annual_current) else None,
        annual_reference_months=len(annual_previous if year > 2025 else annual_current),
        branches=branch_rows, branch_options=branch_options, categories=category_rows, products=products,
        monthly=monthly_rows, components=components, updated=now['updated'], has_controls=bool(controls),
        rows=now['rows'], branch_count=len(branch_rows), category_count=len(category_rows),
        months=[dict(value=m, label=MONTH_NAMES[m]) for m in range(1,13)])


def commercial_panel_from_request(request):
    today = timezone.localdate()
    default = today.replace(day=1)
    default = date(default.year-1, 12, 1) if default.month == 1 else default.replace(month=default.month-1)
    def integer(key, fallback):
        try:
            return int(request.GET.get(key, fallback))
        except (ValueError, TypeError):
            return fallback
    year = integer('sales_year', default.year)
    if not 2025 <= year <= today.year:
        year = default.year
    month = integer('sales_month', default.month)
    if month not in range(13):
        month = default.month
    branch_id = integer('sales_branch', 0) or None
    builder = lambda: _build_panel(year, month, branch_id)
    if getattr(settings, 'RUNNING_TESTS', False):
        panel = builder()
    else:
        panel = get_or_set_versioned_cache(key_parts=('commercial-iva-v1', year, month, branch_id, today.isoformat()),
            scopes=('ventas', 'dashboard'), builder=builder, timeout=300)
    panel = dict(panel)
    panel['years'] = list(range(today.year, 2024, -1))
    panel['scope_label'] = next((b['sucursal__nombre'] for b in panel['branch_options'] if b['sucursal_id'] == branch_id), 'Sucursal seleccionada') if branch_id else 'Toda la red histórica'
    return panel
