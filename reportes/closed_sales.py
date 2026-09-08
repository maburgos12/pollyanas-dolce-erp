"""Closed sales periods shared by the daily card and its monthly comparison."""
from calendar import monthrange
from datetime import date, timedelta
from decimal import Decimal
from itertools import groupby

from django.db.models import F
from django.utils import timezone

from pos_bridge.models import PointDailyBranchIndicator, PointSyncJob
from ventas.services.sales_read_service import get_daily_sales_bulk

CLOSED_SALES_VERSION = 'closed-sales-v1'
MONTHS = ('', 'ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')


def required_sales_branches(day):
    # Reuse the report's existing operational scope, including its exclusions.
    # Lazy import avoids the views -> panels -> closed_sales import cycle.
    from reportes.views import _required_daily_sales_branches
    return _required_daily_sales_branches(day)


def closed_indicator_evidence():
    return PointDailyBranchIndicator.objects.filter(
        sync_job__job_type=PointSyncJob.JOB_TYPE_SALES,
        sync_job__status=PointSyncJob.STATUS_SUCCESS,
        sync_job__finished_at__isnull=False,
        sync_job__started_at__date__gt=F('indicator_date'),
        updated_at__date__gt=F('indicator_date'),
    ).exclude(branch__erp_branch_id__isnull=True)


def latest_closed_sales_date(*, today: date | None = None) -> date | None:
    today = today or timezone.localdate()
    evidence = (closed_indicator_evidence().filter(indicator_date__lt=today)
      .order_by('-indicator_date')
      .values_list('indicator_date', 'branch__erp_branch_id'))
    for day, rows in groupby(evidence.iterator(), key=lambda row: row[0]):
        present = {branch_id for _, branch_id in rows}
        expected = {row['branch_id'] for row in required_sales_branches(day)}
        if expected and expected <= present:
            return day
    return None


def closed_month_comparison(*, cutoff: date) -> dict:
    """Compare the same dates and branch IDs; absent evidence is never a zero."""
    start = cutoff.replace(day=1)
    prev_start = start.replace(year=start.year - 1)
    # Leap years also require equal day counts, not 29 days versus 28.
    last_day = min(cutoff.day, monthrange(prev_start.year, prev_start.month)[1])
    end, prev_end = start.replace(day=last_day), prev_start.replace(day=last_day)
    branches = required_sales_branches(cutoff)
    expected = {row['branch_id'] for row in branches}
    names = {row['branch_id']: row['branch_name'] for row in branches}
    current_days = [start + timedelta(days=i) for i in range(last_day)]
    previous_days = [prev_start + timedelta(days=i) for i in range(last_day)]
    bulk = get_daily_sales_bulk(fechas=current_days + previous_days, sucursales=sorted(expected),
                               dimension='branch', include_indicators=True)
    confirmed_zeros = set(closed_indicator_evidence().filter(
        indicator_date__in=current_days + previous_days,
        branch__erp_branch_id__in=expected, total_amount=0,
    ).values_list('indicator_date', 'branch__erp_branch_id'))

    def totals(days):
        amount = quantity = Decimal('0')
        missing = []
        for day in days:
            payload = bulk['dates'].get(day.isoformat(), {})
            rows = {row['branch_id']: row for row in payload.get('rows', [])}
            for branch_id in expected:
                row = rows.get(branch_id)
                if row is not None and payload.get('coverage_accepted'):
                    amount += Decimal(str(row.get('amount') or 0))
                    quantity += Decimal(str(row.get('units') or 0))
                elif (day, branch_id) in confirmed_zeros:
                    # Only a successful post-close capture can confirm a zero.
                    continue
                else:
                    missing.append(f"{names[branch_id]} ({day.isoformat()})")
        return amount, quantity, missing

    amount, quantity, missing = totals(current_days)
    prev_amount, prev_quantity, prev_missing = totals(previous_days)
    complete = bool(expected) and not missing
    previous_complete = bool(expected) and not prev_missing
    delta = amount - prev_amount if complete and previous_complete else None
    pct = delta / prev_amount * 100 if delta is not None and prev_amount != 0 else None
    gaps = prev_missing + missing
    note = (f'1–{last_day} {MONTHS[start.month]} {start.year} vs '
            f'1–{last_day} {MONTHS[prev_start.month]} {prev_start.year}')
    coverage_note = (f'Faltan datos de {", ".join(gaps[:3])}' + (f' y {len(gaps)-3} sucursal/día más.' if len(gaps) > 3 else '.')) if gaps else ''
    if not expected:
        coverage_note = 'Sin sucursales acreditadas para comparar.'
    elif pct is None and not gaps:
        coverage_note = 'El año previo tiene venta cero; no se puede calcular una variación porcentual.'
    return {
        'month_label': start.strftime('%Y-%m'), 'year': start.year,
        'period_start': start, 'period_end': end,
        'prev_period_start': prev_start, 'prev_period_end': prev_end,
        'is_partial': last_day != monthrange(start.year, start.month)[1],
        'amount': amount if complete else None,
        'quantity': quantity if complete else None,
        'prev_amount': prev_amount if previous_complete else None,
        'prev_quantity': prev_quantity if previous_complete else None,
        'amount_delta': delta, 'amount_delta_pct': pct,
        'qty_delta': quantity - prev_quantity if complete and previous_complete else None,
        'qty_delta_pct': ((quantity - prev_quantity) / prev_quantity * 100)
                         if complete and previous_complete and prev_quantity else None,
        'comparison_tone': 'danger' if pct is not None and pct < 0 else 'success' if pct is not None else 'warning',
        'comparison_label': f'{pct:+.1f}%' if pct is not None else 'Comparativo pendiente',
        'comparison_note': note, 'coverage_note': coverage_note,
        'branch_ids': sorted(expected),
    }
