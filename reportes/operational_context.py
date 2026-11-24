"""Read-only documentary period; it never changes dashboard date filters."""
from calendar import monthrange
from datetime import date
import re
from django.utils import timezone
from ventas.services.operational_context import contexts_for_range


def sales_context_from_request(request, *, visible_date=None):
    visible_date = visible_date or timezone.localdate()
    selected = request.GET.get('contexto_mes', visible_date.strftime('%Y-%m'))
    error = ''
    try:
        if not re.fullmatch(r'\d{4}-\d{2}', selected):
            raise ValueError
        year, month = map(int, selected.split('-'))
        start = date(year, month, 1)
    except (ValueError, TypeError):
        error = 'Período de la bitácora inválido. Use AAAA-MM; se muestra el mes del corte visible.'
        start = visible_date.replace(day=1)
    end = start.replace(day=monthrange(start.year, start.month)[1])
    return dict(operational_contexts=contexts_for_range(start_date=start, end_date=end),
                operational_context_period=start.strftime('%Y-%m'), operational_context_error=error,
                operational_context_filter=True,
                operational_context_params=[(key, value) for key, values in request.GET.lists() if key != 'contexto_mes' for value in values])
