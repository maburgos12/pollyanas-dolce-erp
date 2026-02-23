from decimal import Decimal

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render

from core.access import can_view_reportes
from core.models import Sucursal

from .services import build_discrepancias_report, resolve_period_range


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Discrepancias", "url_name": "control:discrepancias", "active": active == "discrepancias"},
    ]


@login_required
def discrepancias(request):
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para revisar discrepancias.")

    period_raw = (request.GET.get("periodo") or "").strip()
    date_from_raw = (request.GET.get("from") or "").strip()
    date_to_raw = (request.GET.get("to") or "").strip()
    sucursal_id_raw = (request.GET.get("sucursal_id") or "").strip()
    threshold_raw = (request.GET.get("threshold_pct") or "10").strip()

    date_from, date_to, period_resolved = resolve_period_range(
        period_raw=period_raw,
        date_from_raw=date_from_raw,
        date_to_raw=date_to_raw,
    )

    try:
        threshold_pct = Decimal(threshold_raw)
    except Exception:
        threshold_pct = Decimal("10")
    if threshold_pct < 0:
        threshold_pct = Decimal("0")

    sucursal_id = int(sucursal_id_raw) if sucursal_id_raw.isdigit() else None
    report = build_discrepancias_report(
        date_from=date_from,
        date_to=date_to,
        sucursal_id=sucursal_id,
        threshold_pct=threshold_pct,
    )

    context = {
        "module_tabs": _module_tabs("discrepancias"),
        "periodo": period_resolved,
        "date_from": date_from,
        "date_to": date_to,
        "threshold_pct": threshold_pct,
        "sucursal_id": sucursal_id,
        "sucursales": list(Sucursal.objects.filter(activa=True).order_by("codigo")),
        "report": report,
    }
    return render(request, "control/discrepancias.html", context)
