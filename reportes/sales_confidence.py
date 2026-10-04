"""Read-only confidence contract. Never repair, relabel or merge source records."""
from datetime import date
from decimal import Decimal

from django.db.models import (
    Case, Count, Exists, F, IntegerField, Max, OuterRef, Q, Sum, Value, When,
)
from django.db.models.functions import Coalesce, TruncMonth

from reportes.models import FactVentaDiaria

ZERO = Decimal("0")
Q2 = Decimal("0.01")


def _source_priority():
    return Case(
        When(source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE, then=0),
        When(source_kind=FactVentaDiaria.SOURCE_V2, then=1),
        default=2, output_field=IntegerField(),
    )


def build_sales_confidence(*, start_date, end_date):
    """Coverage refers to observed facts, not certified operational completeness.

    Keep historical ERP branch IDs and product keys. Select a single source per
    branch/day (not per product) so alternate catalogs cannot duplicate revenue.
    A positive stored margin alone is never evidence of backed costing.
    """
    candidates = FactVentaDiaria.objects.filter(
        fecha__range=(start_date, end_date),
    ).order_by().annotate(
        priority=_source_priority(), identity=Coalesce("sucursal_id", Value(-1)),
    )
    preferred = candidates.filter(
        fecha=OuterRef("fecha"), identity=OuterRef("identity"),
        priority__lt=OuterRef("priority"),
    )
    # Unassigned rows have no reliable branch identity; retain them as pending.
    facts = candidates.annotate(has_preferred=Exists(preferred)).filter(has_preferred=False)
    summary = facts.aggregate(
        rows=Count("id"), branches=Count("sucursal_id", distinct=True),
        days_observed=Count("fecha", distinct=True), latest_sale=Max("fecha"),
        updated_at=Max("actualizado_en"), net_sales=Sum("venta_neta"),
    )
    identity_pending = facts.filter(
        Q(sucursal__isnull=True) | Q(fecha__lt=F("sucursal__fecha_apertura")),
    ).count()
    groups = facts.annotate(
        month=TruncMonth("fecha"),
        backed_amount=Case(
            When(costo_estimado__gt=0, then=1), default=0, output_field=IntegerField(),
        ),
    ).values(
        "month", "source_kind", "metadata__costing__source",
        "metadata__costing__period", "backed_amount",
    ).annotate(
        rows=Count("id"), positive_sales=Sum("venta_neta", filter=Q(venta_neta__gt=0)),
        cost=Sum("costo_estimado"),
    )
    missing_sales = prior_sales = covered_sales = positive_sales = costs = ZERO
    missing_rows = 0
    sources = set()
    periods = set()
    for group in groups:
        sources.add(group["source_kind"])
        sales = group["positive_sales"] or ZERO
        positive_sales += sales
        try:
            period = date.fromisoformat(group["metadata__costing__period"] or "")
        except (ValueError, TypeError):
            period = None
        backed = (
            group["metadata__costing__source"] == "producto_costo_operativo_mensual"
            and period and period <= group["month"] and group["backed_amount"]
        )
        if not backed:
            missing_sales += sales
            missing_rows += group["rows"]
            continue
        covered_sales += sales
        costs += group["cost"] or ZERO
        periods.add(period)
        if period < group["month"]:
            prior_sales += sales
    summary.update(
        start_date=start_date, end_date=end_date, sources=sorted(sources),
        cost_periods=sorted(periods), identity_pending_rows=identity_pending,
        missing_cost_sales=missing_sales, missing_cost_rows=missing_rows,
        prior_cost_sales=prior_sales,
        cost_coverage_pct=(covered_sales / positive_sales * 100).quantize(Q2) if positive_sales else None,
        margin=((summary["net_sales"] or ZERO) - costs).quantize(Q2) if summary["rows"] and not missing_rows and not identity_pending else None,
        status="Sin datos" if not summary["rows"] else "Provisional" if missing_rows or prior_sales or identity_pending else "Estimado con costo del mes")
    labels = {"AUTHORITATIVE": "Venta histórica ERP", "V2_FACT": "Point v2", "LEGACY": "Point legacy"}
    summary["source_labels"] = [labels.get(source, source) for source in sorted(sources)]
    summary["net_sales"] = summary["net_sales"] or ZERO
    return summary


def reconcile_branch_sales(*, source_rows, snapshot_rows):
    source = {row["branch_id"]: row for row in source_rows}
    snapshots = {row["branch_id"]: row for row in snapshot_rows}
    details = []
    for branch_id in source.keys() | snapshots.keys():
        original, snapshot = source.get(branch_id), snapshots.get(branch_id)
        source_total = original["total"] if original else None
        snapshot_total = snapshot["total"] if snapshot else None
        difference = source_total - snapshot_total if original and snapshot else None
        if difference is None or abs(difference) >= Q2:
            details.append(dict(branch_id=branch_id, name=(original or snapshot)["name"], source_total=source_total, snapshot_total=snapshot_total, difference=difference,
                reason="Sin fuente del periodo" if not original else "Sin cálculo del periodo" if not snapshot else "Importes distintos"))
    source_total = sum((row["total"] for row in source.values()), ZERO)
    snapshot_total = sum((row["total"] for row in snapshots.values()), ZERO)
    return dict(source_total=source_total, snapshot_total=snapshot_total, difference=source_total - snapshot_total,
        reconciled=bool(source and snapshots) and not details, discrepancies=sorted(details, key=lambda row: row["name"]))
