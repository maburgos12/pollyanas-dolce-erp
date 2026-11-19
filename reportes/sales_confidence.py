"""Read-only confidence contract. Never repair, relabel or merge source records."""
from calendar import monthrange
from collections import defaultdict
from datetime import date
from decimal import Decimal

from django.db.models import (
    Case, Count, F, IntegerField, Max, Q, Sum, Value, When,
)
from django.db.models.functions import Coalesce, TruncMonth

from reportes.models import FactVentaDiaria
from django.db.models.expressions import RawSQL

ZERO = Decimal("0")
Q2 = Decimal("0.01")


def _cost_evidence(groups, end_date, *, aplicar_componentes_incrementales=False):
    from pos_bridge.models import PointProduct
    from recetas.utils.normalizacion import normalizar_nombre
    from rentabilidad.services_costos import asignar_costos_filas, productos_nativos_por_sku

    for group in groups:
        group["product_id"] = group["point_product_id"]
        group["product__sku"] = group["producto_clave"]
    nativos = productos_nativos_por_sku([g for g in groups if g["month"].year == 2025])

    ids = {g["point_product_id"] for g in groups if g["point_product_id"]}
    keys = {g["producto_clave"] for g in groups if g["source_kind"] == FactVentaDiaria.SOURCE_AUTHORITATIVE and not g["point_product_id"]}
    identities = defaultdict(set)
    products = {p.pk: p for p in PointProduct.objects.filter(Q(pk__in=ids) | Q(external_id__in=keys) | Q(external_id__in=[f"official:{k}" for k in keys]))}
    for product in products.values():
        # La clave autoritativa es externa: nunca interpretarla como PK ni resolver por nombre solo.
        identities[(product.external_id.removeprefix("official:"), normalizar_nombre(product.name))].add(product.pk)
        identities[(product.external_id, normalizar_nombre(product.name))].add(product.pk)
    for group in groups:
        product_id = group["point_product_id"]
        if not product_id and group["month"].year == 2025 and group["producto_clave"] in nativos:
            candidates = nativos[group["producto_clave"]].get(normalizar_nombre(group["producto_nombre"]), set())
            if len(candidates) == 1:
                product_id = next(iter(candidates))
        elif not product_id and group["source_kind"] == FactVentaDiaria.SOURCE_AUTHORITATIVE:
            candidates = identities.get((group["producto_clave"], normalizar_nombre(group["producto_nombre"])), set())
            if len(candidates) == 1:
                product_id = next(iter(candidates))
        group["resale_product_id"] = product_id
        group["product_id"] = product_id
        product = products.get(product_id)
        group["product__sku"] = product.sku if product else (group["producto_clave"] if group["month"].year == 2025 else "")
        group["product__name"] = group["producto_nombre"]
        # Fact 3694462 / daily fact 737827 crossed Point 1064 (010408)
        # with recipe 139, whose audited Point identity is 1013 (0-2).
        # Reject only the documented October pair; do not infer a replacement.
        if (group["month"] == date(2026, 10, 1)
                and group["source_kind"] == FactVentaDiaria.SOURCE_V2
                and product_id == 1001 and group["receta_id"] == 139
                and product and product.external_id == "official:010408"
                and product.sku == "010408"):
            group["source_recipe_id"] = group["receta_id"]
            group["recipe_identity_conflict"] = True
            group["receta_id"] = None
    for month in {g["month"] for g in groups}:
        cutoff = min(end_date, month.replace(day=monthrange(month.year, month.month)[1]))
        rows = [g for g in groups if g["month"] == month]
        if aplicar_componentes_incrementales:
            asignar_costos_filas(rows, month, cutoff, aplicar_componentes_incrementales=True)
        else:
            asignar_costos_filas(rows, month, cutoff)


def _source_priority():
    return Case(
        When(source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE, then=0),
        When(source_kind=FactVentaDiaria.SOURCE_V2, then=1),
        default=2, output_field=IntegerField(),
    )


def selected_sales_facts(*, start_date, end_date, branch_id=None):
    """Same branch/day precedence as analytics_service; retain closed branches."""
    candidates = FactVentaDiaria.objects.filter(fecha__range=(start_date, end_date)).order_by().annotate(
        priority=_source_priority(), identity=Coalesce("sucursal_id", Value(-1)),
    )
    branch_filter = "AND sucursal_id = %s" if branch_id is not None else ""
    export_filter = "AND branch_id = %s" if branch_id is not None else ""
    derived_filter = "AND d.branch_id = %s" if branch_id is not None else ""
    branch_params = (branch_id,) if branch_id is not None else ()
    if branch_id is not None:
        candidates = candidates.filter(sucursal_id=branch_id)

    # Compute the same branch/day winner once instead of comparing every pair of sales rows.
    selected_ids = RawSQL(f"""
        WITH preferred_days AS MATERIALIZED (
            SELECT fecha, COALESCE(sucursal_id, -1) branch_identity,
                   MIN(CASE WHEN source_kind = %s THEN 0 WHEN source_kind = %s THEN 1 ELSE 2 END) priority
            FROM reportes_factventadiaria
            WHERE fecha BETWEEN %s AND %s {branch_filter}
            GROUP BY fecha, COALESCE(sucursal_id, -1)
        )
        SELECT f.id FROM reportes_factventadiaria f
        JOIN preferred_days p ON p.fecha = f.fecha
            AND p.branch_identity = COALESCE(f.sucursal_id, -1)
            AND p.priority = CASE WHEN f.source_kind = %s THEN 0 WHEN f.source_kind = %s THEN 1 ELSE 2 END
        WHERE f.fecha BETWEEN %s AND %s {branch_filter}
    """, (FactVentaDiaria.SOURCE_AUTHORITATIVE, FactVentaDiaria.SOURCE_V2, start_date, end_date, *branch_params,
           FactVentaDiaria.SOURCE_AUTHORITATIVE, FactVentaDiaria.SOURCE_V2, start_date, end_date, *branch_params))
    facts = candidates.annotate(has_preferred=Value(False)).filter(pk__in=selected_ids)
    # A derived import of the SAME export is not a second sale. This is lineage
    # selection, not a SKU/name equivalence or an edit to historical records.
    mirror_ids = RawSQL(
        f"""
        WITH original_exports AS MATERIALIZED (
            SELECT DISTINCT branch_id, sale_date, source_file
            FROM ventas_autoritativas_point
            WHERE source_sheet = 'category_report' AND source_file <> ''
              AND sale_date BETWEEN %s AND %s {export_filter}
        ), mirrored_keys AS MATERIALIZED (
            SELECT d.branch_id, d.sale_date, d.product_code
            FROM ventas_autoritativas_point d
            JOIN original_exports o ON o.branch_id = d.branch_id
                AND o.sale_date = d.sale_date AND o.source_file = d.source_file
            WHERE d.source_sheet = 'PointSalesDailyProductFact' {derived_filter}
        )
        SELECT f.id FROM reportes_factventadiaria f
        JOIN mirrored_keys k ON k.branch_id = f.sucursal_id
            AND k.sale_date = f.fecha AND k.product_code = f.producto_clave
        WHERE f.source_kind = 'AUTHORITATIVE'
        """, (start_date, end_date, *branch_params, *branch_params),
    )
    return facts.exclude(pk__in=mirror_ids)



def build_sales_confidence(*, start_date, end_date, aplicar_componentes_incrementales=False):
    """Coverage refers to observed facts, not certified operational completeness.

    Keep historical ERP branch IDs and product keys. Select a single source per
    branch/day (not per product) so alternate catalogs cannot duplicate revenue.
    A positive stored margin alone is never evidence of backed costing.
    """
    # Unassigned rows have no reliable branch identity; retain them as pending.
    facts = selected_sales_facts(start_date=start_date, end_date=end_date)
    summary = facts.aggregate(
        rows=Count("id"), branches=Count("sucursal_id", distinct=True),
        days_observed=Count("fecha", distinct=True), latest_sale=Max("fecha"),
        updated_at=Max("actualizado_en"), net_sales=Sum("venta_neta"),
    )
    identity_pending = facts.filter(
        Q(sucursal__isnull=True) | Q(fecha__lt=F("sucursal__fecha_apertura")),
    ).count()
    aggregations = {
        "rows": Count("id"), "positive_sales": Sum("venta_neta", filter=Q(venta_neta__gt=0)),
        "cost": Sum("costo_estimado"), "quantity": Sum("cantidad"),
    }
    if aplicar_componentes_incrementales:
        from django.contrib.postgres.aggregates import JSONBAgg
        from django.db.models import CharField
        from django.db.models.functions import Cast, JSONObject

        aggregations["incremental_quantities"] = JSONBAgg(
            JSONObject(branch=F("sucursal_id"), quantity=Cast("cantidad", CharField())),
            filter=Q(point_product_id=186) | Q(point_product_id__isnull=True, producto_clave="961"),
        )
    groups = list(facts.annotate(
        month=TruncMonth("fecha"),
        positive_quantity=Case(When(cantidad__gt=0, then=1), default=0, output_field=IntegerField()),
        backed_amount=Case(
            When(costo_estimado__gt=0, then=1), default=0, output_field=IntegerField(),
        ),
    ).values(
        "month", "source_kind", "metadata__costing__source",
        "metadata__costing__period", "backed_amount", "positive_quantity",
        "point_product_id", "producto_clave", "producto_nombre", "receta_id",
    ).annotate(**aggregations))
    _cost_evidence(groups, end_date, aplicar_componentes_incrementales=aplicar_componentes_incrementales)
    missing_sales = prior_sales = covered_sales = positive_sales = costs = ZERO
    missing_rows = 0
    authorized_zero_sales = service_unallocated_sales = ZERO
    sources = set()
    periods = set()
    incremental_by_branch = defaultdict(lambda: ZERO)
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
        cost = group["cost"] or ZERO
        evidence = group["cost_evidence"]
        monthly_evidence = (evidence and evidence.get("through") and evidence["through"] <= end_date
                            and evidence["cost"] > ZERO and (not period or evidence["period"] >= period))
        if (monthly_evidence or group.get("recipe_identity_corrected")
                or group.get("recipe_identity_conflict") or (evidence and evidence.get("authorized"))):
            backed = False  # El histórico vigente o la decisión DG reemplazan el costo materializado antiguo.
        if not backed:
            if evidence and (evidence["cost"] > ZERO or evidence.get("authorized")) and group["quantity"] > ZERO and evidence.get("through", end_date) <= end_date:
                backed = True
                period = evidence["period"]
                cost = (group["quantity"] * evidence["cost"]).quantize(Q2)
        if not backed:
            missing_sales += sales
            missing_rows += group["rows"]
            continue
        if evidence and evidence.get("authorized") and evidence["cost"] == ZERO:
            authorized_zero_sales += sales
            if evidence.get("treatment") == "UNALLOCATED_SERVICE":
                service_unallocated_sales += sales
        covered_sales += sales
        if evidence and evidence.get("treatment") == "INCREMENTAL_COMPONENTS":
            for item in group["incremental_quantities"] or []:
                incremental_by_branch[item["branch"]] += Decimal(item["quantity"]) * evidence["cost"]
        else:
            costs += cost
        periods.add(period)
        if period < group["month"]:
            prior_sales += sales
    costs += sum((amount.quantize(Q2) for amount in incremental_by_branch.values()), ZERO)
    summary.update(
        start_date=start_date, end_date=end_date, sources=sorted(sources),
        cost_periods=sorted(periods), identity_pending_rows=identity_pending,
        missing_cost_sales=missing_sales, missing_cost_rows=missing_rows,
        prior_cost_sales=prior_sales, authorized_zero_sales=authorized_zero_sales,
        service_unallocated_sales=service_unallocated_sales,
        cost_coverage_pct=(covered_sales / positive_sales * 100).quantize(Q2) if positive_sales else None,
        margin=((summary["net_sales"] or ZERO) - costs).quantize(Q2) if summary["rows"] and not missing_rows and not identity_pending else None,
        status="Sin datos" if not summary["rows"] else "Provisional" if missing_rows or prior_sales or identity_pending or authorized_zero_sales else "Estimado con costo del mes")
    labels = {"AUTHORITATIVE": "Venta histórica ERP", "V2_FACT": "Point v2", "LEGACY": "Point legacy"}
    summary["source_labels"] = [labels.get(source, source) for source in sorted(sources)]
    summary["net_sales"] = (summary["net_sales"] or ZERO) if summary["rows"] else None
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
