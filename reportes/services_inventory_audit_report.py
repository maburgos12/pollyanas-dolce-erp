"""Read the auditor's persisted projection; never fetch Point or rebuild on GET."""
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
from pos_bridge.services.monthly_product_balance_service import has_documentary_boundary, MonthlyPointProductBalanceService
from pos_bridge.models import PointProduct, PointTransferLine, PointConversionLine, PointWasteLine

from django.core.exceptions import SuspiciousOperation
from django.urls import reverse
from django.db.models import Max, Count, Q
from django.utils.text import slugify
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.services.branch_inventory_traceability_service import canonical_point_branch_identity
from recetas.models import Receta, RecetaCodigoPointAlias
from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditRun


ZERO = Decimal("0")
# Alcance del reporte autorizado por el DG; no reclasifica el catálogo ni los casos.
NON_PRODUCTION_CATEGORIES = {
    "accesorios-de-reposteria", "alegria", "cake-topper", "caketopper",
    "coca-cola", "d-rigaldi", "rigaldi", "granmark", "gran-mark",
    "industrias-lec", "industrias-lek", "plasticos", "regalos", "te",
    "vela-sparklers", "velas", "cafe", "otros-postres", "vaso-preparado-mini",
    "vasos-mini", "vasos-grande", "vasos-preparados", "vasos-preparados-mini",
    "vasos-preparados-chico", "vasos-preparados-mediano", "vasos-preparados-grande",
}
# DG8oct2026: estos Dot Cake sí se fabrican, aunque Point los agrupe en vasos.
MANUFACTURED_CUP_CODES = {"4358", "8734"}
MANUFACTURED_CUP_CATEGORIES = {"vasos-grande", "vasos-preparados-grande"}
QUANTITY_FIELDS = {
    "opening_point": "inventario_inicial", "production": "producido",
    "sales": "vendido", "waste": "merma_reportada",
    "transfer_in": "transferencia_entrada", "transfer_out": "transferencia_salida",
    "conversion_in": "conversion_entrada", "conversion_out": "conversion_salida",
    "identified_adjustment": "ajuste_identificado",
    "expected_closing": "inventario_final_teorico",
    "point_closing": "inventario_final_point_total", "difference": "diferencia_inventario",
}


def audit_report_version(month, branch=""):
    runs = ProductInventoryAuditRun.objects.filter(month=month).values("rebuilt_at", "last_successful_rebuild_at", "status").first()
    cases = ProductInventoryAuditCase.objects.sold_products().filter(month=month)
    if branch:
        if len(str(branch)) > 10 or not str(branch).isdecimal() or int(branch) > 2147483647:
            raise SuspiciousOperation("Sucursal de auditoría inválida")
        if not Sucursal.objects.filter(pk=branch).exists():
            raise SuspiciousOperation("Sucursal de auditoría inválida")
        cases = cases.filter(branch__erp_branch_id=branch)
    revision = cases.aggregate(latest=Max("updated_at"), count=Count("id"))
    return str(("production-scope-v3", runs, revision))


def audit_status(statuses):
    statuses = set(statuses)
    if not statuses:
        return "Aún no auditado"
    if "SOURCE_INCOMPLETE" in statuses:
        return "Falta información"
    if statuses <= {"BALANCED", "RESOLVED"}:
        return "Conciliado"
    return "Pendiente de conciliar"


def case_balance_status(case):
    """Stock arithmetic is separate from the case's approval/traceability lifecycle."""
    if case.movement_status == "SOURCE_INCOMPLETE":
        return "SOURCE_INCOMPLETE"
    return "BALANCED" if case.difference == 0 else "NEEDS_EXPLANATION"


def _commercial_balance(case):
    """Display known arithmetic, never approve a contradictory commercial sale."""
    history = (case.source_trace or {}).get("point_history", {})
    if (case.movement_status != "SOURCE_INCOMPLETE"
            or set(case.issue_codes) - {"SOURCE_INCOMPLETE", "PRODUCT_RESOLVED_BY_NAME"}
            or history.get("unapplied_reason") != "SALES_STOCK_EFFECT_UNVERIFIED"
            or history.get("coverage_status") != "COMPLETE"
            or history.get("unknown_movement_ids") != []
            or not all(has_documentary_boundary(case.source_trace, side) for side in ("opening", "closing"))
            or set(history.get("aggregate_comparison", {})) != {"sales"}):
        return None
    try:
        fields = ("production", "waste", "transfer_in", "transfer_out", "conversion_in",
                  "conversion_out", "identified_adjustment")
        values = {field: Decimal(str(history[field])) for field in fields}
        sales = Decimal(str(history["aggregate_comparison"]["sales"]["aggregate"]))
        stock_sales = Decimal(str(history["sales"]))
        comparison = history["aggregate_comparison"]["sales"]
        if (not all(value.is_finite() for value in (*values.values(), sales, stock_sales))
                or sales == stock_sales or Decimal(str(history["unexplained_remainder"])) != ZERO
                or Decimal(str(comparison["point_history"])) != stock_sales
                or Decimal(str(comparison["difference"])) != stock_sales - sales
                or Decimal(str(history["documentary_opening"])) != case.opening_point
                or Decimal(str(history["point_closing"])) != case.point_closing
                or any(values[field] != getattr(case, field) for field in fields)):
            return None
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return None
    calculated = (case.opening_point + values["production"] + values["transfer_in"] + values["conversion_in"]
            + values["identified_adjustment"] - sales - values["waste"]
            - values["transfer_out"] - values["conversion_out"])
    return calculated if calculated + sales - stock_sales == case.point_closing else None


def _case_quantity(case, field):
    # Missing snapshots are stored as numeric placeholders by the materializer.
    # Do not present those placeholders as evidence of zero stock.
    if case.movement_status == "SOURCE_INCOMPLETE":
        if "CASE_MISSING_FROM_REBUILD" in case.issue_codes:
            return None
        if field in {"difference", "expected_closing"}:
            calculated = _commercial_balance(case)
            return (case.point_closing - calculated if field == "difference" else calculated) if calculated is not None else None
        source = {"opening_point": "opening", "point_closing": "closing"}.get(field)
        if source and not has_documentary_boundary(case.source_trace, source):
            return None
    if field == "sales":
        comparison = (case.source_trace or {}).get("point_history", {}).get(
            "aggregate_comparison", {}
        ).get("sales", {})
        if "aggregate" in comparison:
            return Decimal(str(comparison["aggregate"]))
    return getattr(case, field)


def _report_balance_status(case):
    return "NEEDS_EXPLANATION" if _commercial_balance(case) is not None else case_balance_status(case)


def _confirmed_recipe_map():
    recipes = {r.id: r for r in Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).only(
        "id", "codigo_point", "categoria", "pasa_modulo_produccion", "modo_costeo"
    )}
    codes = defaultdict(set)
    for recipe in recipes.values():
        if recipe.codigo_point:
            codes[recipe.codigo_point.strip().upper()].add(recipe.id)
    for alias in RecetaCodigoPointAlias.objects.filter(activo=True).values("codigo_point", "receta_id"):
        if alias["receta_id"] in recipes:
            codes[alias["codigo_point"].strip().upper()].add(alias["receta_id"])
    return recipes, codes


def _production_report_product(product, recipe, cases):
    categories = {slugify(product.category), slugify(recipe.categoria) if recipe else ""}
    manufactured_cup = bool(recipe and recipe.modo_costeo == Receta.MODO_COSTEO_FABRICADO
                            and (recipe.codigo_point or "").strip().upper() in MANUFACTURED_CUP_CODES
                            and (product.sku or "").strip().upper() == recipe.codigo_point.strip().upper()
                            and categories <= MANUFACTURED_CUP_CATEGORIES)
    if categories & NON_PRODUCTION_CATEGORIES and not manufactured_cup:
        return False
    # Rebanadas/derivados fabricados conservan sus conversiones aunque no se capturen en producción.
    if recipe and recipe.modo_costeo != Receta.MODO_COSTEO_FABRICADO:
        return False
    if "rosca" in categories and not any(
        getattr(case, field) for case in cases for field in QUANTITY_FIELDS
        if field not in {"opening_point", "point_closing", "expected_closing", "difference"}
    ):
        return False
    return True


def _case_pending_reason(case):
    if case.movement_status != "SOURCE_INCOMPLETE":
        return ""
    if _commercial_balance(case) is not None:
        history = case.source_trace["point_history"]
        sales = Decimal(str(history["aggregate_comparison"]["sales"]["aggregate"]))
        return f"Ventas registradas: {sales.normalize():f}; salidas por venta en inventario: {Decimal(str(history['sales'])).normalize():f}. Diferencia pendiente de revisar."
    missing = [label for field, label in (("opening_point", "saldo inicial"), ("point_closing", "saldo final"))
               if _case_quantity(case, field) is None]
    return "Falta comprobar " + " y ".join(missing or ["movimientos del mes"])


def _retired_nonproduction_case(case):
    trace = case.source_trace or {}
    if ("CASE_MISSING_FROM_REBUILD" in case.issue_codes
            and "PRODUCT_RESOLVED_BY_NAME" in case.issue_codes
            and trace.get("waste")
            and not any(trace.get(key) for key in ("opening", "closing", "point_history", "production",
                "sales", "adjustments", "transfers", "conversions", "transfer_in", "transfer_out",
                "conversion_in", "conversion_out", "open_transfer_snapshot_in", "open_transfer_snapshot_out"))
            and not any(getattr(case, field) for field in ("opening_point", "point_closing", "production",
                "sales", "identified_adjustment", "transfer_in", "transfer_out", "conversion_in", "conversion_out"))):
        ids = trace["waste"]
        if not isinstance(ids, list) or any(type(pk) is not int or pk <= 0 for pk in ids):
            return False
        rows = list(PointWasteLine.objects.filter(pk__in=ids).select_related("receta"))
        aliases, _ = canonical_point_branch_identity()
        return (len(rows) == len(set(ids)) and sum((row.quantity for row in rows), ZERO) == case.waste
            and all(row.insumo_id and row.receta_id and row.receta.tipo == Receta.TIPO_PREPARACION
                and aliases.get(row.branch_id, row.branch_id) == aliases.get(case.branch_id, case.branch_id)
                and timezone.localdate(row.movement_at).replace(day=1) == case.month for row in rows))
    if ("CASE_MISSING_FROM_REBUILD" not in case.issue_codes or trace.get("point_history")
            or any(trace.get(key) for key in ("opening", "closing", "production", "sales", "waste", "adjustments"))
            or any(getattr(case, field) for field in ("production", "sales", "waste", "identified_adjustment"))):
        return False
    transfer_ids, conversion_ids = set(trace.get("transfers", [])), set(trace.get("conversions", []))
    if any(type(pk) is not int or pk <= 0 for pk in transfer_ids | conversion_ids):
        return False
    transfers = list(PointTransferLine.objects.filter(pk__in=transfer_ids))
    conversions = list(PointConversionLine.objects.filter(pk__in=conversion_ids).select_related("branch__erp_branch"))
    if not transfer_ids | conversion_ids or len(transfers) != len(transfer_ids) or len(conversions) != len(conversion_ids):
        return False
    for row in transfers:
        if not isinstance(row.raw_payload, dict):
            return False
        detail = row.raw_payload.get("detail", {})
        if not isinstance(detail, dict) or not isinstance(detail.get("Articulo"), str):
            return False
        fk = detail.get("FK_articulo")
        if (type(fk) is not int or fk <= 0 or detail.get("isInsumo") is not False
                or case.branch_id not in (row.origin_branch_id, row.destination_branch_id)
                or timezone.localdate(row.received_at or row.registered_at).replace(day=1) != case.month):
            return False
        product = PointProduct.objects.filter(external_id=str(fk)).first()
        if (not product or detail.get("Articulo", "").strip() != product.name.strip()
                or slugify(product.category) not in NON_PRODUCTION_CATEGORIES
                or product.sku in MANUFACTURED_CUP_CODES):
            return False
    reader = MonthlyPointProductBalanceService()
    return all(timezone.localdate(row.movement_at).replace(day=1) == case.month
        and (row.branch_id == case.branch_id or row.erp_branch_id and row.erp_branch_id == case.branch.erp_branch_id)
        and reader._documentary_commercial_exclusion(row, source="conversions") for row in conversions)


def read_audit_report(month, *, branch=""):
    month = month.replace(day=1)
    branches = list(Sucursal.objects.exclude(codigo__iexact="DEVOLUCIONES").order_by("nombre"))
    selected = None
    if branch:
        selected = next((b for b in branches if str(b.id) == str(branch)), None)
        if selected is None:
            raise SuspiciousOperation("Sucursal de auditoría inválida")
    run = ProductInventoryAuditRun.objects.filter(month=month).first()
    aliases, _ = canonical_point_branch_identity()
    qs = ProductInventoryAuditCase.objects.sold_products().filter(month=month).exclude(
        Q(branch__name__iexact="Devoluciones") | Q(branch__erp_branch__codigo__iexact="DEVOLUCIONES")
    ).select_related("branch__erp_branch", "product")
    # Keep retired evidence in its case, not in a successfully rebuilt current report.
    if run and run.last_successful_rebuild_at and not run.partial_published:
        qs = qs.exclude(issue_codes__contains=["CASE_MISSING_FROM_REBUILD"])
    if selected:
        qs = qs.filter(branch__erp_branch=selected)
    canonical_cases = {}
    for case in qs:
        if _retired_nonproduction_case(case):
            continue
        canonical = aliases.get(case.branch_id, case.branch_id)
        key = canonical, case.product_id
        previous = canonical_cases.get(key)
        rank = (case.branch_id == canonical, case.rebuilt_at, case.id)
        if previous is None or rank > (previous.branch_id == canonical, previous.rebuilt_at, previous.id):
            canonical_cases[key] = case
    grouped = defaultdict(list)
    for case in canonical_cases.values():
        grouped[case.product_id].append(case)
    recipes, codes = _confirmed_recipe_map()
    rows = []
    report_cases = []
    for product_id, cases in grouped.items():
        product = cases[0].product
        matches = set().union(*(codes.get(code.strip().upper(), set()) for code in (product.sku, product.external_id) if code))
        recipe = recipes[next(iter(matches))] if len(matches) == 1 else None
        if not _production_report_product(product, recipe, cases):
            continue
        report_cases.extend(cases)
        row = {
            "product_id": product_id, "receta_id": recipe.id if recipe else None,
            "receta": product.name, "categoria": recipe.categoria if recipe else (product.category or "Sin categoría"),
            "produccion_referencia": bool(recipe and not recipe.pasa_modulo_produccion),
            "estado_inventario": audit_status(_report_balance_status(case) for case in cases),
            "estado_trazabilidad": audit_status(case.movement_status for case in cases),
            "cases": [{"id": c.id, "branch": c.branch.erp_branch.nombre if c.branch.erp_branch_id else c.branch.name,
                       "status": audit_status([_report_balance_status(c)]),
                       "traceability_status": audit_status([c.movement_status]),
                       "pending_reason": _case_pending_reason(c),
                       "opening": _case_quantity(c, "opening_point"),
                       "closing": _case_quantity(c, "point_closing"),
                       "calculated": _case_quantity(c, "expected_closing"),
                       "difference": _case_quantity(c, "difference"),
                       "url": reverse("reportes:inventory_audit_case", args=[c.id])}
                      for c in sorted(cases, key=lambda c: c.branch.name)],
            "conversion_provenance_label": "Origen por identificar" if any(
                "CONVERSION" in code and any(marker in code for marker in ("UNRESOLVED", "NON_DERIVED", "MISSING", "MISMATCH"))
                for case in cases for code in case.issue_codes
            ) else "Movimientos Point",
        }
        row["point_coverage"] = {}
        for field, output in QUANTITY_FIELDS.items():
            values = [_case_quantity(case, field) for case in cases]
            row[output] = None if any(v is None for v in values) else sum(values, ZERO)
            if field in {"opening_point", "point_closing"}:
                known = [value for value in values if value is not None]
                row["point_coverage"][output] = {
                    "known": len(known), "total": len(values), "known_sum": sum(known, ZERO),
                }
        row["familia"] = row["categoria"]
        row["convertido"] = row["conversion_entrada"]
        row["enteros_equivalentes"] = row["conversion_salida"]
        row["conversion_provenance"] = row["conversion_provenance_label"]
        rows.append(row)
    cases = report_cases
    partial = bool(run and run.partial_published)
    stale = bool(run and (not run.last_successful_rebuild_at or (run.rebuilt_at and run.rebuilt_at > run.last_successful_rebuild_at)))
    return {
        "rows": rows, "run": run, "branches": branches, "selected_branch": str(selected.id) if selected else "",
        "selected_branch_label": selected.nombre if selected else "Sucursales de venta y CEDIS",
        "audit_status": audit_status(c.movement_status for c in cases), "stale": stale,
        "partial": partial,
        "updated_at": (run.rebuilt_at if partial else run.last_successful_rebuild_at) if run else None,
        "opening_reference": month - timedelta(days=1),
        "counts": {"balanced": sum(c.movement_status in {"BALANCED", "RESOLVED"} for c in cases),
                   "pending": sum(c.movement_status not in {"BALANCED", "RESOLVED", "SOURCE_INCOMPLETE"} for c in cases),
                   "incomplete": sum(c.movement_status == "SOURCE_INCOMPLETE" for c in cases)},
    }
