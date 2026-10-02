"""Read the auditor's persisted projection; never fetch Point or rebuild on GET."""
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

from django.core.exceptions import SuspiciousOperation
from django.urls import reverse
from django.db.models import Max, Count

from core.models import Sucursal
from pos_bridge.services.branch_inventory_traceability_service import canonical_point_branch_identity
from recetas.models import Receta, RecetaCodigoPointAlias
from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditRun


ZERO = Decimal("0")
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
    return str((runs, revision))


def audit_status(statuses):
    statuses = set(statuses)
    if not statuses:
        return "Aún no auditado"
    if "SOURCE_INCOMPLETE" in statuses:
        return "Falta información"
    if statuses <= {"BALANCED", "RESOLVED"}:
        return "Conciliado"
    return "Pendiente de conciliar"


def _case_quantity(case, field):
    # Missing snapshots are stored as numeric placeholders by the materializer.
    # Do not present those placeholders as evidence of zero stock.
    if case.movement_status == "SOURCE_INCOMPLETE":
        if "CASE_MISSING_FROM_REBUILD" in case.issue_codes:
            return None
        if field in {"difference", "expected_closing"}:
            return None
        source = {"opening_point": "opening", "point_closing": "closing"}.get(field)
        if source and not case.source_trace.get(source):
            return None
    return getattr(case, field)


def _confirmed_recipe_map():
    recipes = {r.id: r for r in Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).only(
        "id", "codigo_point", "categoria", "pasa_modulo_produccion"
    )}
    codes = defaultdict(set)
    for recipe in recipes.values():
        if recipe.codigo_point:
            codes[recipe.codigo_point.strip().upper()].add(recipe.id)
    for alias in RecetaCodigoPointAlias.objects.filter(activo=True).values("codigo_point", "receta_id"):
        if alias["receta_id"] in recipes:
            codes[alias["codigo_point"].strip().upper()].add(alias["receta_id"])
    return recipes, codes


def read_audit_report(month, *, branch=""):
    month = month.replace(day=1)
    branches = list(Sucursal.objects.order_by("nombre"))
    selected = None
    if branch:
        selected = next((b for b in branches if str(b.id) == str(branch)), None)
        if selected is None:
            raise SuspiciousOperation("Sucursal de auditoría inválida")
    run = ProductInventoryAuditRun.objects.filter(month=month).first()
    aliases, _ = canonical_point_branch_identity()
    qs = ProductInventoryAuditCase.objects.sold_products().filter(month=month).select_related("branch__erp_branch", "product")
    if selected:
        qs = qs.filter(branch__erp_branch=selected)
    canonical_cases = {}
    for case in qs:
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
    for product_id, cases in grouped.items():
        product = cases[0].product
        matches = set().union(*(codes.get(code.strip().upper(), set()) for code in (product.sku, product.external_id) if code))
        recipe = recipes[next(iter(matches))] if len(matches) == 1 else None
        row = {
            "product_id": product_id, "receta_id": recipe.id if recipe else None,
            "receta": product.name, "categoria": recipe.categoria if recipe else (product.category or "Sin categoría"),
            "produccion_referencia": bool(recipe and not recipe.pasa_modulo_produccion),
            "estado_inventario": audit_status(case.movement_status for case in cases),
            "cases": [{"id": c.id, "branch": c.branch.erp_branch.nombre if c.branch.erp_branch_id else c.branch.name,
                       "status": audit_status([c.movement_status]), "url": reverse("reportes:inventory_audit_case", args=[c.id])}
                      for c in sorted(cases, key=lambda c: c.branch.name)],
            "conversion_provenance_label": "Origen por identificar" if any(
                "CONVERSION" in code and any(marker in code for marker in ("UNRESOLVED", "NON_DERIVED", "MISSING", "MISMATCH"))
                for case in cases for code in case.issue_codes
            ) else "Movimientos Point",
        }
        for field, output in QUANTITY_FIELDS.items():
            values = [_case_quantity(case, field) for case in cases]
            row[output] = None if any(v is None for v in values) else sum(values, ZERO)
        row["familia"] = row["categoria"]
        row["convertido"] = row["conversion_entrada"]
        row["enteros_equivalentes"] = row["conversion_salida"]
        row["conversion_provenance"] = row["conversion_provenance_label"]
        rows.append(row)
    cases = list(canonical_cases.values())
    stale = bool(run and (not run.last_successful_rebuild_at or (run.rebuilt_at and run.rebuilt_at > run.last_successful_rebuild_at)))
    return {
        "rows": rows, "run": run, "branches": branches, "selected_branch": str(selected.id) if selected else "",
        "selected_branch_label": selected.nombre if selected else "Todas las sucursales y almacenes",
        "audit_status": audit_status(c.movement_status for c in cases), "stale": stale,
        "updated_at": run.last_successful_rebuild_at if run else None,
        "opening_reference": month - timedelta(days=1),
        "counts": {"balanced": sum(c.movement_status in {"BALANCED", "RESOLVED"} for c in cases),
                   "pending": sum(c.movement_status not in {"BALANCED", "RESOLVED", "SOURCE_INCOMPLETE"} for c in cases),
                   "incomplete": sum(c.movement_status == "SOURCE_INCOMPLETE" for c in cases)},
    }
