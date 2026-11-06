"""Lote de producción 2025 revisado: costo ponderado, sin inventar recetas.

El manifiesto versionado fija los pares y hashes autorizados. Staging incompleto,
modificado o duplicado deja el grupo sin costo; no habilita otros lotes/años.
"""
from calendar import monthrange
from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from hashlib import sha256
import json
from pathlib import Path

SOURCE = "POINT_REVIEWED_PRODUCTION_2025"


@lru_cache(maxsize=1)
def _manifest():
    return json.loads((Path(__file__).parent / "data" / "produccion_documental_2025.json").read_text())


def payload_hash(payload):
    return sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                             separators=(",", ":")).encode()).hexdigest()


def _valid_line(row, expected, group):
    raw = row.raw_payload
    if (not isinstance(raw, dict) or row.source_hash != expected["source_hash"]
            or payload_hash(raw) != expected["source_hash"]):
        return False
    production, detail = raw["production"], raw["detail"]
    review = raw["historical_cost_review"]
    production_date = date.fromisoformat(production["Fecha"][:10])
    branch_external_id = expected.get("branch_external_id", group["branch_external_id"])
    unit = group.get("unit", "U")
    native_name = group.get("native_name", group["name"])
    if (review["source"] != SOURCE or row.is_insumo or row.insumo_id or row.receta_id
            or row.erp_branch_id or row.sync_job_id
            or row.branch.external_id != branch_external_id
            or review["branch_external_id"] != branch_external_id
            or production_date.replace(day=1).isoformat() != group["period"]
            or row.production_date != production_date
            or str(production["FK_Produccion"]) != row.production_external_id
            or str(detail["PK_Produccion_detalle"]) != row.detail_external_id
            or str(detail["PK_Producto"]) != group["external_id"]
            or production["Activo"] is not True or production["Status"] != 3
            or detail["Status"] != 3 or detail["IsInsumo"] is not False
            or unit not in {"U", "PZA"} or detail["Unidad"] != unit or row.unit != unit
            or detail["Codigo"] != group["sku"] or row.item_code != group["sku"]
            or native_name.rstrip() != group["name"]
            or detail["Nombre"] != native_name or row.item_name != native_name):
        return False
    # PostgreSQL Numeric rounds ties away from zero; preserve full raw precision
    # for the average, comparing only the storage representation to each column.
    for field, key in (("produced_quantity", "Cantidad_producida"),
                       ("requested_quantity", "Cantidad_solicitada"),
                       ("unit_cost", "Precio_default")):
        value = Decimal(str(detail[key]))
        places = row._meta.get_field(field).decimal_places
        if (not value.is_finite() or value <= 0
                or getattr(row, field) != value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)):
            return False
    return True


def fuentes_produccion_documental(product_ids, periodo, fin_periodo):
    """Sólo grupos completos de producto/mes y su identidad nativa exacta."""
    through = periodo.replace(day=monthrange(periodo.year, periodo.month)[1])
    if periodo.year != 2025 or not product_ids or fin_periodo < through:
        return {}
    from pos_bridge.models import PointProduct, PointProductionLine

    products = {(p.external_id, p.sku, p.name): p.pk
                for p in PointProduct.objects.filter(pk__in=product_ids)}
    groups = [g for g in _manifest()["groups"] if g["period"] == periodo.isoformat()
              and (g["external_id"], g["sku"], g["name"]) in products]
    if not groups:
        return {}
    # Read all copies of the expected native pairs, regardless of their tag/date:
    # a second hash for the same pair must invalidate rather than double count.
    expected_lines = [line for g in groups for line in g["lines"]]
    rows = defaultdict(list)
    for row in PointProductionLine.objects.filter(
        production_external_id__in={line["order"] for line in expected_lines},
        detail_external_id__in={line["detail"] for line in expected_lines},
    ).select_related("branch"):
        rows[(row.production_external_id, row.detail_external_id)].append(row)
    result = {}
    for group in groups:
        quantity, total = Decimal(0), Decimal(0)
        for expected in group["lines"]:
            matches = rows[(expected["order"], expected["detail"])]
            if len(matches) != 1 or not _valid_line(matches[0], expected, group):
                break
            detail = matches[0].raw_payload["detail"]
            qty = Decimal(str(detail["Cantidad_producida"]))
            quantity += qty
            total += qty * Decimal(str(detail["Precio_default"]))
        else:
            if quantity > 0 and quantity == Decimal(group["quantity"]) and total == Decimal(group["cost_total"]):
                result[products[(group["external_id"], group["sku"], group["name"])]] = {
                    "cost": (total / quantity).quantize(Decimal("0.000001")),
                    "period": periodo, "through": through, "source": SOURCE, "coverage_pct": 100,
                    "historical_sku": group["sku"], "historical_name": group["name"],
                    "financial_close": False, "production_line_count": len(group["lines"]),
                }
    return result
