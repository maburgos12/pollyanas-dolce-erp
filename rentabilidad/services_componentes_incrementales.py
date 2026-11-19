"""Tratamiento autorizado de material vendido, independiente del costo total de receta."""
from datetime import date
from decimal import Decimal, InvalidOperation

from pos_bridge.models import PointProduct
from recetas.models import RecetaAgrupacionAddon


PERIOD = date(2026, 10, 1)
METADATA_KEY = "monthly_sold_material_treatments"
CONTRACT = "DG_TOPPING_LABELS_OCT2026_V1"
UNIT_COST = Decimal("1.004099")
COMPONENTS = (
    {"insumo_id": 98, "quantity": "1", "unit": "PZA", "monthly_unit_cost": "0.435795",
     "source_rows": [11095, 12253, 12616, 12575]},
    {"insumo_id": 99, "quantity": "1", "unit": "PZA", "monthly_unit_cost": "0.568304",
     "source_rows": [11235, 11572, 11890, 12256, 12617, 12578, 12840, 12900]},
)


def _decimal(value):
    if not isinstance(value, str):
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def valid_treatment(treatment):
    """Fail closed on unreviewed scope, identity, quantities or component costs."""
    if not isinstance(treatment, dict):
        return False
    expected = {
        "contract": CONTRACT, "scope": "RENTABILIDAD_SOLD_MATERIAL",
        "period": PERIOD.isoformat(), "rule_id": 26, "product_id": 186,
        "external_id": "961", "sku": "1254", "name": "TOPPING ZANAHORIA C",
        "recipe_id": 516, "base_recipe_id": 55, "unit": "PZA",
        "sale_rows": 123, "rounding": "PER_BRANCH_2DP",
    }
    if any(treatment.get(key) != value for key, value in expected.items()):
        return False
    authorization = treatment.get("authorization")
    if not isinstance(authorization, str) or not authorization.strip():
        return False
    if _decimal(treatment.get("unit_cost")) != UNIT_COST or _decimal(treatment.get("sold_quantity")) != 167:
        return False
    if treatment.get("components") != list(COMPONENTS):
        return False
    return treatment.get("absorbed_components") == [
        {"insumo_id": 384, "quantity": "20", "unit": "GR", "base_recipe_id": 55},
    ]


def sold_material_treatments(product_ids, period):
    if period != PERIOD or 186 not in product_ids:
        return {}
    rule = RecetaAgrupacionAddon.objects.filter(
        pk=26, activo=True, status="APPROVED", base_receta_id=55, addon_receta_id=516,
        addon_codigo_point="1254", addon_receta__codigo_point="1254",
    ).first()
    metadata = rule.metadata if rule and isinstance(rule.metadata, dict) else {}
    treatments = metadata.get(METADATA_KEY)
    treatment = treatments.get(period.isoformat()) if isinstance(treatments, dict) else None
    if not valid_treatment(treatment):
        return {}
    if not PointProduct.objects.filter(pk=186, external_id="961", sku="1254").exists():
        return {}
    return {186: {
        "cost": UNIT_COST, "period": period, "through": date(2026, 10, 31),
        "treatment": "INCREMENTAL_COMPONENTS", "authorized": True,
        "scope": "RENTABILIDAD_SOLD_MATERIAL", "rule_id": rule.pk,
        "source": CONTRACT, "authorization": treatment["authorization"],
        "components": treatment["components"], "absorbed_components": treatment["absorbed_components"],
    }}
