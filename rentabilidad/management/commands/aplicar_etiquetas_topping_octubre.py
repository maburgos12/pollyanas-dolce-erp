"""Registra exclusivamente el tratamiento de etiquetas autorizado para octubre."""
from copy import deepcopy
import json
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.db.models import Sum
from django.utils import timezone

from maestros.models import Insumo
from pos_bridge.models import PointProduct
from pos_bridge.services.product_month_source_mutex import lock_product_month_sources
from recetas.models import RecetaAgrupacionAddon
from reportes.services_historical_costing import (
    MonthlyHistoricalCostingService, _normalized_cost_for_insumo, _weight_from_raw, _q6,
)
from rentabilidad.services_componentes_incrementales import (
    COMPONENTS, CONTRACT, METADATA_KEY, PERIOD, UNIT_COST, valid_treatment,
)
from rentabilidad.services_resultado import ventas_para_costeo


AUTHORIZATION = "Mauricio autorizó el tratamiento incremental de dos etiquetas de octubre por $167.69, con crumble en receta base 55; chat Dirección."
BRANCH_QUANTITIES = {1: 55, 2: 17, 3: 10, 5: 8, 6: 6, 7: 17, 8: 18, 9: 11, 17: 14, 18: 11}
END = PERIOD.replace(day=31)


class Command(BaseCommand):
    help = "Previsualiza las etiquetas de Topping de octubre. --apply guarda solo metadata de la regla 26."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        apply = options["apply"]
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" + ("" if apply else ", READ ONLY"))
                cursor.execute("SET LOCAL statement_timeout='8s'")
                cursor.execute("SET LOCAL lock_timeout='1s'")
            lock_product_month_sources([PERIOD])
            query = RecetaAgrupacionAddon.objects.filter(
                pk=26, activo=True, status="APPROVED", base_receta_id=55, addon_receta_id=516,
                addon_codigo_point="1254", addon_receta__codigo_point="1254",
            )
            if apply:
                query = query.select_for_update(of=("self",))
            rule = query.first()
            product = PointProduct.objects.filter(pk=186, external_id="961", sku="1254").first()
            if rule is None or product is None:
                raise CommandError("La identidad aprobada de regla, receta o producto cambió.")
            for recipe in (rule.base_receta, rule.addon_receta):
                if not recipe.lineas.filter(insumo_id=384, cantidad=20, unidad_texto__iexact="Gr").exists():
                    raise CommandError("La base y el complemento deben conservar los 20 g de crumble comprobados.")
            provenance = []
            costing = MonthlyHistoricalCostingService()
            for component in COMPONENTS:
                ingredient = Insumo.objects.get(pk=component["insumo_id"])
                prices = [row for row in costing._purchase_like_costs(insumo=ingredient, period_end=END)
                          if row.fecha >= PERIOD and row.costo_unitario > 0]
                if [row.pk for row in prices] != component["source_rows"]:
                    raise CommandError("Cambió el conjunto de fuentes de costo de octubre.")
                weighted_total = Decimal("0")
                weight_total = Decimal("0")
                for price in prices:
                    normalized, normalization = _normalized_cost_for_insumo(price, ingredient)
                    if normalized is None or not normalized.is_finite() or normalized <= 0:
                        raise CommandError("Costo de etiqueta o unidad sin confirmar.")
                    weight = _weight_from_raw(price)
                    if not weight.is_finite() or weight <= 0:
                        raise CommandError("Cantidad de compra inválida.")
                    weighted_total += normalized * weight
                    weight_total += weight
                    provenance.append({"insumo_id": ingredient.pk, "source_id": price.pk,
                        "date": price.fecha.isoformat(), "normalized_unit_cost": str(normalized),
                        "weight": str(weight), "normalization": normalization})
                if _q6(weighted_total / weight_total) != Decimal(component["monthly_unit_cost"]):
                    raise CommandError("El costo mensual ya no coincide con el importe autorizado.")
            sales = ventas_para_costeo(PERIOD, END)
            addon_sales = list(sales.filter(product_id=product.pk).values(
                "id", "sale_date", "sucursal_historica_id", "quantity", "gross_amount", "net_amount"))
            by_branch = {}
            daily_addons = {}
            base = {(row["sale_date"], row["sucursal_historica_id"]): row["quantity"]
                    for row in sales.filter(receta_id=55).exclude(product_id=186).values(
                        "sale_date", "sucursal_historica_id").annotate(quantity=Sum("quantity"))}
            for sale in addon_sales:
                quantity = sale["quantity"]
                if (not quantity.is_finite() or quantity <= 0 or sale["gross_amount"] != 0
                        or sale["net_amount"] != 0):
                    raise CommandError("La venta gratuita o su absorción en la base cambió.")
                branch_id = sale["sucursal_historica_id"]
                by_branch[branch_id] = by_branch.get(branch_id, Decimal("0")) + quantity
                key = sale["sale_date"], branch_id
                daily_addons[key] = daily_addons.get(key, Decimal("0")) + quantity
            if any(base.get(key, 0) < quantity for key, quantity in daily_addons.items()):
                raise CommandError("La venta diaria del complemento supera las unidades de su base.")
            if len(addon_sales) != 123 or by_branch != BRANCH_QUANTITIES:
                raise CommandError("Las ventas deben coincidir con las 123 filas y 167 unidades aprobadas.")
            branch_costs = {pk: (quantity * UNIT_COST).quantize(Decimal("0.01"))
                            for pk, quantity in by_branch.items()}
            if sum(branch_costs.values()) != Decimal("167.69"):
                raise CommandError("El reparto por sucursal no coincide con $167.69.")
            treatment = {
                "contract": CONTRACT, "scope": "RENTABILIDAD_SOLD_MATERIAL",
                "authorization": AUTHORIZATION, "period": PERIOD.isoformat(), "rule_id": 26,
                "product_id": 186, "external_id": "961", "sku": "1254", "name": "TOPPING ZANAHORIA C",
                "recipe_id": 516, "base_recipe_id": 55, "unit": "PZA", "unit_cost": str(UNIT_COST),
                "components": deepcopy(list(COMPONENTS)),
                "absorbed_components": [{"insumo_id": 384, "quantity": "20", "unit": "GR", "base_recipe_id": 55}],
                "sold_quantity": "167", "sale_rows": 123, "rounding": "PER_BRANCH_2DP",
                "branch_quantities": {str(pk): str(qty) for pk, qty in by_branch.items()},
                "branch_costs": {str(pk): str(cost) for pk, cost in branch_costs.items()},
                "source_costs": provenance, "source_sale_ids": sorted(row["id"] for row in addon_sales),
            }
            if not isinstance(rule.metadata, dict):
                raise CommandError("La metadata existente requiere revisión.")
            before = deepcopy(rule.metadata)
            metadata = deepcopy(before)
            treatments = metadata.setdefault(METADATA_KEY, {})
            if not isinstance(treatments, dict):
                raise CommandError("El historial de tratamientos requiere revisión.")
            existing = treatments.get(PERIOD.isoformat())
            if existing is not None:
                comparable = {key: value for key, value in existing.items() if key != "applied_at"} if isinstance(existing, dict) else None
                if not valid_treatment(existing) or comparable != treatment:
                    raise CommandError("Ya existe un tratamiento distinto. No se sobrescribe.")
            else:
                treatments[PERIOD.isoformat()] = {**treatment, "applied_at": timezone.now().isoformat()}
                if apply:
                    rule.metadata = metadata
                    rule.save(update_fields=["metadata", "actualizado_en"])
            self.stdout.write(json.dumps({"applied": apply, "already_present": existing is not None,
                "rule_id": rule.pk, "total_incremental": "167.69", "branch_costs": branch_costs,
                "before": before, "after": metadata}, default=str, ensure_ascii=False, sort_keys=True))
