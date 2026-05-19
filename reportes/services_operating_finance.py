from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import logging

from django.db import OperationalError, ProgrammingError, transaction
from django.db.models import Sum

from core.models import Sucursal
from pos_bridge.models import PointDailySale, PointMonthlySalesOfficial
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import Receta, RecetaCostoSemanal
from reportes.models import (
    CategoriaGasto,
    CentroCosto,
    EmpresaResultadoMensual,
    GastoOperativoMensual,
    ProductBusinessRule,
    ProductoCostoOperativoMensual,
    ProductoPricingDecisionMensual,
    ProductoReventaCosto,
    ProductoReventaCostoHistoricoMensual,
    ProductoSucursalContribucionMensual,
    RecetaCostoHistoricoMensual,
    ReglaAsignacionGasto,
)
from reportes.product_business_rules import (
    CRITICAL_FIXED_REVENTA_PRODUCT_NAMES,
    normalize_product_name,
)
from ventas.services.sales_read_service import get_sales_range

logger = logging.getLogger(__name__)
COST_GUARDRAIL_MAX_COGS_TO_SALES_RATIO = Decimal("2.0")

# Productos definidos por negocio como REVENTA fija.
# Confirmación esperada: Dirección / negocio.
# Estos productos no deben impactar costo de fabricación
# ni contribución por receta.
FORCED_NON_RECIPE_RESALE_PRODUCT_NAMES = set(CRITICAL_FIXED_REVENTA_PRODUCT_NAMES)


def month_bounds(period_start: date) -> tuple[date, date]:
    next_month = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return period_start, next_month - timedelta(days=1)


def as_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass
class OperatingSnapshotSummary:
    period_start: date
    product_cost_rows: int
    branch_contribution_rows: int
    pricing_rows: int
    company_result_created: bool


DEFAULT_CATEGORY_DEFINITIONS = [
    {
        "codigo": "MANO_OBRA_PROD",
        "nombre": "Mano de obra producción",
        "capa_objetivo": CategoriaGasto.CAPA_FABRICACION,
        "bucket": CategoriaGasto.BUCKET_MANO_OBRA,
        "impacta_costo_producto": True,
        "impacta_contribucion_sucursal": False,
    },
    {
        "codigo": "INDIRECTO_PROD",
        "nombre": "Indirecto producción",
        "capa_objetivo": CategoriaGasto.CAPA_FABRICACION,
        "bucket": CategoriaGasto.BUCKET_INDIRECTO,
        "impacta_costo_producto": True,
        "impacta_contribucion_sucursal": False,
    },
    {
        "codigo": "EMPAQUE_PROD",
        "nombre": "Empaque producción",
        "capa_objetivo": CategoriaGasto.CAPA_FABRICACION,
        "bucket": CategoriaGasto.BUCKET_EMPAQUE,
        "impacta_costo_producto": True,
        "impacta_contribucion_sucursal": False,
    },
    {
        "codigo": "OPEX_TOTAL_SUC",
        "nombre": "Gasto operativo total sucursal",
        "capa_objetivo": CategoriaGasto.CAPA_SUCURSAL,
        "bucket": CategoriaGasto.BUCKET_COMERCIAL,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": True,
    },
    {
        "codigo": "RENTA_SUC",
        "nombre": "Renta sucursal",
        "capa_objetivo": CategoriaGasto.CAPA_SUCURSAL,
        "bucket": CategoriaGasto.BUCKET_COMERCIAL,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": True,
    },
    {
        "codigo": "NOMINA_SUC",
        "nombre": "Nómina sucursal",
        "capa_objetivo": CategoriaGasto.CAPA_SUCURSAL,
        "bucket": CategoriaGasto.BUCKET_COMERCIAL,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": True,
    },
    {
        "codigo": "INDIRECTO_SUC",
        "nombre": "Indirecto sucursal",
        "capa_objetivo": CategoriaGasto.CAPA_SUCURSAL,
        "bucket": CategoriaGasto.BUCKET_COMERCIAL,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": True,
    },
    {
        "codigo": "PLATAFORMAS",
        "nombre": "Plataformas y comisiones",
        "capa_objetivo": CategoriaGasto.CAPA_SUCURSAL,
        "bucket": CategoriaGasto.BUCKET_COMERCIAL,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": True,
    },
    {
        "codigo": "DELIVERY",
        "nombre": "Delivery",
        "capa_objetivo": CategoriaGasto.CAPA_SUCURSAL,
        "bucket": CategoriaGasto.BUCKET_LOGISTICA,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": True,
    },
    {
        "codigo": "ADMIN_CORP",
        "nombre": "Administración corporativa",
        "capa_objetivo": CategoriaGasto.CAPA_EMPRESA,
        "bucket": CategoriaGasto.BUCKET_CORPORATIVO,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": False,
    },
    {
        "codigo": "SISTEMAS_CORP",
        "nombre": "Sistemas corporativo",
        "capa_objetivo": CategoriaGasto.CAPA_EMPRESA,
        "bucket": CategoriaGasto.BUCKET_CORPORATIVO,
        "impacta_costo_producto": False,
        "impacta_contribucion_sucursal": False,
    },
]


class OperatingFinanceBootstrapService:
    def bootstrap(self) -> dict[str, int]:
        created = {
            "centros_costo": 0,
            "categorias_gasto": 0,
            "reglas_asignacion": 0,
        }

        centros = [
            ("CEDIS", "CEDIS", CentroCosto.TIPO_CEDIS, None),
            ("PROD", "Producción central", CentroCosto.TIPO_PRODUCCION, None),
            ("CORP", "Corporativo", CentroCosto.TIPO_CORPORATIVO, None),
            ("LOG", "Logística", CentroCosto.TIPO_LOGISTICA, None),
        ]
        for codigo, nombre, tipo, sucursal in centros:
            _, was_created = CentroCosto.objects.get_or_create(
                codigo=codigo,
                defaults={"nombre": nombre, "tipo": tipo, "sucursal": sucursal, "activo": True},
            )
            created["centros_costo"] += int(was_created)

        for sucursal in Sucursal.objects.order_by("codigo"):
            _, was_created = CentroCosto.objects.get_or_create(
                codigo=f"SUC_{sucursal.codigo}",
                defaults={
                    "nombre": f"Sucursal {sucursal.nombre}",
                    "tipo": CentroCosto.TIPO_SUCURSAL,
                    "sucursal": sucursal,
                    "activo": True,
                },
            )
            created["centros_costo"] += int(was_created)

        for definition in DEFAULT_CATEGORY_DEFINITIONS:
            _, was_created = CategoriaGasto.objects.get_or_create(
                codigo=definition["codigo"],
                defaults={**definition, "impacta_utilidad_empresa": True, "activo": True},
            )
            created["categorias_gasto"] += int(was_created)

        default_rules = [
            ("MO producción por costo MP", "MANO_OBRA_PROD", "PROD", ReglaAsignacionGasto.BASE_COSTO_MP),
            ("Indirecto producción por costo MP", "INDIRECTO_PROD", "PROD", ReglaAsignacionGasto.BASE_COSTO_MP),
            ("Empaque producción por unidades", "EMPAQUE_PROD", "PROD", ReglaAsignacionGasto.BASE_UNIDADES),
            ("Gasto operativo total sucursal por ventas", "OPEX_TOTAL_SUC", None, ReglaAsignacionGasto.BASE_VENTAS),
            ("Renta sucursal por ventas", "RENTA_SUC", None, ReglaAsignacionGasto.BASE_VENTAS),
            ("Nómina sucursal por ventas", "NOMINA_SUC", None, ReglaAsignacionGasto.BASE_VENTAS),
            ("Indirecto sucursal por ventas", "INDIRECTO_SUC", None, ReglaAsignacionGasto.BASE_VENTAS),
            ("Plataformas por ventas", "PLATAFORMAS", None, ReglaAsignacionGasto.BASE_VENTAS),
            ("Delivery por ventas", "DELIVERY", None, ReglaAsignacionGasto.BASE_VENTAS),
            ("Corporativo sin reparto a producto", "ADMIN_CORP", "CORP", ReglaAsignacionGasto.BASE_NONE),
            ("Sistemas sin reparto a producto", "SISTEMAS_CORP", "CORP", ReglaAsignacionGasto.BASE_NONE),
        ]
        for nombre, categoria_codigo, centro_codigo, base in default_rules:
            categoria = CategoriaGasto.objects.get(codigo=categoria_codigo)
            centro = CentroCosto.objects.filter(codigo=centro_codigo).first() if centro_codigo else None
            _, was_created = ReglaAsignacionGasto.objects.get_or_create(
                nombre=nombre,
                categoria_gasto=categoria,
                centro_costo=centro,
                defaults={"base_reparto": base, "activo": True},
            )
            created["reglas_asignacion"] += int(was_created)
        return created


class OperatingFinanceSnapshotService:
    def __init__(self) -> None:
        self.sales_matcher = PointSalesMatchingService()
        self._resolved_recipe_cache: dict[tuple[str, str], Receta | None] = {}
        self._product_business_rule_cache: dict[str, tuple[str, bool]] | None = None

    def _build_sales_payload(self, row: PointDailySale) -> dict:
        raw_payload = row.raw_payload or {}
        return {
            "family": raw_payload.get("family") or raw_payload.get("Familia") or "",
            "category": (
                (row.product.category if row.product_id else "")
                or raw_payload.get("category")
                or raw_payload.get("Categoria")
                or ""
            ),
            "name": row.product.name if row.product_id else "",
            "sku": row.product.sku if row.product_id else "",
        }

    def _forced_non_recipe_bucket_for_row(self, row: PointDailySale) -> str | None:
        product_name = normalize_product_name(row.product.name if row.product_id else "")
        fixed_classification = self._fixed_classification_for_product_name(product_name)
        if fixed_classification in {
            ProductBusinessRule.CLASSIFICATION_REVENTA,
            ProductBusinessRule.CLASSIFICATION_ACCESORIO,
            ProductBusinessRule.CLASSIFICATION_SERVICIO,
        }:
            return fixed_classification
        if product_name in FORCED_NON_RECIPE_RESALE_PRODUCT_NAMES:
            return "REVENTA"
        return None

    def _load_product_business_rule_cache(self) -> dict[str, tuple[str, bool]]:
        if self._product_business_rule_cache is None:
            try:
                self._product_business_rule_cache = {
                    row.normalized_name: (row.classification, row.is_fixed)
                    for row in ProductBusinessRule.objects.all().only("normalized_name", "classification", "is_fixed")
                    if row.normalized_name
                }
            except (OperationalError, ProgrammingError):
                # Keep production behavior stable during the rollout window
                # before the ProductBusinessRule migration is applied.
                self._product_business_rule_cache = {}
        return self._product_business_rule_cache

    def _fixed_classification_for_product_name(self, product_name: str) -> str | None:
        if not product_name:
            return None
        cached = self._load_product_business_rule_cache().get(product_name)
        if not cached:
            return None
        classification, is_fixed = cached
        if not is_fixed:
            return None
        return classification

    def _is_non_recipe_commercial_row(self, row: PointDailySale) -> bool:
        if row.receta_id and row.receta is not None:
            if row.receta.modo_costeo == Receta.MODO_COSTEO_SERVICIO:
                return True
            if row.receta.modo_costeo == Receta.MODO_COSTEO_REVENTA:
                return False
        return self.sales_matcher.infer_cost_mode(self._build_sales_payload(row)) == Receta.MODO_COSTEO_SERVICIO

    def _resolve_recipe_for_row(self, row: PointDailySale) -> tuple[Receta | None, bool]:
        if row.receta_id and row.receta is not None:
            return row.receta, False
        if not row.product_id:
            return None, False

        cache_key = (row.product.sku or "", row.product.name or "")
        if cache_key not in self._resolved_recipe_cache:
            self._resolved_recipe_cache[cache_key] = self.sales_matcher.resolve_receta(
                codigo_point=row.product.sku or "",
                point_name=row.product.name or "",
            )
        receta = self._resolved_recipe_cache[cache_key]
        return receta, receta is not None

    def _is_non_recipe_commercial_row_for_recipe(
        self,
        row: PointDailySale,
        receta: Receta | None,
    ) -> bool:
        if self._forced_non_recipe_bucket_for_row(row) is not None:
            return True
        if receta is not None:
            if receta.modo_costeo == Receta.MODO_COSTEO_SERVICIO:
                return True
            if receta.modo_costeo == Receta.MODO_COSTEO_REVENTA:
                return False
        return self._is_non_recipe_commercial_row(row)

    def _resolve_allocation_rule(
        self,
        *,
        categoria: CategoriaGasto,
        centro_costo: CentroCosto | None,
    ) -> ReglaAsignacionGasto | None:
        if centro_costo is not None:
            exact_rule = (
                ReglaAsignacionGasto.objects.filter(
                    activo=True,
                    categoria_gasto=categoria,
                    centro_costo=centro_costo,
                )
                .order_by("prioridad", "id")
                .first()
            )
            if exact_rule is not None:
                return exact_rule
        return (
            ReglaAsignacionGasto.objects.filter(
                activo=True,
                categoria_gasto=categoria,
                centro_costo__isnull=True,
            )
            .order_by("prioridad", "id")
            .first()
        )

    def _latest_recipe_costs(self, *, period_start: date, period_end: date) -> dict[int, dict]:
        latest: dict[int, dict] = {}
        monthly_rows = (
            RecetaCostoHistoricoMensual.objects.filter(
                periodo=period_start,
                receta__tipo="PRODUCTO_FINAL",
            )
            .select_related("receta")
            .order_by("receta_id")
        )
        for row in monthly_rows:
            monthly_unit_cost = as_decimal(row.costo_por_unidad_rendimiento)
            unit_cost_field = "costo_por_unidad_rendimiento"
            if monthly_unit_cost <= 0:
                monthly_unit_cost = as_decimal(row.costo_total)
                unit_cost_field = "costo_total"
            latest[row.receta_id] = {
                "costo_mp": monthly_unit_cost,
                "costo_total": as_decimal(row.costo_total),
                "source": "MONTHLY_HISTORICAL",
                "source_period": row.periodo.isoformat(),
                "unit_cost_field": unit_cost_field,
            }

        qs = (
            RecetaCostoSemanal.objects.filter(
                scope_type=RecetaCostoSemanal.SCOPE_RECIPE,
                week_start__lte=period_end,
                receta__tipo="PRODUCTO_FINAL",
            )
            .select_related("receta")
            .order_by("receta_id", "-week_start", "-id")
        )
        for row in qs:
            if row.receta_id in latest:
                continue
            latest[row.receta_id] = {
                "costo_mp": as_decimal(row.costo_mp),
                "costo_total": as_decimal(row.costo_total),
                "source": "WEEKLY_SNAPSHOT",
                "source_period": row.week_start.isoformat(),
                "unit_cost_field": "costo_mp",
            }
        return latest

    def _apply_product_cost_guardrail(
        self,
        *,
        receta_id: int,
        cost_components: dict[str, Decimal],
        asp: Decimal,
        source: dict,
    ) -> tuple[dict[str, Decimal], dict]:
        costo_fabricacion_unit = sum(cost_components.values(), Decimal("0"))
        metadata = {
            "guardrail_applied": False,
            "cost_source": source.get("source", ""),
            "cost_source_period": source.get("source_period", ""),
            "unit_cost_field": source.get("unit_cost_field", ""),
        }
        if asp <= 0 or costo_fabricacion_unit <= 0:
            return cost_components, metadata

        max_allowed = asp * COST_GUARDRAIL_MAX_COGS_TO_SALES_RATIO
        if costo_fabricacion_unit <= max_allowed:
            return cost_components, metadata

        metadata.update(
            {
                "guardrail_applied": True,
                "guardrail_reason": "COSTO_FABRICACION_UNIT_GT_2X_ASP",
                "guardrail_threshold_ratio": str(COST_GUARDRAIL_MAX_COGS_TO_SALES_RATIO),
                "guardrail_asp": str(asp),
                "raw_costo_mp_unit": str(cost_components["costo_mp_unit"]),
                "raw_mano_obra_prod_unit": str(cost_components["mano_obra_prod_unit"]),
                "raw_indirecto_prod_unit": str(cost_components["indirecto_prod_unit"]),
                "raw_empaque_prod_unit": str(cost_components["empaque_prod_unit"]),
                "raw_costo_fabricacion_unit": str(costo_fabricacion_unit),
                "receta_id": receta_id,
            }
        )
        return (
            {
                "costo_mp_unit": Decimal("0"),
                "mano_obra_prod_unit": Decimal("0"),
                "indirecto_prod_unit": Decimal("0"),
                "empaque_prod_unit": Decimal("0"),
            },
            metadata,
        )

    def _sales_by_recipe_branch(self, period_start: date, period_end: date) -> dict[tuple[int, int], dict]:
        data: dict[tuple[int, int], dict] = {}
        dynamic_recipe_resolutions = 0
        rows = (
            PointDailySale.objects.filter(sale_date__range=(period_start, period_end), receta__isnull=False)
            .select_related("product", "branch__erp_branch", "receta")
            .order_by("id")
        )
        unresolved_rows = (
            PointDailySale.objects.filter(sale_date__range=(period_start, period_end), receta__isnull=True)
            .select_related("product", "branch__erp_branch", "receta")
            .order_by("id")
        )
        for row in rows:
            receta, _ = self._resolve_recipe_for_row(row)
            if self._is_non_recipe_commercial_row_for_recipe(row, receta):
                continue
            sucursal_id = row.branch.erp_branch_id if row.branch_id else None
            receta_id = receta.id if receta is not None else None
            if not receta_id or not sucursal_id:
                continue
            key = (receta_id, sucursal_id)
            bucket = data.setdefault(key, {"unidades": Decimal("0"), "venta": Decimal("0"), "asp": Decimal("0")})
            bucket["unidades"] += as_decimal(row.quantity)
            bucket["venta"] += as_decimal(row.total_amount)
        for row in unresolved_rows:
            receta, resolved_dynamically = self._resolve_recipe_for_row(row)
            if resolved_dynamically:
                dynamic_recipe_resolutions += 1
            if receta is None:
                continue
            if self._is_non_recipe_commercial_row_for_recipe(row, receta):
                continue
            sucursal_id = row.branch.erp_branch_id if row.branch_id else None
            receta_id = receta.id
            if not receta_id or not sucursal_id:
                continue
            key = (receta_id, sucursal_id)
            bucket = data.setdefault(key, {"unidades": Decimal("0"), "venta": Decimal("0"), "asp": Decimal("0")})
            bucket["unidades"] += as_decimal(row.quantity)
            bucket["venta"] += as_decimal(row.total_amount)
        for payload in data.values():
            payload["asp"] = (payload["venta"] / payload["unidades"]) if payload["unidades"] > 0 else Decimal("0")
        if dynamic_recipe_resolutions > 0:
            logger.info(
                "operating_finance dynamically resolved recipe rows in sales_by_recipe_branch",
                extra={
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "dynamic_recipe_resolutions": dynamic_recipe_resolutions,
                },
            )
        return data

    def _company_sales_total(self, period_start: date, period_end: date) -> tuple[Decimal, str]:
        official = PointMonthlySalesOfficial.objects.filter(month_start=period_start).first()
        if official is not None:
            return as_decimal(official.total_amount), "POINT_MONTHLY_OFFICIAL"
        aggregate = get_sales_range(
            start_date=period_start,
            end_date=period_end,
            coverage_policy="prefer_complete",
        )
        source = str(aggregate.get("source") or "none").upper()
        return as_decimal(aggregate.get("monto")), f"SALES_READ_{source}"

    def _split_unmapped_sales(self, period_start: date, period_end: date) -> tuple[Decimal, Decimal]:
        non_recipe_total = Decimal("0")
        candidate_recipe_total = Decimal("0")
        dynamic_recipe_resolutions = 0
        rows = (
            PointDailySale.objects.filter(
                sale_date__range=(period_start, period_end),
            )
            .select_related("product", "receta")
            .order_by("id")
        )
        for row in rows:
            amount = as_decimal(row.total_amount)
            receta, resolved_dynamically = self._resolve_recipe_for_row(row)
            if resolved_dynamically:
                dynamic_recipe_resolutions += 1
            if receta is not None and not self._is_non_recipe_commercial_row_for_recipe(row, receta):
                continue
            if self._is_non_recipe_commercial_row_for_recipe(row, receta):
                non_recipe_total += amount
            else:
                candidate_recipe_total += amount
        if dynamic_recipe_resolutions > 0:
            logger.info(
                "operating_finance dynamically resolved recipe rows in split_unmapped_sales",
                extra={
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "dynamic_recipe_resolutions": dynamic_recipe_resolutions,
                },
            )
        return non_recipe_total, candidate_recipe_total

    def _resale_cost_total(self, period_start: date, period_end: date) -> tuple[Decimal, dict[str, Decimal | int]]:
        resale_rows = []
        all_product_ids = set()
        dynamic_recipe_resolutions = 0
        rows = (
            PointDailySale.objects.filter(
                sale_date__range=(period_start, period_end),
                product_id__isnull=False,
            )
            .select_related("product", "receta")
            .order_by("id")
        )
        row_buffer = list(rows)
        for row in row_buffer:
            all_product_ids.add(row.product_id)

        costs: dict[int, Decimal] = {}
        for historical in ProductoReventaCostoHistoricoMensual.objects.filter(
            producto_point_id__in=all_product_ids,
            periodo=period_start,
        ):
            costs[historical.producto_point_id] = as_decimal(historical.costo_promedio)

        missing_product_ids = [product_id for product_id in all_product_ids if product_id not in costs]
        if missing_product_ids:
            for current in ProductoReventaCosto.objects.filter(
                producto_point_id__in=missing_product_ids,
                fecha_vigencia__lte=period_end,
            ).order_by("producto_point_id", "-fecha_vigencia", "-id"):
                if current.producto_point_id not in costs:
                    costs[current.producto_point_id] = as_decimal(current.costo_unitario)

        forced_resale_names = {
            name
            for name, (classification, is_fixed) in self._load_product_business_rule_cache().items()
            if is_fixed and classification == ProductBusinessRule.CLASSIFICATION_REVENTA
        }
        for row in row_buffer:
            receta, resolved_dynamically = self._resolve_recipe_for_row(row)
            if resolved_dynamically:
                dynamic_recipe_resolutions += 1
            product_name = normalize_product_name(row.product.name if row.product_id else "")
            forced_bucket = self._forced_non_recipe_bucket_for_row(row)
            is_recipe_resale = receta is not None and receta.modo_costeo == Receta.MODO_COSTEO_REVENTA
            is_fixed_resale = forced_bucket == ProductBusinessRule.CLASSIFICATION_REVENTA or product_name in forced_resale_names
            has_resale_cost = row.product_id in costs
            if not is_recipe_resale and not is_fixed_resale and not has_resale_cost:
                continue
            resale_rows.append(row)

        total = Decimal("0")
        rows_with_cost = 0
        rows_without_cost = 0
        sale_total = Decimal("0")
        for row in resale_rows:
            sale_total += as_decimal(row.total_amount)
            cost = costs.get(row.product_id)
            if cost is None:
                rows_without_cost += 1
                continue
            rows_with_cost += 1
            total += cost * as_decimal(row.quantity)

        return total.quantize(Decimal("0.01")), {
            "venta_reventa_total": sale_total.quantize(Decimal("0.01")),
            "reventa_rows": len(resale_rows),
            "reventa_rows_with_cost": rows_with_cost,
            "reventa_rows_without_cost": rows_without_cost,
            "reventa_products_with_cost": len(costs),
            "dynamic_recipe_resolutions": dynamic_recipe_resolutions,
        }

    def _profitability_totals(self, period_start: date) -> dict[str, Decimal | int]:
        try:
            from rentabilidad.models_rentabilidad import SucursalRentabilidad
        except Exception:
            return {}

        rows = list(SucursalRentabilidad.objects.filter(periodo=period_start))
        if not rows:
            return {}

        ventas_brutas = sum((as_decimal(row.ventas_brutas) for row in rows), Decimal("0"))
        descuentos = sum((as_decimal(row.descuentos) for row in rows), Decimal("0"))
        devoluciones = sum((as_decimal(row.devoluciones) for row in rows), Decimal("0"))
        costo_materia_prima = sum((as_decimal(row.costo_materia_prima) for row in rows), Decimal("0"))
        costo_reventa = sum((as_decimal(row.costo_reventa) for row in rows), Decimal("0"))
        gasto_fijo = sum(
            (
                as_decimal(row.renta)
                + as_decimal(row.nomina_directa)
                + as_decimal(row.servicios_luz_agua)
                + as_decimal(row.mantenimiento)
                + as_decimal(row.gastos_admin_prorrateados)
                + as_decimal(row.otros_gastos_fijos)
                for row in rows
            ),
            Decimal("0"),
        )
        ventas_netas = ventas_brutas - descuentos - devoluciones
        return {
            "rows": len(rows),
            "ventas_netas": ventas_netas.quantize(Decimal("0.01")),
            "costo_materia_prima": costo_materia_prima.quantize(Decimal("0.01")),
            "costo_reventa": costo_reventa.quantize(Decimal("0.01")),
            "gasto_fijo": gasto_fijo.quantize(Decimal("0.01")),
        }

    def _allocate_amounts(
        self,
        *,
        basis: str,
        total_amount: Decimal,
        base_map: dict[object, Decimal],
    ) -> dict[object, Decimal]:
        denominator = sum((value for value in base_map.values()), Decimal("0"))
        if total_amount == 0 or denominator <= 0:
            return {key: Decimal("0") for key in base_map}
        return {key: (total_amount * value / denominator) for key, value in base_map.items()}

    @transaction.atomic
    def build_snapshot(
        self,
        *,
        period_start: date,
        gross_margin_target: Decimal = Decimal("0.65"),
        contribution_margin_target: Decimal = Decimal("0.18"),
    ) -> OperatingSnapshotSummary:
        period_start, period_end = month_bounds(period_start)
        sales = self._sales_by_recipe_branch(period_start, period_end)
        cost_map = self._latest_recipe_costs(period_start=period_start, period_end=period_end)

        recipe_totals: dict[int, dict] = defaultdict(lambda: {"units": Decimal("0"), "sales": Decimal("0")})
        branch_totals: dict[int, dict] = defaultdict(lambda: {"units": Decimal("0"), "sales": Decimal("0")})
        for (receta_id, sucursal_id), payload in sales.items():
            recipe_totals[receta_id]["units"] += payload["unidades"]
            recipe_totals[receta_id]["sales"] += payload["venta"]
            branch_totals[sucursal_id]["units"] += payload["unidades"]
            branch_totals[sucursal_id]["sales"] += payload["venta"]

        manufacturing_bucket_allocations: dict[str, dict[int, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
        branch_commercial_allocations: dict[str, dict[tuple[int, int], Decimal]] = defaultdict(lambda: defaultdict(Decimal))
        company_expense_total = Decimal("0")

        gastos = (
            GastoOperativoMensual.objects.filter(
                periodo=period_start,
                tipo_dato=GastoOperativoMensual.TIPO_DATO_REAL,
            )
            .select_related("centro_costo", "categoria_gasto")
            .order_by("id")
        )
        for gasto in gastos:
            categoria = gasto.categoria_gasto
            regla = self._resolve_allocation_rule(categoria=categoria, centro_costo=gasto.centro_costo)
            base_reparto = regla.base_reparto if regla is not None else ReglaAsignacionGasto.BASE_NONE
            monto = as_decimal(gasto.monto)

            if categoria.capa_objetivo == CategoriaGasto.CAPA_FABRICACION:
                if base_reparto == ReglaAsignacionGasto.BASE_COSTO_MP:
                    base_map = {
                        receta_id: recipe_totals[receta_id]["units"] * cost_map.get(receta_id, {}).get("costo_mp", Decimal("0"))
                        for receta_id in recipe_totals
                    }
                elif base_reparto == ReglaAsignacionGasto.BASE_UNIDADES:
                    base_map = {receta_id: recipe_totals[receta_id]["units"] for receta_id in recipe_totals}
                else:
                    base_map = {}
                allocated = self._allocate_amounts(basis=base_reparto, total_amount=monto, base_map=base_map)
                for receta_id, value in allocated.items():
                    manufacturing_bucket_allocations[categoria.bucket][receta_id] += value
            elif categoria.capa_objetivo == CategoriaGasto.CAPA_SUCURSAL:
                if gasto.centro_costo.sucursal_id:
                    sucursal_id = gasto.centro_costo.sucursal_id
                    if base_reparto == ReglaAsignacionGasto.BASE_UNIDADES:
                        base_map = {
                            (receta_id, branch_id): payload["unidades"]
                            for (receta_id, branch_id), payload in sales.items()
                            if branch_id == sucursal_id
                        }
                    else:
                        base_map = {
                            (receta_id, branch_id): payload["venta"]
                            for (receta_id, branch_id), payload in sales.items()
                            if branch_id == sucursal_id
                        }
                    allocated = self._allocate_amounts(basis=base_reparto, total_amount=monto, base_map=base_map)
                    for key, value in allocated.items():
                        branch_commercial_allocations[categoria.codigo][key] += value
            else:
                company_expense_total += monto

        product_rows = 0
        for receta_id, totals in recipe_totals.items():
            units = totals["units"]
            sales_total = totals["sales"]
            asp = sales_total / units if units > 0 else Decimal("0")
            base_cost = cost_map.get(receta_id, {})
            costo_mp = base_cost.get("costo_mp", Decimal("0"))
            manufacturing_by_bucket = {
                bucket: amounts.get(receta_id, Decimal("0"))
                for bucket, amounts in manufacturing_bucket_allocations.items()
            }
            mano_obra_unit = manufacturing_by_bucket.get(CategoriaGasto.BUCKET_MANO_OBRA, Decimal("0")) / units if units > 0 else Decimal("0")
            indirecto_unit = manufacturing_by_bucket.get(CategoriaGasto.BUCKET_INDIRECTO, Decimal("0")) / units if units > 0 else Decimal("0")
            empaque_unit = manufacturing_by_bucket.get(CategoriaGasto.BUCKET_EMPAQUE, Decimal("0")) / units if units > 0 else Decimal("0")
            cost_components, cost_metadata = self._apply_product_cost_guardrail(
                receta_id=receta_id,
                asp=asp,
                source=base_cost,
                cost_components={
                    "costo_mp_unit": costo_mp,
                    "mano_obra_prod_unit": mano_obra_unit,
                    "indirecto_prod_unit": indirecto_unit,
                    "empaque_prod_unit": empaque_unit,
                },
            )
            costo_mp = cost_components["costo_mp_unit"]
            mano_obra_unit = cost_components["mano_obra_prod_unit"]
            indirecto_unit = cost_components["indirecto_prod_unit"]
            empaque_unit = cost_components["empaque_prod_unit"]
            costo_fabricacion_unit = costo_mp + mano_obra_unit + indirecto_unit + empaque_unit
            ProductoCostoOperativoMensual.objects.update_or_create(
                periodo=period_start,
                receta_id=receta_id,
                defaults={
                    "unidades_base": units,
                    "venta_total": sales_total,
                    "asp": asp,
                    "costo_mp_unit": costo_mp,
                    "mano_obra_prod_unit": mano_obra_unit,
                    "indirecto_prod_unit": indirecto_unit,
                    "empaque_prod_unit": empaque_unit,
                    "costo_fabricacion_unit": costo_fabricacion_unit,
                    "metadata": {
                        "period_end": period_end.isoformat(),
                        **cost_metadata,
                    },
                },
            )
            product_rows += 1

        branch_rows = 0
        for (receta_id, sucursal_id), payload in sales.items():
            units = payload["unidades"]
            sales_total = payload["venta"]
            asp = payload["asp"]
            cost_row = ProductoCostoOperativoMensual.objects.filter(periodo=period_start, receta_id=receta_id).first()
            costo_producto_unit = as_decimal(cost_row.costo_fabricacion_unit if cost_row else 0)
            costo_producto_total = costo_producto_unit * units
            gasto_comercial_total = sum(
                allocations.get((receta_id, sucursal_id), Decimal("0"))
                for allocations in branch_commercial_allocations.values()
            )
            gasto_comercial_unit = gasto_comercial_total / units if units > 0 else Decimal("0")
            contribucion_total = sales_total - costo_producto_total - gasto_comercial_total
            contribucion_unit = contribucion_total / units if units > 0 else Decimal("0")
            margen_contribucion_pct = contribucion_total / sales_total if sales_total > 0 else Decimal("0")
            ProductoSucursalContribucionMensual.objects.update_or_create(
                periodo=period_start,
                receta_id=receta_id,
                sucursal_id=sucursal_id,
                defaults={
                    "unidades_vendidas": units,
                    "venta_total": sales_total,
                    "asp": asp,
                    "costo_producto_unit": costo_producto_unit,
                    "costo_producto_total": costo_producto_total,
                    "gasto_comercial_unit": gasto_comercial_unit,
                    "gasto_comercial_total": gasto_comercial_total,
                    "contribucion_total": contribucion_total,
                    "contribucion_unit": contribucion_unit,
                    "margen_contribucion_pct": margen_contribucion_pct,
                    "metadata": {},
                },
            )
            branch_rows += 1

        venta_costeada_total = sum((payload["venta"] for payload in sales.values()), Decimal("0"))
        venta_total, sales_total_source = self._company_sales_total(period_start, period_end)
        venta_sin_mapear_total = max(venta_total - venta_costeada_total, Decimal("0"))
        venta_no_receta_total, venta_receta_sin_match_total = self._split_unmapped_sales(period_start, period_end)
        costo_fabricacion_total_calculado = sum(
            (row.costo_producto_total for row in ProductoSucursalContribucionMensual.objects.filter(periodo=period_start)),
            Decimal("0"),
        )
        costo_materia_prima_total_calculado = sum(
            (row.costo_mp_unit * row.unidades_base for row in ProductoCostoOperativoMensual.objects.filter(periodo=period_start)),
            Decimal("0"),
        ).quantize(Decimal("0.01"))
        mano_obra_prod_total = sum(
            (row.mano_obra_prod_unit * row.unidades_base for row in ProductoCostoOperativoMensual.objects.filter(periodo=period_start)),
            Decimal("0"),
        ).quantize(Decimal("0.01"))
        indirecto_prod_total = sum(
            (row.indirecto_prod_unit * row.unidades_base for row in ProductoCostoOperativoMensual.objects.filter(periodo=period_start)),
            Decimal("0"),
        ).quantize(Decimal("0.01"))
        empaque_prod_total = sum(
            (row.empaque_prod_unit * row.unidades_base for row in ProductoCostoOperativoMensual.objects.filter(periodo=period_start)),
            Decimal("0"),
        ).quantize(Decimal("0.01"))
        costo_reventa_total_calculado, resale_metadata = self._resale_cost_total(period_start, period_end)
        gasto_comercial_total_calculado = sum(
            (row.gasto_comercial_total for row in ProductoSucursalContribucionMensual.objects.filter(periodo=period_start)),
            Decimal("0"),
        )
        profitability_totals = self._profitability_totals(period_start)
        uses_profitability_source = bool(profitability_totals)
        venta_financiera_total = as_decimal(
            profitability_totals.get("ventas_netas") if uses_profitability_source else venta_total
        )
        costo_materia_prima_total = as_decimal(
            profitability_totals.get("costo_materia_prima") if uses_profitability_source else costo_materia_prima_total_calculado
        )
        costo_reventa_total = as_decimal(
            profitability_totals.get("costo_reventa") if uses_profitability_source else costo_reventa_total_calculado
        )
        gasto_comercial_total = as_decimal(
            profitability_totals.get("gasto_fijo") if uses_profitability_source else gasto_comercial_total_calculado
        )
        costo_fabricacion_total = (
            costo_materia_prima_total + mano_obra_prod_total + indirecto_prod_total + empaque_prod_total
        ).quantize(Decimal("0.01"))
        margen_bruto_total = venta_financiera_total - costo_materia_prima_total - costo_reventa_total
        contribucion_total = margen_bruto_total - gasto_comercial_total
        utilidad_operativa_total = contribucion_total - company_expense_total
        if not uses_profitability_source:
            utilidad_operativa_total -= mano_obra_prod_total + indirecto_prod_total + empaque_prod_total
        _, company_created = EmpresaResultadoMensual.objects.update_or_create(
            periodo=period_start,
            defaults={
                "venta_total": venta_financiera_total,
                "costo_materia_prima_total": costo_materia_prima_total,
                "costo_reventa_total": costo_reventa_total,
                "mano_obra_prod_total": mano_obra_prod_total,
                "indirecto_prod_total": indirecto_prod_total,
                "empaque_prod_total": empaque_prod_total,
                "costo_fabricacion_total": costo_fabricacion_total,
                "margen_bruto_total": margen_bruto_total,
                "gasto_comercial_total": gasto_comercial_total,
                "contribucion_total": contribucion_total,
                "gasto_corporativo_total": company_expense_total,
                "utilidad_operativa_total": utilidad_operativa_total,
                "metadata": {
                    "period_end": period_end.isoformat(),
                    "sales_total_source": sales_total_source,
                    "financial_totals_source": (
                        "RENTABILIDAD_SUCURSAL" if uses_profitability_source else "OPERATING_FINANCE_CALCULATED"
                    ),
                    "rentabilidad_rows": int(profitability_totals.get("rows", 0)) if uses_profitability_source else 0,
                    "rentabilidad_ventas_netas": str(profitability_totals.get("ventas_netas", "0")) if uses_profitability_source else "0",
                    "venta_total_calculada": str(venta_total),
                    "costo_materia_prima_calculado": str(costo_materia_prima_total_calculado),
                    "costo_reventa_calculado": str(costo_reventa_total_calculado),
                    "gasto_comercial_calculado": str(gasto_comercial_total_calculado),
                    "costo_fabricacion_calculado": str(costo_fabricacion_total_calculado),
                    "venta_costeada_total": str(venta_costeada_total),
                    "venta_sin_mapear_total": str(venta_sin_mapear_total),
                    "venta_no_receta_total": str(venta_no_receta_total),
                    "venta_receta_sin_match_total": str(venta_receta_sin_match_total),
                    "venta_reventa_total": str(resale_metadata["venta_reventa_total"]),
                    "reventa_rows": resale_metadata["reventa_rows"],
                    "reventa_rows_with_cost": resale_metadata["reventa_rows_with_cost"],
                    "reventa_rows_without_cost": resale_metadata["reventa_rows_without_cost"],
                    "reventa_products_with_cost": resale_metadata["reventa_products_with_cost"],
                    "sales_mapping_coverage_pct": str(
                        (venta_costeada_total / venta_total * Decimal("100")) if venta_total > 0 else Decimal("0")
                    ),
                },
            },
        )

        pricing_rows = 0
        aggregated_branch = (
            ProductoSucursalContribucionMensual.objects.filter(periodo=period_start)
            .values("receta_id")
            .annotate(
                unidades=Sum("unidades_vendidas"),
                venta=Sum("venta_total"),
                contribucion=Sum("contribucion_total"),
            )
        )
        for row in aggregated_branch:
            receta_id = row["receta_id"]
            units = as_decimal(row["unidades"])
            sales_total = as_decimal(row["venta"])
            contribucion_total_producto = as_decimal(row["contribucion"])
            cost_row = ProductoCostoOperativoMensual.objects.filter(periodo=period_start, receta_id=receta_id).first()
            if cost_row is None:
                continue
            asp = sales_total / units if units > 0 else Decimal("0")
            costo_fabricacion_unit = as_decimal(cost_row.costo_fabricacion_unit)
            contribucion_unit = contribucion_total_producto / units if units > 0 else Decimal("0")
            margen_bruto_pct = (asp - costo_fabricacion_unit) / asp if asp > 0 else Decimal("0")
            margen_contribucion_pct = contribucion_total_producto / sales_total if sales_total > 0 else Decimal("0")
            precio_objetivo_bruto = (
                costo_fabricacion_unit / (Decimal("1") - gross_margin_target)
                if gross_margin_target < Decimal("1") and (Decimal("1") - gross_margin_target) > 0
                else Decimal("0")
            )
            target_contribution_unit = contribution_margin_target * asp if asp > 0 else Decimal("0")
            precio_objetivo_contribucion = max(
                precio_objetivo_bruto,
                costo_fabricacion_unit + max(target_contribution_unit, Decimal("0")),
            )
            gap = max(precio_objetivo_bruto, precio_objetivo_contribucion) - asp
            impacto = gap * units if gap > 0 and units > 0 else Decimal("0")

            if margen_contribucion_pct < Decimal("0.10"):
                action = ProductoPricingDecisionMensual.ACCION_REFORMULAR
            elif gap > Decimal("0") and margen_bruto_pct < gross_margin_target:
                action = ProductoPricingDecisionMensual.ACCION_SUBIR_PRECIO
            elif margen_bruto_pct < gross_margin_target and costo_fabricacion_unit > 0:
                action = ProductoPricingDecisionMensual.ACCION_CORREGIR_COSTO
            elif margen_bruto_pct >= gross_margin_target and units > 0 and margen_contribucion_pct >= contribution_margin_target:
                action = ProductoPricingDecisionMensual.ACCION_DEFENDER
            else:
                action = ProductoPricingDecisionMensual.ACCION_PROMOVER

            ProductoPricingDecisionMensual.objects.update_or_create(
                periodo=period_start,
                receta_id=receta_id,
                defaults={
                    "asp_actual": asp,
                    "costo_fabricacion_unit": costo_fabricacion_unit,
                    "contribucion_unit": contribucion_unit,
                    "margen_bruto_pct": margen_bruto_pct,
                    "margen_contribucion_pct": margen_contribucion_pct,
                    "precio_objetivo_bruto": precio_objetivo_bruto,
                    "precio_objetivo_contribucion": precio_objetivo_contribucion,
                    "gap_precio": gap,
                    "impacto_estimado": impacto,
                    "accion_sugerida": action,
                    "metadata": {
                        "gross_margin_target": str(gross_margin_target),
                        "contribution_margin_target": str(contribution_margin_target),
                    },
                },
            )
            pricing_rows += 1

        return OperatingSnapshotSummary(
            period_start=period_start,
            product_cost_rows=product_rows,
            branch_contribution_rows=branch_rows,
            pricing_rows=pricing_rows,
            company_result_created=company_created,
        )
