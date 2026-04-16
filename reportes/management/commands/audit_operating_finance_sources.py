from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Avg, Sum

from pos_bridge.models import PointDailySale, PointMonthlySalesOfficial
from reportes.models import EmpresaResultadoMensual, RecetaCostoHistoricoMensual
from reportes.services_operating_finance import OperatingFinanceSnapshotService
from ventas.services.sales_read_service import get_sales_range


def _parse_period(raw: str) -> date:
    try:
        parsed = datetime.strptime(raw, "%Y-%m").date()
    except ValueError as exc:
        raise CommandError("Usa --period YYYY-MM.") from exc
    return parsed.replace(day=1)


def _as_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _pct_diff(base: Decimal, other: Decimal) -> Decimal:
    if base == 0:
        return Decimal("0") if other == 0 else Decimal("100")
    return ((other - base) / base) * Decimal("100")


def _row_descriptor(row: PointDailySale, *, resolved_recipe=None, resolved_dynamically: bool = False) -> dict[str, object]:
    branch = getattr(row.branch, "erp_branch", None) if row.branch_id else None
    product = row.product
    recipe = resolved_recipe or row.receta
    return {
        "row_id": row.id,
        "sale_date": row.sale_date.isoformat() if row.sale_date else "",
        "amount": str(_as_decimal(row.total_amount)),
        "quantity": str(_as_decimal(row.quantity)),
        "recipe_id": getattr(recipe, "id", None),
        "recipe_name": getattr(recipe, "nombre", "") if recipe else "",
        "recipe_resolved_dynamically": resolved_dynamically,
        "branch_id": getattr(branch, "id", None),
        "branch_code": getattr(branch, "codigo", "") if branch else "",
        "branch_name": getattr(branch, "nombre", "") if branch else "",
        "point_product_id": getattr(product, "id", None) if product else None,
        "sku": getattr(product, "sku", "") if product else "",
        "product_name": getattr(product, "name", "") if product else "",
        "product_category": getattr(product, "category", "") if product else "",
    }


class Command(BaseCommand):
    help = "Audita fuentes de ventas/costo para un mes operativo: oficial, diario, mapeado y resultado empresa."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Mes a auditar en formato YYYY-MM.")

    def handle(self, *args, **options):
        period = _parse_period(options["period"])
        next_month = (period.replace(day=28) + timedelta(days=4)).replace(day=1)
        period_end = next_month - timedelta(days=1)

        official = PointMonthlySalesOfficial.objects.filter(month_start=period).first()
        daily_qs = (
            PointDailySale.objects.filter(sale_date__year=period.year, sale_date__month=period.month)
            .select_related("product", "receta", "branch__erp_branch")
            .order_by("id")
        )
        mapped_qs = daily_qs.filter(receta_id__isnull=False, branch__erp_branch_id__isnull=False)
        company = EmpresaResultadoMensual.objects.filter(periodo=period).first()
        historical_qs = RecetaCostoHistoricoMensual.objects.filter(periodo=period, lineas_totales__gt=0)
        snapshot_service = OperatingFinanceSnapshotService()

        raw_total = _as_decimal(daily_qs.aggregate(v=Sum("total_amount"))["v"])
        raw_net_total = _as_decimal(daily_qs.aggregate(v=Sum("net_amount"))["v"])
        mapped_total = _as_decimal(mapped_qs.aggregate(v=Sum("total_amount"))["v"])
        mapped_net_total = _as_decimal(mapped_qs.aggregate(v=Sum("net_amount"))["v"])

        canonical = get_sales_range(
            start_date=period,
            end_date=period_end,
            coverage_policy="prefer_complete",
        )
        canonical_total = _as_decimal(canonical.get("monto"))
        official_total = _as_decimal(official.total_amount if official else 0)

        sales_by_recipe_branch = snapshot_service._sales_by_recipe_branch(period, period_end)
        venta_costeada_total = sum((payload["venta"] for payload in sales_by_recipe_branch.values()), Decimal("0"))
        venta_no_receta_total, venta_receta_sin_match_total = snapshot_service._split_unmapped_sales(period, period_end)
        venta_sin_mapear_total = venta_no_receta_total + venta_receta_sin_match_total

        non_recipe_bucket_totals = {"REVENTA": Decimal("0"), "ACCESORIO": Decimal("0"), "SERVICIO": Decimal("0")}
        non_recipe_bucket_counts = {"REVENTA": 0, "ACCESORIO": 0, "SERVICIO": 0}
        row_counts = {
            "with_recipe_id": 0,
            "without_recipe_id": 0,
            "with_erp_branch_id": 0,
            "without_erp_branch_id": 0,
        }
        top_non_recipe_rows: list[dict[str, object]] = []
        top_candidate_recipe_rows: list[dict[str, object]] = []

        for row in daily_qs.iterator():
            amount = _as_decimal(row.total_amount)
            if row.receta_id:
                row_counts["with_recipe_id"] += 1
            else:
                row_counts["without_recipe_id"] += 1
            if row.branch_id and row.branch.erp_branch_id:
                row_counts["with_erp_branch_id"] += 1
            else:
                row_counts["without_erp_branch_id"] += 1

            receta, resolved_dynamically = snapshot_service._resolve_recipe_for_row(row)
            is_non_recipe = snapshot_service._is_non_recipe_commercial_row_for_recipe(row, receta)

            if receta is not None and not is_non_recipe:
                continue

            if is_non_recipe:
                bucket = (
                    snapshot_service._forced_non_recipe_bucket_for_row(row)
                    or snapshot_service.sales_matcher.infer_non_recipe_bucket(
                        snapshot_service._build_sales_payload(row)
                    )
                )
                non_recipe_bucket_totals[bucket] += amount
                non_recipe_bucket_counts[bucket] += 1
                row_payload = _row_descriptor(
                    row,
                    resolved_recipe=receta,
                    resolved_dynamically=resolved_dynamically,
                )
                row_payload["bucket"] = bucket
                top_non_recipe_rows.append(row_payload)
            else:
                top_candidate_recipe_rows.append(
                    _row_descriptor(
                        row,
                        resolved_recipe=receta,
                        resolved_dynamically=resolved_dynamically,
                    )
                )

        top_non_recipe_rows = sorted(
            top_non_recipe_rows,
            key=lambda item: (-float(item["amount"]), item["product_name"], item["row_id"]),
        )[:20]
        top_candidate_recipe_rows = sorted(
            top_candidate_recipe_rows,
            key=lambda item: (-float(item["amount"]), item["product_name"], item["row_id"]),
        )[:20]

        classified_total = venta_costeada_total + venta_no_receta_total + venta_receta_sin_match_total
        classification_diff_abs = raw_total - classified_total
        classification_diff_pct = _pct_diff(raw_total, classified_total)
        raw_vs_canonical_abs = canonical_total - raw_total
        raw_vs_canonical_pct = _pct_diff(raw_total, canonical_total)
        raw_vs_official_abs = official_total - raw_total
        raw_vs_official_pct = _pct_diff(raw_total, official_total)
        warnings: list[str] = []
        if classification_diff_abs != 0:
            warnings.append(
                "La suma de venta_costeada_total + venta_no_receta_total + venta_receta_sin_match_total no cuadra contra la venta cruda del periodo."
            )
        if official and official_total != raw_total:
            warnings.append(
                "PointMonthlySalesOfficial difiere de la suma cruda PointDailySale del periodo."
            )
        if canonical_total != raw_total:
            warnings.append(
                "La capa canónica por rango difiere de la suma cruda PointDailySale del periodo."
            )

        payload = {
            "period": period.isoformat(),
            "sources": {
                "official_month_total": str(official_total),
                "official_month_net": str(_as_decimal(official.net_amount if official else 0)),
                "official_report_path": official.report_path if official else "",
                "canonical_range_total": str(canonical_total),
                "canonical_range_source": str(canonical.get("source") or "none").upper(),
                "daily_sales_total": str(raw_total),
                "daily_sales_net": str(raw_net_total),
                "mapped_sales_total": str(mapped_total),
                "mapped_sales_net": str(mapped_net_total),
                "unmapped_sales_total": str(raw_total - mapped_total),
                "daily_rows": daily_qs.count(),
                "mapped_rows": mapped_qs.count(),
            },
            "commercial_classification": {
                "venta_costeada_total": str(venta_costeada_total),
                "venta_no_receta_total": str(venta_no_receta_total),
                "venta_receta_sin_match_total": str(venta_receta_sin_match_total),
                "venta_sin_mapear_total": str(venta_sin_mapear_total),
                "non_recipe_bucket_totals": {key: str(value) for key, value in non_recipe_bucket_totals.items()},
                "non_recipe_bucket_counts": non_recipe_bucket_counts,
                "row_counts": row_counts,
                "top_non_recipe_products": top_non_recipe_rows,
                "top_candidate_recipe_rows": top_candidate_recipe_rows,
            },
            "validations": {
                "classification_balance": {
                    "raw_total": str(raw_total),
                    "classified_total": str(classified_total),
                    "difference_abs": str(classification_diff_abs),
                    "difference_pct": str(classification_diff_pct),
                    "balanced": classification_diff_abs == 0,
                },
                "source_comparison": {
                    "raw_vs_canonical": {
                        "raw_total": str(raw_total),
                        "canonical_total": str(canonical_total),
                        "difference_abs": str(raw_vs_canonical_abs),
                        "difference_pct": str(raw_vs_canonical_pct),
                    },
                    "raw_vs_official": {
                        "raw_total": str(raw_total),
                        "official_total": str(official_total),
                        "difference_abs": str(raw_vs_official_abs),
                        "difference_pct": str(raw_vs_official_pct),
                    },
                },
                "warnings": warnings,
            },
            "historical_costing": {
                "avg_coverage_pct": str(_as_decimal(historical_qs.aggregate(v=Avg("coverage_pct"))["v"])),
                "partial_recipes": historical_qs.filter(coverage_pct__lt=100).count(),
                "full_recipes": historical_qs.filter(coverage_pct=100).count(),
            },
            "company_result": {
                "venta_total": str(_as_decimal(company.venta_total if company else 0)),
                "costo_fabricacion_total": str(_as_decimal(company.costo_fabricacion_total if company else 0)),
                "gasto_comercial_total": str(_as_decimal(company.gasto_comercial_total if company else 0)),
                "gasto_corporativo_total": str(_as_decimal(company.gasto_corporativo_total if company else 0)),
                "utilidad_operativa_total": str(_as_decimal(company.utilidad_operativa_total if company else 0)),
                "metadata": company.metadata if company else {},
            },
        }

        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
