from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from pos_bridge.models import PointDailySale, PointInventorySnapshot, PointProductionLine, PointSyncJob, PointWasteLine
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import (
    ProductoMonthClosure,
    ProductoMonthClosureLine,
    Receta,
    RecetaEquivalencia,
    RecetaPresentacionDerivada,
    VentaHistorica,
)
from recetas.utils.cierre_equivalencias import resolve_closure_recipe_quantity
from recetas.utils.normalizacion import normalizar_nombre
from reportes.models import FactProduccionDiaria

ZERO = Decimal("0")
POINT_BRIDGE_SALES_SOURCE = "POINT_BRIDGE_SALES"
OFFICIAL_CATEGORY_REPORT_SOURCE = "POINT_OFFICIAL_MONTHLY_CATEGORY_REPORT"
OFFICIAL_POINT_DAILY_SOURCE = "/Report/PrintReportes?idreporte=3"
CLOSURE_EXCLUDED_NAME_TOKENS = (
    "vaso ",
    "vasos ",
    "letrero",
    "vela",
    "accesorio",
    "regalo",
    "topping ",
    " sin preparar",
)
CLOSURE_EXCLUDED_META_TOKENS = (
    "vaso preparado",
    "vasos preparados",
    "accesorio",
    "accesorios",
    "vela",
    "velas",
    "regalo",
    "regalos",
    "bebida",
    "bebidas",
    "letrero",
    "letreros",
)


class ProductMonthClosureError(Exception):
    pass


@dataclass
class _AggregateBucket:
    value: Decimal = ZERO
    row_count: int = 0
    direct_value: Decimal = ZERO
    derived_value: Decimal = ZERO
    cedis_value: Decimal = ZERO
    sucursales_value: Decimal = ZERO
    snapshot_count: int = 0
    has_catalog_issue: bool = False
    issue_notes: set[str] | None = None

    def __post_init__(self):
        if self.issue_notes is None:
            self.issue_notes = set()


class ProductMonthClosureService:
    DEFAULT_SNAPSHOT_TOLERANCE_DAYS = 3

    def __init__(
        self,
        matcher: PointSalesMatchingService | None = None,
        balance_service: MonthlyPointProductBalanceService | None = None,
        refresh_official_sales: bool = False,
    ):
        self.matcher = matcher or PointSalesMatchingService()
        self.official_sales_report_service = PointSalesCategoryReportService()
        self.balance_service = balance_service or MonthlyPointProductBalanceService(
            matcher=self.matcher,
            official_sales_report_service=self.official_sales_report_service,
            refresh_official_sales=refresh_official_sales,
        )
        self.refresh_official_sales = bool(refresh_official_sales)

    def build(
        self,
        *,
        month: str | date,
        rebuild: bool = False,
        lock_after_build: bool = False,
        built_by=None,
        approval_note: str = "",
        approval_reason: str = "",
        approval_channel: str = "service",
        refresh_official_sales: bool | None = None,
    ) -> ProductoMonthClosure:
        month_start = self._parse_month(month)
        existing_closure = ProductoMonthClosure.objects.filter(month_start=month_start).order_by("-id").first()
        if existing_closure is not None and existing_closure.is_locked:
            if rebuild:
                raise ProductMonthClosureError(f"El cierre {month_start:%Y-%m} esta bloqueado y no permite rebuild.")
            raise ProductMonthClosureError(f"El cierre {month_start:%Y-%m} esta bloqueado.")

        plan = self.preview(month=month_start, refresh_official_sales=refresh_official_sales)
        month_end = plan["month_end"]
        now = timezone.now()
        notes = plan["notes"]

        with transaction.atomic():
            closure, _ = ProductoMonthClosure.objects.get_or_create(
                month_start=month_start,
                defaults={"month_end": month_end},
            )
            closure.lines.all().delete()

            closure.month_end = month_end
            closure.status = ProductoMonthClosure.STATUS_DRAFT
            closure.opening_source = plan["opening_source"]
            closure.opening_reference_date = plan["opening_reference_date"]
            closure.upstream_sync_cutoff_at = now
            closure.built_at = now
            closure.built_by = built_by
            closure.notes = notes
            closure.metadata = {
                **dict(plan["metadata"]),
                "rebuild": bool(rebuild),
            }
            closure.is_locked = False
            closure.save()

            for row in plan["line_rows"]:
                ProductoMonthClosureLine.objects.create(
                    closure=closure,
                    receta_padre=row["receta"],
                    inventario_inicial_teorico=row["inventario_inicial_teorico"],
                    produccion_mes=row["produccion_mes"],
                    venta_directa_enteros=row["venta_directa_enteros"],
                    venta_derivada_equivalente=row["venta_derivada_equivalente"],
                    venta_total_equivalente=row["venta_total_equivalente"],
                    merma_directa_enteros=row["merma_directa_enteros"],
                    merma_derivada_equivalente=row["merma_derivada_equivalente"],
                    merma_total_equivalente=row["merma_total_equivalente"],
                    inventario_final_teorico=row["inventario_final_teorico"],
                    inventario_final_point_cedis=row["inventario_final_point_cedis"],
                    inventario_final_point_sucursales=row["inventario_final_point_sucursales"],
                    inventario_final_point_total=row["inventario_final_point_total"],
                    diferencia_teorico_vs_point=row["diferencia_teorico_vs_point"],
                    estado_auditoria=row["estado_auditoria"],
                    detalle_auditoria=row["detalle_auditoria"],
                    source_closing_snapshot_count=row["source_closing_snapshot_count"],
                    source_snapshot_count=row["source_snapshot_count"],
                    source_sale_rows=row["source_sale_rows"],
                    source_production_rows=row["source_production_rows"],
                    source_waste_rows=row["source_waste_rows"],
                    has_catalog_issue=row["has_catalog_issue"],
                    catalog_issue_note=row["catalog_issue_note"],
                    metadata={
                        **row["metadata"],
                        "opening_source": plan["opening_source"],
                    },
                )

            closure.status = ProductoMonthClosure.STATUS_BUILT
            closure.is_locked = False
            closure.save(update_fields=["status", "is_locked", "updated_at"])

            if lock_after_build:
                closure = self.lock(
                    closure=closure,
                    locked_by=built_by,
                    reason=approval_reason or "lock_after_build",
                    note=approval_note,
                    channel=approval_channel,
                )

        return closure

    def preview(
        self,
        *,
        month: str | date,
        refresh_official_sales: bool | None = None,
    ) -> dict[str, object]:
        month_start = self._parse_month(month)
        balance = self.balance_service.build(
            month_start,
            refresh_official_sales=(
                self.refresh_official_sales if refresh_official_sales is None else bool(refresh_official_sales)
            ),
        )
        if not balance.rows:
            raise ProductMonthClosureError(f"No hay datos para construir cierre mensual {month_start:%Y-%m}.")
        source_issues = set(balance.issues)
        for source_name in ("opening_snapshot", "closing_snapshot", "sales"):
            source = dict(balance.sources.get(source_name) or {})
            if source and source.get("authoritative") is False:
                source_issues.add("MONTH_SOURCE_INCOMPLETE")
        line_rows = self._project_canonical_balance(balance=balance, global_issues=source_issues)
        if not line_rows:
            raise ProductMonthClosureError(
                f"No hay productos terminados elegibles para construir el cierre mensual {month_start:%Y-%m}."
            )
        blocking_issues = sorted(source_issues | {issue for row in line_rows for issue in row["metadata"]["issues"]})
        opening_meta = self._compact_source_metadata(balance.sources.get("opening_snapshot") or {})
        closing_meta = self._compact_source_metadata(balance.sources.get("closing_snapshot") or {})
        sales_meta = self._compact_source_metadata(balance.sources.get("sales") or {})
        sales_meta["mode"] = sales_meta.get("selected_source") or sales_meta.get("mode") or ""
        validation = {
            "warnings": self._json_compatible(balance.warnings),
            "blocking_issues": blocking_issues,
            "lock_ready": not blocking_issues,
            "catalog_issue_line_count": sum(row["has_catalog_issue"] for row in line_rows),
            "sales_source_mode": sales_meta.get("selected_source") or sales_meta.get("mode") or "",
            "sales_job_id": None,
            "sales_job_status": "",
            "sales_official_rows": int(balance.source_counts.get("sales_rows") or 0),
            "sales_legacy_rows": 0,
            "closing_inventory": {
                "snapshot_rows": int(balance.source_counts.get("closing_snapshot_rows") or 0),
                "matched_recipe_count": sum(row["source_closing_snapshot_count"] > 0 for row in line_rows),
                "selected_dates": closing_meta.get("selected_dates", []),
            },
        }
        totals = {
            "opening": sum((row["inventario_inicial_teorico"] for row in line_rows), ZERO),
            "production": sum((row["produccion_mes"] for row in line_rows), ZERO),
            "sales": sum((row["venta_total_equivalente"] for row in line_rows), ZERO),
            "waste": sum((row["merma_total_equivalente"] for row in line_rows), ZERO),
            "ending": sum((row["inventario_final_teorico"] for row in line_rows), ZERO),
            "closing_cedis": sum((row["inventario_final_point_cedis"] for row in line_rows), ZERO),
            "closing_sucursales": sum((row["inventario_final_point_sucursales"] for row in line_rows), ZERO),
            "closing_total": sum((row["inventario_final_point_total"] for row in line_rows), ZERO),
            "difference": sum((row["diferencia_teorico_vs_point"] for row in line_rows), ZERO),
        }
        opening_reference_date = balance.effective_snapshot_dates.get("opening")
        notes = (
            f"Cierre {month_start:%Y-%m} proyectado del contrato canónico POINT_PRODUCT_BALANCE_V1. "
            "Las fuentes y advertencias se conservan en metadata."
        )
        return {
            "month_start": month_start,
            "month_end": balance.month_end,
            "opening_source": ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            "opening_reference_date": opening_reference_date,
            "notes": notes,
            "line_rows": line_rows,
            "metadata": {
                "opening_meta": opening_meta,
                "sales_meta": sales_meta,
                "fact_meta": {"source": "MonthlyPointProductBalanceService", "status": "canonical"},
                "closing_inventory_meta": closing_meta,
                "recipe_count": len(line_rows),
                "validation": validation,
                "balance": self._json_compatible(
                    {
                        "contract": "POINT_PRODUCT_BALANCE_V1",
                        "issues": balance.issues,
                        "warnings": balance.warnings,
                        "source_names": sorted(balance.sources),
                        "effective_snapshot_dates": balance.effective_snapshot_dates,
                        "source_counts": balance.source_counts,
                    }
                ),
            },
            "totals": totals,
        }

    @staticmethod
    def _json_compatible(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, dict) or hasattr(value, "items"):
            return {str(key): ProductMonthClosureService._json_compatible(item) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [ProductMonthClosureService._json_compatible(item) for item in value]
        if isinstance(value, set) or isinstance(value, frozenset):
            return [ProductMonthClosureService._json_compatible(item) for item in sorted(value, key=str)]
        return value

    def _compact_source_metadata(self, source):
        raw = self._json_compatible(source)
        compact = {}
        coverage_keys = ("coverage_keys", "mapped_recipe_keys", "branch_ids", "branch_codes")
        for key, value in raw.items():
            if any(token in key for token in coverage_keys) and isinstance(value, list):
                digest = sha256(repr(value).encode()).hexdigest()[:16]
                compact[f"{key}_count"] = len(value)
                compact[f"{key}_hash"] = digest
                compact[f"{key}_sample"] = value[:5]
                continue
            compact[key] = value
        return compact

    @staticmethod
    def _decimal_text(value: Decimal) -> str:
        return format(Decimal(value).normalize(), "f")

    def _project_canonical_balance(self, *, balance, global_issues=None):
        recipes = Receta.objects.in_bulk(balance.rows.keys())
        raw_recipe_ids = set(recipes)
        equivalences = {}
        for item in RecetaEquivalencia.objects.select_related("receta_padre").filter(
            receta_porcion_id__in=raw_recipe_ids, activo=True
        ).order_by("id"):
            equivalences.setdefault(item.receta_porcion_id, item)
        derived_relations = {}
        for item in RecetaPresentacionDerivada.objects.select_related("receta_padre").filter(
            receta_derivada_id__in=raw_recipe_ids, activo=True
        ).order_by("id"):
            derived_relations.setdefault(item.receta_derivada_id, item)
        global_issues = set(balance.issues if global_issues is None else global_issues)
        buckets: dict[int, dict[str, object]] = {}
        for receta_id, raw_row in sorted(balance.rows.items()):
            receta = recipes.get(receta_id)
            if receta is None:
                continue
            parent, _converted_one, equivalence_issue, is_derived, conversion_source = self._resolve_projected_recipe(
                receta, equivalences.get(receta.id), derived_relations.get(receta.id)
            )
            if parent is None or not self._is_recipe_eligible_for_closure(parent):
                continue
            bucket = buckets.setdefault(
                parent.id,
                {
                    "receta": parent,
                    "opening": ZERO,
                    "opening_missing": False,
                    "production": ZERO,
                    "sales_direct": ZERO,
                    "sales_derived": ZERO,
                    "waste_direct": ZERO,
                    "waste_derived": ZERO,
                    "conversion_in": ZERO,
                    "conversion_out": ZERO,
                    "calculated": ZERO,
                    "calculated_missing": False,
                    "closing": ZERO,
                    "closing_missing": False,
                    "closing_cedis": ZERO,
                    "closing_cedis_missing": False,
                    "closing_sucursales": ZERO,
                    "closing_sucursales_missing": False,
                    "point_difference": ZERO,
                    "point_difference_missing": False,
                    "issues": set(),
                    "origins": set(),
                    "source_counts": {},
                    "raw_recipe_ids": [],
                    "point_statuses": set(),
                },
            )
            factor = Decimal(_converted_one)
            bucket["raw_recipe_ids"].append(receta.id)
            bucket["issues"].update(global_issues)
            bucket["issues"].update(raw_row.issues)
            bucket["point_statuses"].add(raw_row.status)
            if equivalence_issue:
                bucket["issues"].add(equivalence_issue)
            if raw_row.conversion_origin:
                bucket["origins"].add(raw_row.conversion_origin)
            if conversion_source:
                bucket["origins"].add(conversion_source)
            for count_name, count in raw_row.source_counts.items():
                bucket["source_counts"][count_name] = bucket["source_counts"].get(count_name, 0) + int(count)
            for field, raw_value, missing_flag in (
                ("opening", raw_row.opening_point, "opening_missing"),
                ("calculated", raw_row.calculated_closing, "calculated_missing"),
                ("closing", raw_row.closing_point, "closing_missing"),
                ("closing_cedis", raw_row.closing_point_cedis, "closing_cedis_missing"),
                ("closing_sucursales", raw_row.closing_point_sucursales, "closing_sucursales_missing"),
                ("point_difference", raw_row.difference_point, "point_difference_missing"),
            ):
                if raw_value is None:
                    bucket[missing_flag] = True
                else:
                    bucket[field] += Decimal(raw_value) * factor
            bucket["production"] += Decimal(raw_row.production) * factor
            bucket["conversion_in"] += Decimal(raw_row.conversion_in) * factor
            bucket["conversion_out"] += Decimal(raw_row.conversion_out) * factor
            sales = Decimal(raw_row.sales) * factor
            waste = Decimal(raw_row.waste) * factor
            if is_derived:
                bucket["sales_derived"] += sales
                bucket["waste_derived"] += waste
            else:
                bucket["sales_direct"] += sales
                bucket["waste_direct"] += waste

        rows = []
        for parent_id in sorted(buckets):
            bucket = buckets[parent_id]
            issues = set(bucket["issues"])
            if bucket["opening_missing"]:
                issues.add("OPENING_SNAPSHOT_MISSING")
            if bucket["closing_missing"]:
                issues.add("CLOSING_SNAPSHOT_MISSING")
            scopes_available = not bool(bucket["closing_cedis_missing"] or bucket["closing_sucursales_missing"])
            if scopes_available and abs(bucket["closing"] - bucket["closing_cedis"] - bucket["closing_sucursales"]) > Decimal("0.01"):
                scopes_available = False
            if not scopes_available:
                issues.add("CLOSING_SNAPSHOT_SCOPE_MISSING")
            if bucket["calculated_missing"]:
                issues.add("CALCULATED_CLOSING_MISSING")
            if bucket["point_difference_missing"]:
                issues.add("POINT_DIFFERENCE_MISSING")
            point_difference = None if bucket["point_difference_missing"] else bucket["point_difference"]
            legacy_difference = ZERO if point_difference is None else -point_difference
            audit_status, audit_detail = self._canonical_audit_status(
                issues=issues,
                closing_missing=bool(bucket["closing_missing"]),
                point_difference=point_difference,
                waste_total=bucket["waste_direct"] + bucket["waste_derived"],
            )
            metadata = self._json_compatible(
                {
                    "balance_contract": "POINT_PRODUCT_BALANCE_V1",
                    "point_conversion_in": self._decimal_text(bucket["conversion_in"]),
                    "point_conversion_out": self._decimal_text(bucket["conversion_out"]),
                    "conversion_origin": sorted(bucket["origins"]),
                    "source_counts": bucket["source_counts"],
                    "issues": sorted(issues),
                    "point_difference": "" if point_difference is None else self._decimal_text(point_difference),
                    "point_status": self._projected_point_status(
                        point_statuses=bucket["point_statuses"],
                        point_difference=point_difference,
                        issues=issues,
                    ),
                    "raw_recipe_ids": sorted(bucket["raw_recipe_ids"]),
                    "point_final_scopes_available": scopes_available,
                }
            )
            rows.append(
                {
                    "receta": bucket["receta"],
                    "inventario_inicial_teorico": bucket["opening"],
                    "produccion_mes": bucket["production"],
                    "venta_directa_enteros": bucket["sales_direct"],
                    "venta_derivada_equivalente": bucket["sales_derived"],
                    "venta_total_equivalente": bucket["sales_direct"] + bucket["sales_derived"],
                    "merma_directa_enteros": bucket["waste_direct"],
                    "merma_derivada_equivalente": bucket["waste_derived"],
                    "merma_total_equivalente": bucket["waste_direct"] + bucket["waste_derived"],
                    "inventario_final_teorico": bucket["calculated"],
                    "inventario_final_point_cedis": bucket["closing_cedis"],
                    "inventario_final_point_sucursales": bucket["closing_sucursales"],
                    "inventario_final_point_total": bucket["closing"],
                    "diferencia_teorico_vs_point": legacy_difference,
                    "estado_auditoria": audit_status,
                    "detalle_auditoria": audit_detail,
                    "source_closing_snapshot_count": int(bucket["source_counts"].get("closing_snapshot_rows", 0)),
                    "source_snapshot_count": int(bucket["source_counts"].get("opening_snapshot_rows", 0)),
                    "source_sale_rows": int(bucket["source_counts"].get("sales_rows", 0)),
                    "source_production_rows": int(bucket["source_counts"].get("production_rows", 0)),
                    "source_waste_rows": int(bucket["source_counts"].get("waste_rows", 0)),
                    "has_catalog_issue": bool(issues),
                    "catalog_issue_note": " | ".join(sorted(issues))[:255],
                    "metadata": metadata,
                }
            )
        return rows

    @staticmethod
    def _resolve_projected_recipe(receta, equivalence, derived_relation):
        if receta.excluir_cierre:
            return None, ZERO, "", False, "EXCLUIDA"
        if equivalence is not None:
            factor = Decimal(str(equivalence.factor_conversion or 0))
            if factor <= ZERO:
                return receta, Decimal("1"), f"Equivalencia de cierre sin factor valido para {receta.nombre}", False, "DIRECTA"
            return equivalence.receta_padre, Decimal("1") / factor, "", equivalence.receta_padre_id != receta.id, "EQUIVALENCIA"
        if derived_relation is None:
            return receta, Decimal("1"), "", False, "DIRECTA"
        units = Decimal(str(derived_relation.unidades_por_padre or 0))
        if units <= ZERO:
            return receta, Decimal("1"), f"Relacion derivada sin unidades_por_padre para {receta.nombre}", False, "DIRECTA"
        return derived_relation.receta_padre, Decimal("1") / units, "", True, "PRESENTACION_DERIVADA"

    @staticmethod
    def _canonical_status(*, point_difference: Decimal | None, issues: set[str]) -> str:
        if issues or point_difference is None:
            return "REVISAR_FUENTE"
        if abs(point_difference) <= Decimal("0.01"):
            return "COINCIDE"
        return "POINT_MAYOR" if point_difference > ZERO else "POINT_MENOR"

    def _projected_point_status(self, *, point_statuses, point_difference, issues):
        if issues:
            return "REVISAR_FUENTE"
        if len(point_statuses) == 1:
            return next(iter(point_statuses))
        return self._canonical_status(point_difference=point_difference, issues=issues)

    def _canonical_audit_status(self, *, issues, closing_missing, point_difference, waste_total):
        if closing_missing:
            return (
                ProductoMonthClosureLine.AUDIT_STATUS_SIN_INVENTARIO_FISICO,
                "El contrato canónico no tiene inventario final Point autoritativo.",
            )
        if issues:
            return (
                ProductoMonthClosureLine.AUDIT_STATUS_REVISAR_CATALOGO,
                "La línea conserva incidencias de fuente, catálogo o conversión del contrato canónico.",
            )
        return self._resolve_audit_status(
            has_catalog_issue=False,
            closing_inventory_available=True,
            difference=-point_difference,
            waste_total=waste_total,
        )

    def build_bootstrap_seed(
        self,
        *,
        month: str | date,
        seed_rows: list[dict[str, object]],
        source_label: str,
        source_path: str = "",
        source_sheet: str = "",
        built_by=None,
        rebuild: bool = False,
        approval_note: str = "",
        approval_reason: str = "",
        approval_channel: str = "service_bootstrap",
    ) -> ProductoMonthClosure:
        month_start = self._parse_month(month)
        month_end = date(month_start.year, month_start.month, monthrange(month_start.year, month_start.month)[1])
        existing_closure = ProductoMonthClosure.objects.filter(month_start=month_start).order_by("-id").first()
        if existing_closure is not None and existing_closure.is_locked:
            if rebuild:
                raise ProductMonthClosureError(f"El cierre {month_start:%Y-%m} esta bloqueado y no permite rebuild.")
            raise ProductMonthClosureError(f"El cierre {month_start:%Y-%m} esta bloqueado.")

        line_rows, opening_meta, validation = self._build_bootstrap_seed_rows(seed_rows=seed_rows)
        notes = self._build_bootstrap_notes(
            month_start=month_start,
            source_label=source_label,
            validation=validation,
        )
        now = timezone.now()

        with transaction.atomic():
            closure, _ = ProductoMonthClosure.objects.get_or_create(
                month_start=month_start,
                defaults={"month_end": month_end},
            )
            closure.lines.all().delete()
            closure.month_end = month_end
            closure.status = ProductoMonthClosure.STATUS_DRAFT
            closure.opening_source = ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED
            closure.opening_reference_date = month_end
            closure.upstream_sync_cutoff_at = now
            closure.built_at = now
            closure.built_by = built_by
            closure.notes = notes
            closure.metadata = {
                "opening_meta": opening_meta,
                "validation": validation,
                "bootstrap_seed": {
                    "is_seed": True,
                    "source_label": source_label,
                    "source_path": source_path,
                    "source_sheet": source_sheet,
                    "seed_month": month_start.isoformat(),
                },
                "approval": {
                    "note": (approval_note or "").strip(),
                    "reason": (approval_reason or "").strip(),
                    "channel": (approval_channel or "service_bootstrap").strip(),
                },
                "recipe_count": len(line_rows),
                "rebuild": bool(rebuild),
            }
            closure.is_locked = False
            closure.save()

            for row in line_rows:
                ProductoMonthClosureLine.objects.create(
                    closure=closure,
                    receta_padre=row["receta"],
                    inventario_inicial_teorico=row["inventario_inicial_teorico"],
                    produccion_mes=ZERO,
                    venta_directa_enteros=ZERO,
                    venta_derivada_equivalente=ZERO,
                    venta_total_equivalente=ZERO,
                    merma_directa_enteros=ZERO,
                    merma_derivada_equivalente=ZERO,
                    merma_total_equivalente=ZERO,
                    inventario_final_teorico=row["inventario_final_teorico"],
                    inventario_final_point_cedis=ZERO,
                    inventario_final_point_sucursales=ZERO,
                    inventario_final_point_total=ZERO,
                    diferencia_teorico_vs_point=row["inventario_final_teorico"],
                    estado_auditoria=ProductoMonthClosureLine.AUDIT_STATUS_SIN_INVENTARIO_FISICO,
                    detalle_auditoria="Bootstrap histórico sin inventario físico Point del cierre.",
                    source_closing_snapshot_count=0,
                    source_snapshot_count=0,
                    source_sale_rows=0,
                    source_production_rows=0,
                    source_waste_rows=0,
                    has_catalog_issue=row["has_catalog_issue"],
                    catalog_issue_note=row["catalog_issue_note"],
                    metadata=row["metadata"],
                )

            closure.status = ProductoMonthClosure.STATUS_BUILT
            closure.is_locked = False
            closure.save(update_fields=["status", "is_locked", "updated_at"])

        return closure

    def lock(
        self,
        *,
        closure: ProductoMonthClosure,
        locked_by=None,
        reason: str = "",
        note: str = "",
        channel: str = "service",
    ) -> ProductoMonthClosure:
        if closure.is_locked:
            raise ProductMonthClosureError(f"El cierre {closure.month_start:%Y-%m} ya esta bloqueado.")
        if closure.status != ProductoMonthClosure.STATUS_BUILT:
            raise ProductMonthClosureError(
                f"El cierre {closure.month_start:%Y-%m} debe estar construido antes de bloquearse."
            )

        lines = list(closure.lines.all())
        if not lines:
            raise ProductMonthClosureError(f"El cierre {closure.month_start:%Y-%m} no tiene lineas para bloquear.")

        issue_rows = [line for line in lines if line.has_catalog_issue]
        if issue_rows:
            raise ProductMonthClosureError(
                f"El cierre {closure.month_start:%Y-%m} tiene incidencias de catalogo y no puede bloquearse."
            )

        validation = dict((closure.metadata or {}).get("validation") or {})
        unmatched_products = list(((closure.metadata or {}).get("opening_meta") or {}).get("unmatched_products") or [])
        if unmatched_products:
            raise ProductMonthClosureError(
                f"El cierre {closure.month_start:%Y-%m} tiene productos de opening sin homologacion y no puede bloquearse."
            )
        if validation.get("snapshot_missing_exact_day") and not validation.get("snapshot_within_tolerance"):
            raise ProductMonthClosureError(
                f"El cierre {closure.month_start:%Y-%m} no tiene snapshot valido dentro de tolerancia y no puede bloquearse."
            )
        blocking_issues = list(validation.get("blocking_issues") or [])
        if blocking_issues:
            raise ProductMonthClosureError(
                f"El cierre {closure.month_start:%Y-%m} tiene incidencias activas y no puede bloquearse: {blocking_issues[0]}"
            )

        lock_time = timezone.now()
        metadata = dict(closure.metadata or {})
        metadata["lock_event"] = {
            "locked_at": lock_time.isoformat(),
            "locked_by": getattr(locked_by, "username", "") if locked_by else "",
            "reason": (reason or "").strip(),
            "note": (note or "").strip(),
            "channel": (channel or "service").strip(),
            "line_count": len(lines),
            "catalog_issue_line_count": len(issue_rows),
            "snapshot_fallback_used": bool(validation.get("snapshot_fallback_used")),
            "upstream_sync_cutoff_at": closure.upstream_sync_cutoff_at.isoformat() if closure.upstream_sync_cutoff_at else "",
        }

        closure.metadata = metadata
        closure.status = ProductoMonthClosure.STATUS_LOCKED
        closure.is_locked = True
        closure.save(update_fields=["metadata", "status", "is_locked", "updated_at"])
        return closure

    def _parse_month(self, month: str | date) -> date:
        if isinstance(month, date):
            return date(month.year, month.month, 1)
        try:
            parsed = datetime.strptime(str(month).strip(), "%Y-%m").date()
        except ValueError as exc:
            raise ProductMonthClosureError("Usa formato YYYY-MM para el mes.") from exc
        return date(parsed.year, parsed.month, 1)

    def _previous_month_start(self, month_start: date) -> date:
        prev_end = month_start - timedelta(days=1)
        return date(prev_end.year, prev_end.month, 1)

    def _load_opening(self, *, month_start: date):
        previous_month_start = self._previous_month_start(month_start)
        previous_closure = (
            ProductoMonthClosure.objects.prefetch_related("lines")
            .filter(month_start=previous_month_start, status__in=[ProductoMonthClosure.STATUS_BUILT, ProductoMonthClosure.STATUS_LOCKED])
            .order_by("-built_at", "-id")
            .first()
        )
        if previous_closure is not None:
            buckets: dict[int, _AggregateBucket] = {}
            previous_metadata = dict(previous_closure.metadata or {})
            previous_opening_meta = dict(previous_metadata.get("opening_meta") or {})
            previous_validation = dict(previous_metadata.get("validation") or {})
            previous_closing_inventory = dict(previous_validation.get("closing_inventory") or {})
            use_physical_closing = bool(int(previous_closing_inventory.get("snapshot_rows") or 0) > 0)
            for line in previous_closure.lines.select_related("receta_padre").all():
                if use_physical_closing:
                    opening_value = Decimal(str(line.inventario_final_point_total or 0))
                    cedis_value = Decimal(str(line.inventario_final_point_cedis or 0))
                    sucursales_value = Decimal(str(line.inventario_final_point_sucursales or 0))
                    snapshot_count = int(line.source_closing_snapshot_count or 0)
                else:
                    opening_value = Decimal(str(line.inventario_final_teorico or 0))
                    cedis_value = ZERO
                    sucursales_value = ZERO
                    snapshot_count = 0
                buckets[line.receta_padre_id] = _AggregateBucket(
                    value=opening_value,
                    cedis_value=cedis_value,
                    sucursales_value=sucursales_value,
                    snapshot_count=snapshot_count,
                )
            bootstrap_seed = dict(previous_metadata.get("bootstrap_seed") or {})
            unmatched_products = list(previous_opening_meta.get("unmatched_products") or [])
            if not unmatched_products:
                unmatched_products = list(bootstrap_seed.get("unmatched_products") or [])
            return (
                ProductoMonthClosure.OPENING_SOURCE_PREVIOUS_CLOSURE,
                previous_closure.month_end,
                buckets,
                {
                    "previous_closure_id": previous_closure.id,
                    "previous_month_start": previous_month_start.isoformat(),
                    "unmatched_products": unmatched_products[:50],
                    "upstream_validation_blocking_issues": list(previous_validation.get("blocking_issues") or [])[:20],
                    "bootstrap_seeded": bool(bootstrap_seed.get("is_seed")),
                    "bootstrap_source_label": bootstrap_seed.get("source_label") or "",
                    "previous_closure_opening_basis": "physical_point_closing" if use_physical_closing else "theoretical_closing",
                },
            )

        snapshot_date = month_start - timedelta(days=1)
        buckets, snapshot_meta = self._load_opening_from_snapshots(snapshot_date=snapshot_date)
        effective_date_raw = snapshot_meta.get("snapshot_effective_date") or snapshot_date.isoformat()
        effective_date = (
            effective_date_raw
            if isinstance(effective_date_raw, date)
            else datetime.strptime(str(effective_date_raw), "%Y-%m-%d").date()
        )
        return (
            ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            effective_date,
            buckets,
            snapshot_meta,
        )

    def _load_opening_from_snapshots(self, *, snapshot_date: date):
        tolerance_days = getattr(settings, "PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS", self.DEFAULT_SNAPSHOT_TOLERANCE_DAYS)
        current_timezone = timezone.get_current_timezone()
        target_start = timezone.make_aware(datetime.combine(snapshot_date, time.min), current_timezone)
        target_end = timezone.make_aware(datetime.combine(snapshot_date, time.max), current_timezone)
        before_at = (
            PointInventorySnapshot.objects.filter(captured_at__lte=target_end)
            .order_by("-captured_at", "-id")
            .values_list("captured_at", flat=True)
            .first()
        )
        after_at = (
            PointInventorySnapshot.objects.filter(captured_at__gte=target_start)
            .order_by("captured_at", "id")
            .values_list("captured_at", flat=True)
            .first()
        )
        candidates = [value for value in [before_at, after_at] if value is not None]
        if not candidates:
            raise ProductMonthClosureError(
                f"No existe snapshot Point para resolver inventario inicial al cierre de {snapshot_date.isoformat()}."
            )
        selected_at = min(candidates, key=lambda value: abs(value - target_start))
        effective_date = timezone.localtime(selected_at, current_timezone).date()
        day_start = timezone.make_aware(datetime.combine(effective_date, time.min), current_timezone)
        day_end = timezone.make_aware(datetime.combine(effective_date, time.max), current_timezone)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (branch_id, product_id) id
                FROM pos_bridge_inventory_snapshots
                WHERE captured_at >= %s AND captured_at <= %s
                ORDER BY branch_id, product_id, captured_at DESC, id DESC
                """,
                [day_start, day_end],
            )
            snapshot_ids = [row[0] for row in cursor.fetchall()]
        snapshots = (
            PointInventorySnapshot.objects.select_related("product", "branch")
            .filter(id__in=snapshot_ids)
            .order_by("product__name", "branch__name", "id")
        )
        if not snapshots.exists():
            raise ProductMonthClosureError(
                f"No existe snapshot Point para resolver inventario inicial al cierre de {snapshot_date.isoformat()}."
            )

        buckets: dict[int, _AggregateBucket] = {}
        unmatched_products: list[str] = []
        for snap in snapshots:
            receta = self.matcher.resolve_receta(codigo_point=snap.product.sku, point_name=snap.product.name)
            if receta is None:
                unmatched_products.append(snap.product.name)
                continue
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(receta=receta, quantity=Decimal(str(snap.stock or 0)))
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.snapshot_count += 1
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty

        if not buckets:
            raise ProductMonthClosureError(
                f"Los snapshots Point de {snapshot_date.isoformat()} no pudieron homologarse a recetas ERP."
            )
        snapshot_missing_exact_day = effective_date != snapshot_date
        days_from_target = abs((effective_date - snapshot_date).days)
        return buckets, {
            "snapshot_date": snapshot_date.isoformat(),
            "snapshot_effective_date": effective_date.isoformat(),
            "snapshot_tolerance_days": int(tolerance_days),
            "snapshot_missing_exact_day": snapshot_missing_exact_day,
            "snapshot_within_tolerance": bool(days_from_target <= int(tolerance_days)),
            "snapshot_fallback_used": snapshot_missing_exact_day,
            "snapshot_days_from_target": days_from_target,
            "unmatched_products": unmatched_products[:50],
        }

    def _load_closing_inventory(self, *, month_end: date):
        snapshot_ids, snapshot_meta = self._latest_snapshot_ids_near_date(snapshot_date=month_end)
        if not snapshot_ids:
            snapshot_meta["warnings"] = ["No existe snapshot Point para inventario final del mes."]
            return {}, snapshot_meta

        snapshots = (
            PointInventorySnapshot.objects.select_related("product", "branch", "branch__erp_branch")
            .filter(id__in=snapshot_ids)
            .order_by("product__name", "branch__name", "id")
        )

        buckets: dict[int, _AggregateBucket] = {}
        unmatched_products: list[str] = []
        for snap in snapshots:
            receta = self.matcher.resolve_receta(codigo_point=snap.product.sku, point_name=snap.product.name)
            if receta is None:
                unmatched_products.append(snap.product.name)
                continue
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=receta,
                quantity=Decimal(str(snap.stock or 0)),
            )
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.snapshot_count += 1
            if self._is_cedis_inventory_scope(snap):
                bucket.cedis_value += qty
            else:
                bucket.sucursales_value += qty
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty

        snapshot_meta["unmatched_products"] = unmatched_products[:50]
        snapshot_meta["snapshot_rows"] = len(snapshot_ids)
        snapshot_meta["matched_recipe_count"] = len(buckets)
        return buckets, snapshot_meta

    def _latest_snapshot_ids_near_date(self, *, snapshot_date: date):
        tolerance_days = getattr(settings, "PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS", self.DEFAULT_SNAPSHOT_TOLERANCE_DAYS)
        current_timezone = timezone.get_current_timezone()
        target_start = timezone.make_aware(datetime.combine(snapshot_date, time.min), current_timezone)
        target_end = timezone.make_aware(datetime.combine(snapshot_date, time.max), current_timezone)
        before_at = (
            PointInventorySnapshot.objects.filter(captured_at__lte=target_end)
            .order_by("-captured_at", "-id")
            .values_list("captured_at", flat=True)
            .first()
        )
        after_at = (
            PointInventorySnapshot.objects.filter(captured_at__gte=target_start)
            .order_by("captured_at", "id")
            .values_list("captured_at", flat=True)
            .first()
        )
        candidates = [value for value in [before_at, after_at] if value is not None]
        if not candidates:
            return [], {
                "snapshot_date": snapshot_date.isoformat(),
                "snapshot_effective_date": "",
                "snapshot_tolerance_days": int(tolerance_days),
                "snapshot_missing_exact_day": True,
                "snapshot_within_tolerance": False,
                "snapshot_fallback_used": False,
                "snapshot_days_from_target": None,
                "snapshot_rows": 0,
            }

        selected_at = min(candidates, key=lambda value: abs(value - target_start))
        effective_date = timezone.localtime(selected_at, current_timezone).date()
        day_start = timezone.make_aware(datetime.combine(effective_date, time.min), current_timezone)
        day_end = timezone.make_aware(datetime.combine(effective_date, time.max), current_timezone)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (branch_id, product_id) id
                FROM pos_bridge_inventory_snapshots
                WHERE captured_at >= %s AND captured_at <= %s
                ORDER BY branch_id, product_id, captured_at DESC, id DESC
                """,
                [day_start, day_end],
            )
            snapshot_ids = [row[0] for row in cursor.fetchall()]
        days_from_target = abs((effective_date - snapshot_date).days)
        return snapshot_ids, {
            "snapshot_date": snapshot_date.isoformat(),
            "snapshot_effective_date": effective_date.isoformat(),
            "snapshot_tolerance_days": int(tolerance_days),
            "snapshot_missing_exact_day": effective_date != snapshot_date,
            "snapshot_within_tolerance": bool(days_from_target <= int(tolerance_days)),
            "snapshot_fallback_used": effective_date != snapshot_date,
            "snapshot_days_from_target": days_from_target,
            "snapshot_rows": len(snapshot_ids),
        }

    def _is_cedis_inventory_scope(self, snapshot: PointInventorySnapshot) -> bool:
        branch = snapshot.branch
        erp_branch = getattr(branch, "erp_branch", None)
        code = str(getattr(erp_branch, "codigo", "") or "").strip().upper()
        name = normalizar_nombre(getattr(branch, "name", "") or "")
        if code in {"CEDIS", "DEVOLUCIONES", "ALMACEN"}:
            return True
        if "cedis" in name or "produccion" in name or "devolucion" in name or "almacen" in name:
            return True
        if erp_branch is not None and not bool(getattr(erp_branch, "activa", True)):
            return True
        return False

    def _load_production(self, *, month_start: date, month_end: date):
        fact_buckets = self._load_movement_from_production_facts(
            month_start=month_start,
            month_end=month_end,
            field_name="producido",
        )
        if fact_buckets:
            return fact_buckets

        buckets: dict[int, _AggregateBucket] = {}
        rows = (
            PointProductionLine.objects.select_related("receta")
            .filter(
                production_date__gte=month_start,
                production_date__lte=month_end,
                receta__isnull=False,
                receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
            )
            .exclude(receta__modo_costeo=Receta.MODO_COSTEO_SERVICIO)
            .order_by("id")
        )
        for row in rows:
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=row.receta,
                quantity=Decimal(str(row.produced_quantity or 0)),
            )
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.row_count += 1
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty
        return buckets

    def _load_sales(self, *, month_start: date, month_end: date):
        sales_source_mode = str(
            getattr(settings, "PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE", "AUTO")
        ).strip().upper() or "AUTO"
        prefer_official = sales_source_mode in {"AUTO", "OFFICIAL_MONTHLY_REPORT"}
        official_error = None
        if prefer_official:
            try:
                buckets, sales_meta = self._load_sales_from_official_monthly_report(
                    month_start=month_start,
                    month_end=month_end,
                )
                return buckets, sales_meta
            except Exception as exc:  # noqa: BLE001
                if sales_source_mode == "OFFICIAL_MONTHLY_REPORT":
                    raise ProductMonthClosureError(
                        f"No se pudo cargar el reporte oficial de ventas Point para {month_start:%Y-%m}: {exc}"
                    ) from exc
                official_error = exc
                fallback_buckets, fallback_meta = self._load_sales_from_point_daily_sales_official(
                    month_start=month_start,
                    month_end=month_end,
                )
                if fallback_buckets:
                    fallback_meta["fallback_reason"] = str(exc)
                    fallback_meta["warnings"] = [
                        "No se pudo usar el reporte oficial mensual; se uso PointDailySale oficial por sucursal y dia."
                    ]
                    return fallback_buckets, fallback_meta

        fact_buckets = self._load_movement_from_production_facts(
            month_start=month_start,
            month_end=month_end,
            field_name="vendido",
        )
        if fact_buckets:
            return fact_buckets, {
                "source": "FactProduccionDiaria",
                "mode": "production_facts",
                "start_date": month_start.isoformat(),
                "end_date": month_end.isoformat(),
            }

        if official_error is not None:
            bridge_buckets, bridge_meta = self._load_sales_from_bridge_history(
                month_start=month_start,
                month_end=month_end,
            )
            bridge_meta["fallback_reason"] = str(official_error)
            bridge_meta["warnings"] = [
                "No se pudo usar el reporte oficial mensual; se uso VentaHistorica POINT_BRIDGE_SALES."
            ]
            return bridge_buckets, bridge_meta
        return self._load_sales_from_bridge_history(month_start=month_start, month_end=month_end)

    def _load_sales_from_point_daily_sales_official(self, *, month_start: date, month_end: date):
        buckets: dict[int, _AggregateBucket] = {}
        rows = (
            PointDailySale.objects.select_related("receta")
            .filter(
                sale_date__gte=month_start,
                sale_date__lte=month_end,
                receta__isnull=False,
                source_endpoint=OFFICIAL_POINT_DAILY_SOURCE,
            )
            .order_by("id")
        )
        for row in rows:
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=row.receta,
                quantity=Decimal(str(row.quantity or 0)),
            )
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.row_count += 1
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
        return buckets, {
            "source": OFFICIAL_POINT_DAILY_SOURCE,
            "mode": "official_point_daily_sales",
            "start_date": month_start.isoformat(),
            "end_date": month_end.isoformat(),
        }

    def _load_sales_from_bridge_history(self, *, month_start: date, month_end: date):
        buckets: dict[int, _AggregateBucket] = {}
        rows = (
            VentaHistorica.objects.select_related("receta")
            .filter(
                fecha__gte=month_start,
                fecha__lte=month_end,
                fuente=POINT_BRIDGE_SALES_SOURCE,
                receta__isnull=False,
            )
            .order_by("id")
        )
        for row in rows:
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=row.receta,
                quantity=Decimal(str(row.cantidad or 0)),
            )
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.row_count += 1
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
        return buckets, {
            "source": POINT_BRIDGE_SALES_SOURCE,
            "mode": "bridge_history",
            "start_date": month_start.isoformat(),
            "end_date": month_end.isoformat(),
        }

    def _load_sales_from_official_monthly_report(self, *, month_start: date, month_end: date):
        report = self.official_sales_report_service.fetch_report(
            start_date=month_start,
            end_date=month_end,
            branch_external_id=None,
            branch_display_name=None,
            credito=None,
        )
        parsed = self.official_sales_report_service.parse_report(report_path=report.report_path)
        buckets: dict[int, _AggregateBucket] = {}
        for row in parsed.rows:
            point_name = str(row.get("Nombre") or "").strip()
            codigo_point = str(row.get("Codigo") or "").strip()
            if not point_name and not codigo_point:
                continue
            receta = self.matcher.resolve_receta(codigo_point=codigo_point, point_name=point_name)
            if receta is None:
                continue
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=receta,
                quantity=Decimal(str(row.get("Cantidad") or 0)),
            )
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.row_count += 1
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
        return buckets, {
            "source": OFFICIAL_CATEGORY_REPORT_SOURCE,
            "mode": "official_monthly_report",
            "start_date": month_start.isoformat(),
            "end_date": month_end.isoformat(),
            "report_path": report.report_path,
            "request_url": report.request_url,
            "summary": {key: str(value) for key, value in parsed.summary.items()},
            "row_count": len(parsed.rows),
        }

    def _load_waste(self, *, month_start: date, month_end: date):
        fact_buckets = self._load_movement_from_production_facts(
            month_start=month_start,
            month_end=month_end,
            field_name="merma",
        )
        if fact_buckets:
            return fact_buckets

        start_dt = timezone.make_aware(datetime.combine(month_start, time.min), timezone.get_current_timezone())
        end_dt = timezone.make_aware(datetime.combine(month_end, time.max), timezone.get_current_timezone())
        buckets: dict[int, _AggregateBucket] = {}
        rows = (
            PointWasteLine.objects.select_related("receta")
            .filter(movement_at__gte=start_dt, movement_at__lte=end_dt, receta__isnull=False)
            .order_by("id")
        )
        for row in rows:
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=row.receta,
                quantity=Decimal(str(row.quantity or 0)),
            )
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.row_count += 1
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
        return buckets

    def _ensure_month_facts(self, *, month_start: date, month_end: date) -> dict[str, object]:
        existing_rows = FactProduccionDiaria.objects.filter(fecha__gte=month_start, fecha__lte=month_end).count()
        if existing_rows:
            return {
                "status": "existing",
                "source": "FactProduccionDiaria",
                "fact_rows": existing_rows,
                "month_start": month_start.isoformat(),
                "month_end": month_end.isoformat(),
            }

        staging_counts = {
            "point_daily_sales": PointDailySale.objects.filter(
                sale_date__gte=month_start,
                sale_date__lte=month_end,
                receta_id__isnull=False,
            ).count(),
            "point_production_lines": PointProductionLine.objects.filter(
                production_date__gte=month_start,
                production_date__lte=month_end,
                receta_id__isnull=False,
                is_insumo=False,
            ).count(),
            "point_waste_lines": PointWasteLine.objects.filter(
                movement_at__date__gte=month_start,
                movement_at__date__lte=month_end,
                receta_id__isnull=False,
            ).count(),
        }
        if not any(staging_counts.values()):
            return {
                "status": "missing",
                "source": "FactProduccionDiaria",
                "fact_rows": 0,
                "staging_counts": staging_counts,
                "month_start": month_start.isoformat(),
                "month_end": month_end.isoformat(),
            }

        try:
            from reportes.analytics_service import rebuild_production_facts, rebuild_sales_facts

            sales_fact_rows = rebuild_sales_facts(start_date=month_start, end_date=month_end)
            production_fact_rows = rebuild_production_facts(start_date=month_start, end_date=month_end)
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "generation_failed",
                "source": "FactProduccionDiaria",
                "fact_rows": 0,
                "staging_counts": staging_counts,
                "error": str(exc),
                "month_start": month_start.isoformat(),
                "month_end": month_end.isoformat(),
            }

        return {
            "status": "generated",
            "source": "FactProduccionDiaria",
            "sales_fact_rows": sales_fact_rows,
            "production_fact_rows": production_fact_rows,
            "staging_counts": staging_counts,
            "month_start": month_start.isoformat(),
            "month_end": month_end.isoformat(),
        }

    def _load_movement_from_production_facts(
        self,
        *,
        month_start: date,
        month_end: date,
        field_name: str,
    ) -> dict[int, _AggregateBucket]:
        buckets: dict[int, _AggregateBucket] = {}
        rows = (
            FactProduccionDiaria.objects.select_related("receta")
            .filter(
                fecha__gte=month_start,
                fecha__lte=month_end,
                receta__isnull=False,
                **{f"{field_name}__gt": 0},
            )
            .order_by("id")
        )
        for row in rows:
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=row.receta,
                quantity=Decimal(str(getattr(row, field_name) or 0)),
            )
            if parent_receta is None:
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.row_count += 1
            if is_derived:
                bucket.derived_value += qty
            else:
                bucket.direct_value += qty
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)
        return buckets

    def _canonical_recipe_quantity(self, *, receta: Receta, quantity: Decimal):
        parent_receta, qty, issue_note, is_derived, _source = resolve_closure_recipe_quantity(receta, quantity)
        return parent_receta, qty, issue_note, is_derived

    def _resolve_audit_status(
        self,
        *,
        has_catalog_issue: bool,
        closing_inventory_available: bool,
        difference: Decimal,
        waste_total: Decimal,
    ) -> tuple[str, str]:
        tolerance = Decimal("0.01")
        if has_catalog_issue:
            return (
                ProductoMonthClosureLine.AUDIT_STATUS_REVISAR_CATALOGO,
                "La línea tiene una incidencia de catálogo u homologación.",
            )
        if not closing_inventory_available:
            return (
                ProductoMonthClosureLine.AUDIT_STATUS_SIN_INVENTARIO_FISICO,
                "No hay snapshot Point de inventario final para comparar contra el teórico.",
            )
        if abs(difference) <= tolerance:
            if Decimal(str(waste_total or 0)) > tolerance:
                return (
                    ProductoMonthClosureLine.AUDIT_STATUS_CUADRA_CON_MERMA,
                    "El cierre cuadra después de descontar la merma registrada.",
                )
            return (
                ProductoMonthClosureLine.AUDIT_STATUS_CUADRA,
                "El inventario teórico coincide con el inventario final Point.",
            )
        if difference < ZERO:
            return (
                ProductoMonthClosureLine.AUDIT_STATUS_SOBRANTE_FISICO,
                "Point reporta más inventario físico que el inventario teórico.",
            )
        return (
            ProductoMonthClosureLine.AUDIT_STATUS_FALTANTE_NO_EXPLICADO,
            "El inventario teórico es mayor al inventario físico Point.",
        )

    def _build_closing_inventory_validation(
        self,
        *,
        closing_meta: dict,
        line_rows: list[dict[str, object]],
        totals: dict[str, Decimal],
    ) -> dict[str, object]:
        warnings: list[str] = []
        blocking_issues: list[str] = []
        snapshot_rows = int(closing_meta.get("snapshot_rows") or 0)
        snapshot_within_tolerance = bool(closing_meta.get("snapshot_within_tolerance"))
        matched_recipe_count = int(closing_meta.get("matched_recipe_count") or 0)
        unmatched_products = list(closing_meta.get("unmatched_products") or [])
        total_check = totals["closing_cedis"] + totals["closing_sucursales"]
        inventory_math_ok = abs(total_check - totals["closing_total"]) <= Decimal("0.01")

        if snapshot_rows <= 0:
            blocking_issues.append("No existe inventario final Point para auditar el cierre mensual.")
        elif not snapshot_within_tolerance:
            blocking_issues.append("El snapshot de inventario final Point esta fuera de tolerancia.")
        if snapshot_rows > 0 and matched_recipe_count <= 0:
            blocking_issues.append("El inventario final Point no pudo homologarse a recetas ERP.")
        if unmatched_products:
            warnings.append("El inventario final Point trae productos sin homologacion Point -> ERP.")
        if not inventory_math_ok:
            blocking_issues.append("La suma CEDIS + sucursales no coincide con el total Point del cierre.")

        review_rows = [
            {
                "label": "Inventario Point disponible",
                "status": "Correcto" if snapshot_rows > 0 and snapshot_within_tolerance else "Revisar",
                "passed": bool(snapshot_rows > 0 and snapshot_within_tolerance),
                "detail": (
                    f"{snapshot_rows} snapshots; fecha efectiva "
                    f"{closing_meta.get('snapshot_effective_date') or 'sin fecha'}."
                ),
            },
            {
                "label": "Homologación de productos",
                "status": "Correcto" if matched_recipe_count > 0 else "Revisar",
                "passed": bool(matched_recipe_count > 0),
                "detail": f"{matched_recipe_count} recetas homologadas en el inventario final.",
            },
            {
                "label": "Consistencia matemática",
                "status": "Correcto" if inventory_math_ok and line_rows else "Revisar",
                "passed": bool(inventory_math_ok and line_rows),
                "detail": (
                    f"CEDIS + sucursales = {total_check}; total Point = {totals['closing_total']}."
                ),
            },
        ]

        return {
            "warnings": list(dict.fromkeys(warnings)),
            "blocking_issues": list(dict.fromkeys(blocking_issues)),
            "automation_reviews": review_rows,
            "closing_inventory": {
                "snapshot_rows": snapshot_rows,
                "matched_recipe_count": matched_recipe_count,
                "unmatched_products_count": len(unmatched_products),
                "snapshot_effective_date": closing_meta.get("snapshot_effective_date") or "",
                "closing_cedis": str(totals["closing_cedis"]),
                "closing_sucursales": str(totals["closing_sucursales"]),
                "closing_total": str(totals["closing_total"]),
                "difference": str(totals["difference"]),
            },
        }

    def _build_validation_summary(self, *, opening_meta: dict) -> dict[str, object]:
        unmatched_products = list(opening_meta.get("unmatched_products") or [])
        snapshot_fallback_used = bool(opening_meta.get("snapshot_fallback_used"))
        snapshot_within_tolerance = bool(opening_meta.get("snapshot_within_tolerance", True))
        snapshot_missing_exact_day = bool(opening_meta.get("snapshot_missing_exact_day"))
        bootstrap_seeded = bool(opening_meta.get("bootstrap_seeded"))
        upstream_validation_blocking_issues = list(opening_meta.get("upstream_validation_blocking_issues") or [])
        warnings: list[str] = []
        blocking_issues: list[str] = []

        if snapshot_fallback_used:
            warnings.append("El opening uso snapshot previo dentro de tolerancia.")
        if bootstrap_seeded:
            warnings.append("El opening proviene de un bootstrap historico aprobado.")
        if opening_meta.get("previous_closure_opening_basis") == "theoretical_closing":
            warnings.append("El opening heredado usa cierre teorico previo porque no habia inventario fisico Point.")
        if unmatched_products:
            blocking_issues.append("Existen productos del opening sin homologacion Point -> ERP.")
        if snapshot_missing_exact_day and not snapshot_within_tolerance:
            blocking_issues.append("No existe snapshot exacto ni valido dentro de tolerancia para el opening.")
        if upstream_validation_blocking_issues:
            blocking_issues.append("El opening heredado proviene de un cierre previo con incidencias activas.")

        return {
            "snapshot_fallback_used": snapshot_fallback_used,
            "snapshot_missing_exact_day": snapshot_missing_exact_day,
            "snapshot_within_tolerance": snapshot_within_tolerance,
            "bootstrap_seeded": bootstrap_seeded,
            "upstream_opening_issue_count": len(upstream_validation_blocking_issues),
            "unmatched_opening_products_count": len(unmatched_products),
            "warnings": list(dict.fromkeys(warnings)),
            "blocking_issues": list(dict.fromkeys(blocking_issues)),
        }

    def _build_sales_validation_summary(self, *, month_start: date, month_end: date, sales_meta: dict) -> dict[str, object]:
        mode = str((sales_meta or {}).get("mode") or "").strip()
        warnings: list[str] = []
        blocking_issues: list[str] = []
        sales_job = None
        official_rows = 0
        legacy_rows = 0

        if mode == "official_point_daily_sales":
            month_rows = PointDailySale.objects.filter(sale_date__gte=month_start, sale_date__lte=month_end)
            official_rows = month_rows.filter(source_endpoint=OFFICIAL_POINT_DAILY_SOURCE).count()
            legacy_rows = month_rows.filter(source_endpoint="/Report/VentasCategorias").count()
            sales_job = self._find_latest_official_sales_job(month_start=month_start, month_end=month_end)
            if official_rows <= 0:
                blocking_issues.append("No existen filas oficiales en PointDailySale para soportar el cierre mensual.")
            if legacy_rows > 0:
                blocking_issues.append("PointDailySale todavia mezcla filas legacy y oficiales en el mes.")
            if sales_job is None:
                blocking_issues.append("No existe un job oficial de ventas trazable para el mes que soporta el cierre.")
            elif sales_job.status != PointSyncJob.STATUS_SUCCESS:
                blocking_issues.append(
                    f"El job oficial de ventas del mes termino en estado {sales_job.status} y el cierre no debe bloquearse."
                )
        elif mode == "bridge_history":
            blocking_issues.append(
                "La venta del cierre proviene de VentaHistorica POINT_BRIDGE_SALES y requiere validacion manual previa al lock."
            )
        elif mode == "official_monthly_report":
            warnings.append("El cierre usa el reporte oficial mensual agregado de Point.")
        elif mode == "production_facts":
            warnings.append("El cierre usa FactProduccionDiaria consolidado desde PostgreSQL ERP.")

        return {
            "sales_source_mode": mode,
            "sales_job_id": sales_job.id if sales_job is not None else None,
            "sales_job_status": sales_job.status if sales_job is not None else "",
            "sales_official_rows": official_rows,
            "sales_legacy_rows": legacy_rows,
            "warnings": list(dict.fromkeys(warnings)),
            "blocking_issues": list(dict.fromkeys(blocking_issues)),
        }

    def _find_latest_official_sales_job(self, *, month_start: date, month_end: date) -> PointSyncJob | None:
        for job in PointSyncJob.objects.filter(job_type=PointSyncJob.JOB_TYPE_SALES).order_by("-started_at", "-id")[:50]:
            params = dict(job.parameters or {})
            if params.get("source") != "POINT_OFFICIAL_REPORT":
                continue
            start_raw = str(params.get("start_date") or "").strip()
            end_raw = str(params.get("end_date") or "").strip()
            if not start_raw or not end_raw:
                continue
            try:
                start_date = datetime.strptime(start_raw, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_raw, "%Y-%m-%d").date()
            except ValueError:
                continue
            if start_date == month_start and end_date == month_end:
                return job
        return None

    def _build_notes(
        self,
        *,
        opening_source: str,
        month_start: date,
        prev_month_end: date,
        opening_meta: dict,
        sales_meta: dict,
        validation: dict[str, object],
    ) -> str:
        if opening_source == ProductoMonthClosure.OPENING_SOURCE_PREVIOUS_CLOSURE:
            message = (
                f"Cierre {month_start:%Y-%m} construido con opening desde cierre previo "
                f"{opening_meta.get('previous_month_start', prev_month_end.isoformat())}."
            )
            if opening_meta.get("bootstrap_seeded"):
                message += " El opening arrastra un bootstrap historico como semilla."
        else:
            effective_date = opening_meta.get("snapshot_effective_date") or prev_month_end
            message = f"Cierre {month_start:%Y-%m} construido con snapshot Point al {effective_date}."
        if validation.get("snapshot_fallback_used"):
            message += " Se uso fallback de snapshot dentro de tolerancia."
        if validation.get("unmatched_opening_products_count"):
            message += (
                f" Hay {validation['unmatched_opening_products_count']} producto(s) del opening sin homologacion."
            )
        if validation.get("upstream_opening_issue_count"):
            message += " El opening arrastra incidencias del cierre previo."
        if (sales_meta or {}).get("mode") == "official_monthly_report":
            message += " La venta usa reporte oficial mensual de Point."
        elif (sales_meta or {}).get("mode") == "official_point_daily_sales":
            message += " La venta usa PointDailySale oficial por sucursal y dia."
        elif (sales_meta or {}).get("mode") == "production_facts":
            message += " La venta usa FactProduccionDiaria consolidado desde PostgreSQL ERP."
        elif (sales_meta or {}).get("mode") == "bridge_history":
            message += " La venta usa VentaHistorica POINT_BRIDGE_SALES."
        closing_inventory = dict(validation.get("closing_inventory") or {})
        if closing_inventory.get("snapshot_effective_date"):
            message += f" El inventario final Point usa snapshot efectivo {closing_inventory['snapshot_effective_date']}."
        warnings = list((sales_meta or {}).get("warnings") or [])
        if warnings:
            message += f" {' '.join(warnings)}"
        message += " Se excluyen preparaciones, accesorios, letreros y vasos preparados."
        return message

    def _build_bootstrap_seed_rows(self, *, seed_rows: list[dict[str, object]]):
        buckets: dict[int, _AggregateBucket] = {}
        unmatched_products: list[str] = []
        imported_rows = 0
        direct_rows = 0
        derived_rows_ignored = 0

        for row in seed_rows:
            receta = row.get("receta")
            if receta is None:
                unmatched_name = str(row.get("source_name") or "").strip()
                if unmatched_name:
                    unmatched_products.append(unmatched_name)
                continue

            imported_rows += 1
            parent_receta, qty, issue_note, is_derived = self._canonical_recipe_quantity(
                receta=receta,
                quantity=Decimal(str(row.get("quantity") or 0)),
            )
            if parent_receta is None:
                continue
            if not self._is_recipe_eligible_for_closure(parent_receta):
                continue
            if is_derived:
                derived_rows_ignored += 1
                continue
            bucket = buckets.setdefault(parent_receta.id, _AggregateBucket())
            bucket.value += qty
            bucket.row_count += 1
            bucket.direct_value += qty
            direct_rows += 1
            if issue_note:
                bucket.has_catalog_issue = True
                bucket.issue_notes.add(issue_note)

        if not buckets:
            raise ProductMonthClosureError("El bootstrap no produjo recetas homologadas para sembrar el opening historico.")

        opening_meta = {
            "bootstrap_seeded": True,
            "unmatched_products": unmatched_products[:50],
            "imported_rows": imported_rows,
            "direct_rows": direct_rows,
            "derived_rows_ignored": derived_rows_ignored,
        }
        validation = self._build_validation_summary(opening_meta=opening_meta)
        line_rows: list[dict[str, object]] = []

        for receta_id in sorted(buckets):
            receta = Receta.objects.get(pk=receta_id)
            bucket = buckets[receta_id]
            line_rows.append(
                {
                    "receta": receta,
                    "inventario_inicial_teorico": bucket.value,
                    "inventario_final_teorico": bucket.value,
                    "has_catalog_issue": bucket.has_catalog_issue,
                    "catalog_issue_note": " | ".join(sorted(bucket.issue_notes or set()))[:255],
                    "metadata": {
                        "opening_source": ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
                        "bootstrap_seed_rows": bucket.row_count,
                        "bootstrap_direct_value": str(bucket.direct_value),
                        "bootstrap_derived_value_ignored": str(bucket.derived_value),
                    },
                }
            )

        return line_rows, opening_meta, validation

    def _is_recipe_eligible_for_closure(self, receta: Receta) -> bool:
        if receta.excluir_cierre:
            return False
        if receta.tipo != Receta.TIPO_PRODUCTO_FINAL:
            return False
        normalized_name = normalizar_nombre(receta.nombre or "")
        normalized_meta = " ".join(
            part for part in [
                normalizar_nombre(receta.categoria or ""),
                normalizar_nombre(receta.familia or ""),
            ] if part
        )
        if normalized_name.startswith("sabor "):
            return False
        if normalized_name.endswith(" kg") or normalized_name.endswith(" kilo"):
            return False
        if any(token in normalized_name for token in CLOSURE_EXCLUDED_NAME_TOKENS):
            return False
        if any(token in normalized_meta for token in CLOSURE_EXCLUDED_META_TOKENS):
            return False
        return True

    def _build_bootstrap_notes(
        self,
        *,
        month_start: date,
        source_label: str,
        validation: dict[str, object],
    ) -> str:
        message = (
            f"Cierre {month_start:%Y-%m} sembrado con bootstrap historico desde {source_label}. "
            "Se usa solo como semilla auditada para destrabar el opening del siguiente mes."
        )
        if validation.get("unmatched_opening_products_count"):
            message += (
                f" Quedaron {validation['unmatched_opening_products_count']} producto(s) del opening sin homologacion."
            )
        return message
