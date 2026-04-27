from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from pos_bridge.models import PointDailySale, PointInventorySnapshot, PointProductionLine, PointSyncJob, PointWasteLine
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import (
    ProductoMonthClosure,
    ProductoMonthClosureLine,
    Receta,
    VentaHistorica,
)
from recetas.utils.derived_product_presentations import get_active_derived_relation
from recetas.utils.normalizacion import normalizar_nombre

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
    snapshot_count: int = 0
    has_catalog_issue: bool = False
    issue_notes: set[str] | None = None

    def __post_init__(self):
        if self.issue_notes is None:
            self.issue_notes = set()


class ProductMonthClosureService:
    DEFAULT_SNAPSHOT_TOLERANCE_DAYS = 3

    def __init__(self, matcher: PointSalesMatchingService | None = None):
        self.matcher = matcher or PointSalesMatchingService()
        self.official_sales_report_service = PointSalesCategoryReportService()

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
    ) -> ProductoMonthClosure:
        month_start = self._parse_month(month)
        existing_closure = ProductoMonthClosure.objects.filter(month_start=month_start).order_by("-id").first()
        if existing_closure is not None and existing_closure.is_locked:
            if rebuild:
                raise ProductMonthClosureError(f"El cierre {month_start:%Y-%m} esta bloqueado y no permite rebuild.")
            raise ProductMonthClosureError(f"El cierre {month_start:%Y-%m} esta bloqueado.")

        plan = self.preview(month=month_start)
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
                    source_snapshot_count=row["source_snapshot_count"],
                    source_sale_rows=row["source_sale_rows"],
                    source_production_rows=row["source_production_rows"],
                    source_waste_rows=row["source_waste_rows"],
                    has_catalog_issue=row["has_catalog_issue"],
                    catalog_issue_note=row["catalog_issue_note"],
                    metadata={
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

    def preview(self, *, month: str | date) -> dict[str, object]:
        month_start = self._parse_month(month)
        month_end = date(month_start.year, month_start.month, monthrange(month_start.year, month_start.month)[1])
        prev_month_end = month_start - timedelta(days=1)

        opening_source, opening_reference_date, openings, opening_meta = self._load_opening(month_start=month_start)
        production = self._load_production(month_start=month_start, month_end=month_end)
        sales, sales_meta = self._load_sales(month_start=month_start, month_end=month_end)
        waste = self._load_waste(month_start=month_start, month_end=month_end)

        recipe_ids = set(openings) | set(production) | set(sales) | set(waste)
        if not recipe_ids:
            raise ProductMonthClosureError(f"No hay datos para construir cierre mensual {month_start:%Y-%m}.")

        line_rows: list[dict[str, object]] = []
        validation = self._build_validation_summary(opening_meta=opening_meta)
        sales_validation = self._build_sales_validation_summary(
            month_start=month_start,
            month_end=month_end,
            sales_meta=sales_meta,
        )
        validation["warnings"] = list(
            dict.fromkeys(list(validation.get("warnings") or []) + list(sales_validation.get("warnings") or []))
        )
        validation["blocking_issues"] = list(
            dict.fromkeys(
                list(validation.get("blocking_issues") or []) + list(sales_validation.get("blocking_issues") or [])
            )
        )
        validation.update(
            {
                "sales_source_mode": sales_validation.get("sales_source_mode", ""),
                "sales_job_id": sales_validation.get("sales_job_id"),
                "sales_job_status": sales_validation.get("sales_job_status", ""),
                "sales_official_rows": sales_validation.get("sales_official_rows", 0),
                "sales_legacy_rows": sales_validation.get("sales_legacy_rows", 0),
            }
        )
        totals = {
            "opening": ZERO,
            "production": ZERO,
            "sales": ZERO,
            "waste": ZERO,
            "ending": ZERO,
        }
        catalog_issue_count = 0

        for receta_id in sorted(recipe_ids):
            receta = Receta.objects.get(pk=receta_id)
            if not self._is_recipe_eligible_for_closure(receta):
                continue
            opening_bucket = openings.get(receta_id, _AggregateBucket())
            production_bucket = production.get(receta_id, _AggregateBucket())
            sales_bucket = sales.get(receta_id, _AggregateBucket())
            waste_bucket = waste.get(receta_id, _AggregateBucket())
            sale_total = sales_bucket.direct_value + sales_bucket.derived_value
            waste_total = waste_bucket.direct_value + waste_bucket.derived_value
            ending_total = opening_bucket.value + production_bucket.value - sale_total - waste_total
            issue_notes = set()
            for bucket in (opening_bucket, production_bucket, sales_bucket, waste_bucket):
                if bucket.has_catalog_issue:
                    issue_notes.update(bucket.issue_notes or set())
            if issue_notes:
                catalog_issue_count += 1

            totals["opening"] += opening_bucket.value
            totals["production"] += production_bucket.value
            totals["sales"] += sale_total
            totals["waste"] += waste_total
            totals["ending"] += ending_total

            line_rows.append(
                {
                    "receta": receta,
                    "inventario_inicial_teorico": opening_bucket.value,
                    "produccion_mes": production_bucket.value,
                    "venta_directa_enteros": sales_bucket.direct_value,
                    "venta_derivada_equivalente": sales_bucket.derived_value,
                    "venta_total_equivalente": sale_total,
                    "merma_directa_enteros": waste_bucket.direct_value,
                    "merma_derivada_equivalente": waste_bucket.derived_value,
                    "merma_total_equivalente": waste_total,
                    "inventario_final_teorico": ending_total,
                    "source_snapshot_count": opening_bucket.snapshot_count,
                    "source_sale_rows": sales_bucket.row_count,
                    "source_production_rows": production_bucket.row_count,
                    "source_waste_rows": waste_bucket.row_count,
                    "has_catalog_issue": bool(issue_notes),
                    "catalog_issue_note": " | ".join(sorted(issue_notes))[:255],
                }
            )

        if not line_rows:
            raise ProductMonthClosureError(
                f"No hay productos terminados elegibles para construir el cierre mensual {month_start:%Y-%m}."
            )

        validation["catalog_issue_line_count"] = catalog_issue_count
        if catalog_issue_count:
            validation["blocking_issues"].append("Existen lineas con incidencias de catalogo o derivadas.")
        validation["lock_ready"] = not validation["blocking_issues"]
        notes = self._build_notes(
            opening_source=opening_source,
            month_start=month_start,
            prev_month_end=prev_month_end,
            opening_meta=opening_meta,
            sales_meta=sales_meta,
            validation=validation,
        )
        return {
            "month_start": month_start,
            "month_end": month_end,
            "opening_source": opening_source,
            "opening_reference_date": opening_reference_date,
            "notes": notes,
            "line_rows": line_rows,
            "metadata": {
                "opening_meta": opening_meta,
                "sales_meta": sales_meta,
                "recipe_count": len(recipe_ids),
                "validation": validation,
            },
            "totals": totals,
        }

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
            for line in previous_closure.lines.select_related("receta_padre").all():
                buckets[line.receta_padre_id] = _AggregateBucket(value=Decimal(str(line.inventario_final_teorico or 0)))
            previous_metadata = dict(previous_closure.metadata or {})
            previous_opening_meta = dict(previous_metadata.get("opening_meta") or {})
            previous_validation = dict(previous_metadata.get("validation") or {})
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
        target_start = timezone.make_aware(datetime.combine(snapshot_date, time.min), timezone.get_current_timezone())
        target_end = timezone.make_aware(datetime.combine(snapshot_date, time.max), timezone.get_current_timezone())
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
        effective_date = selected_at.date()
        day_start = timezone.make_aware(datetime.combine(effective_date, time.min), timezone.get_current_timezone())
        day_end = timezone.make_aware(datetime.combine(effective_date, time.max), timezone.get_current_timezone())
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

    def _load_production(self, *, month_start: date, month_end: date):
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
                bridge_buckets, bridge_meta = self._load_sales_from_bridge_history(
                    month_start=month_start,
                    month_end=month_end,
                )
                bridge_meta["fallback_reason"] = str(exc)
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
        relation = get_active_derived_relation(receta)
        if relation is None:
            return receta, quantity, "", False
        units_per_parent = Decimal(str(relation.unidades_por_padre or 0))
        if units_per_parent <= 0:
            return receta, quantity, f"Relacion derivada sin unidades_por_padre para {receta.nombre}", False
        return relation.receta_padre, quantity / units_per_parent, "", True

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
        elif (sales_meta or {}).get("mode") == "bridge_history":
            message += " La venta usa VentaHistorica POINT_BRIDGE_SALES."
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
