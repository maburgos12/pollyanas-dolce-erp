from __future__ import annotations

from calendar import monthrange
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal

from django.db import models, transaction
from django.db.models import Q
from django.utils import timezone
from unidecode import unidecode

from control.models import DevolucionSucursalMatriz, MermaMensualSucursal
from core.models import Sucursal
from pos_bridge.models import PointTransferLine, PointWasteLine
from recetas.models import Receta, VentaHistorica
from recetas.utils.cierre_equivalencias import resolve_closure_recipe_quantity
from ventas.services.sales_canonical_source import POINT_BRIDGE_SALES_SOURCE


ZERO = Decimal("0")


@dataclass
class ConsolidationResult:
    period: date
    dry_run: bool
    source_rows: int
    grouped_rows: int
    created: int = 0
    updated: int = 0
    skipped: int = 0
    branch_homologated_rows: int = 0
    unresolved_branch_rows: int = 0
    total_units: Decimal = ZERO
    total_cost: Decimal = ZERO
    warnings: list[str] | None = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []

    def as_dict(self) -> dict[str, object]:
        return {
            "period": self.period.strftime("%Y-%m"),
            "dry_run": self.dry_run,
            "source_rows": self.source_rows,
            "grouped_rows": self.grouped_rows,
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "branch_homologated_rows": self.branch_homologated_rows,
            "unresolved_branch_rows": self.unresolved_branch_rows,
            "total_units": str(self.total_units),
            "total_cost": str(self.total_cost),
            "warnings": self.warnings[:30],
        }


def parse_month(period: str | date) -> date:
    if isinstance(period, date):
        return date(period.year, period.month, 1)
    parsed = datetime.strptime(str(period).strip(), "%Y-%m").date()
    return date(parsed.year, parsed.month, 1)


def month_bounds(period: date):
    start = date(period.year, period.month, 1)
    end = date(period.year, period.month, monthrange(period.year, period.month)[1])
    return start, end


def aware_day_bounds(start: date, end: date):
    tz = timezone.get_current_timezone()
    return (
        timezone.make_aware(datetime.combine(start, time.min), tz),
        timezone.make_aware(datetime.combine(end, time.max), tz),
    )


class MermaDevolucionAuditService:
    def consolidar_mermas(self, *, period: str | date, dry_run: bool = True) -> ConsolidationResult:
        month_start, month_end = month_bounds(parse_month(period))
        start_dt, end_dt = aware_day_bounds(month_start, month_end)
        rows = (
            PointWasteLine.objects.select_related("branch", "erp_branch", "receta")
            .filter(movement_at__gte=start_dt, movement_at__lte=end_dt)
            .order_by("id")
        )
        branch_matcher = _SucursalNameMatcher()
        buckets: dict[tuple[int | None, str, int | None, str], dict[str, object]] = {}
        skipped = 0
        warnings: list[str] = []
        branch_homologated_rows = 0
        unresolved_branch_rows = 0

        for row in rows:
            sucursal = row.erp_branch
            branch_point_name = row.branch.name if row.branch_id else ""
            branch_resolution = "POINT_ERP_BRANCH"
            if sucursal is None:
                sucursal = branch_matcher.find(branch_point_name)
                if sucursal is not None:
                    branch_homologated_rows += 1
                    branch_resolution = "NAME_MATCH"
                else:
                    unresolved_branch_rows += 1
                    branch_resolution = "UNRESOLVED"
                    warnings.append(f"Merma sin sucursal ERP homologada: {row.item_name} ({branch_point_name})")
            receta = row.receta
            qty = Decimal(str(row.quantity or 0))
            if receta is not None:
                parent, qty, _issue, _derived, _source = resolve_closure_recipe_quantity(receta, qty)
                if parent is None:
                    skipped += 1
                    continue
                receta = parent
            product_name = receta.nombre if receta is not None else (row.item_name or row.item_code or "SIN_RECETA")
            branch_key = sucursal.id if sucursal else None
            key = (branch_key, _normalize_match_text(branch_point_name), receta.id if receta else None, product_name)
            bucket = buckets.setdefault(
                key,
                {
                    "sucursal": sucursal,
                    "branch_point": branch_point_name,
                    "branch_resolution": branch_resolution,
                    "receta": receta,
                    "nombre_producto": product_name,
                    "unidades_merma": ZERO,
                    "costo_merma": ZERO,
                    "justificaciones": Counter(),
                    "source_ids": [],
                },
            )
            line_cost = Decimal(str(row.total_cost or 0))
            if line_cost <= 0:
                line_cost = Decimal(str(row.unit_cost or 0)) * Decimal(str(row.quantity or 0))
            bucket["unidades_merma"] += qty
            bucket["costo_merma"] += line_cost
            if row.justification:
                bucket["justificaciones"][row.justification.strip()[:200]] += 1
            bucket["source_ids"].append(row.id)
            if bucket["branch_resolution"] == "UNRESOLVED" and branch_resolution == "NAME_MATCH":
                bucket["sucursal"] = sucursal
                bucket["branch_resolution"] = branch_resolution

        sales_units = self._sales_units_by_branch_recipe(month_start=month_start, month_end=month_end)
        result = ConsolidationResult(
            period=month_start,
            dry_run=dry_run,
            source_rows=rows.count(),
            grouped_rows=len(buckets),
            skipped=skipped,
            branch_homologated_rows=branch_homologated_rows,
            unresolved_branch_rows=unresolved_branch_rows,
            warnings=warnings,
        )

        with transaction.atomic():
            for (_branch_id, _branch_point_key, receta_id, product_name), bucket in buckets.items():
                sold = ZERO
                if bucket["sucursal"] is not None:
                    sold = sales_units.get((bucket["sucursal"].id, receta_id), ZERO)
                waste_units = Decimal(str(bucket["unidades_merma"] or 0))
                pct = ZERO
                if sold > 0:
                    pct = (waste_units / sold * Decimal("100")).quantize(Decimal("0.01"))
                justification = ""
                if bucket["justificaciones"]:
                    justification = bucket["justificaciones"].most_common(1)[0][0]
                result.total_units += waste_units
                result.total_cost += Decimal(str(bucket["costo_merma"] or 0))
                if dry_run:
                    continue
                obj, created = MermaMensualSucursal.objects.update_or_create(
                    periodo=month_start,
                    sucursal=bucket["sucursal"],
                    receta=bucket["receta"],
                    nombre_producto=product_name,
                    defaults={
                        "unidades_merma": waste_units,
                        "costo_merma": Decimal(str(bucket["costo_merma"] or 0)).quantize(Decimal("0.01")),
                        "unidades_vendidas": sold,
                        "pct_merma_sobre_venta": pct,
                        "justificacion_principal": justification,
                        "fuente": "POINT_BRIDGE_WASTE",
                        "metadata": {
                            "source_model": "pos_bridge.PointWasteLine",
                            "source_ids": bucket["source_ids"][:200],
                            "branch_point": bucket["branch_point"],
                            "branch_resolution": bucket["branch_resolution"],
                        },
                    },
                )
                result.created += int(created)
                result.updated += int(not created)
            if dry_run:
                transaction.set_rollback(True)
        result.total_cost = result.total_cost.quantize(Decimal("0.01"))
        return result

    def clasificar_devoluciones(self, *, period: str | date, dry_run: bool = True) -> ConsolidationResult:
        month_start, month_end = month_bounds(parse_month(period))
        start_dt, end_dt = aware_day_bounds(month_start, month_end)
        rows = (
            PointTransferLine.objects.select_related("origin_branch", "destination_branch", "erp_origin_branch", "receta")
            .filter(registered_at__gte=start_dt, registered_at__lte=end_dt, receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
            .filter(self._destination_devoluciones_filter())
            .exclude(self._origin_non_store_filter())
            .exclude(is_cancelled=True)
            .order_by("id")
        )
        result = ConsolidationResult(
            period=month_start,
            dry_run=dry_run,
            source_rows=rows.count(),
            grouped_rows=rows.count(),
        )

        with transaction.atomic():
            for row in rows:
                qty = self._transfer_quantity(row)
                if qty <= 0:
                    result.skipped += 1
                    continue
                cost = (qty * Decimal(str(row.unit_cost or 0))).quantize(Decimal("0.01"))
                result.total_units += qty
                result.total_cost += cost
                if row.erp_origin_branch_id is None:
                    result.warnings.append(f"Devolucion sin sucursal ERP origen: {row.origin_branch.name} · {row.item_name}")
                if dry_run:
                    continue
                _, created = DevolucionSucursalMatriz.objects.update_or_create(
                    transfer_line=row,
                    defaults={
                        "periodo": month_start,
                        "sucursal_origen": row.erp_origin_branch,
                        "receta": row.receta,
                        "unidades": qty,
                        "costo_estimado": cost,
                        "motivo": DevolucionSucursalMatriz.MOTIVO_VIDA_UTIL,
                        "metadata": {
                            "source_model": "pos_bridge.PointTransferLine",
                            "flujo": "SUCURSAL_DEVOLUCIONES",
                            "es_perdida": False,
                            "nota": "Punto intermedio por vida útil; no se reconoce merma hasta transferencia a MERMA.",
                            "origin_branch_name": row.origin_branch.name,
                            "origin_branch_external_id": row.origin_branch.external_id,
                            "destination_branch_name": row.destination_branch.name,
                            "destination_branch_external_id": row.destination_branch.external_id,
                            "item_name": row.item_name,
                            "transfer_external_id": row.transfer_external_id,
                            "registered_at": row.registered_at.isoformat() if row.registered_at else "",
                        },
                    },
                )
                result.created += int(created)
                result.updated += int(not created)
            if dry_run:
                transaction.set_rollback(True)
        result.total_cost = result.total_cost.quantize(Decimal("0.01"))
        return result

    def _sales_units_by_branch_recipe(self, *, month_start: date, month_end: date) -> dict[tuple[int, int | None], Decimal]:
        buckets: dict[tuple[int, int | None], Decimal] = {}
        rows = (
            VentaHistorica.objects.select_related("sucursal", "receta")
            .filter(fecha__gte=month_start, fecha__lte=month_end, fuente=POINT_BRIDGE_SALES_SOURCE, sucursal__isnull=False)
            .order_by("id")
        )
        for row in rows:
            receta = row.receta
            qty = Decimal(str(row.cantidad or 0))
            if receta is not None:
                parent, qty, _issue, _derived, _source = resolve_closure_recipe_quantity(receta, qty)
                receta = parent
            key = (row.sucursal_id, receta.id if receta is not None else None)
            buckets[key] = buckets.get(key, ZERO) + qty
        return buckets

    def _destination_devoluciones_filter(self):
        return Q(destination_branch__name__icontains="DEVOLUCIONES") | Q(destination_branch__normalized_name__icontains="devoluciones")

    def _origin_non_store_filter(self):
        return (
            Q(erp_origin_branch__codigo__iexact="MATRIZ")
            | Q(origin_branch__name__icontains="MATRIZ")
            | Q(origin_branch__name__icontains="DEVOLUCIONES")
            | Q(origin_branch__normalized_name__icontains="devoluciones")
            | Q(origin_branch__name__icontains="MERMA")
            | Q(origin_branch__normalized_name__icontains="merma")
        )

    def _transfer_quantity(self, row: PointTransferLine) -> Decimal:
        for value in (row.received_quantity, row.sent_quantity, row.requested_quantity):
            qty = Decimal(str(value or 0))
            if qty > 0:
                return qty
        return ZERO


def merma_audit_context(*, period: str | date, sucursal_id: int | None = None) -> dict[str, object]:
    month_start = parse_month(period)
    mermas = MermaMensualSucursal.objects.select_related("sucursal", "receta").filter(periodo=month_start)
    devoluciones = DevolucionSucursalMatriz.objects.select_related("sucursal_origen", "receta", "transfer_line").filter(periodo=month_start)
    if sucursal_id:
        mermas = mermas.filter(sucursal_id=sucursal_id)
        devoluciones = devoluciones.filter(sucursal_origen_id=sucursal_id)
    merma_rows = list(mermas.order_by("-costo_merma", "sucursal__codigo", "nombre_producto"))
    devolucion_rows = list(devoluciones.order_by("-unidades", "receta__nombre")[:200])
    total_cost = sum((row.costo_merma for row in merma_rows), ZERO)
    total_waste_units = sum((row.unidades_merma for row in merma_rows), ZERO)
    total_sold_units = sum((row.unidades_vendidas for row in merma_rows), ZERO)
    pct = ZERO
    if total_sold_units > 0:
        pct = (total_waste_units / total_sold_units * Decimal("100")).quantize(Decimal("0.01"))
    trend = [
        {
            "periodo": row["periodo"].strftime("%Y-%m"),
            "costo_merma": row["costo"] or ZERO,
        }
        for row in MermaMensualSucursal.objects.values("periodo").annotate(costo=models.Sum("costo_merma")).order_by("periodo")
    ]
    return {
        "period": month_start,
        "sucursal_id": sucursal_id,
        "mermas": merma_rows,
        "devoluciones": devolucion_rows,
        "kpis": {
            "total_costo_merma": total_cost,
            "pct_merma_sobre_venta": pct,
            "top_productos": merma_rows[:3],
            "top_sucursales": _top_sucursales(merma_rows),
        },
        "trend": trend,
        "sucursales": Sucursal.objects.filter(activa=True).order_by("codigo"),
    }


def _top_sucursales(rows: list[MermaMensualSucursal]) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for row in rows:
        key = str(row.sucursal_id or row.metadata.get("branch_point", "SIN_SUCURSAL"))
        bucket = buckets.setdefault(
            key,
            {
                "sucursal": row.sucursal,
                "sucursal_label": row.sucursal.codigo if row.sucursal_id else row.metadata.get("branch_point", "Sin sucursal ERP"),
                "costo": ZERO,
            },
        )
        bucket["costo"] += row.costo_merma
    return sorted(buckets.values(), key=lambda item: item["costo"], reverse=True)[:3]


def _normalize_match_text(value: str) -> str:
    return " ".join(unidecode((value or "")).strip().lower().split())


class _SucursalNameMatcher:
    def __init__(self):
        self._by_name: dict[str, Sucursal] | None = None

    def find(self, value: str) -> Sucursal | None:
        normalized = _normalize_match_text(value)
        if not normalized:
            return None
        index = self._index()
        exact = index.get(normalized)
        if exact is not None:
            return exact
        candidates = {
            sucursal
            for key, sucursal in index.items()
            if len(key) >= 4 and len(normalized) >= 4 and (key in normalized or normalized in key)
        }
        if len(candidates) == 1:
            return candidates.pop()
        return None

    def _index(self) -> dict[str, Sucursal]:
        if self._by_name is None:
            by_name: dict[str, Sucursal] = {}
            for sucursal in Sucursal.objects.all():
                for value in (sucursal.codigo, sucursal.nombre):
                    normalized = _normalize_match_text(value)
                    if normalized:
                        by_name.setdefault(normalized, sucursal)
            self._by_name = by_name
        return self._by_name
