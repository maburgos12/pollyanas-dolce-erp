from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum
from django.utils import timezone

from recetas.models import VentaHistorica
from ventas.models import EventoVenta, EventoVentaDetailSnapshot, EventoVentaFinancial, EventoVentaProjectionArtifact
from ventas.services.financials import EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE
from ventas.services.forecasting import (
    _event_executive_main_day_benchmark_sales,
    _select_event_homologue_window,
    build_event_executive_projection_model,
    executive_event_product_scope,
)
ZERO = Decimal("0")
REVIEW_CODES = [
    "DÍADELNIÑO-260430-001",
    "DÍADELASMADRES-260510-001",
    "DÍADELPADRE-260621-001",
]


@dataclass
class AuditCheck:
    name: str
    passed: bool
    detail: str


class Command(BaseCommand):
    help = (
        "Ejecuta una auditoría de 10 revisiones sobre el loop de forecast estacional y "
        "compara 2026 vs 2025 día por día para los eventos revisados."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--event-code",
            action="append",
            dest="event_codes",
            help="Código de evento a auditar. Repetible. Si se omite, usa Niño/Madres/Padre.",
        )
        parser.add_argument(
            "--enforce-status",
            action="store_true",
            help="Aplica el sync de estado con guardrails antes de auditar.",
        )
        parser.add_argument(
            "--write-report",
            action="store_true",
            help="Escribe un reporte markdown en output/forecast_audits/.",
        )

    def handle(self, *args, **options):
        event_codes = options.get("event_codes") or REVIEW_CODES
        enforce_status = bool(options.get("enforce_status"))
        write_report = bool(options.get("write_report"))

        events = list(EventoVenta.objects.filter(code__in=event_codes).order_by("main_date", "code"))
        if len(events) != len(set(event_codes)):
            found = {event.code for event in events}
            missing = [code for code in event_codes if code not in found]
            raise CommandError(f"No se encontraron todos los eventos solicitados. Faltan: {missing}")

        sections: list[str] = []
        sections.append("# Auditoría de 10 revisiones del loop de forecast estacional")
        sections.append("")
        sections.append(f"- Ejecutada: {timezone.localtime().isoformat()}")
        sections.append(f"- Eventos: {', '.join(event.code for event in events)}")
        sections.append(f"- enforce_status: {'sí' if enforce_status else 'no'}")
        sections.append("")

        for event in events:
            forecast_qs = event.forecasts.all().select_related("product", "branch")
            if not forecast_qs.exists():
                raise CommandError(f"El evento {event.code} no tiene forecast persistido.")

            snapshot = EventoVentaDetailSnapshot.objects.filter(sales_event=event).first()
            snapshot_payload = (snapshot.payload_json or {}) if snapshot else {}
            executive_dataset = snapshot_payload.get("executive_dataset") or {}
            dataset = {
                "summary": executive_dataset.get("summary") or {
                    "sales": snapshot_payload.get("week_projected_revenue") or "0",
                    "qty": snapshot_payload.get("week_total_qty") or "0",
                },
                "daily_rows": executive_dataset.get("daily_rows") or [],
                "plausibility": executive_dataset.get("plausibility") or {},
                "projection_model": executive_dataset.get("projection_model") or {},
            }
            product_ids = {row.product_id for row in forecast_qs}
            branch_ids = {row.branch_id for row in forecast_qs}
            homologue_start, homologue_end, homologue_main_day, homologue_mode = _select_event_homologue_window(
                event,
                product_ids=product_ids,
                branch_ids=branch_ids,
            )
            executive_model = dataset.get("projection_model") or build_event_executive_projection_model(
                event,
                forecast_rows=list(forecast_qs),
            )
            dg_main_day_benchmark = _event_executive_main_day_benchmark_sales(event)
            current_version = event.version
            base_financial = event.financials.filter(scenario="BASE").first()
            artifact_qs = event.projection_artifacts.filter(forecast_version=current_version)
            sanity = self._main_day_sanity(
                event=event,
                dataset=dataset,
                product_ids=product_ids,
                branch_ids=branch_ids,
                homologue_main_day=homologue_main_day,
                executive_model=executive_model,
            )

            active_products = list(event.products.filter(is_active=True).select_related("product"))
            out_of_scope_products: list[str] = []
            for link in active_products:
                allowed, reason = executive_event_product_scope(link.product)
                if not allowed:
                    out_of_scope_products.append(f"{link.product.nombre} [{reason}]")

            checks: list[AuditCheck] = []
            checks.append(AuditCheck("forecast_persistido", forecast_qs.exists(), f"filas={forecast_qs.count()}"))
            checks.append(
                AuditCheck(
                    "snapshot_persistido",
                    snapshot is not None and bool((snapshot.payload_json or {}).get("executive_dataset")),
                    f"snapshot_version={getattr(snapshot, 'snapshot_version', 'N/A')}",
                )
            )
            checks.append(
                AuditCheck(
                    "financial_base_persistido",
                    base_financial is not None,
                    f"base_sales={getattr(base_financial, 'estimated_sales', 'N/A')}",
                )
            )
            required_model_fields = {
                "benchmark_source",
                "same_store_factor",
                "expansion_factor",
                "contraction_factor",
                "mix_adjustment_source",
                "final_projection_reasoning",
            }
            missing_fields = sorted(field for field in required_model_fields if field not in executive_model)
            checks.append(
                AuditCheck(
                    "modelo_ejecutivo_trazable",
                    not missing_fields,
                    f"missing={missing_fields or 'ninguno'}",
                )
            )
            checks.append(
                AuditCheck(
                    "scope_comercial_sin_reservas_invalidas",
                    not out_of_scope_products,
                    f"out_of_scope={out_of_scope_products or 'ninguno'}",
                )
            )
            plausibility = dataset.get("plausibility", {}) or {}
            checks.append(
                AuditCheck(
                    "semana_no_flaggeada_por_plausibilidad",
                    not bool(plausibility.get("flagged")),
                    f"reason={plausibility.get('reason')} ref={plausibility.get('reference_sales_ceiling')}",
                )
            )
            target_total_qty = Decimal(str(executive_model.get("target_total_qty") or 0))
            current_total_qty = Decimal(str(executive_model.get("current_total_qty") or 0))
            checks.append(
                AuditCheck(
                    "semana_dentro_del_techo_ejecutivo",
                    target_total_qty <= ZERO or current_total_qty <= (target_total_qty + EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE),
                    (
                        f"current={current_total_qty} target={target_total_qty} "
                        f"tol={EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE.quantize(Decimal('0.001'))}"
                    ),
                )
            )
            checks.append(
                AuditCheck(
                    "dia_principal_sobre_piso_homologo",
                    not bool(sanity.get("below_floor")),
                    f"current={sanity.get('current_main_total')} floor={sanity.get('floor_target')} homologue={sanity.get('homologue_main_day')}",
                )
            )
            findings = self._local_guard_findings(
                dataset=dataset,
                executive_model=executive_model,
                sanity=sanity,
            )
            review_state_incorrect = (
                event.status in {EventoVenta.STATUS_LISTO_REVISION, EventoVenta.STATUS_PENDIENTE_DG} and bool(findings)
            )
            checks.append(
                AuditCheck(
                    "estado_sin_inconsistencia_de_guard",
                    not review_state_incorrect,
                    f"status={event.status} findings={len(findings)}",
                )
            )
            daily_comparisons = self._daily_comparisons(
                event=event,
                dataset=dataset,
                product_ids=product_ids,
                branch_ids=branch_ids,
                homologue_start=homologue_start,
                homologue_end=homologue_end,
            )
            checks.append(
                AuditCheck(
                    "comparativa_diaria_completa_vs_2025",
                    len(daily_comparisons) == len(dataset.get("daily_rows") or []),
                    f"dias={len(daily_comparisons)}/{len(dataset.get('daily_rows') or [])}",
                )
            )

            status_reset = False
            if enforce_status and review_state_incorrect:
                previous_status = event.status
                event.status = EventoVenta.STATUS_MODELADO
                event.save(update_fields=["status", "updated_at"])
                event.refresh_from_db()
                status_reset = True
                findings = [f"status_reset: {previous_status} -> {event.status}", *findings]

            product_comparisons = self._product_week_comparisons(
                event=event,
                product_ids=product_ids,
                branch_ids=branch_ids,
                homologue_start=homologue_start,
                homologue_end=homologue_end,
            )

            sections.extend(self._render_event_section(
                event=event,
                dataset=dataset,
                checks=checks,
                daily_comparisons=daily_comparisons,
                product_comparisons=product_comparisons,
                executive_model=executive_model,
                homologue_start=homologue_start,
                homologue_end=homologue_end,
                homologue_main_day=homologue_main_day,
                homologue_mode=homologue_mode,
                dg_main_day_benchmark=dg_main_day_benchmark,
                artifacts=list(artifact_qs),
                findings=findings,
                status_reset=status_reset,
            ))

        report = "\n".join(sections).rstrip() + "\n"
        self.stdout.write(report)

        if write_report:
            out_dir = Path("output/forecast_audits")
            out_dir.mkdir(parents=True, exist_ok=True)
            report_path = out_dir / f"seasonal_forecast_audit_{timezone.localdate().isoformat()}.md"
            report_path.write_text(report, encoding="utf-8")
            self.stdout.write(self.style.SUCCESS(f"Reporte escrito en {report_path}"))

    def _daily_comparisons(
        self,
        *,
        event: EventoVenta,
        dataset: dict,
        product_ids: set[int],
        branch_ids: set[int],
        homologue_start,
        homologue_end,
    ) -> list[dict[str, object]]:
        historical_by_day = {
            row["fecha"]: {
                "qty_2025": Decimal(str(row.get("qty") or 0)).quantize(Decimal("0.001")),
                "sales_2025": Decimal(str(row.get("sales") or 0)).quantize(Decimal("0.01")),
            }
            for row in (
                VentaHistorica.objects.filter(
                    fecha__range=(homologue_start, homologue_end),
                    receta_id__in=product_ids,
                    sucursal_id__in=branch_ids,
                )
                .values("fecha")
                .annotate(qty=Sum("cantidad"), sales=Sum("monto_total"))
                .order_by("fecha")
            )
        }
        current_by_day = {
            self._coerce_date(row["date"]): {
                "qty_2026": Decimal(str(row.get("qty") or 0)).quantize(Decimal("0.001")),
                "sales_2026": Decimal(str(row.get("sales") or 0)).quantize(Decimal("0.01")),
            }
            for row in (dataset.get("daily_rows") or [])
            if self._coerce_date(row.get("date"))
        }

        rows: list[dict[str, object]] = []
        day = event.analysis_start_date
        while day <= event.analysis_end_date:
            homologue_day = homologue_start + timedelta(days=(day - event.analysis_start_date).days)
            current = current_by_day.get(day, {"qty_2026": ZERO, "sales_2026": ZERO})
            historical = historical_by_day.get(homologue_day, {"qty_2025": ZERO, "sales_2025": ZERO})
            qty_2025 = Decimal(str(historical["qty_2025"]))
            sales_2025 = Decimal(str(historical["sales_2025"]))
            qty_2026 = Decimal(str(current["qty_2026"]))
            sales_2026 = Decimal(str(current["sales_2026"]))
            rows.append(
                {
                    "date_2026": day,
                    "date_2025": homologue_day,
                    "qty_2026": qty_2026,
                    "qty_2025": qty_2025,
                    "qty_delta": (qty_2026 - qty_2025).quantize(Decimal("0.001")),
                    "qty_ratio": ((qty_2026 / qty_2025).quantize(Decimal("0.0001")) if qty_2025 > ZERO else None),
                    "sales_2026": sales_2026,
                    "sales_2025": sales_2025,
                    "sales_delta": (sales_2026 - sales_2025).quantize(Decimal("0.01")),
                    "sales_ratio": ((sales_2026 / sales_2025).quantize(Decimal("0.0001")) if sales_2025 > ZERO else None),
                }
            )
            day += timedelta(days=1)
        return rows

    def _local_guard_findings(self, *, dataset: dict, executive_model: dict, sanity: dict) -> list[str]:
        findings: list[str] = []
        plausibility = dataset.get("plausibility", {}) or {}
        if plausibility.get("flagged"):
            findings.append(
                "semana_flaggeada_plausibilidad:"
                f" reason={plausibility.get('reason')}"
                f" ref={plausibility.get('reference_sales_ceiling')}"
            )
        target_total_qty = Decimal(str(executive_model.get("target_total_qty") or 0))
        current_total_qty = Decimal(str(executive_model.get("current_total_qty") or 0))
        if target_total_qty > ZERO and current_total_qty > (target_total_qty + EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE):
            findings.append(
                f"semana_sobre_techo: current={current_total_qty.quantize(Decimal('0.001'))} "
                f"target={target_total_qty.quantize(Decimal('0.001'))} "
                f"tol={EVENT_EXECUTIVE_TARGET_QTY_TOLERANCE.quantize(Decimal('0.001'))}"
            )
        if sanity.get("below_floor"):
            findings.append(
                f"dia_principal_bajo_piso: current={Decimal(str(sanity.get('current_main_total') or 0)).quantize(Decimal('0.001'))} "
                f"floor={Decimal(str(sanity.get('floor_target') or 0)).quantize(Decimal('0.001'))}"
            )
        return findings

    def _product_week_comparisons(
        self,
        *,
        event: EventoVenta,
        product_ids: set[int],
        branch_ids: set[int],
        homologue_start,
        homologue_end,
    ) -> list[dict[str, object]]:
        current_rows = (
            event.forecasts.filter(product_id__in=product_ids)
            .values("product_id", "product__nombre")
            .annotate(qty_2026=Sum("final_forecast"))
            .order_by("-qty_2026", "product__nombre")
        )
        hist_map = {
            row["receta_id"]: Decimal(str(row.get("qty_2025") or 0)).quantize(Decimal("0.001"))
            for row in (
                VentaHistorica.objects.filter(
                    fecha__range=(homologue_start, homologue_end),
                    receta_id__in=product_ids,
                    sucursal_id__in=branch_ids,
                )
                .values("receta_id")
                .annotate(qty_2025=Sum("cantidad"))
            )
        }
        comparisons: list[dict[str, object]] = []
        for row in current_rows[:15]:
            qty_2026 = Decimal(str(row.get("qty_2026") or 0)).quantize(Decimal("0.001"))
            qty_2025 = hist_map.get(row["product_id"], ZERO).quantize(Decimal("0.001"))
            comparisons.append(
                {
                    "product_name": row["product__nombre"],
                    "qty_2026": qty_2026,
                    "qty_2025": qty_2025,
                    "qty_delta": (qty_2026 - qty_2025).quantize(Decimal("0.001")),
                    "qty_ratio": ((qty_2026 / qty_2025).quantize(Decimal("0.0001")) if qty_2025 > ZERO else None),
                }
            )
        return comparisons

    def _render_event_section(
        self,
        *,
        event: EventoVenta,
        dataset: dict,
        checks: list[AuditCheck],
        daily_comparisons: list[dict[str, object]],
        product_comparisons: list[dict[str, object]],
        executive_model: dict[str, object],
        homologue_start,
        homologue_end,
        homologue_main_day,
        homologue_mode,
        dg_main_day_benchmark,
        artifacts: list[EventoVentaProjectionArtifact],
        findings: list[str],
        status_reset: bool,
    ) -> list[str]:
        summary = dataset["summary"]
        daily_rows = dataset.get("daily_rows") or []
        main_day_row = next(
            (row for row in daily_rows if self._coerce_date(row.get("date")) == event.main_date),
            None,
        )
        passed = sum(1 for check in checks if check.passed)
        lines: list[str] = []
        lines.append(f"## {event.code} · {event.name}")
        lines.append("")
        lines.append(f"- status: `{event.status}`")
        lines.append(f"- ventana 2026: `{event.analysis_start_date}` → `{event.analysis_end_date}`")
        lines.append(f"- homólogo 2025: `{homologue_start}` → `{homologue_end}` (`{homologue_mode}`)")
        lines.append(f"- día principal 2026: `{event.main_date}`")
        lines.append(f"- día principal homólogo: `{homologue_main_day}`")
        lines.append(f"- semana 2026: `${Decimal(str(summary['sales'])).quantize(Decimal('0.01'))}`")
        lines.append(f"- día principal 2026: `${Decimal(str(main_day_row['sales'] if main_day_row else 0)).quantize(Decimal('0.01'))}`")
        if dg_main_day_benchmark:
            lines.append(f"- benchmark DG día principal: `${Decimal(str(dg_main_day_benchmark)).quantize(Decimal('0.01'))}`")
        lines.append(
            f"- modelo ejecutivo: benchmark=`{executive_model.get('benchmark_source')}` "
            f"same_store=`{executive_model.get('same_store_factor')}` "
            f"expansion=`{executive_model.get('expansion_factor')}` "
            f"contraction=`{executive_model.get('contraction_factor')}`"
        )
        lines.append(f"- checks aprobados: `{passed}/10`")
        lines.append(f"- artifacts v{event.version}: `{len(artifacts)}`")
        lines.append(f"- status_reset_aplicado: `{'sí' if status_reset else 'no'}`")
        lines.append("")
        lines.append("### 10 revisiones")
        for idx, check in enumerate(checks, start=1):
            status = "OK" if check.passed else "FAIL"
            lines.append(f"{idx}. `{status}` {check.name}: {check.detail}")
        lines.append("")
        lines.append("### Comparativa diaria 2026 vs 2025")
        lines.append("| Día 2026 | Día 2025 | Qty 2026 | Qty 2025 | Ratio Qty | Venta 2026 | Venta 2025 | Ratio Venta |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for row in daily_comparisons:
            qty_ratio = f"{row['qty_ratio']}" if row["qty_ratio"] is not None else "N/A"
            sales_ratio = f"{row['sales_ratio']}" if row["sales_ratio"] is not None else "N/A"
            lines.append(
                f"| {row['date_2026']} | {row['date_2025']} | {row['qty_2026']} | {row['qty_2025']} | "
                f"{qty_ratio} | ${row['sales_2026']} | ${row['sales_2025']} | {sales_ratio} |"
            )
        lines.append("")
        lines.append("### Top productos semana 2026 vs 2025")
        lines.append("| Producto | Qty 2026 | Qty 2025 | Delta | Ratio |")
        lines.append("|---|---:|---:|---:|---:|")
        for row in product_comparisons:
            qty_ratio = f"{row['qty_ratio']}" if row["qty_ratio"] is not None else "N/A"
            lines.append(
                f"| {row['product_name']} | {row['qty_2026']} | {row['qty_2025']} | {row['qty_delta']} | {qty_ratio} |"
            )
        lines.append("")
        lines.append("### Findings del guard local")
        if findings:
            for finding in findings:
                lines.append(f"- {finding}")
        else:
            lines.append("- sin findings bloqueantes")
        lines.append("")
        return lines

    def _coerce_date(self, value) -> date | None:
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value:
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
        return None

    def _main_day_sanity(
        self,
        *,
        event: EventoVenta,
        dataset: dict,
        product_ids: set[int],
        branch_ids: set[int],
        homologue_main_day,
        executive_model: dict[str, object],
    ) -> dict[str, object]:
        current_main_row = next(
            (row for row in (dataset.get("daily_rows") or []) if str(row.get("date")) == event.main_date.isoformat()),
            None,
        )
        current_main_total = Decimal(str((current_main_row or {}).get("qty") or 0)).quantize(Decimal("0.001"))
        hist_main_total = Decimal(
            str(
                VentaHistorica.objects.filter(
                    fecha=homologue_main_day,
                    receta_id__in=product_ids,
                    sucursal_id__in=branch_ids,
                ).aggregate(total=Sum("cantidad")).get("total")
                or 0
            )
        ).quantize(Decimal("0.001"))
        growth_anchor_factor = Decimal(
            str(executive_model.get("growth_anchor_factor") or executive_model.get("same_store_factor") or 1)
        )
        floor_target = (hist_main_total * growth_anchor_factor * Decimal("0.98")).quantize(Decimal("0.001")) if hist_main_total > ZERO else ZERO
        below_floor = hist_main_total >= Decimal("120") and current_main_total > ZERO and current_main_total < floor_target
        return {
            "homologue_main_day": homologue_main_day,
            "current_main_total": current_main_total,
            "hist_main_total": hist_main_total,
            "homologue_ytd_factor": growth_anchor_factor.quantize(Decimal("0.0001")),
            "floor_target": floor_target,
            "below_floor": below_floor,
        }
