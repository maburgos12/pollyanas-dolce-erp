from __future__ import annotations

import hashlib
import json
import logging
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from core.models import Notificacion
from core.notificaciones import crear_notificacion
from logistica.models import DiscrepanciaLogistica, RutaCargaChecklistLinea
from pos_bridge.models import PointTransferLine
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from reportes.services_inventory_audit_report import case_balance_status
from pos_bridge.services.branch_inventory_traceability_service import (
    canonical_point_branch_identity,
)
from pos_bridge.services.daily_inventory_break_service import (
    DailyBreakProjection,
    DailyBreakStatus,
    DailyInventoryBreakService,
)
from reportes.models import ProductInventoryAuditCase
from rrhh.models import Empleado


logger = logging.getLogger(__name__)
LOCAL_TZ = ZoneInfo("America/Mazatlan")
SOURCE_LABELS = {
    "sales": "ventas",
    "production": "producciones",
    "waste": "mermas",
    "transfer_in": "entradas por transferencia",
    "transfer_out": "salidas por transferencia",
    "transfer_return": "retornos al origen",
    "conversion_in": "entradas por conversión",
    "conversion_out": "salidas por conversión",
}
POINT_HISTORY_LABELS = {
    "production": "producción",
    "sales": "ventas",
    "waste": "mermas",
    "transfer_in": "entradas por transferencia",
    "transfer_out": "salidas por transferencia",
    "identified_adjustment": "ajustes de inventario",
}


def _quantity_label(value) -> str:
    return format(Decimal(str(value or 0)).normalize(), "f")


@dataclass(frozen=True)
class InvestigationResult:
    attention_level: str
    responsible_area: str
    assigned_to_id: int | None
    assignment_reason: str
    summary: dict
    fingerprint: str


class InventoryAuditAgent:
    """Builds an auditable projection from records already stored in the ERP."""

    OPEN_LOGISTICS_STATES = (
        DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE,
        DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA,
    )
    HIGH_ISSUES = {
        "MISSING_CONVERSION_ORIGIN",
        "MISSING_CONVERSION_DESTINATION",
        "TRANSFER_QUANTITY_MISMATCH",
        "NEGATIVE_EXPECTED_CLOSING",
    }
    AREA_DEPARTMENT = {
        ProductInventoryAuditCase.ResponsibleArea.LOGISTICS: Empleado.DEP_LOGISTICA,
        ProductInventoryAuditCase.ResponsibleArea.SALES: Empleado.DEP_VENTAS,
        ProductInventoryAuditCase.ResponsibleArea.PRODUCTION: Empleado.DEP_PRODUCCION,
        ProductInventoryAuditCase.ResponsibleArea.ADMINISTRATION: Empleado.DEP_ADMINISTRACION,
    }

    def __init__(self):
        self._discrepancy_cache = None
        self._head_cache: dict[str, tuple[int | None, str]] = {}
        self._recurrence_cache = None
        self._daily_break_cache = None
        self._transfer_cache = None
        self._history_cache = None

    def run_month(self, month, *, dry_run: bool = False) -> dict[str, int]:
        month = month.replace(day=1)
        counters = {
            "total": 0,
            "high": 0,
            "normal": 0,
            "grouped": 0,
            "assigned": 0,
            "unassigned": 0,
            "updated": 0,
            "notification_groups": 0,
            "notifications": 0,
        }
        notification_groups: dict[tuple[int, str], list[tuple[int, str]]] = {}
        branch_aliases, _ = canonical_point_branch_identity()
        month_rows = list(
            ProductInventoryAuditCase.objects.sold_products().filter(
                month=month,
                branch_id__in=set(branch_aliases.values()),
            )
            .order_by("id")
            .values("id", "product_id", "branch_id", "branch__erp_branch_id", "source_trace")
        )
        self._prepare_month_context(month, month_rows)
        try:
            for row in month_rows:
                with transaction.atomic():
                    case = (
                        ProductInventoryAuditCase.objects.select_for_update(of=("self",))
                        .select_related("branch", "product")
                        .get(pk=row["id"])
                    )
                    result = self.investigate_case(case)
                    counters["total"] += 1
                    counters[result.attention_level.lower()] += 1
                    counters["assigned" if result.assigned_to_id else "unassigned"] += 1

                    changed = self._projection_changed(case, result)
                    should_notify = (
                        result.attention_level == ProductInventoryAuditCase.AttentionLevel.HIGH
                        and result.assigned_to_id is not None
                        and case.last_notified_fingerprint != result.fingerprint
                    )
                    if dry_run:
                        if should_notify:
                            notification_groups.setdefault(
                                (result.assigned_to_id, result.responsible_area), []
                            ).append((case.id, result.fingerprint))
                        continue

                    if changed:
                        case.attention_level = result.attention_level
                        case.responsible_area = result.responsible_area
                        case.assigned_to_id = result.assigned_to_id
                        case.assignment_reason = result.assignment_reason
                        case.investigation_summary = result.summary
                        case.investigation_fingerprint = result.fingerprint
                        case.investigated_at = timezone.now()
                        case.save(
                            update_fields=[
                                "attention_level",
                                "responsible_area",
                                "assigned_to",
                                "assignment_reason",
                                "investigation_summary",
                                "investigation_fingerprint",
                                "investigated_at",
                                "updated_at",
                            ]
                        )
                        counters["updated"] += 1

                    if should_notify:
                        notification_groups.setdefault(
                            (result.assigned_to_id, result.responsible_area), []
                        ).append((case.id, result.fingerprint))
            counters["notification_groups"] = len(notification_groups)
            if not dry_run:
                counters["notifications"] = self._notify_groups(
                    month, notification_groups
                )
        finally:
            self._discrepancy_cache = None
            self._head_cache = {}
            self._recurrence_cache = None
            self._daily_break_cache = None
            self._transfer_cache = None
            self._history_cache = None
        return counters

    @staticmethod
    def _notify_groups(month, notification_groups) -> int:
        created = 0
        area_labels = dict(ProductInventoryAuditCase.ResponsibleArea.choices)
        for (assigned_to_id, area), expected_cases in sorted(
            notification_groups.items()
        ):
            expected_by_id = dict(expected_cases)
            with transaction.atomic():
                cases = list(
                    ProductInventoryAuditCase.objects.select_for_update()
                    .filter(id__in=expected_by_id)
                    .order_by("id")
                )
                pending = [
                    case
                    for case in cases
                    if case.attention_level
                    == ProductInventoryAuditCase.AttentionLevel.HIGH
                    and case.assigned_to_id == assigned_to_id
                    and case.investigation_fingerprint == expected_by_id[case.id]
                    and case.last_notified_fingerprint
                    != case.investigation_fingerprint
                ]
                if not pending:
                    continue
                notification = crear_notificacion(
                    usuario=pending[0].assigned_to,
                    titulo=f"Inventario: {len(pending)} diferencias requieren atención",
                    mensaje=(
                        f"{len(pending)} diferencias de atención inmediata en "
                        f"{area_labels[area]}. El agente auditor las concentró para revisión."
                    ),
                    url=(
                        f"{reverse('reportes:inventory_audit')}?month={month:%Y-%m}"
                        f"&attention={ProductInventoryAuditCase.AttentionLevel.HIGH}"
                    ),
                    tipo=Notificacion.TIPO_SISTEMA,
                    prioridad=Notificacion.PRIORIDAD_ALTA,
                    objeto_tipo="reportes.ProductInventoryAuditCaseGroup",
                    objeto_id=f"{month:%Y-%m}:{area}:{assigned_to_id}",
                )
                if notification is None:
                    continue
                for case in pending:
                    case.last_notified_fingerprint = case.investigation_fingerprint
                    case.save(
                        update_fields=["last_notified_fingerprint", "updated_at"]
                    )
                created += 1
        return created

    def _prepare_month_context(self, month, month_rows):
        transfer_ids = {
            int(transfer_id)
            for row in month_rows
            for transfer_id in (row["source_trace"] or {}).get("transfers", [])
        }
        discrepancy_cache = {transfer_id: [] for transfer_id in transfer_ids}
        if transfer_ids:
            discrepancies = (
                DiscrepanciaLogistica.objects.filter(
                    linea_carga__point_transfer_line_id__in=transfer_ids,
                    estado__in=self.OPEN_LOGISTICS_STATES,
                )
                .select_related("ruta", "asignado_a", "linea_carga")
                .order_by("id")
            )
            for item in discrepancies:
                discrepancy_cache[item.linea_carga.point_transfer_line_id].append(item)
        self._discrepancy_cache = discrepancy_cache
        self._transfer_cache = self._transfer_evidence(transfer_ids)

        product_ids = {row["product_id"] for row in month_rows}
        recurrence_sets: dict[tuple[int, tuple[str, int]], set] = {}
        if product_ids:
            prior_rows = (
                ProductInventoryAuditCase.objects.filter(product_id__in=product_ids)
                .filter(month__lt=month)
                .exclude(difference=0)
                .exclude(
                    movement_status__in=(
                        ProductInventoryAuditCase.MovementStatus.BALANCED,
                        ProductInventoryAuditCase.MovementStatus.RESOLVED,
                        ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
                    )
                )
                .values("product_id", "branch_id", "branch__erp_branch_id", "month")
            )
            for row in prior_rows:
                branch_key = self._branch_key(
                    row["branch_id"], row["branch__erp_branch_id"]
                )
                recurrence_sets.setdefault(
                    (row["product_id"], branch_key), set()
                ).add(row["month"])
        self._recurrence_cache = {
            key: len(months) for key, months in recurrence_sets.items()
        }

        cases = list(
            ProductInventoryAuditCase.objects.filter(
                id__in=[row["id"] for row in month_rows]
            )
            .select_related("branch", "product")
            .order_by("id")
        )
        self._history_cache = AuditStockHistoryService().reconcile_many(
            [case for case in cases if case.movement_status not in {"BALANCED", "RESOLVED"}],
            month,
            include_zero_difference=True,
        )
        try:
            self._daily_break_cache = DailyInventoryBreakService().build_month(
                month, cases
            )
        except Exception:
            logger.exception("No fue posible proyectar los cortes diarios de inventario")
            self._daily_break_cache = {
                case.id: self._insufficient_projection(
                    "No fue posible leer los cortes diarios conservados."
                )
                for case in cases
            }

    def investigate_case(self, case: ProductInventoryAuditCase) -> InvestigationResult:
        issue_codes = sorted(set(case.issue_codes or []))
        discrepancies = self._related_logistics_discrepancies(case)
        recurrence_count = self._recurrence_count(case)
        facts = self._facts(case, discrepancies)
        hypotheses: list[str] = []
        missing: list[str] = []
        balance_status = case_balance_status(case)
        transfers = self._case_transfers(case)
        for transfer in transfers:
            facts.append(transfer["fact"])
            if transfer["missing"]:
                missing.append(transfer["missing"])
        source_issues = (case.source_trace or {}).get("source_issues", [])
        for issue in source_issues:
            if issue.get("code") not in {"PRODUCT_RESOLVED_BY_SKU", "PRODUCT_RESOLVED_BY_NAME"}:
                missing.append(issue["message"])
        point_history = (case.source_trace or {}).get("point_history")
        if not isinstance(point_history, dict):
            point_history = {}
        history_resolved = False
        cached_history = (self._history_cache or {}).get((case.branch_id, case.product_id))
        if not point_history and cached_history is not None:
            point_history = cached_history.as_dict(
                opening=case.opening_point, point_closing=case.point_closing)
        if (point_history.get("coverage_status") == "COMPLETE"
                and not point_history.get("unknown_movement_ids")
                and balance_status != "SOURCE_INCOMPLETE"):
            conversion_out = Decimal(str(point_history.get("conversion_out") or 0))
            conversion_in = Decimal(str(point_history.get("conversion_in") or 0))
            remainder = Decimal(
                str(point_history.get("unexplained_remainder") or 0)
            )
            if conversion_out or conversion_in:
                net_out = conversion_out - conversion_in
                net_label = (
                    f"una salida de {_quantity_label(net_out)}"
                    if net_out >= 0
                    else f"una entrada de {_quantity_label(abs(net_out))}"
                )
                facts.append(
                    "Point acredita "
                    f"{_quantity_label(conversion_out)} piezas de salida por conversión "
                    f"y {_quantity_label(conversion_in)} piezas de entrada por conversión; "
                    f"el efecto neto es {net_label}."
                )
            comparison = point_history.get("aggregate_comparison") or {}
            for source, label in POINT_HISTORY_LABELS.items():
                gap = comparison.get(source) or {}
                if gap:
                    facts.append(
                        "El historial Point registra "
                        f"{_quantity_label(gap.get('point_history'))} piezas de {label}; "
                        "el reporte agregado registraba "
                        f"{_quantity_label(gap.get('aggregate'))}."
                    )
            conversion_gap = comparison.get("conversion_out") or {}
            if conversion_gap:
                facts.append(
                    "El reporte agregado no incluyó "
                    f"{_quantity_label(conversion_gap.get('difference'))} piezas de salida "
                    "por conversión; el historial transaccional sí las conserva."
                )
            if remainder == 0:
                history_resolved = True
                facts.append(
                    f"El historial transaccional de Point explica el cierre de "
                    f"{_quantity_label(point_history.get('point_closing'))} sin "
                    "unidades pendientes de localizar."
                )
        daily_break = (
            self._daily_break_cache.get(case.id)
            if self._daily_break_cache is not None
            else None
        ) or self._insufficient_projection(
            "No se preparó una proyección diaria para este expediente."
        )

        if case.movement_status == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE:
            attention = ProductInventoryAuditCase.AttentionLevel.GROUPED
            area = ProductInventoryAuditCase.ResponsibleArea.ADMINISTRATION
            assigned_to_id = None
            assignment_reason = "Caso agrupado: la fuente mensual está incompleta."
            opening_date = case.month - timedelta(days=1)
            closing_date = date(case.month.year, case.month.month,
                                monthrange(case.month.year, case.month.month)[1])
            for source, stamp in (("opening", opening_date), ("closing", closing_date)):
                if not (case.source_trace or {}).get(source):
                    missing.append(
                        f"Falta fuente de {'apertura' if source == 'opening' else 'cierre'} Point del "
                        f"{stamp:%d/%m/%Y}: {case.product.name} en {case.branch.name}. "
                        "El valor vacío no acredita inventario cero.")
            if not missing:
                missing.extend(issue["message"] for issue in case.run.source_issues)
            if not missing:
                missing.append(f"No se conservó la evidencia que originó SOURCE_INCOMPLETE "
                               f"en el expediente {case.pk}; reconstruir desde las fuentes guardadas.")
        else:
            area = self._responsible_area(issue_codes, discrepancies)
            attention = self._attention_level(
                case, issue_codes, discrepancies, recurrence_count
            )
            if attention == ProductInventoryAuditCase.AttentionLevel.HIGH:
                assigned_to_id, assignment_reason = self._resolve_assignee(
                    area, discrepancies
                )
            else:
                assigned_to_id = None
                assignment_reason = "Caso registrado sin asignación individual por su prioridad."
            if "MISSING_CONVERSION_ORIGIN" in issue_codes:
                hypotheses.append("La conversión está comprobada, pero Point no identifica el producto origen.")
                missing.append("Identificar el producto origen de la conversión sin asignarlo por aproximación.")
            if "MISSING_CONVERSION_DESTINATION" in issue_codes:
                hypotheses.append("Point no identifica el producto destino de la conversión.")
                missing.append("Identificar el producto destino de la conversión.")
            if "TRANSFER_QUANTITY_MISMATCH" in issue_codes and not discrepancies and not transfers:
                missing.append("Relacionar la transferencia con una evidencia logística explícita.")

        if balance_status == "BALANCED":
            facts.append("El saldo de inventario cuadra con Point; los documentos pendientes "
                         "no se presentan como piezas faltantes.")
        elif point_history.get("coverage_status") == "INCOMPLETE":
            missing.append(f"El historial Point conservado de {case.product.name} en "
                           f"{case.branch.name} no cubre todo {case.month:%m/%Y}; "
                           "no acredita el saldo mensual completo.")
        elif not point_history and balance_status == "NEEDS_EXPLANATION":
            missing.append(f"No hay historial transaccional Point conservado para "
                           f"{case.product.name} en {case.branch.name} que explique "
                           f"la diferencia de {_quantity_label(case.difference)} en {case.month:%m/%Y}.")
        if point_history.get("unknown_movement_ids"):
            missing.append("Movimientos Point sin efecto identificado: " +
                           ", ".join(map(str, point_history["unknown_movement_ids"])) + ".")

        if history_resolved or balance_status != "NEEDS_EXPLANATION":
            pass
        elif daily_break.status == DailyBreakStatus.FOUND:
            checkpoint = daily_break.first_mismatch_checkpoint
            if checkpoint is not None:
                facts.append(
                    "El primer corte fuera del saldo posible fue "
                    f"{self._checkpoint_label(checkpoint)} con diferencia "
                    f"{checkpoint.difference}."
                )
        elif daily_break.status == DailyBreakStatus.INCONCLUSIVE:
            missing.append(
                "Point no informa la hora de todos los movimientos del día; "
                "el corte permanece dentro del rango posible."
            )
        elif daily_break.status == DailyBreakStatus.INSUFFICIENT_EVIDENCE:
            missing.append(
                "No existe un corte intermedio suficiente para localizar el inicio de la diferencia."
            )

        summary = {
            "balance_status": balance_status,
            "traceability_status": (
                "COMPLETE" if case.movement_status in {"BALANCED", "RESOLVED"}
                and not discrepancies else "PENDING"),
            "transfer_evidence": transfers,
            "facts": facts,
            "hypotheses": hypotheses,
            "missing": list(dict.fromkeys(missing)),
            "daily_break": (
                {} if history_resolved or balance_status != "NEEDS_EXPLANATION"
                else self._daily_break_summary(case, daily_break)
            ),
            "point_history": point_history,
            "related_logistics_discrepancy_ids": [item.id for item in discrepancies],
            "recurrence_count": recurrence_count,
            "grouping_key": self._grouping_key(case, issue_codes),
        }
        payload = {
            "case_id": case.id,
            "calculation_fingerprint": case.calculation_fingerprint,
            "attention_level": attention,
            "responsible_area": area,
            "assigned_to_id": assigned_to_id,
            "summary": summary,
        }
        fingerprint = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return InvestigationResult(
            attention_level=attention,
            responsible_area=area,
            assigned_to_id=assigned_to_id,
            assignment_reason=assignment_reason,
            summary=summary,
            fingerprint=fingerprint,
        )

    @staticmethod
    def _transfer_evidence(transfer_ids):
        """Reuse explicit Point line links; never match a route by name or quantity."""
        lines = {}
        for line in RutaCargaChecklistLinea.objects.filter(
                point_transfer_line_id__in=transfer_ids).exclude(estatus='SUPERADA').select_related('parada__ruta'):
            lines[line.point_transfer_line_id] = line
        evidence = {}
        for transfer in PointTransferLine.objects.filter(id__in=transfer_ids).select_related(
                'origin_branch', 'destination_branch').order_by('id'):
            if transfer.is_received and transfer.received_at and transfer.is_finalized and transfer.sent_quantity == transfer.received_quantity:
                continue
            ref = f"{transfer.transfer_external_id}/{transfer.detail_external_id}"
            origin = transfer.origin_branch.name if transfer.origin_branch_id else 'Origen no identificado'
            destination = transfer.destination_branch.name if transfer.destination_branch_id else 'Destino no identificado'
            stamp = transfer.sent_at or transfer.registered_at
            sent, received = _quantity_label(transfer.sent_quantity), _quantity_label(transfer.received_quantity)
            fact = f"Transferencia Point {ref}, {origin} → {destination}: {sent} enviadas"
            if stamp:
                fact += f" el {stamp.astimezone(LOCAL_TZ):%d/%m/%Y}"
            has_receipt = bool(transfer.is_received and transfer.received_at)
            fact += f", {received} recibidas." if has_receipt else "; sin recepción registrada."
            line = lines.get(transfer.pk)
            route_id = None
            if line:
                route_id = line.parada.ruta_id
                loaded = 'sin captura' if line.cantidad_cargada is None else _quantity_label(line.cantidad_cargada)
                fact += (f" Logística: ruta {line.parada.ruta.folio}, carga {loaded}, "
                         f"recepción de parada {line.parada.get_entrega_estado_display()}.")
            missing = ''
            if not has_receipt:
                missing = f"Transferencia {ref}: falta acreditar recepción en {destination} o retorno a {origin} de {sent} unidades."
            elif not transfer.is_finalized:
                missing = f"Transferencia {ref}: falta finalización Point; no se presume retorno al origen."
            elif transfer.sent_quantity != transfer.received_quantity:
                missing = f"Transferencia {ref}: conciliar {sent} enviadas contra {received} recibidas"
                if transfer.received_quantity < transfer.sent_quantity:
                    returned = _quantity_label(transfer.sent_quantity - transfer.received_quantity)
                    fact += f" Point contabiliza retorno de {returned} al origen {origin}; falta acreditar custodia física."
                missing += f"; {'revisar evidencia de la ruta' if line else 'no hay línea logística ligada al movimiento Point'} ."
            evidence[transfer.pk] = {'id': transfer.pk, 'reference': ref, 'origin': origin,
                'destination': destination, 'sent': sent, 'received_quantity': received,
                'received': has_receipt, 'finalized': transfer.is_finalized,
                'route_id': route_id, 'fact': fact, 'missing': missing}
        return evidence

    def _case_transfers(self, case):
        ids = (case.source_trace or {}).get('transfers', [])
        cache = self._transfer_cache
        if cache is None:
            cache = self._transfer_evidence(ids)
        return [cache[key] for key in ids if key in cache]

    @staticmethod
    def _insufficient_projection(message):
        return DailyBreakProjection(
            status=DailyBreakStatus.INSUFFICIENT_EVIDENCE,
            last_matching_checkpoint=None,
            first_mismatch_checkpoint=None,
            minimum=None,
            maximum=None,
            movement_ids_by_source={},
            warnings=(message,),
        )

    def _daily_break_summary(self, case, projection):
        data = projection.as_dict()
        data.update(
            {
                "last_matching_label": self._checkpoint_label(
                    projection.last_matching_checkpoint
                ),
                "first_mismatch_label": self._checkpoint_label(
                    projection.first_mismatch_checkpoint
                ),
                "unlocated_quantity": format(abs(Decimal(case.difference)), "f"),
                "movement_labels": [
                    f"{len(ids)} movimiento(s) de {SOURCE_LABELS.get(source, source)}"
                    for source, ids in sorted(
                        projection.movement_ids_by_source.items()
                    )
                    if ids
                ],
            }
        )
        return data

    @staticmethod
    def _checkpoint_label(checkpoint):
        if checkpoint is None:
            return "Sin corte anterior comprobado"
        return checkpoint.captured_at.astimezone(LOCAL_TZ).strftime("%d/%m/%Y %H:%M")

    @staticmethod
    def _projection_changed(case, result):
        return any(
            (
                case.attention_level != result.attention_level,
                case.responsible_area != result.responsible_area,
                case.assigned_to_id != result.assigned_to_id,
                case.assignment_reason != result.assignment_reason,
                case.investigation_summary != result.summary,
                case.investigation_fingerprint != result.fingerprint,
            )
        )

    def _related_logistics_discrepancies(self, case):
        transfer_ids = (case.source_trace or {}).get("transfers", [])
        if not transfer_ids:
            return []
        if self._discrepancy_cache is not None:
            return sorted(
                {
                    item.id: item
                    for transfer_id in transfer_ids
                    for item in self._discrepancy_cache.get(int(transfer_id), [])
                }.values(),
                key=lambda item: item.id,
            )
        return list(
            DiscrepanciaLogistica.objects.filter(
                linea_carga__point_transfer_line_id__in=transfer_ids,
                estado__in=self.OPEN_LOGISTICS_STATES,
            )
            .select_related("ruta", "asignado_a")
            .order_by("id")
        )

    @staticmethod
    def _facts(case, discrepancies):
        if case.movement_status == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE:
            facts = ["La fuente está incompleta; aún no se puede confirmar el saldo ni la diferencia."]
        else:
            facts = [
                f"Point cerró con {case.point_closing}; el saldo calculado es {case.expected_closing}.",
                f"La diferencia comprobada es {case.difference} unidad(es).",
            ]
        facts.extend(
            f"La discrepancia logística #{item.id} está ligada a la ruta {item.ruta.folio}."
            for item in discrepancies
        )
        return facts

    def _responsible_area(self, issue_codes, discrepancies):
        if discrepancies or {"TRANSFER_QUANTITY_MISMATCH", "INCOMPLETE_TRANSFER"} & set(issue_codes):
            return ProductInventoryAuditCase.ResponsibleArea.LOGISTICS
        if {"MISSING_CONVERSION_ORIGIN", "MISSING_CONVERSION_DESTINATION"} & set(issue_codes):
            return ProductInventoryAuditCase.ResponsibleArea.PRODUCTION
        if "MISSING_WASTE" in issue_codes:
            return ProductInventoryAuditCase.ResponsibleArea.SALES
        return ProductInventoryAuditCase.ResponsibleArea.ADMINISTRATION

    def _attention_level(self, case, issue_codes, discrepancies, recurrence_count):
        if case.movement_status in {
            ProductInventoryAuditCase.MovementStatus.BALANCED,
            ProductInventoryAuditCase.MovementStatus.RESOLVED,
        } and not discrepancies:
            return ProductInventoryAuditCase.AttentionLevel.GROUPED
        if (
            discrepancies
            or self.HIGH_ISSUES.intersection(issue_codes)
            or case.expected_closing < 0
            or (case.difference != 0 and recurrence_count > 0)
        ):
            return ProductInventoryAuditCase.AttentionLevel.HIGH
        if abs(case.difference) <= 1:
            return ProductInventoryAuditCase.AttentionLevel.GROUPED
        return ProductInventoryAuditCase.AttentionLevel.NORMAL

    def _recurrence_count(self, case):
        branch_key = self._branch_key(case.branch_id, case.branch.erp_branch_id)
        if self._recurrence_cache is not None:
            return self._recurrence_cache.get((case.product_id, branch_key), 0)
        branch_filter = {"branch_id": case.branch_id}
        if case.branch.erp_branch_id:
            branch_filter = {"branch__erp_branch_id": case.branch.erp_branch_id}
        return (
            ProductInventoryAuditCase.objects.filter(
                product_id=case.product_id,
                **branch_filter,
            )
            .filter(month__lt=case.month)
            .exclude(difference=0)
            .exclude(
                movement_status__in=(
                    ProductInventoryAuditCase.MovementStatus.BALANCED,
                    ProductInventoryAuditCase.MovementStatus.RESOLVED,
                    ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
                )
            )
            .values("month")
            .distinct()
            .count()
        )

    @staticmethod
    def _branch_key(branch_id, erp_branch_id):
        return ("erp", erp_branch_id) if erp_branch_id else ("point", branch_id)

    def _resolve_assignee(self, area, discrepancies):
        explicit_ids = {item.asignado_a_id for item in discrepancies if item.asignado_a_id}
        if len(explicit_ids) == 1:
            return explicit_ids.pop(), "Responsable reutilizado de la discrepancia logística relacionada."
        if len(explicit_ids) > 1:
            return None, "Hay más de una persona asignada en las discrepancias logísticas relacionadas."

        department = self.AREA_DEPARTMENT[area]
        if department in self._head_cache:
            return self._head_cache[department]
        candidates = list(
            Empleado.objects.filter(
                activo=True,
                departamento=department,
                nivel_organizacional=Empleado.NIVEL_JEFATURA,
                usuario_erp__is_active=True,
            ).values_list("usuario_erp_id", flat=True)[:2]
        )
        if len(candidates) == 1:
            result = (
                candidates[0],
                f"Jefatura única activa del área {department.lower()}.",
            )
            self._head_cache[department] = result
            return result
        if len(candidates) > 1:
            result = (
                None,
                f"Hay más de una jefatura activa en {department.lower()}; requiere selección humana.",
            )
            self._head_cache[department] = result
            return result
        result = (None, f"No existe una jefatura activa única en {department.lower()}.")
        self._head_cache[department] = result
        return result

    @staticmethod
    def _grouping_key(case, issue_codes):
        if case.movement_status == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE:
            return f"{case.month:%Y-%m}:SOURCE_INCOMPLETE"
        if abs(case.difference) <= 1:
            return f"{case.month:%Y-%m}:MINOR:{case.branch_id}"
        return f"{case.month:%Y-%m}:{issue_codes[0] if issue_codes else 'UNEXPLAINED'}"
