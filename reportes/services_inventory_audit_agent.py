from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from core.models import Notificacion
from core.notificaciones import crear_notificacion
from logistica.models import DiscrepanciaLogistica
from reportes.models import ProductInventoryAuditCase
from rrhh.models import Empleado


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
            "notifications": 0,
        }
        month_rows = list(
            ProductInventoryAuditCase.objects.filter(month=month)
            .order_by("id")
            .values("id", "product_id", "branch_id", "branch__erp_branch_id", "source_trace")
        )
        self._prepare_month_context(month, month_rows)
        try:
            for row in month_rows:
                with transaction.atomic():
                    case = (
                        ProductInventoryAuditCase.objects.select_for_update()
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
                        notification = crear_notificacion(
                            usuario=case.assigned_to,
                            titulo="Inventario: diferencia de atención inmediata",
                            mensaje=(
                                f"{case.branch.name} · {case.product.name} · "
                                f"diferencia {case.difference}. Revisa la trazabilidad disponible."
                            ),
                            url=reverse("reportes:inventory_audit_case", args=[case.id]),
                            tipo=Notificacion.TIPO_SISTEMA,
                            prioridad=Notificacion.PRIORIDAD_ALTA,
                            objeto_tipo="reportes.ProductInventoryAuditCase",
                            objeto_id=case.id,
                        )
                        if notification is not None:
                            case.last_notified_fingerprint = result.fingerprint
                            case.save(
                                update_fields=["last_notified_fingerprint", "updated_at"]
                            )
                            counters["notifications"] += 1
        finally:
            self._discrepancy_cache = None
            self._head_cache = {}
            self._recurrence_cache = None
        return counters

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

        product_ids = {row["product_id"] for row in month_rows}
        recurrence_sets: dict[tuple[int, tuple[str, int]], set] = {}
        if product_ids:
            prior_rows = (
                ProductInventoryAuditCase.objects.filter(product_id__in=product_ids)
                .exclude(month=month)
                .exclude(
                    movement_status__in=(
                        ProductInventoryAuditCase.MovementStatus.BALANCED,
                        ProductInventoryAuditCase.MovementStatus.RESOLVED,
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

    def investigate_case(self, case: ProductInventoryAuditCase) -> InvestigationResult:
        issue_codes = sorted(set(case.issue_codes or []))
        discrepancies = self._related_logistics_discrepancies(case)
        recurrence_count = self._recurrence_count(case)
        facts = self._facts(case, discrepancies)
        hypotheses: list[str] = []
        missing: list[str] = []

        if case.movement_status == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE:
            attention = ProductInventoryAuditCase.AttentionLevel.GROUPED
            area = ProductInventoryAuditCase.ResponsibleArea.ADMINISTRATION
            assigned_to_id = None
            assignment_reason = "Caso agrupado: la fuente mensual está incompleta."
            missing.append("Completar o recuperar la fuente faltante antes de conciliar el producto.")
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
            if "TRANSFER_QUANTITY_MISMATCH" in issue_codes and not discrepancies:
                missing.append("Relacionar la transferencia con una evidencia logística explícita.")

        summary = {
            "facts": facts,
            "hypotheses": hypotheses,
            "missing": missing,
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
        if discrepancies or "TRANSFER_QUANTITY_MISMATCH" in issue_codes:
            return ProductInventoryAuditCase.ResponsibleArea.LOGISTICS
        if {"MISSING_CONVERSION_ORIGIN", "MISSING_CONVERSION_DESTINATION"} & set(issue_codes):
            return ProductInventoryAuditCase.ResponsibleArea.PRODUCTION
        if "MISSING_WASTE" in issue_codes:
            return ProductInventoryAuditCase.ResponsibleArea.SALES
        return ProductInventoryAuditCase.ResponsibleArea.ADMINISTRATION

    def _attention_level(self, case, issue_codes, discrepancies, recurrence_count):
        if (
            discrepancies
            or self.HIGH_ISSUES.intersection(issue_codes)
            or case.expected_closing < 0
            or recurrence_count > 0
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
            .exclude(month=case.month)
            .exclude(
                movement_status__in=(
                    ProductInventoryAuditCase.MovementStatus.BALANCED,
                    ProductInventoryAuditCase.MovementStatus.RESOLVED,
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
