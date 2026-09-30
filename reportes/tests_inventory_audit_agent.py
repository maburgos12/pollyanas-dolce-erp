from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from django.contrib.auth import get_user_model

from core.models import Notificacion, Sucursal
from logistica.models import (
    DiscrepanciaLogistica,
    ParadaRuta,
    PuntoLogistico,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
)
from pos_bridge.models import PointBranch, PointProduct, PointTransferLine
from pos_bridge.services.daily_inventory_break_service import (
    DailyBreakProjection,
    DailyBreakStatus,
)
from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditRun
from rrhh.models import Empleado


class InventoryAuditAgentFixtures:
    @classmethod
    def setUpTestData(cls):
        cls.month = date(2026, 8, 1)
        cls.branch = PointBranch.objects.create(
            external_id="AUDITOR-AGENT-BRANCH",
            name="Sucursal agente auditor",
        )
        cls.product = PointProduct.objects.create(
            external_id="AUDITOR-AGENT-PRODUCT",
            sku="AGENT-001",
            name="Producto agente auditor",
        )
        cls.audit_run = ProductInventoryAuditRun.objects.create(
            month=cls.month,
            status=ProductInventoryAuditRun.Status.READY,
            calculation_fingerprint="a" * 64,
        )

    def make_case(self, **overrides):
        values = {
            "run": self.audit_run,
            "month": self.month,
            "branch": self.branch,
            "product": self.product,
            "opening_point": Decimal("10"),
            "production": Decimal("0"),
            "sales": Decimal("0"),
            "waste": Decimal("0"),
            "transfer_in": Decimal("0"),
            "transfer_out": Decimal("0"),
            "conversion_in": Decimal("0"),
            "conversion_out": Decimal("0"),
            "identified_adjustment": Decimal("0"),
            "expected_closing": Decimal("10"),
            "point_closing": Decimal("9"),
            "difference": Decimal("-1"),
            "movement_status": ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
            "calculation_fingerprint": "b" * 64,
            "rebuilt_at": timezone.now(),
        }
        values.update(overrides)
        return ProductInventoryAuditCase.objects.create(**values)


class InventoryAuditAgentModelTests(InventoryAuditAgentFixtures, TestCase):
    def test_case_starts_grouped_without_inventing_assignee(self):
        case = self.make_case()

        self.assertEqual(
            case.attention_level,
            ProductInventoryAuditCase.AttentionLevel.GROUPED,
        )
        self.assertEqual(
            case.responsible_area,
            ProductInventoryAuditCase.ResponsibleArea.ADMINISTRATION,
        )
        self.assertIsNone(case.assigned_to_id)
        self.assertEqual(case.assignment_reason, "")
        self.assertEqual(case.investigation_summary, {})
        self.assertEqual(case.investigation_fingerprint, "")
        self.assertEqual(case.last_notified_fingerprint, "")
        self.assertIsNone(case.investigated_at)


class InventoryAuditAgentServiceTests(InventoryAuditAgentFixtures, TestCase):
    def _head(self, *, username, department):
        user = get_user_model().objects.create_user(username=username)
        Empleado.objects.create(
            codigo=f"HEAD-{username}",
            nombre=username,
            departamento=department,
            nivel_organizacional=Empleado.NIVEL_JEFATURA,
            usuario_erp=user,
        )
        return user

    def _logistics_discrepancy(self, *, assigned_to):
        origin_erp = Sucursal.objects.create(codigo="AUD-ORIGIN", nombre="CEDIS prueba")
        destination_erp = Sucursal.objects.create(
            codigo="AUD-DESTINATION",
            nombre="Sucursal destino prueba",
        )
        self.branch.erp_branch = destination_erp
        self.branch.save(update_fields=["erp_branch", "updated_at"])
        origin = PointBranch.objects.create(
            external_id="AUDITOR-AGENT-ORIGIN",
            name="CEDIS prueba",
            erp_branch=origin_erp,
        )
        transfer = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=self.branch,
            erp_origin_branch=origin_erp,
            erp_destination_branch=destination_erp,
            transfer_external_id="37934",
            detail_external_id="532992",
            source_hash="c" * 64,
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            received_at=timezone.now(),
            item_name=self.product.name,
            item_code=self.product.sku,
            requested_quantity=Decimal("2"),
            sent_quantity=Decimal("2"),
            received_quantity=Decimal("1"),
            is_received=True,
            is_finalized=True,
        )
        route = RutaEntrega.objects.create(
            folio="RUT-202608-0029",
            nombre="Ruta auditoría",
            fecha_ruta=self.month,
            created_by=assigned_to,
        )
        point = PuntoLogistico.objects.create(
            sucursal=destination_erp,
            nombre=destination_erp.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud=Decimal("25.570000"),
            longitud=Decimal("-108.470000"),
        )
        stop = ParadaRuta.objects.create(ruta=route, punto=point, orden=1)
        checklist = RutaCargaChecklist.objects.create(ruta=route)
        line = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=stop,
            point_transfer_line=transfer,
            transfer_external_id=transfer.transfer_external_id,
            detail_external_id=transfer.detail_external_id,
            source_hash=transfer.source_hash,
            item_code=self.product.sku,
            item_name=self.product.name,
            erp_origin_branch=origin_erp,
            erp_destination_branch=destination_erp,
            cantidad_solicitada=Decimal("2"),
            cantidad_enviada_esperada=Decimal("2"),
            cantidad_cargada=Decimal("2"),
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
        )
        discrepancy = DiscrepanciaLogistica.objects.create(
            ruta=route,
            parada=stop,
            linea_carga=line,
            origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
            cantidad_enviada=Decimal("2"),
            cantidad_cargada=Decimal("2"),
            cantidad_recibida=Decimal("1"),
            motivo="diferencia_recepcion_point",
            estado=DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA,
            asignado_a=assigned_to,
            creado_por=assigned_to,
        )
        return transfer, discrepancy

    def test_exact_transfer_relation_reuses_logistics_owner(self):
        logistics_owner = self._head(
            username="jefatura.logistica",
            department=Empleado.DEP_LOGISTICA,
        )
        transfer, discrepancy = self._logistics_discrepancy(
            assigned_to=logistics_owner
        )
        case = self.make_case(
            issue_codes=["TRANSFER_QUANTITY_MISMATCH"],
            source_trace={"transfers": [transfer.id]},
        )

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        result = InventoryAuditAgent().investigate_case(case)

        self.assertEqual(
            result.attention_level,
            ProductInventoryAuditCase.AttentionLevel.HIGH,
        )
        self.assertEqual(
            result.responsible_area,
            ProductInventoryAuditCase.ResponsibleArea.LOGISTICS,
        )
        self.assertEqual(result.assigned_to_id, discrepancy.asignado_a_id)
        self.assertEqual(
            result.summary["related_logistics_discrepancy_ids"],
            [discrepancy.id],
        )
        self.assertTrue(
            any("RUT-202608-0029" in fact for fact in result.summary["facts"])
        )

    def test_ambiguous_area_candidates_leave_person_unassigned(self):
        self._head(
            username="jefatura.produccion.1",
            department=Empleado.DEP_PRODUCCION,
        )
        self._head(
            username="jefatura.produccion.2",
            department=Empleado.DEP_PRODUCCION,
        )
        case = self.make_case(
            issue_codes=["MISSING_CONVERSION_ORIGIN"],
            conversion_in=Decimal("12"),
        )

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        result = InventoryAuditAgent().investigate_case(case)

        self.assertEqual(
            result.attention_level,
            ProductInventoryAuditCase.AttentionLevel.HIGH,
        )
        self.assertEqual(
            result.responsible_area,
            ProductInventoryAuditCase.ResponsibleArea.PRODUCTION,
        )
        self.assertIsNone(result.assigned_to_id)
        self.assertIn("más de una jefatura", result.assignment_reason.lower())

    def test_source_incomplete_is_grouped_without_per_product_assignment(self):
        self._head(
            username="jefatura.administracion",
            department=Empleado.DEP_ADMINISTRACION,
        )
        case = self.make_case(
            movement_status=ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
            issue_codes=["SOURCE_INCOMPLETE"],
        )

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        result = InventoryAuditAgent().investigate_case(case)

        self.assertEqual(
            result.attention_level,
            ProductInventoryAuditCase.AttentionLevel.GROUPED,
        )
        self.assertEqual(
            result.responsible_area,
            ProductInventoryAuditCase.ResponsibleArea.ADMINISTRATION,
        )
        self.assertIsNone(result.assigned_to_id)
        self.assertIn("fuente", " ".join(result.summary["missing"]).lower())

    def test_minor_difference_stays_grouped_without_individual_assignment(self):
        self._head(
            username="jefatura.administracion.menores",
            department=Empleado.DEP_ADMINISTRACION,
        )
        case = self.make_case(difference=Decimal("-1"))

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        result = InventoryAuditAgent().investigate_case(case)

        self.assertEqual(
            result.attention_level,
            ProductInventoryAuditCase.AttentionLevel.GROUPED,
        )
        self.assertIsNone(result.assigned_to_id)
        self.assertIn("sin asignación individual", result.assignment_reason.lower())

    def test_recurrence_in_another_month_promotes_case_to_high_attention(self):
        previous_month = date(2026, 7, 1)
        previous_run = ProductInventoryAuditRun.objects.create(
            month=previous_month,
            status=ProductInventoryAuditRun.Status.READY,
            calculation_fingerprint="d" * 64,
        )
        self.make_case(
            run=previous_run,
            month=previous_month,
            calculation_fingerprint="e" * 64,
        )
        current = self.make_case(difference=Decimal("-1"))

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        result = InventoryAuditAgent().investigate_case(current)

        self.assertEqual(
            result.attention_level,
            ProductInventoryAuditCase.AttentionLevel.HIGH,
        )
        self.assertEqual(result.summary["recurrence_count"], 1)

    def test_second_equal_run_does_not_duplicate_notification(self):
        logistics_owner = self._head(
            username="jefatura.logistica.idempotente",
            department=Empleado.DEP_LOGISTICA,
        )
        transfer, _discrepancy = self._logistics_discrepancy(
            assigned_to=logistics_owner
        )
        case = self.make_case(
            issue_codes=["TRANSFER_QUANTITY_MISMATCH"],
            source_trace={"transfers": [transfer.id]},
        )

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        first = InventoryAuditAgent().run_month(self.month)
        second = InventoryAuditAgent().run_month(self.month)

        case.refresh_from_db()
        self.assertEqual(first["notifications"], 1)
        self.assertEqual(second["notifications"], 0)
        self.assertEqual(case.assigned_to_id, logistics_owner.id)
        self.assertEqual(case.last_notified_fingerprint, case.investigation_fingerprint)
        self.assertEqual(
            Notificacion.objects.filter(
                objeto_tipo="reportes.ProductInventoryAuditCaseGroup",
            ).count(),
            1,
        )

    def test_multiple_high_cases_for_same_owner_create_one_grouped_notification(self):
        production_owner = self._head(
            username="jefatura.produccion.resumen",
            department=Empleado.DEP_PRODUCCION,
        )
        second_product = PointProduct.objects.create(
            external_id="AUDITOR-AGENT-PRODUCT-2",
            sku="AGENT-002",
            name="Segundo producto agente auditor",
        )
        first = self.make_case(
            issue_codes=["MISSING_CONVERSION_ORIGIN"],
            conversion_in=Decimal("12"),
        )
        second = self.make_case(
            product=second_product,
            issue_codes=["MISSING_CONVERSION_ORIGIN"],
            conversion_in=Decimal("8"),
            calculation_fingerprint="f" * 64,
        )

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        result = InventoryAuditAgent().run_month(self.month)

        first.refresh_from_db()
        second.refresh_from_db()
        notification = Notificacion.objects.get(
            objeto_tipo="reportes.ProductInventoryAuditCaseGroup"
        )
        self.assertEqual(result["notifications"], 1)
        self.assertEqual(notification.usuario_id, production_owner.id)
        self.assertIn("2 diferencias", notification.mensaje)
        self.assertEqual(first.last_notified_fingerprint, first.investigation_fingerprint)
        self.assertEqual(second.last_notified_fingerprint, second.investigation_fingerprint)

    @patch("reportes.services_inventory_audit_agent.DailyInventoryBreakService")
    def test_dry_run_writes_nothing(self, service_class):
        case = self.make_case(
            issue_codes=["MISSING_CONVERSION_ORIGIN"],
            conversion_in=Decimal("12"),
        )
        service_class.return_value.build_month.return_value = {
            case.id: DailyBreakProjection(
                status=DailyBreakStatus.FOUND,
                last_matching_checkpoint=None,
                first_mismatch_checkpoint=None,
                minimum=Decimal("8"),
                maximum=Decimal("8"),
                movement_ids_by_source={},
                warnings=(),
            )
        }
        before = {
            "attention_level": case.attention_level,
            "responsible_area": case.responsible_area,
            "assigned_to_id": case.assigned_to_id,
            "investigation_summary": case.investigation_summary,
            "investigation_fingerprint": case.investigation_fingerprint,
            "investigated_at": case.investigated_at,
        }

        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        result = InventoryAuditAgent().run_month(self.month, dry_run=True)

        case.refresh_from_db()
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["high"], 1)
        self.assertEqual(
            {
                "attention_level": case.attention_level,
                "responsible_area": case.responsible_area,
                "assigned_to_id": case.assigned_to_id,
                "investigation_summary": case.investigation_summary,
                "investigation_fingerprint": case.investigation_fingerprint,
                "investigated_at": case.investigated_at,
            },
            before,
        )
        self.assertFalse(Notificacion.objects.exists())
        service_class.return_value.build_month.assert_called_once()
        InventoryAuditAgent().run_month(self.month)
        case.refresh_from_db()
        self.assertEqual(case.investigation_summary["daily_break"]["status"], "FOUND")

        service_class.return_value.build_month.side_effect = RuntimeError("fuente temporal")
        with self.assertLogs(
            "reportes.services_inventory_audit_agent", level="ERROR"
        ):
            failed = InventoryAuditAgent().run_month(self.month)
        repeated = InventoryAuditAgent().run_month(self.month)

        case.refresh_from_db()
        self.assertEqual(failed["total"], 1)
        self.assertEqual(
            case.investigation_summary["daily_break"]["status"],
            "INSUFFICIENT_EVIDENCE",
        )
        self.assertTrue(case.investigation_summary["daily_break"]["warnings"])
        self.assertEqual(repeated["updated"], 0)
        self.assertEqual(repeated["notifications"], 0)
