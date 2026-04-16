from io import StringIO
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.db import OperationalError
from django.urls import reverse
from django.test import TestCase
from django.test.utils import override_settings
from django.utils import timezone

from core.models import AuditLog
from core.models import Departamento, Sucursal
from compras.models import PresupuestoCompraPeriodo, PresupuestoCompraProveedor, SolicitudCompra
from inventario.models import AjusteInventario, ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from orquestacion.catalog import RULES
from orquestacion.memory_control import append_controlled_memory_entry
from orquestacion.models import AgentCapability, AgentDefinition, AgentGoalDelegation, AgentLoopCheckpoint, AgentSuggestion, AgentTask, MemoryProposal, OrchestrationRule, OrchestrationRun
from orquestacion.services.event_chain_scheduler import list_sales_event_chain_candidates, run_sales_event_chain_batch
from orquestacion.services.agent_runtime import Goal, build_agent_context, load_agent_memory, resolve_tool_registry, run_agent_goal
from orquestacion.services.memory_proposals import apply_memory_proposal, approve_memory_proposal, propose_memory_update, reject_memory_proposal
from orquestacion.services.rule_runners import (
    run_daily_production_plan_missing,
    run_inventory_adjustment_authorization_guard,
    run_near_expiry_or_low_rotation_review,
    run_plan_demand_production_purchase_chain,
    run_purchase_exception_requires_dg_approval,
)
from orquestacion.tasks import task_run_rule, task_run_sales_event_chains
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, PronosticoVenta, Receta, SolicitudVenta, VentaHistorica
from ventas.models import EventoVenta, EventoVentaFinancial, EventoVentaForecast, EventoVentaProducto, EventoVentaSucursal


class OrquestacionModelsTest(TestCase):
    def setUp(self):
        self.departamento = Departamento.objects.create(codigo="OPS", nombre="Operaciones")
        self.agent = AgentDefinition.objects.create(
            code="director_operativo",
            name="Director Operativo",
            domain="operaciones",
            status=AgentDefinition.STATUS_ACTIVE,
            owner_department=self.departamento,
            allowed_tools_json=["api.reportes_bi_dashboard"],
            allowed_actions_json=["delegate", "summarize"],
            requires_human_approval_default=True,
            priority_order=10,
        )

    def test_agent_and_capability_can_be_created(self):
        capability = AgentCapability.objects.create(
            agent=self.agent,
            capability_key="inventory.read.low_stock",
            scope_type=AgentCapability.SCOPE_READ,
            resource_key="GET /api/pos-bridge/inventory/low-stock/",
        )

        self.assertEqual(str(self.agent), "Director Operativo (director_operativo)")
        self.assertEqual(capability.agent, self.agent)

    def test_rule_run_task_and_suggestion_can_be_linked(self):
        rule = OrchestrationRule.objects.create(
            code="low_stock_branch",
            name="Quiebre probable por sucursal",
            trigger_type=OrchestrationRule.TRIGGER_THRESHOLD,
            source_event="inventory.low_stock_detected",
            condition_json={"metric": "low_stock", "threshold": 1},
            primary_agent=self.agent,
            action_mode=OrchestrationRule.ACTION_RECOMMEND,
            cooldown_minutes=60,
            is_active=True,
        )
        run = OrchestrationRun.objects.create(
            run_key="run-low-stock-001",
            trigger_source="scheduler",
            rule=rule,
            status=OrchestrationRun.STATUS_RUNNING,
        )
        task = AgentTask.objects.create(
            run=run,
            agent=self.agent,
            title="Revisar quiebre probable",
            task_type="low_stock_branch_review",
            priority=AgentTask.PRIORITY_HIGH,
        )
        suggestion = AgentSuggestion.objects.create(
            task=task,
            suggestion_type="restock_alert",
            domain="inventario",
            severity=AgentSuggestion.SEVERITY_HIGH,
            summary="Revisar quiebre probable en sucursal",
            recommended_action="Escalar a compras",
            requires_approval=True,
        )

        self.assertEqual(task.run, run)
        self.assertEqual(suggestion.task, task)
        self.assertEqual(suggestion.decision_status, AgentSuggestion.DECISION_PENDING)


class SeedOrquestacionCatalogTest(TestCase):
    def test_seed_command_creates_initial_catalog_idempotently(self):
        out = StringIO()

        call_command("seed_orquestacion_catalog", stdout=out)
        call_command("seed_orquestacion_catalog", stdout=out)

        self.assertEqual(Departamento.objects.filter(codigo="OPS").count(), 1)
        self.assertEqual(AgentDefinition.objects.filter(code="director_operativo").count(), 1)
        self.assertEqual(OrchestrationRule.objects.count(), len(RULES))
        self.assertTrue(
            AgentCapability.objects.filter(
                agent__code="agente_demanda_ventas",
                capability_key="sales.read.summary",
            ).exists()
        )
        self.assertTrue(OrchestrationRule.objects.filter(code="integration_job_failure_review").exists())
        self.assertTrue(OrchestrationRule.objects.filter(code="purchase_exception_requires_dg_approval").exists())
        self.assertTrue(OrchestrationRule.objects.filter(code="cold_chain_temperature_breach").exists())
        self.assertTrue(OrchestrationRule.objects.filter(code="daily_production_plan_missing").exists())
        self.assertTrue(OrchestrationRule.objects.filter(code="plan_demand_production_purchase_chain").exists())
        self.assertTrue(OrchestrationRule.objects.filter(code="sales_event_operational_chain_review").exists())

    def test_seed_command_sets_updated_cold_chain_range(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())

        rule = OrchestrationRule.objects.get(code="cold_chain_temperature_breach")

        self.assertEqual(rule.condition_json["logic"]["outside_range_c"], [3, 5])


class OrquestacionDashboardViewTest(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.admin = user_model.objects.create_user(username="orch_admin", password="pass123")
        self.reader = user_model.objects.create_user(username="orch_reader", password="pass123")

        admin_group, _ = Group.objects.get_or_create(name="ADMIN")
        reader_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.admin.groups.add(admin_group)
        self.reader.groups.add(reader_group)
        self.rule = OrchestrationRule.objects.get(code="daily_production_plan_missing")
        self.agent = AgentDefinition.objects.get(code="agente_produccion")
        self.run = OrchestrationRun.objects.create(
            run_key="dashboard-run-001",
            trigger_source="test",
            rule=self.rule,
            status=OrchestrationRun.STATUS_SUCCESS,
        )
        self.task = AgentTask.objects.create(
            run=self.run,
            agent=self.agent,
            title="Validar plan faltante",
            task_type="daily_production_plan_missing",
            priority=AgentTask.PRIORITY_HIGH,
        )
        AgentSuggestion.objects.create(
            task=self.task,
            suggestion_type="missing_daily_production_plan",
            domain="produccion",
            severity=AgentSuggestion.SEVERITY_CRITICAL,
            summary="No hay plan de Produccion cargado",
            recommended_action="Escalar a Director Operativo",
            requires_approval=True,
        )

    def test_dashboard_allows_admin(self):
        self.client.force_login(self.admin)

        response = self.client.get(reverse("orquestacion:dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Orquestación operativa")
        self.assertContains(response, "Director Operativo")
        self.assertContains(response, "No hay plan de Produccion cargado")
        self.assertContains(response, reverse("recetas:plan_produccion"))

    def test_dashboard_denies_reader(self):
        self.client.force_login(self.reader)

        response = self.client.get(reverse("orquestacion:dashboard"))

        self.assertEqual(response.status_code, 403)

    def test_dashboard_handles_missing_memory_proposal_table(self):
        self.client.force_login(self.admin)

        with patch("orquestacion.views.MemoryProposal.objects.exists", side_effect=OperationalError("missing table")):
            response = self.client.get(reverse("orquestacion:dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "tabla no disponible aún; falta aplicar migraciones")
        self.assertContains(response, "python manage.py migrate")

    def test_dashboard_filters_suggestions_by_rule_and_severity(self):
        self.client.force_login(self.admin)

        response = self.client.get(
            reverse("orquestacion:dashboard"),
            {
                "rule": "daily_production_plan_missing",
                "severity": AgentSuggestion.SEVERITY_CRITICAL,
                "decision": AgentSuggestion.DECISION_PENDING,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No hay plan de Produccion cargado")
        self.assertContains(response, "Plan diario de Produccion no cargado en sistema")

    def test_dashboard_builds_precise_purchase_source_link(self):
        purchase_rule = OrchestrationRule.objects.get(code="purchase_exception_requires_dg_approval")
        purchase_agent = AgentDefinition.objects.get(code="agente_compras")
        purchase_run = OrchestrationRun.objects.create(
            run_key="dashboard-run-purchase-001",
            trigger_source="test",
            rule=purchase_rule,
            status=OrchestrationRun.STATUS_SUCCESS,
        )
        purchase_task = AgentTask.objects.create(
            run=purchase_run,
            agent=purchase_agent,
            title="Validar excepcion compra",
            task_type="purchase_exception_requires_dg_approval",
            priority=AgentTask.PRIORITY_HIGH,
        )
        AgentSuggestion.objects.create(
            task=purchase_task,
            suggestion_type="purchase_exception_requires_dg_approval",
            domain="compras",
            severity=AgentSuggestion.SEVERITY_HIGH,
            summary="Solicitud SOL-009 requiere validacion DG",
            recommended_action="Escalar a DG",
            requires_approval=True,
            details_json={"solicitud_folio": "SOL-009"},
        )

        self.client.force_login(self.admin)
        response = self.client.get(
            reverse("orquestacion:dashboard"),
            {"rule": purchase_rule.code, "decision": AgentSuggestion.DECISION_PENDING},
        )

        self.assertEqual(response.status_code, 200)
        suggestion = response.context["filtered_suggestions"][0]
        self.assertEqual(suggestion.source_url, f"{reverse('compras:solicitudes')}?q=SOL-009")

    def test_dashboard_builds_precise_inventory_source_link(self):
        inventory_rule = OrchestrationRule.objects.get(code="inventory_adjustment_authorization_guard")
        inventory_agent = AgentDefinition.objects.get(code="agente_conciliacion")
        inventory_run = OrchestrationRun.objects.create(
            run_key="dashboard-run-inventory-001",
            trigger_source="test",
            rule=inventory_rule,
            status=OrchestrationRun.STATUS_SUCCESS,
        )
        inventory_task = AgentTask.objects.create(
            run=inventory_run,
            agent=inventory_agent,
            title="Validar ajuste",
            task_type="inventory_adjustment_authorization_guard",
            priority=AgentTask.PRIORITY_HIGH,
        )
        AgentSuggestion.objects.create(
            task=inventory_task,
            suggestion_type="inventory_adjustment_authorization_guard",
            domain="inventario",
            severity=AgentSuggestion.SEVERITY_HIGH,
            summary="Ajuste AJ-001 requiere revision",
            recommended_action="Regularizar bitacora",
            requires_approval=True,
            details_json={"ajuste_id": 41, "estatus": "PENDIENTE"},
        )

        self.client.force_login(self.admin)
        response = self.client.get(
            reverse("orquestacion:dashboard"),
            {"rule": inventory_rule.code, "decision": AgentSuggestion.DECISION_PENDING},
        )

        self.assertEqual(response.status_code, 200)
        suggestion = response.context["filtered_suggestions"][0]
        self.assertEqual(suggestion.source_url, f"{reverse('inventario:ajustes')}?ajuste_id=41&estatus=PENDIENTE")

    def test_dashboard_builds_rotation_review_source_link(self):
        rotation_rule = OrchestrationRule.objects.get(code="near_expiry_or_low_rotation_review")
        rotation_agent = AgentDefinition.objects.get(code="agente_conciliacion")
        rotation_run = OrchestrationRun.objects.create(
            run_key="dashboard-run-rotation-001",
            trigger_source="test",
            rule=rotation_rule,
            status=OrchestrationRun.STATUS_SUCCESS,
        )
        rotation_task = AgentTask.objects.create(
            run=rotation_run,
            agent=rotation_agent,
            title="Revisar baja rotacion",
            task_type="near_expiry_or_low_rotation_review",
            priority=AgentTask.PRIORITY_MEDIUM,
        )
        AgentSuggestion.objects.create(
            task=rotation_task,
            suggestion_type="near_expiry_or_low_rotation_review",
            domain="inventario",
            severity=AgentSuggestion.SEVERITY_WARNING,
            summary="Existencia de baja rotacion detectada",
            recommended_action="Revisar alertas",
            requires_approval=False,
            details_json={"canonical_insumo_id": 99},
        )

        self.client.force_login(self.admin)
        response = self.client.get(
            reverse("orquestacion:dashboard"),
            {"rule": rotation_rule.code},
        )

        self.assertEqual(response.status_code, 200)
        suggestion = response.context["filtered_suggestions"][0]
        self.assertEqual(suggestion.source_url, f"{reverse('inventario:alertas')}?q=99")

    def test_dashboard_builds_plan_chain_source_link(self):
        chain_rule = OrchestrationRule.objects.get(code="plan_demand_production_purchase_chain")
        chain_agent = AgentDefinition.objects.get(code="director_operativo")
        chain_run = OrchestrationRun.objects.create(
            run_key="dashboard-run-chain-001",
            trigger_source="test",
            rule=chain_rule,
            status=OrchestrationRun.STATUS_SUCCESS,
            context_json={"plan_id": 77, "production_date": "2026-03-31"},
            result_summary_json={"plan_id": 77},
        )
        chain_task = AgentTask.objects.create(
            run=chain_run,
            agent=chain_agent,
            title="Consolidar cadena",
            task_type="plan_director_chain_review",
            priority=AgentTask.PRIORITY_HIGH,
        )
        AgentSuggestion.objects.create(
            task=chain_task,
            suggestion_type="plan_director_chain_review",
            domain="operaciones",
            severity=AgentSuggestion.SEVERITY_HIGH,
            summary="Dirección Operativa debe validar la cadena del plan 77",
            recommended_action="Revisar cadena completa",
            requires_approval=True,
            details_json={"plan_id": 77},
        )

        self.client.force_login(self.admin)
        response = self.client.get(
            reverse("orquestacion:dashboard"),
            {"rule": chain_rule.code},
        )

        self.assertEqual(response.status_code, 200)
        suggestion = response.context["filtered_suggestions"][0]
        self.assertEqual(suggestion.source_url, f"{reverse('recetas:plan_produccion')}?plan_id=77")


class DailyProductionPlanRunnerTest(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="ops_runner", password="pass123")

    def test_runner_skips_before_cutoff(self):
        result = run_daily_production_plan_missing(
            reference_dt=datetime(2026, 3, 30, 8, 30),
            created_by=self.user,
        )

        self.assertFalse(result.created)
        self.assertEqual(result.status, "skipped_before_cutoff")
        self.assertEqual(OrchestrationRun.objects.count(), 0)

    def test_runner_creates_run_task_and_suggestion_when_plan_is_missing(self):
        result = run_daily_production_plan_missing(
            reference_dt=datetime(2026, 3, 30, 9, 30),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        self.assertEqual(OrchestrationRun.objects.count(), 1)
        self.assertEqual(AgentTask.objects.count(), 1)
        self.assertEqual(AgentSuggestion.objects.count(), 1)
        suggestion = AgentSuggestion.objects.get()
        self.assertEqual(suggestion.severity, AgentSuggestion.SEVERITY_CRITICAL)
        self.assertIn("2026-03-30", suggestion.summary)
        self.assertEqual(AuditLog.objects.filter(model="orquestacion.AgentSuggestion").count(), 1)

    def test_runner_marks_success_when_plan_exists(self):
        PlanProduccion.objects.create(
            nombre="Plan 30 marzo",
            fecha_produccion=datetime(2026, 3, 30).date(),
        )

        result = run_daily_production_plan_missing(
            reference_dt=datetime(2026, 3, 30, 9, 30),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(OrchestrationRun.objects.count(), 1)
        self.assertEqual(AgentTask.objects.count(), 0)
        self.assertEqual(AgentSuggestion.objects.count(), 0)

    def test_runner_respects_cooldown_for_same_date(self):
        first = run_daily_production_plan_missing(
            reference_dt=datetime(2026, 3, 30, 9, 30),
            created_by=self.user,
        )
        second = run_daily_production_plan_missing(
            reference_dt=datetime(2026, 3, 30, 10, 0),
            created_by=self.user,
        )

        self.assertEqual(first.status, "success_issue_created")
        self.assertFalse(second.created)
        self.assertEqual(second.status, "skipped_cooldown")
        self.assertEqual(OrchestrationRun.objects.count(), 1)

    def test_management_command_runs_supported_rule(self):
        out = StringIO()

        call_command(
            "run_orchestration_rule",
            rule="daily_production_plan_missing",
            reference_datetime="2026-03-30T09:30:00",
            username="ops_runner",
            stdout=out,
        )

        self.assertIn("success_issue_created", out.getvalue())

    def test_celery_task_runs_supported_rule(self):
        payload = task_run_rule.run(rule_code="daily_production_plan_missing", force=True)

        self.assertEqual(payload["status"], "success_issue_created")
        self.assertEqual(OrchestrationRun.objects.count(), 1)


class PlanDemandProductionPurchaseChainRunnerTest(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="chain_runner", password="pass123")
        self.unidad = UnidadMedida.objects.create(codigo="kg-chain", nombre="Kilogramo chain", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1)
        self.sucursal = Sucursal.objects.create(codigo="MZ01", nombre="Matriz")
        self.proveedor = Proveedor.objects.create(nombre="Proveedor cadena", activo=True)

    def _create_recipe_and_input(self, *, suffix: str, with_stock: Decimal) -> tuple[PlanProduccion, Receta, Insumo]:
        insumo = Insumo.objects.create(
            codigo_point=f"PT-{suffix}",
            nombre_point=f"Harina Point {suffix}",
            nombre=f"Harina local {suffix}",
            categoria="Materia prima",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        CostoInsumo.objects.create(
            insumo=insumo,
            proveedor=self.proveedor,
            costo_unitario=Decimal("32.50"),
            source_hash=f"cost-chain-{suffix}",
        )
        if with_stock is not None:
            ExistenciaInsumo.objects.create(
                insumo=insumo,
                stock_actual=with_stock,
                stock_minimo=Decimal("2"),
                inventario_promedio=max(with_stock, Decimal("2")),
                consumo_diario_promedio=Decimal("1"),
            )
        receta = Receta.objects.create(
            nombre=f"Pastel cadena {suffix}",
            hash_contenido=f"hash-chain-{suffix}",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pasteles",
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=insumo,
            insumo_texto=insumo.display_name,
            cantidad=Decimal("2.000000"),
            unidad=self.unidad,
            unidad_texto="kg",
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
            costo_unitario_snapshot=Decimal("32.500000"),
        )
        plan = PlanProduccion.objects.create(
            nombre=f"Plan cadena {suffix}",
            fecha_produccion=datetime(2026, 3, 31).date(),
            creado_por=self.user,
        )
        PlanProduccionItem.objects.create(plan=plan, receta=receta, cantidad=Decimal("10"))
        return plan, receta, insumo

    def test_runner_creates_four_tasks_for_cross_area_chain(self):
        plan, receta, _insumo = self._create_recipe_and_input(suffix="alert", with_stock=Decimal("1"))
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=self.sucursal,
            fecha=datetime(2026, 3, 15).date(),
            cantidad=Decimal("8"),
            tickets=4,
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=self.sucursal,
            fecha=datetime(2026, 3, 20).date(),
            cantidad=Decimal("6"),
            tickets=3,
        )
        PronosticoVenta.objects.create(receta=receta, periodo="2026-03", cantidad=Decimal("14"))
        SolicitudVenta.objects.create(
            receta=receta,
            sucursal=self.sucursal,
            alcance=SolicitudVenta.ALCANCE_SEMANA,
            periodo="2026-03",
            fecha_inicio=datetime(2026, 3, 28).date(),
            fecha_fin=datetime(2026, 3, 31).date(),
            cantidad=Decimal("12"),
        )

        result = run_plan_demand_production_purchase_chain(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        self.assertEqual(OrchestrationRun.objects.count(), 1)
        self.assertEqual(AgentTask.objects.count(), 4)
        self.assertEqual(AgentSuggestion.objects.count(), 4)
        severities = set(AgentSuggestion.objects.values_list("severity", flat=True))
        self.assertIn(AgentSuggestion.SEVERITY_WARNING, severities)
        self.assertIn(AgentSuggestion.SEVERITY_CRITICAL, severities)
        self.assertTrue(AgentSuggestion.objects.filter(suggestion_type="plan_purchase_chain_review").exists())
        purchase_suggestion = AgentSuggestion.objects.get(suggestion_type="plan_purchase_chain_review")
        self.assertEqual(purchase_suggestion.details_json["shortage_count"], 1)
        director_suggestion = AgentSuggestion.objects.get(suggestion_type="plan_director_chain_review")
        self.assertEqual(director_suggestion.details_json["plan_id"], plan.id)
        self.assertIn("purchase_shortage_detected", director_suggestion.details_json["driver_codes"])

    def test_runner_returns_no_issue_when_chain_is_healthy(self):
        plan, receta, _insumo = self._create_recipe_and_input(suffix="clean", with_stock=Decimal("25"))
        for offset in range(35):
            VentaHistorica.objects.create(
                receta=receta,
                sucursal=self.sucursal,
                fecha=datetime(2026, 2, 20).date() + timedelta(days=offset),
                cantidad=Decimal("5"),
                tickets=2,
            )
        PronosticoVenta.objects.create(receta=receta, periodo="2026-03", cantidad=Decimal("50"))

        result = run_plan_demand_production_purchase_chain(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(OrchestrationRun.objects.count(), 1)
        self.assertEqual(AgentTask.objects.count(), 0)
        self.assertEqual(AgentSuggestion.objects.count(), 0)
        run = OrchestrationRun.objects.get()
        self.assertEqual(run.result_summary_json["plan_id"], plan.id)

    def test_runner_respects_cooldown_for_same_plan(self):
        plan, receta, _insumo = self._create_recipe_and_input(suffix="cooldown", with_stock=Decimal("0"))
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=self.sucursal,
            fecha=datetime(2026, 3, 20).date(),
            cantidad=Decimal("5"),
            tickets=2,
        )

        first = run_plan_demand_production_purchase_chain(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )
        second = run_plan_demand_production_purchase_chain(
            reference_dt=datetime(2026, 3, 31, 11, 0),
            created_by=self.user,
        )

        self.assertEqual(first.status, "success_issue_created")
        self.assertFalse(second.created)
        self.assertEqual(second.status, "skipped_cooldown")
        self.assertEqual(OrchestrationRun.objects.count(), 1)

    def test_runner_returns_no_issue_when_plan_is_missing(self):
        result = run_plan_demand_production_purchase_chain(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(OrchestrationRun.objects.count(), 0)
        self.assertEqual(AgentTask.objects.count(), 0)
        self.assertEqual(AgentSuggestion.objects.count(), 0)


class PurchaseExceptionRunnerTest(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="compras_runner", password="pass123")
        self.unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1)
        self.proveedor = Proveedor.objects.create(nombre="Proveedor DG", activo=True)
        self.insumo = Insumo.objects.create(
            nombre="Azucar DG",
            categoria="Materia prima",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )

    def test_runner_creates_suggestion_for_amount_gt_5000(self):
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=self.proveedor,
            costo_unitario="100.00",
            source_hash="costo-azucar-dg-1",
        )
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="compras",
            insumo=self.insumo,
            proveedor_sugerido=self.proveedor,
            cantidad="60",
            fecha_requerida=datetime(2026, 3, 30).date(),
            estatus=SolicitudCompra.STATUS_BORRADOR,
        )

        result = run_purchase_exception_requires_dg_approval(
            reference_dt=datetime(2026, 3, 30, 11, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        suggestion = AgentSuggestion.objects.get()
        self.assertEqual(suggestion.details_json["solicitud_id"], solicitud.id)
        self.assertIn("amount_gt_5000", suggestion.details_json["reason_codes"])
        self.assertIn("quotes_below_required_minimum", suggestion.details_json["reason_codes"])
        self.assertEqual(suggestion.details_json["cotizaciones_requeridas"], 3)

    def test_runner_creates_suggestion_for_out_of_catalog_request(self):
        SolicitudCompra.objects.create(
            area="Compras",
            solicitante="compras",
            insumo=self.insumo,
            proveedor_sugerido=self.proveedor,
            cantidad="1",
            fecha_requerida=datetime(2026, 3, 30).date(),
            estatus=SolicitudCompra.STATUS_EN_REVISION,
            fuera_de_catalogo=True,
            cotizaciones_requeridas=2,
            cotizaciones_recibidas=0,
            justificacion_excepcion="Proveedor temporal no homologado.",
        )

        result = run_purchase_exception_requires_dg_approval(
            reference_dt=datetime(2026, 3, 30, 11, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        suggestion = AgentSuggestion.objects.get()
        self.assertIn("out_of_catalog", suggestion.details_json["reason_codes"])
        self.assertIn("quotes_below_required_minimum", suggestion.details_json["reason_codes"])
        self.assertEqual(suggestion.details_json["cotizaciones_requeridas"], 2)
        self.assertEqual(suggestion.details_json["cotizaciones_recibidas"], 0)

    def test_runner_creates_suggestion_when_monthly_budget_is_exceeded(self):
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=self.proveedor,
            costo_unitario="100.00",
            source_hash="costo-azucar-dg-2",
        )
        PresupuestoCompraPeriodo.objects.create(
            periodo_tipo=PresupuestoCompraPeriodo.TIPO_MES,
            periodo_mes="2026-03",
            monto_objetivo="500.00",
            actualizado_por=self.user,
        )
        PresupuestoCompraProveedor.objects.create(
            presupuesto_periodo=PresupuestoCompraPeriodo.objects.get(periodo_tipo=PresupuestoCompraPeriodo.TIPO_MES, periodo_mes="2026-03"),
            proveedor=self.proveedor,
            monto_objetivo="400.00",
            actualizado_por=self.user,
        )
        SolicitudCompra.objects.create(
            area="Compras",
            solicitante="compras",
            insumo=self.insumo,
            proveedor_sugerido=self.proveedor,
            cantidad="6",
            fecha_requerida=datetime(2026, 3, 30).date(),
            estatus=SolicitudCompra.STATUS_EN_REVISION,
        )

        result = run_purchase_exception_requires_dg_approval(
            reference_dt=datetime(2026, 3, 30, 11, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        suggestion = AgentSuggestion.objects.get()
        self.assertIn("monthly_budget_exceeded", suggestion.details_json["reason_codes"])
        self.assertIn("provider_budget_exceeded", suggestion.details_json["reason_codes"])

    def test_runner_respects_cooldown_per_request(self):
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=self.proveedor,
            costo_unitario="100.00",
            source_hash="costo-azucar-dg-3",
        )
        SolicitudCompra.objects.create(
            area="Compras",
            solicitante="compras",
            insumo=self.insumo,
            proveedor_sugerido=self.proveedor,
            cantidad="60",
            fecha_requerida=datetime(2026, 3, 30).date(),
            estatus=SolicitudCompra.STATUS_BORRADOR,
        )

        first = run_purchase_exception_requires_dg_approval(
            reference_dt=datetime(2026, 3, 30, 11, 0),
            created_by=self.user,
        )
        second = run_purchase_exception_requires_dg_approval(
            reference_dt=datetime(2026, 3, 30, 12, 0),
            created_by=self.user,
        )

        self.assertEqual(first.status, "success_issue_created")
        self.assertEqual(second.status, "success_no_issue")
        self.assertEqual(AgentSuggestion.objects.count(), 1)

    def test_management_command_runs_purchase_exception_rule(self):
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=self.proveedor,
            costo_unitario="100.00",
            source_hash="costo-azucar-dg-4",
        )
        SolicitudCompra.objects.create(
            area="Compras",
            solicitante="compras",
            insumo=self.insumo,
            proveedor_sugerido=self.proveedor,
            cantidad="60",
            fecha_requerida=datetime(2026, 3, 30).date(),
            estatus=SolicitudCompra.STATUS_BORRADOR,
        )
        out = StringIO()

        call_command(
            "run_orchestration_rule",
            rule="purchase_exception_requires_dg_approval",
            reference_datetime="2026-03-30T11:00:00",
            username="compras_runner",
            stdout=out,
        )

        self.assertIn("success_issue_created", out.getvalue())


class InventoryAdjustmentAuthorizationGuardRunnerTest(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="inventario_runner", password="pass123")
        self.reviewer = user_model.objects.create_user(username="admin_reviewer", password="pass123")
        self.unidad = UnidadMedida.objects.create(codigo="g", nombre="Gramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1)
        self.insumo = Insumo.objects.create(
            nombre="Harina guard",
            categoria="Materia prima",
            unidad_base=self.unidad,
            activo=True,
        )

    def test_runner_creates_suggestion_for_stale_pending_adjustment(self):
        ajuste = AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema="10",
            cantidad_fisica="8",
            motivo="Conteo fisico",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.user,
            creado_en=timezone.make_aware(datetime(2026, 3, 30, 5, 0)),
        )

        result = run_inventory_adjustment_authorization_guard(
            reference_dt=datetime(2026, 3, 30, 11, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        suggestion = AgentSuggestion.objects.get()
        self.assertEqual(suggestion.details_json["ajuste_id"], ajuste.id)
        self.assertIn("pending_approval_stale", suggestion.details_json["reason_codes"])

    def test_runner_creates_critical_suggestion_for_applied_adjustment_without_audit_fields(self):
        AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema="10",
            cantidad_fisica="12",
            motivo="Correccion manual",
            estatus=AjusteInventario.STATUS_APLICADO,
            solicitado_por=self.user,
            aprobado_por=None,
            aprobado_en=None,
            aplicado_en=timezone.make_aware(datetime(2026, 3, 30, 9, 0)),
        )

        result = run_inventory_adjustment_authorization_guard(
            reference_dt=datetime(2026, 3, 30, 11, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        suggestion = AgentSuggestion.objects.get()
        self.assertEqual(suggestion.severity, AgentSuggestion.SEVERITY_CRITICAL)
        self.assertIn("applied_without_approval_actor", suggestion.details_json["reason_codes"])
        self.assertIn("applied_without_approval_timestamp", suggestion.details_json["reason_codes"])

    def test_runner_returns_no_issue_for_cleanly_reviewed_adjustment(self):
        AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema="10",
            cantidad_fisica="12",
            motivo="Ajuste validado",
            estatus=AjusteInventario.STATUS_APLICADO,
            solicitado_por=self.user,
            aprobado_por=self.reviewer,
            aprobado_en=timezone.make_aware(datetime(2026, 3, 30, 9, 10)),
            aplicado_en=timezone.make_aware(datetime(2026, 3, 30, 9, 10)),
        )

        result = run_inventory_adjustment_authorization_guard(
            reference_dt=datetime(2026, 3, 30, 11, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(AgentSuggestion.objects.count(), 0)

    def test_management_command_runs_inventory_adjustment_guard(self):
        AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema="10",
            cantidad_fisica="8",
            motivo="Conteo fisico",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.user,
            creado_en=timezone.make_aware(datetime(2026, 3, 30, 5, 0)),
        )
        out = StringIO()

        call_command(
            "run_orchestration_rule",
            rule="inventory_adjustment_authorization_guard",
            reference_datetime="2026-03-30T11:00:00",
            username="inventario_runner",
            stdout=out,
        )

        self.assertIn("success_issue_created", out.getvalue())


class NearExpiryOrLowRotationRunnerTest(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="rotation_runner", password="pass123")
        self.unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1)

    def test_runner_creates_suggestion_for_zero_consumption_with_stock(self):
        insumo = Insumo.objects.create(
            nombre="Chocolate lento",
            categoria="Materia prima",
            unidad_base=self.unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("12"),
            stock_minimo=Decimal("4"),
            inventario_promedio=Decimal("6"),
            consumo_diario_promedio=Decimal("0"),
        )

        result = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        suggestion = AgentSuggestion.objects.get()
        self.assertEqual(suggestion.suggestion_type, "near_expiry_or_low_rotation_review")
        self.assertEqual(suggestion.severity, AgentSuggestion.SEVERITY_HIGH)
        self.assertIn("zero_consumption_with_stock", suggestion.details_json["reason_codes"])
        self.assertEqual(suggestion.details_json["expiry_evaluation_status"], "missing_source_data")

    def test_runner_skips_zero_consumption_when_absolute_stock_is_not_material(self):
        insumo = Insumo.objects.create(
            nombre="Esencia minima",
            categoria="Materia prima",
            unidad_base=self.unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("5"),
            stock_minimo=Decimal("1"),
            inventario_promedio=Decimal("1"),
            consumo_diario_promedio=Decimal("0"),
        )

        result = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(AgentSuggestion.objects.count(), 0)

    def test_runner_creates_suggestion_for_excessive_days_of_cover(self):
        insumo = Insumo.objects.create(
            nombre="Fruta lenta",
            categoria="Materia prima",
            unidad_base=self.unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("30"),
            stock_minimo=Decimal("4"),
            inventario_promedio=Decimal("10"),
            consumo_diario_promedio=Decimal("1"),
        )

        result = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_issue_created")
        suggestion = AgentSuggestion.objects.get()
        self.assertEqual(suggestion.severity, AgentSuggestion.SEVERITY_WARNING)
        self.assertIn("days_of_cover_above_threshold", suggestion.details_json["reason_codes"])
        self.assertEqual(suggestion.details_json["days_of_cover"], 30.0)

    def test_runner_skips_packaging_categories_by_scope(self):
        insumo = Insumo.objects.create(
            nombre="Capacillo filtrado",
            categoria="Desechables",
            unidad_base=self.unidad,
            tipo_item=Insumo.TIPO_MATERIA_PRIMA,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("25200"),
            stock_minimo=Decimal("10"),
            inventario_promedio=Decimal("14600"),
            consumo_diario_promedio=Decimal("0.160"),
        )

        result = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(AgentSuggestion.objects.count(), 0)

    def test_runner_returns_no_issue_when_rotation_is_healthy(self):
        insumo = Insumo.objects.create(
            nombre="Azucar sana",
            categoria="Materia prima",
            unidad_base=self.unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("8"),
            stock_minimo=Decimal("6"),
            inventario_promedio=Decimal("10"),
            consumo_diario_promedio=Decimal("2"),
        )

        result = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(AgentSuggestion.objects.count(), 0)

    def test_runner_skips_items_without_reference_metrics(self):
        insumo = Insumo.objects.create(
            nombre="Empaque sin metrica",
            categoria="Empaque",
            unidad_base=self.unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("200"),
            stock_minimo=Decimal("0"),
            inventario_promedio=Decimal("0"),
            consumo_diario_promedio=Decimal("0"),
        )

        result = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )

        self.assertTrue(result.created)
        self.assertEqual(result.status, "success_no_issue")
        self.assertEqual(AgentSuggestion.objects.count(), 0)

    def test_runner_respects_cooldown_per_canonical_insumo(self):
        insumo = Insumo.objects.create(
            nombre="Mantequilla lenta",
            categoria="Materia prima",
            unidad_base=self.unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("12"),
            stock_minimo=Decimal("4"),
            inventario_promedio=Decimal("6"),
            consumo_diario_promedio=Decimal("0"),
        )

        first = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 10, 0),
            created_by=self.user,
        )
        second = run_near_expiry_or_low_rotation_review(
            reference_dt=datetime(2026, 3, 31, 12, 0),
            created_by=self.user,
        )

        self.assertEqual(first.status, "success_issue_created")
        self.assertEqual(second.status, "success_no_issue")
        self.assertEqual(AgentSuggestion.objects.count(), 1)

    def test_management_command_runs_rotation_review_rule(self):
        insumo = Insumo.objects.create(
            nombre="Leche lenta",
            categoria="Materia prima",
            unidad_base=self.unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            stock_actual=Decimal("12"),
            stock_minimo=Decimal("4"),
            inventario_promedio=Decimal("6"),
            consumo_diario_promedio=Decimal("0"),
        )
        out = StringIO()

        call_command(
            "run_orchestration_rule",
            rule="near_expiry_or_low_rotation_review",
            reference_datetime="2026-03-31T10:00:00",
            username="rotation_runner",
            stdout=out,
        )

        self.assertIn("success_issue_created", out.getvalue())


class AgentRuntimeTests(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="agent_runtime_admin",
            password="pass123",
            email="runtime@example.com",
        )
        self.branch = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.product = Receta.objects.create(
            nombre="Pastel Runtime",
            nombre_normalizado="pastel runtime",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Temporada",
            hash_contenido="agent-runtime-pastel",
        )

    def _create_event(self, *, status: str) -> EventoVenta:
        event = EventoVenta.objects.create(
            name="Dia de la Madre Runtime",
            event_type="TEMPORADA",
            main_date=datetime(2026, 5, 10).date(),
            analysis_start_date=datetime(2026, 5, 5).date(),
            analysis_end_date=datetime(2026, 5, 11).date(),
            status=status,
        )
        EventoVentaSucursal.objects.create(sales_event=event, branch=self.branch)
        EventoVentaProducto.objects.create(sales_event=event, product=self.product)
        EventoVentaForecast.objects.create(
            sales_event=event,
            branch=self.branch,
            product=self.product,
            forecast_date=event.main_date,
            final_forecast=Decimal("5"),
            conservative_forecast=Decimal("4"),
            aggressive_forecast=Decimal("6"),
            explanation_json={"base_method": "event_anchor_ytd_weighted"},
        )
        EventoVentaFinancial.objects.create(
            sales_event=event,
            scenario=event.scenario_focus,
            estimated_sales=Decimal("1500"),
            estimated_cogs=Decimal("700"),
            estimated_gross_profit=Decimal("800"),
            estimated_margin=Decimal("53.33"),
            expected_roi=Decimal("114.29"),
        )
        return event

    def test_build_agent_context_reads_expected_files_in_order(self):
        goal = Goal(
            goal_type="sales_event_publication_guard",
            objective="Revisar publicación de evento",
            entity_type="ventas.EventoVenta",
            entity_id=1,
        )

        context = build_agent_context(goal, base_dir=".")

        self.assertEqual(
            context.files_in_order[:4],
            [
                "AGENTS.md",
                ".agent/skills/README.md",
                ".agent/skills/00-core/skill-erp-context/SKILL.md",
                ".agent/skills/00-core/skill-director-general-mode/SKILL.md",
            ],
        )
        self.assertIn(".agent/skills/60-automation-ops/skill-sales-event-publication-guard/SKILL.md", context.loaded_files)
        self.assertIn("Persistent Guard For Sales Event Projection Work", context.context_markdown)

    def test_load_agent_memory_parses_stable_sections(self):
        memory = load_agent_memory(base_dir=".")

        self.assertTrue(memory.stable_facts)
        self.assertTrue(memory.recurrent_errors)
        self.assertIn(
            "No dejar que helpers de generacion de compras muevan el evento a Compras automaticamente.",
            memory.recurrent_errors,
        )

    def test_tool_registry_exposes_documentary_and_executable_tools(self):
        goal = Goal(
            goal_type="sales_event_publication_guard",
            objective="Revisar publicación de evento",
            entity_type="ventas.EventoVenta",
            entity_id=1,
        )

        registry = resolve_tool_registry(goal)

        self.assertTrue(any(entry.kind == "skill" and not entry.executable for entry in registry.entries))
        self.assertTrue(any(entry.tool_key == "python.sales_event_publication_guard" and entry.executable for entry in registry.entries))
        self.assertTrue(any(entry.tool_key == "python.persist_projection_artifacts" and entry.executable for entry in registry.entries))

    def test_tool_registry_resolves_catalog_gateway_aliases(self):
        goal = Goal(
            goal_type="operational_chain_review",
            objective="Coordinar cadena operativa del evento",
            entity_type="ventas.EventoVenta",
            entity_id=1,
        )

        registry = resolve_tool_registry(goal)

        self.assertTrue(
            any(
                entry.declared_tool_key == "api.reportes_bi_dashboard"
                and entry.tool_key == "erp.get_dashboard"
                and entry.kind == "gateway"
                and entry.binding_state == "gateway_alias_resolved"
                for entry in registry.entries
            )
        )
        self.assertTrue(
            any(
                entry.declared_tool_key == "api.pos_bridge_sync_jobs"
                and entry.tool_key == "erp.get_sync_jobs"
                and entry.kind == "gateway"
                for entry in registry.entries
            )
        )
        self.assertTrue(
            any(
                entry.declared_tool_key == "api.integraciones_point_resumen"
                and entry.tool_key == "api.integraciones_point_resumen"
                and entry.binding_state == "unresolved_declared_tool"
                for entry in registry.entries
            )
        )

    def test_run_agent_goal_proposes_memory_for_unresolved_gateway_binding(self):
        event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)
        goal = Goal(
            goal_type="operational_chain_review",
            objective="Coordinar cadena operativa y registrar gaps",
            agent_code="director_operativo",
            entity_type="ventas.EventoVenta",
            entity_id=event.id,
            requested_action="review",
        )

        result = run_agent_goal(goal, actor=self.user, base_dir=".")

        self.assertEqual(result.status, OrchestrationRun.STATUS_PARTIAL)
        proposal = MemoryProposal.objects.get(
            proposed_by_agent__code="director_operativo",
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
        )
        self.assertEqual(proposal.section, MemoryProposal.SECTION_GAP)
        self.assertEqual(proposal.status, MemoryProposal.STATUS_PROPOSED)
        self.assertEqual(proposal.run_id, result.run_id)
        self.assertEqual(proposal.task_id, result.task_id)

    def test_seeded_agents_define_supported_goals_and_handoffs(self):
        director = AgentDefinition.objects.get(code="director_operativo")
        ventas_agent = AgentDefinition.objects.get(code="agente_publicacion_eventos_ventas")
        produccion_agent = AgentDefinition.objects.get(code="agente_produccion")
        compras_agent = AgentDefinition.objects.get(code="agente_compras")

        self.assertIn("operational_chain_review", director.supported_goal_types_json)
        self.assertIn("agente_publicacion_eventos_ventas", director.handoff_targets_json)
        self.assertIn("sales_event_publication_guard", ventas_agent.supported_goal_types_json)
        self.assertIn("production_readiness_guard", produccion_agent.supported_goal_types_json)
        self.assertIn("purchase_review_guard", compras_agent.supported_goal_types_json)

    def test_run_agent_goal_rejects_wrong_agent_for_goal(self):
        event = self._create_event(status=EventoVenta.STATUS_VALIDADO_PROD)
        goal = Goal(
            goal_type="sales_event_publication_guard",
            objective="Intento con agente equivocado",
            agent_code="agente_compras",
            entity_type="ventas.EventoVenta",
            entity_id=event.id,
            requested_action="review",
        )

        with self.assertRaisesMessage(ValueError, "no soporta el goal"):
            run_agent_goal(goal, actor=self.user, base_dir=".")

    def test_run_agent_goal_blocks_publish_when_event_not_ready(self):
        event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)
        goal = Goal(
            goal_type="sales_event_publication_guard",
            objective="Publicar evento comercial cuando esté listo",
            entity_type="ventas.EventoVenta",
            entity_id=event.id,
            requested_action="publish_if_safe",
        )

        result = run_agent_goal(goal, actor=self.user, base_dir=".")

        self.assertEqual(result.status, OrchestrationRun.STATUS_PARTIAL)
        self.assertEqual(result.decision, "block")
        self.assertTrue(any(finding.code == "workflow_gate_not_ready" for finding in result.blocking_findings))
        self.assertEqual(AgentTask.objects.get(id=result.task_id).status, AgentTask.STATUS_BLOCKED)
        self.assertTrue(AgentSuggestion.objects.filter(task_id=result.task_id).exists())
        self.assertGreaterEqual(AgentLoopCheckpoint.objects.filter(run_id=result.run_id).count(), 5)

    def test_run_agent_goal_publishes_when_event_is_ready(self):
        event = self._create_event(status=EventoVenta.STATUS_VALIDADO_PROD)
        goal = Goal(
            goal_type="sales_event_publication_guard",
            objective="Publicar artifacts del evento validado",
            entity_type="ventas.EventoVenta",
            entity_id=event.id,
            requested_action="publish_if_safe",
        )

        result = run_agent_goal(goal, actor=self.user, base_dir=".")

        self.assertEqual(result.status, OrchestrationRun.STATUS_SUCCESS)
        self.assertEqual(result.decision, "publish")
        self.assertEqual(AgentTask.objects.get(id=result.task_id).status, AgentTask.STATUS_RESOLVED)
        self.assertEqual(AgentSuggestion.objects.filter(task_id=result.task_id).count(), 0)
        self.assertEqual(event.projection_artifacts.filter(forecast_version=event.version).count(), 5)
        self.assertGreaterEqual(AgentLoopCheckpoint.objects.filter(run_id=result.run_id).count(), 6)

    def test_operational_chain_review_delegates_to_specialists(self):
        event = self._create_event(status=EventoVenta.STATUS_VALIDADO_PROD)
        production_plan = event.production_plans.create(plan_date=event.main_date, status="CONFIRMADO")
        event.input_requirements.create(
            production_plan=production_plan,
            input_item=Insumo.objects.create(
                nombre="Harina runtime",
                categoria="Materia prima",
                unidad_base=UnidadMedida.objects.create(nombre="Kilogramo", codigo="kg"),
                activo=True,
            ),
            required_qty=Decimal("10"),
            on_hand_qty=Decimal("8"),
            reserved_qty=Decimal("0"),
            net_shortage_qty=Decimal("2"),
            unit_cost_estimate=Decimal("25"),
        )
        event.purchase_requirements.create(
            input_requirement=event.input_requirements.first(),
            suggested_purchase_qty=Decimal("2"),
            estimated_cost=Decimal("50"),
        )
        goal = Goal(
            goal_type="operational_chain_review",
            objective="Coordinar cadena operativa del evento",
            agent_code="director_operativo",
            entity_type="ventas.EventoVenta",
            entity_id=event.id,
            requested_action="review",
        )

        result = run_agent_goal(goal, actor=self.user, base_dir=".")

        self.assertEqual(result.status, OrchestrationRun.STATUS_SUCCESS)
        self.assertEqual(result.decision, "complete_review")
        self.assertEqual(len(result.delegations), 3)
        self.assertEqual(
            [delegation["goal_type"] for delegation in result.delegations],
            [
                "sales_event_publication_guard",
                "production_readiness_guard",
                "purchase_review_guard",
            ],
        )
        self.assertEqual(AgentGoalDelegation.objects.filter(parent_run_id=result.run_id).count(), 3)
        self.assertTrue(
            AgentGoalDelegation.objects.filter(
                parent_run_id=result.run_id,
                from_agent__code="director_operativo",
                to_agent__code="agente_publicacion_eventos_ventas",
                goal_type="sales_event_publication_guard",
            ).exists()
        )

    def test_operational_chain_review_adds_reconciliation_when_chain_blocks(self):
        event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)
        goal = Goal(
            goal_type="operational_chain_review",
            objective="Coordinar cadena operativa bloqueada",
            agent_code="director_operativo",
            entity_type="ventas.EventoVenta",
            entity_id=event.id,
            requested_action="review",
        )

        result = run_agent_goal(goal, actor=self.user, base_dir=".")

        self.assertEqual(result.status, OrchestrationRun.STATUS_PARTIAL)
        self.assertEqual(
            [delegation["goal_type"] for delegation in result.delegations],
            [
                "sales_event_publication_guard",
                "production_readiness_guard",
                "purchase_review_guard",
                "reconciliation_guard",
            ],
        )

    def test_run_agent_goal_command_outputs_structured_result(self):
        event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)
        out = StringIO()

        call_command(
            "run_agent_goal",
            goal="sales_event_publication_guard",
            event_id=event.id,
            requested_action="review",
            username="agent_runtime_admin",
            objective="Revisar objetivo de publicación",
            stdout=out,
        )

        self.assertIn('"decision": "complete_review"', out.getvalue())

    def test_run_agent_goal_command_inferrs_agent_from_goal_type(self):
        event = self._create_event(status=EventoVenta.STATUS_VALIDADO_PROD)
        production_plan = event.production_plans.create(plan_date=event.main_date, status="CONFIRMADO")
        input_requirement = event.input_requirements.create(
            production_plan=production_plan,
            input_item=Insumo.objects.create(
                nombre="Azucar runtime",
                categoria="Materia prima",
                unidad_base=UnidadMedida.objects.create(nombre="Kilogramo cmd", codigo="kg-cmd"),
                activo=True,
            ),
            required_qty=Decimal("5"),
            on_hand_qty=Decimal("4"),
            reserved_qty=Decimal("0"),
            net_shortage_qty=Decimal("1"),
            unit_cost_estimate=Decimal("20"),
        )
        event.purchase_requirements.create(
            input_requirement=input_requirement,
            suggested_purchase_qty=Decimal("1"),
            estimated_cost=Decimal("20"),
        )
        out = StringIO()

        call_command(
            "run_agent_goal",
            goal="operational_chain_review",
            event_id=event.id,
            requested_action="review",
            username="agent_runtime_admin",
            objective="Coordinar cadena operativa",
            stdout=out,
        )

        self.assertIn('"decision": "complete_review"', out.getvalue())

    def test_run_orchestration_rule_command_supports_sales_event_operational_chain(self):
        event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)
        out = StringIO()

        call_command(
            "run_orchestration_rule",
            rule="sales_event_operational_chain_review",
            event_id=event.id,
            username="agent_runtime_admin",
            stdout=out,
        )

        self.assertIn("success_issue_created", out.getvalue())

    def test_task_run_rule_supports_sales_event_operational_chain(self):
        event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)

        payload = task_run_rule.run(
            rule_code="sales_event_operational_chain_review",
            event_id=event.id,
            force=True,
        )

        self.assertEqual(payload["status"], "success_issue_created")
        self.assertIsNotNone(payload["run_id"])

    def test_list_sales_event_chain_candidates_is_generic_by_event_state(self):
        candidate_event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)
        closed_event = self._create_event(status=EventoVenta.STATUS_CERRADO)

        candidates = list_sales_event_chain_candidates(event_ids=[candidate_event.id, closed_event.id], limit=10)

        self.assertEqual([candidate.event_id for candidate in candidates], [candidate_event.id])
        self.assertIn("forecast_ready_pending_production_validation", candidates[0].reasons)

    def test_run_sales_event_chain_batch_processes_generic_candidates(self):
        event_one = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)
        event_two = self._create_event(status=EventoVenta.STATUS_VALIDADO_PROD)
        production_plan = event_two.production_plans.create(plan_date=event_two.main_date, status="CONFIRMADO")
        input_requirement = event_two.input_requirements.create(
            production_plan=production_plan,
            input_item=Insumo.objects.create(
                nombre="Harina batch",
                categoria="Materia prima",
                unidad_base=UnidadMedida.objects.create(nombre="Kilogramo batch", codigo="kg-batch"),
                activo=True,
            ),
            required_qty=Decimal("5"),
            on_hand_qty=Decimal("4"),
            reserved_qty=Decimal("0"),
            net_shortage_qty=Decimal("1"),
            unit_cost_estimate=Decimal("20"),
        )
        event_two.purchase_requirements.create(
            input_requirement=input_requirement,
            suggested_purchase_qty=Decimal("1"),
            estimated_cost=Decimal("20"),
        )

        results = run_sales_event_chain_batch(
            event_ids=[event_one.id, event_two.id],
            created_by=self.user,
            trigger_source="test_batch",
            limit=10,
        )

        self.assertEqual(len(results), 2)
        self.assertEqual({result.status for result in results}, {"success_issue_created", "success_no_issue"})

    def test_task_run_sales_event_chains_supports_batch_candidates(self):
        event = self._create_event(status=EventoVenta.STATUS_ENVIADO_PROD)

        payload = task_run_sales_event_chains.run(event_ids=[event.id], limit=10)

        self.assertEqual(payload["processed"], 1)
        self.assertEqual(payload["results"][0]["status"], "success_issue_created")


class MemoryControlTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="memory_runtime_admin",
            password="pass123",
            email="memory@example.com",
        )

    def test_append_controlled_memory_entry_writes_once_and_logs_audit(self):
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            (tmp_path / "memory.md").write_text(Path("memory.md").read_text(encoding="utf-8"), encoding="utf-8")

            result = append_controlled_memory_entry(
                section="fact",
                text="La escritura controlada de memoria exige evidencia verificable y evita duplicados.",
                evidence_refs=["docs/CIERRE_AUDITORIA_Y_REPORTE_CONSOLIDADO_AGENTES_ERP.md"],
                source="test.memory_control",
                actor=self.user,
                base_dir=tmp_path,
            )

            self.assertTrue(result.written)
            contents = (tmp_path / "memory.md").read_text(encoding="utf-8")
            self.assertEqual(
                contents.count("La escritura controlada de memoria exige evidencia verificable y evita duplicados."),
                1,
            )

            audit = AuditLog.objects.filter(model="memory.md", object_id="Hechos estables confirmados").latest("id")
            self.assertEqual(audit.payload["source"], "test.memory_control")

            duplicate = append_controlled_memory_entry(
                section="fact",
                text="La escritura controlada de memoria exige evidencia verificable y evita duplicados.",
                evidence_refs=["docs/CIERRE_AUDITORIA_Y_REPORTE_CONSOLIDADO_AGENTES_ERP.md"],
                source="test.memory_control",
                actor=self.user,
                base_dir=tmp_path,
            )

            self.assertFalse(duplicate.written)
            contents_after_duplicate = (tmp_path / "memory.md").read_text(encoding="utf-8")
            self.assertEqual(
                contents_after_duplicate.count("La escritura controlada de memoria exige evidencia verificable y evita duplicados."),
                1,
            )

    def test_record_agent_memory_command_requires_evidence_and_writes_gap(self):
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            (tmp_path / "memory.md").write_text(Path("memory.md").read_text(encoding="utf-8"), encoding="utf-8")
            out = StringIO()

            call_command(
                "record_agent_memory",
                section="gap",
                text="El binding total catálogo -> gateway aún requiere cubrir aliases sin equivalencia clara.",
                source="test.record_agent_memory",
                evidence=["docs/AGENTS_RUNTIME_SNAPSHOT.json"],
                username="memory_runtime_admin",
                base_dir=str(tmp_path),
                stdout=out,
            )

            contents = (tmp_path / "memory.md").read_text(encoding="utf-8")
            self.assertIn("El binding total catálogo -> gateway aún requiere cubrir aliases sin equivalencia clara.", contents)
            self.assertIn('"written": true', out.getvalue())


class MemoryProposalServiceTests(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="memory_proposal_admin",
            password="pass123",
            email="proposal@example.com",
        )
        self.agent = AgentDefinition.objects.get(code="director_operativo")

    def test_propose_memory_update_dedupes_and_reopens_rejected(self):
        first = propose_memory_update(
            section="gap",
            summary="Binding faltante",
            statement="El agente director_operativo todavía no resuelve api.integraciones_point_resumen contra el gateway.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.91,
            evidence_refs=["docs/AGENTS_RUNTIME_SNAPSHOT.json"],
            proposed_by_agent=self.agent,
        )

        self.assertTrue(first.created)
        self.assertEqual(first.proposal.detected_count, 1)

        reject_memory_proposal(first.proposal, actor=self.user, reason="Aún no listo para memoria final.")
        second = propose_memory_update(
            section="gap",
            summary="Binding faltante",
            statement="El agente director_operativo todavía no resuelve api.integraciones_point_resumen contra el gateway.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.98,
            evidence_refs=[
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
                "orquestacion/tool_binding.py",
            ],
            proposed_by_agent=self.agent,
        )

        self.assertFalse(second.created)
        self.assertTrue(second.reopened)
        self.assertEqual(second.proposal.status, MemoryProposal.STATUS_PROPOSED)
        self.assertEqual(second.proposal.detected_count, 2)
        self.assertIn("orquestacion/tool_binding.py", second.proposal.evidence_refs_json)

    def test_tool_binding_gap_autoapproves_on_second_detection(self):
        first = propose_memory_update(
            section="gap",
            summary="Binding faltante auto",
            statement="El agente director_operativo declara api.integraciones_point_resumen pero el gateway actual no tiene un binding real confirmado para esa key.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.98,
            evidence_refs=[
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
                "orquestacion/tool_binding.py",
            ],
            proposed_by_agent=self.agent,
        )
        self.assertEqual(first.proposal.category, "tool_binding_gap")
        self.assertEqual(first.proposal.status, MemoryProposal.STATUS_PROPOSED)
        self.assertEqual(first.proposal.approval_mode, MemoryProposal.APPROVAL_MODE_MANUAL)

        second = propose_memory_update(
            section="gap",
            summary="Binding faltante auto",
            statement="El agente director_operativo declara api.integraciones_point_resumen pero el gateway actual no tiene un binding real confirmado para esa key.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.99,
            evidence_refs=[
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
                "orquestacion/tool_binding.py",
                "orquestacion/services/agent_runtime.py",
            ],
            proposed_by_agent=self.agent,
        )

        self.assertFalse(second.created)
        self.assertEqual(second.proposal.status, MemoryProposal.STATUS_APPROVED)
        self.assertEqual(second.proposal.approval_mode, MemoryProposal.APPROVAL_MODE_AUTO)
        self.assertIn("autoapproved:tool_binding_gap", second.proposal.auto_approval_reason)
        self.assertIsNotNone(second.proposal.auto_approved_at)

    def test_business_like_statement_never_autoapproves(self):
        first = propose_memory_update(
            section="gap",
            summary="Criterio riesgoso",
            statement="El negocio debería priorizar compras antes que producción cuando haya presión comercial.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.99,
            evidence_refs=[
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
                "orquestacion/tool_binding.py",
            ],
            proposed_by_agent=self.agent,
        )
        second = propose_memory_update(
            section="gap",
            summary="Criterio riesgoso",
            statement="El negocio debería priorizar compras antes que producción cuando haya presión comercial.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.99,
            evidence_refs=[
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
                "orquestacion/tool_binding.py",
                "orquestacion/services/agent_runtime.py",
            ],
            proposed_by_agent=self.agent,
        )

        self.assertEqual(first.proposal.category, "")
        self.assertEqual(second.proposal.status, MemoryProposal.STATUS_PROPOSED)
        self.assertEqual(second.proposal.approval_mode, MemoryProposal.APPROVAL_MODE_MANUAL)

    @override_settings(ORQUESTACION_MEMORY_AUTO_APPROVAL_ENABLED=False)
    def test_feature_flag_can_disable_autoapproval(self):
        first = propose_memory_update(
            section="gap",
            summary="Binding bloqueado por flag",
            statement="El agente director_operativo declara api.integraciones_point_resumen pero el gateway actual no tiene un binding real confirmado para esa key.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.98,
            evidence_refs=[
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
                "orquestacion/tool_binding.py",
            ],
            proposed_by_agent=self.agent,
        )
        second = propose_memory_update(
            section="gap",
            summary="Binding bloqueado por flag",
            statement="El agente director_operativo declara api.integraciones_point_resumen pero el gateway actual no tiene un binding real confirmado para esa key.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.99,
            evidence_refs=[
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
                "orquestacion/tool_binding.py",
                "orquestacion/services/agent_runtime.py",
            ],
            proposed_by_agent=self.agent,
        )

        self.assertEqual(first.proposal.status, MemoryProposal.STATUS_PROPOSED)
        self.assertEqual(second.proposal.status, MemoryProposal.STATUS_PROPOSED)

    def test_apply_memory_proposal_writes_memory_and_marks_applied(self):
        proposal_result = propose_memory_update(
            section="fact",
            summary="Ruta oficial de memoria",
            statement="Las propuestas aprobadas de memoria se escriben con control y auditoría antes de modificar memory.md.",
            source_type=MemoryProposal.SOURCE_MANUAL,
            source_reference="manual.memory_review",
            confidence_score=1.0,
            evidence_refs=["orquestacion/memory_control.py"],
            proposed_by_agent=self.agent,
        )
        proposal = approve_memory_proposal(proposal_result.proposal, actor=self.user)

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            (tmp_path / "memory.md").write_text(Path("memory.md").read_text(encoding="utf-8"), encoding="utf-8")
            applied = apply_memory_proposal(proposal, actor=self.user, base_dir=tmp_path)

        self.assertEqual(applied.status, MemoryProposal.STATUS_APPLIED)
        self.assertTrue(applied.applied_result_json["written"])
        self.assertEqual(applied.applied_result_json["reason"], "appended")


class MemoryProposalViewTests(TestCase):
    def setUp(self):
        call_command("seed_orquestacion_catalog", stdout=StringIO())
        user_model = get_user_model()
        self.admin = user_model.objects.create_user(username="memory_admin", password="pass123")
        self.reader = user_model.objects.create_user(username="memory_reader", password="pass123")

        admin_group, _ = Group.objects.get_or_create(name="ADMIN")
        reader_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.admin.groups.add(admin_group)
        self.reader.groups.add(reader_group)
        self.agent = AgentDefinition.objects.get(code="director_operativo")
        self.proposal = propose_memory_update(
            section="gap",
            summary="Gap controlado de binding",
            statement="El director operativo aún declara tools sin binding real confirmado contra el gateway.",
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference="director_operativo:operational_chain_review:api.integraciones_point_resumen",
            confidence_score=0.95,
            evidence_refs=["docs/AGENTS_RUNTIME_SNAPSHOT.json"],
            proposed_by_agent=self.agent,
        ).proposal

    def test_memory_proposals_list_allows_admin_and_denies_reader(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse("orquestacion:memory_proposals"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Propuestas de memoria operativa")
        self.assertContains(response, "Gap controlado de binding")

        self.client.force_login(self.reader)
        denied = self.client.get(reverse("orquestacion:memory_proposals"))
        self.assertEqual(denied.status_code, 403)

    def test_memory_proposals_list_handles_missing_table(self):
        self.client.force_login(self.admin)

        with patch("orquestacion.views.MemoryProposal.objects.exists", side_effect=OperationalError("missing table")):
            response = self.client.get(reverse("orquestacion:memory_proposals"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "orquestacion_memoryproposal")
        self.assertContains(response, "python manage.py migrate")

    @override_settings(ORQUESTACION_MEMORY_BASE_DIR=None)
    def test_memory_proposal_detail_can_approve_reject_and_apply(self):
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            (tmp_path / "memory.md").write_text(Path("memory.md").read_text(encoding="utf-8"), encoding="utf-8")

            with override_settings(ORQUESTACION_MEMORY_BASE_DIR=tmp_path):
                self.client.force_login(self.admin)
                approve_response = self.client.post(
                    reverse("orquestacion:memory_proposal_detail", args=[self.proposal.id]),
                    {
                        "action": "approve",
                        "summary": self.proposal.summary,
                        "statement": "El director operativo mantiene un gap real de binding con api.integraciones_point_resumen.",
                    },
                    follow=True,
                )
                self.assertEqual(approve_response.status_code, 200)
                self.proposal.refresh_from_db()
                self.assertEqual(self.proposal.status, MemoryProposal.STATUS_APPROVED)

                apply_response = self.client.post(
                    reverse("orquestacion:memory_proposal_detail", args=[self.proposal.id]),
                    {"action": "apply"},
                    follow=True,
                )
                self.assertEqual(apply_response.status_code, 200)
                self.proposal.refresh_from_db()
                self.assertEqual(self.proposal.status, MemoryProposal.STATUS_APPLIED)
                self.assertIn(
                    "El director operativo mantiene un gap real de binding con api.integraciones_point_resumen.",
                    (tmp_path / "memory.md").read_text(encoding="utf-8"),
                )

                reject_target = propose_memory_update(
                    section="error",
                    summary="Error de prueba",
                    statement="No se debe escribir memoria sin evidencia.",
                    source_type=MemoryProposal.SOURCE_MANUAL,
                    source_reference="manual.reject.test",
                    confidence_score=0.7,
                    evidence_refs=["orquestacion/memory_control.py"],
                    proposed_by_agent=self.agent,
                ).proposal
                reject_response = self.client.post(
                    reverse("orquestacion:memory_proposal_detail", args=[reject_target.id]),
                    {"action": "reject", "rejection_reason": "Ruido temporal."},
                    follow=True,
                )
                self.assertEqual(reject_response.status_code, 200)
                reject_target.refresh_from_db()
                self.assertEqual(reject_target.status, MemoryProposal.STATUS_REJECTED)
