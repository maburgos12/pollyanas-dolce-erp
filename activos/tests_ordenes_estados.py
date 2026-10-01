from concurrent.futures import ThreadPoolExecutor
from html import unescape
import re
from datetime import timedelta
from decimal import Decimal
from threading import Barrier
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import close_old_connections
from django.test import TestCase, TransactionTestCase
from django.urls import reverse
from django.utils import timezone

from core.models import AuditLog
from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento
from activos.services_ordenes import cambiar_estatus_orden


class OrdenStatesTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser("estados", "estados@test.com", "test")
        self.client.force_login(self.user)
        self.activo = Activo.objects.create(nombre="Equipo estados")
        self.plan = PlanMantenimiento.objects.create(
            activo_ref=self.activo, nombre="Semanal", frecuencia_dias=7,
            ultima_ejecucion=timezone.localdate() - timedelta(days=20),
        )

    def order(self, **kwargs):
        return OrdenMantenimiento.objects.create(activo_ref=self.activo, plan_ref=self.plan, **kwargs)

    def write(self, channel, order, target, **extra):
        data = {"estatus": target, **extra}
        if channel == "web":
            return self.client.post(reverse("activos:orden_estatus", args=[order.id, target]),
                                    data, HTTP_X_REQUESTED_WITH="XMLHttpRequest")
        if channel == "api":
            return self.client.post(reverse("api_activos_orden_estatus", args=[order.id]), data)
        if channel == "mobile":
            return self.client.post(f"/api/mantenimiento/bandeja/orden/{order.id}/actualizar/", data)
        return self.client.patch(f"/api/mantenimiento/ordenes/{order.id}/", data,
                                 content_type="application/json")

    def test_transition_matrix_and_repeats_match_every_consumer(self):
        for channel in ("web", "api", "mobile", "detail"):
            for initial in ("PENDIENTE", "EN_PROCESO", "CERRADA", "CANCELADA"):
                for target in ("PENDIENTE", "EN_PROCESO", "CERRADA", "CANCELADA"):
                    with self.subTest(channel=channel, initial=initial, target=target):
                        order = self.order(estatus=initial, costo_otros=Decimal("12"))
                        before = order.actualizado_en
                        allowed = initial == target or (
                            initial == "PENDIENTE" and target in {"EN_PROCESO", "CERRADA", "CANCELADA"}
                        ) or (initial == "EN_PROCESO" and target in {"CERRADA", "CANCELADA"})
                        response = self.write(channel, order, target)
                        self.assertEqual(response.status_code, 200 if allowed else 400)
                        order.refresh_from_db()
                        self.assertEqual(order.estatus, target if allowed else initial)
                        self.assertEqual(order.costo_otros, Decimal("12"))
                        effects = int(allowed and initial != target)
                        self.assertEqual(order.bitacora.count(), effects)
                        self.assertEqual(AuditLog.objects.filter(model="activos.OrdenMantenimiento", object_id=str(order.id)).count(), effects)
                        if not effects:
                            self.assertEqual(order.actualizado_en, before)
                        if effects and target == "CERRADA":
                            self.plan.refresh_from_db()
                            self.assertEqual(order.fecha_cierre, timezone.localdate())
                            self.assertEqual(self.plan.ultima_ejecucion, timezone.localdate())
                            self.assertEqual(self.plan.proxima_ejecucion, timezone.localdate() + timedelta(days=7))
                        if effects and target == "EN_PROCESO":
                            self.assertEqual(order.fecha_inicio, timezone.localdate())

    def test_invalid_transition_precedes_cost_provider_or_comment_capture(self):
        for channel in ("mobile", "detail"):
            order = self.order(estatus="CANCELADA", responsable="Actual", costo_otros=Decimal("12"))
            response = self.write(channel, order, "CERRADA", proveedor_servicio="Proveedor nuevo",
                                  responsable="Otro", costo_real="300", costo_adicional="300", comentario="Nota")
            self.assertEqual(response.status_code, 400)
            order.refresh_from_db()
            self.assertEqual((order.responsable, order.costo_otros), ("Actual", Decimal("12")))
            self.assertFalse(order.bitacora.exists())
        order = self.order(estatus="CANCELADA", costo_otros=Decimal("12"))
        self.client.post(reverse("activos:ordenes"), {"action": "update_costos", "orden_id": order.id,
                         "cerrar_orden": "1", "costo_otros": "999"})
        order.refresh_from_db()
        self.assertEqual(order.costo_otros, Decimal("12"))
        self.assertFalse(order.bitacora.exists())

    def test_explicit_metadata_still_creates_event_and_additive_cost(self):
        order = self.order(estatus="CERRADA", costo_otros=Decimal("12"))
        for _ in range(2):
            response = self.write("detail", order, "CERRADA", costo_adicional="3", comentario="Servicio")
            self.assertEqual(response.status_code, 200)
        order.refresh_from_db()
        self.assertEqual(order.costo_otros, Decimal("18"))
        self.assertEqual(order.bitacora.count(), 2)
        self.assertFalse(AuditLog.objects.filter(model="activos.OrdenMantenimiento", object_id=str(order.id)).exists())

    def test_async_refresh_preserves_filters_and_removes_unavailable_actions(self):
        order = self.order()
        other = self.order()
        query = "estatus=ABIERTAS&enterprise_gap=SIN_RESPONSABLE"
        response = self.write("web", order, "CERRADA", return_query=query)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["target"], "#ordenes-registradas")
        html = response.json()["html"]
        self.assertNotIn(order.folio, html)
        self.assertIn(other.folio, html)
        self.assertIn('value="estatus=ABIERTAS&amp;enterprise_gap=SIN_RESPONSABLE"', html)
        emitted_query = unescape(re.search(r'name="return_query" value="([^"]*)"', html).group(1))
        second = self.write("web", other, "EN_PROCESO", return_query=emitted_query)
        self.assertIn('value="estatus=ABIERTAS&amp;enterprise_gap=SIN_RESPONSABLE"', second.json()["html"])
        response = self.client.post(reverse("activos:orden_estatus", args=[other.id, "CANCELADA"]),
                                    {"return_query": query})
        self.assertEqual(response.url, reverse("activos:ordenes") + "?" + query + "#ordenes-registradas")
        page = self.client.get(reverse("activos:ordenes"), {"estatus": "CERRADA"})
        self.assertNotContains(page, reverse("activos:orden_estatus", args=[order.id, "EN_PROCESO"]))
        self.assertNotContains(page, reverse("activos:orden_estatus", args=[order.id, "CANCELADA"]))

    def test_metadata_failure_rolls_back_transition_and_plan(self):
        order = self.order()
        before = (self.plan.ultima_ejecucion, self.plan.proxima_ejecucion)
        # First bitacora is canonical status, second explicit follow-up fails.
        original = BitacoraMantenimiento.objects.create
        def create(**kwargs):
            if kwargs["accion"] != "ESTATUS":
                raise RuntimeError("fallo metadata")
            return original(**kwargs)
        with patch("mantenimiento.views.BitacoraMantenimiento.objects.create", side_effect=create):
            with self.assertRaises(RuntimeError):
                self.write("mobile", order, "CERRADA", costo_real="300", comentario="Servicio")
        order.refresh_from_db()
        self.plan.refresh_from_db()
        self.assertEqual(order.estatus, "PENDIENTE")
        self.assertEqual(order.costo_otros, Decimal("0"))
        self.assertEqual((self.plan.ultima_ejecucion, self.plan.proxima_ejecucion), before)
        self.assertFalse(order.bitacora.exists())
        self.assertFalse(AuditLog.objects.filter(model="activos.OrdenMantenimiento", object_id=str(order.id)).exists())

    def test_write_permissions_remain_required(self):
        viewer = get_user_model().objects.create_user("sin_acceso", password="test")
        self.client.force_login(viewer)
        order = self.order()
        for channel in ("web", "api", "mobile", "detail"):
            self.assertEqual(self.write(channel, order, "CERRADA").status_code, 403)
        order.refresh_from_db()
        self.assertEqual(order.estatus, "PENDIENTE")
        self.assertFalse(order.bitacora.exists())

    def test_failures_roll_back_order_plan_bitacora_and_audit(self):
        for failure in ("activos.services_ordenes.BitacoraMantenimiento.objects.create",
                        "activos.services_ordenes.log_event"):
            order = self.order()
            plan_before = (self.plan.ultima_ejecucion, self.plan.proxima_ejecucion)
            def audit_then_fail(*args, **kwargs):
                from core.audit import log_event
                log_event(*args, **kwargs)
                raise RuntimeError("fallo después de auditar")
            effect = audit_then_fail if failure.endswith("log_event") else RuntimeError("fallo simulado")
            with patch(failure, side_effect=effect):
                with self.assertRaises(RuntimeError):
                    cambiar_estatus_orden(order.id, "CERRADA", self.user)
            order.refresh_from_db()
            self.plan.refresh_from_db()
            self.assertEqual(order.estatus, "PENDIENTE")
            self.assertIsNone(order.fecha_cierre)
            self.assertEqual((self.plan.ultima_ejecucion, self.plan.proxima_ejecucion), plan_before)
            self.assertFalse(order.bitacora.exists())
            self.assertFalse(AuditLog.objects.filter(model="activos.OrdenMantenimiento", object_id=str(order.id)).exists())


class OrdenConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user("concurrente")
        activo = Activo.objects.create(nombre="Equipo concurrente")
        self.plan = PlanMantenimiento.objects.create(activo_ref=activo, nombre="Semanal", frecuencia_dias=7)
        self.order = OrdenMantenimiento.objects.create(activo_ref=activo, plan_ref=self.plan)

    def test_concurrent_close_has_exactly_one_effect(self):
        barrier = Barrier(2)
        def close():
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                return cambiar_estatus_orden(self.order.id, "CERRADA", self.user)[2]
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(close) for _ in range(2)]
            result = [future.result(timeout=20) for future in futures]
        self.assertEqual(sorted(result), [False, True])
        self.assertEqual(self.order.bitacora.count(), 1)
        self.assertEqual(AuditLog.objects.filter(model="activos.OrdenMantenimiento", object_id=str(self.order.id)).count(), 1)
        self.plan.refresh_from_db()
        self.assertEqual(self.plan.ultima_ejecucion, timezone.localdate())
