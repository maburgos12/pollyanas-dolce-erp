from datetime import date, datetime
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.branch_catalog import POINT_NETWORK_BRANCH_CODES
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailyBranchIndicator, PointWasteLine
from recetas.views.plan import _build_dg_operacion_dashboard_payload
from reportes.models import DgOperacionSnapshot
from reportes.services_dg_operacion_snapshot import serialize_dg_operacion_payload


class DgTicketMermaTests(TestCase):
    cut = date(2026, 11, 13)

    def setUp(self):
        self.url = reverse("recetas:dg_operacion_dashboard")
        self.staff = get_user_model().objects.create_superuser("dg_prueba", password="test")
        self.client.force_login(self.staff)

    def branch(self, code, **kwargs):
        branch, _ = Sucursal.objects.get_or_create(codigo=code, defaults={"nombre": code, **kwargs})
        return PointBranch.objects.create(external_id=code, name=code, erp_branch=branch)

    def test_corte_anterior_muestra_todas_las_sucursales_y_el_importe_promedio(self):
        for code in POINT_NETWORK_BRANCH_CODES:
            if code == "CRUCERO":
                self.branch(code, fecha_apertura=date(2026, 11, 14))
                continue
            branch = self.branch(code)
            if code == "EL_TUNEL":
                continue
            PointDailyBranchIndicator.objects.create(
                branch=branch,
                indicator_date=self.cut,
                total_amount=Decimal("0") if code == "GUAMUCHIL" else Decimal("380.68"),
                total_tickets=0 if code == "GUAMUCHIL" else 2,
            )
        PointDailyBranchIndicator.objects.create(
            branch=self.branch("CEDIS"), indicator_date=self.cut, total_amount=Decimal("800.00"), total_tickets=1,
        )
        payload = _build_dg_operacion_dashboard_payload(fecha_operacion=self.cut)
        payload.pop("point_ticket_summary", None)
        saved_payload = serialize_dg_operacion_payload(payload)
        DgOperacionSnapshot.objects.create(fecha_operacion=self.cut, payload=saved_payload)

        response = self.client.get(self.url, {"fecha_operacion": self.cut})
        self.assertContains(response, "8 de 10 sucursales con tickets")
        self.assertContains(response, "190.34")
        self.assertContains(response, "Sin tickets registrados")
        self.assertContains(response, "Cero tickets")
        rows = response.context["point_ticket_summary"]["rows"]
        self.assertEqual(len(rows), 10)
        self.assertNotIn("CEDIS", [row["code"] for row in rows])
        self.assertNotIn("CRUCERO", [row["code"] for row in rows])
        self.assertEqual(DgOperacionSnapshot.objects.get(fecha_operacion=self.cut).payload, saved_payload)

    def test_detalle_merma_respeta_fecha_y_sucursal_con_la_misma_autorizacion(self):
        source = self.branch("GUAMUCHIL")
        other = self.branch("MATRIZ")
        for point_branch, day, hour, item, cost, unit_cost in (
            (source, 13, 0, "Producto del corte", "40.00", "0"),
            (source, 13, 12, "Producto sin costo", "0", "0"),
            (source, 13, 17, "Producto con estimación", "0", "5.00"),
            (source, 12, 23, "Otro día", "40.00", "0"),
            (source, 14, 0, "Fuera del corte", "40.00", "0"),
            (other, 13, 12, "Otra sucursal", "40.00", "0"),
        ):
            PointWasteLine.objects.create(
                branch=point_branch,
                erp_branch=point_branch.erp_branch,
                movement_external_id=item,
                source_hash=item,
                movement_at=timezone.make_aware(datetime(2026, 11, day, hour, 0)),
                item_name=item,
                quantity=Decimal("2"),
                unit="PZA",
                total_cost=Decimal(cost),
                unit_cost=Decimal(unit_cost),
            )

        filters = {"fecha_operacion": self.cut, "merma_detalle": "1", "sucursal_id": source.erp_branch_id}
        response = self.client.get(self.url, filters)
        self.assertContains(response, "Producto del corte")
        self.assertContains(response, "40.00")
        self.assertNotContains(response, "Otro día")
        self.assertNotContains(response, "Fuera del corte")
        self.assertContains(response, "Sin costo registrado")
        self.assertEqual(
            [(row["cost_point"], row["cost_estimate"]) for row in response.context["rows"]],
            [(Decimal("40"), None), (None, None), (None, Decimal("10"))],
        )
        self.assertNotContains(response, "Otra sucursal")
        self.assertFalse(DgOperacionSnapshot.objects.exists())
        self.assertEqual(self.client.get(self.url, {**filters, "sucursal_id": "inválido"}).status_code, 404)
        user = get_user_model().objects.create_user("sin_acceso_dg")
        self.client.force_login(user)
        self.assertEqual(self.client.get(self.url, filters).status_code, 403)
