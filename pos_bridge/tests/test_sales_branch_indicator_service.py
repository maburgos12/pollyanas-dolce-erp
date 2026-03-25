from __future__ import annotations

from datetime import date, datetime, timezone as dt_timezone
from decimal import Decimal

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailyBranchIndicator
from pos_bridge.services.sales_branch_indicator_service import (
    PointBranchIndicatorPayload,
    PointSalesBranchIndicatorService,
)


class PointSalesBranchIndicatorServiceTests(TestCase):
    def test_canonical_branches_prefers_numeric_external_id(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        PointBranch.objects.create(
            external_id="Matriz",
            name="Matriz",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=sucursal,
            updated_at=datetime(2026, 3, 21, tzinfo=dt_timezone.utc),
        )
        numeric = PointBranch.objects.create(
            external_id="1",
            name="Matriz",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=sucursal,
            updated_at=datetime(2026, 3, 20, tzinfo=dt_timezone.utc),
        )

        branches = PointSalesBranchIndicatorService.canonical_branches()

        self.assertEqual(len(branches), 1)
        self.assertEqual(branches[0].id, numeric.id)

    def test_persist_branch_day_upserts_indicator(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        branch = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=sucursal)
        service = PointSalesBranchIndicatorService()

        payload = PointBranchIndicatorPayload(
            branch=branch,
            indicator_date=date(2026, 3, 20),
            contado_amount=Decimal("100.00"),
            credito_amount=Decimal("20.00"),
            contado_tickets=4,
            credito_tickets=1,
            contado_avg_ticket=Decimal("25.00"),
            credito_avg_ticket=Decimal("20.00"),
            total_amount=Decimal("120.00"),
            total_tickets=5,
            total_avg_ticket=Decimal("24.00"),
            raw_payload={"response": {"ok": True}},
        )

        _, created = service.persist_branch_day(indicator_payload=payload)
        self.assertTrue(created)

        payload.total_amount = Decimal("150.00")
        payload.total_tickets = 6
        payload.total_avg_ticket = Decimal("25.00")
        _, created = service.persist_branch_day(indicator_payload=payload)
        self.assertFalse(created)

        indicator = PointDailyBranchIndicator.objects.get(branch=branch, indicator_date=date(2026, 3, 20))
        self.assertEqual(indicator.total_amount, Decimal("150.00"))
        self.assertEqual(indicator.total_tickets, 6)
