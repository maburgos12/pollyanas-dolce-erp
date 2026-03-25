from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from pos_bridge.services.realtime_inventory_service import RealtimeInventoryService


class FakeSyncService:
    def __init__(self):
        self.calls = []

    def run_inventory_sync(self, *, triggered_by=None, branch_filter=None, limit_branches=None):
        self.calls.append(
            {
                "triggered_by": triggered_by,
                "branch_filter": branch_filter,
                "limit_branches": limit_branches,
            }
        )
        return SimpleNamespace(
            id=len(self.calls),
            status="SUCCESS",
            result_summary={"branch_filter": branch_filter or ""},
        )


class RealtimeInventoryServiceTests(SimpleTestCase):
    def test_run_sync_skips_when_outside_operation_hours(self):
        service = RealtimeInventoryService(sync_service=FakeSyncService())
        with patch("pos_bridge.services.realtime_inventory_service.is_within_operation_hours", return_value=False):
            jobs = service.run_sync(force=False)
        self.assertEqual(jobs, [])
        self.assertEqual(service.sync_service.calls, [])

    def test_run_sync_iterates_configured_branches(self):
        fake_sync = FakeSyncService()
        with patch.dict(os.environ, {"POS_BRIDGE_REALTIME_BRANCHES": "MATRIZ,COLOSIO"}, clear=False):
            service = RealtimeInventoryService(sync_service=fake_sync)
        with patch.object(service, "has_running_inventory_job", return_value=False):
            jobs = service.run_sync(force=True)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(fake_sync.calls[0]["branch_filter"], "MATRIZ")
        self.assertEqual(fake_sync.calls[1]["branch_filter"], "COLOSIO")

    def test_run_sync_uses_all_branches_when_env_is_empty(self):
        fake_sync = FakeSyncService()
        with patch.dict(os.environ, {"POS_BRIDGE_REALTIME_BRANCHES": ""}, clear=False):
            service = RealtimeInventoryService(sync_service=fake_sync)
        with patch.object(service, "has_running_inventory_job", return_value=False):
            jobs = service.run_sync(force=True)
        self.assertEqual(len(jobs), 1)
        self.assertIsNone(fake_sync.calls[0]["branch_filter"])

    def test_run_sync_skips_if_inventory_job_is_already_running(self):
        fake_sync = FakeSyncService()
        service = RealtimeInventoryService(sync_service=fake_sync)
        with patch.object(service, "has_running_inventory_job", return_value=True):
            jobs = service.run_sync(force=True)
        self.assertEqual(jobs, [])
        self.assertEqual(fake_sync.calls, [])
