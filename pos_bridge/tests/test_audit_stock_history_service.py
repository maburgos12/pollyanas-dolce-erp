import hashlib
import json
import base64
import zlib
from copy import deepcopy
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointProduct,
    PointProductHistoryImport,
    PointProductHistoryRow,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
)
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryError, AuditStockHistoryService


def _row(
    movement_id,
    movement,
    stamp,
    quantity,
    previous,
    new,
    *,
    cancelled=False,
):
    return {
        "FK_Movimiento": movement_id,
        "FK_Tipo_Movimiento": movement_id,
        "Movimiento": movement,
        "Fecha": stamp,
        "Cantidad": quantity,
        "Existencia_anterior": previous,
        "Existencia_nueva": new,
        "Costo_Total": 0,
        "Costo_Unitario": 0,
        "Cancelado": cancelled,
    }


class _FakePointClient:
    def __init__(self, rows):
        self.rows = rows
        self.login_calls = 0
        self.history_calls = []

    def login(self):
        self.login_calls += 1

    def get_stock_history(self, product_id, branch_id, *, movements=500):
        self.history_calls.append((str(product_id), str(branch_id), movements))
        return list(self.rows)


class AuditStockHistoryServiceTests(TestCase):
    def _original_evidence(self, rows, *, limit=300, retrieved="2026-10-04T18:26:15.406476+00:00"):
        return {
            "source": "POINT_STOCK_HISTORY_API", "domain": "PRODUCT", "response_complete": True,
            "branch_id": self.branch.pk, "product_id": self.product.pk,
            "request": {"path": "/Stock/GetHistorial", "params": {
                "tipo": "false", "almacen": str(self.branch.external_id),
                "pkproducto": str(self.product.external_id), "movimientos": str(limit), "tipoMovimiento": "",
            }},
            "retrieved_at": retrieved, "history_limit": limit, "fetched_rows": len(rows),
            "raw_sha256": hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest(),
            "original_locator": {"source_file": "/evidence/original.jsonl", "source_line": 1},
            "request_provenance": {"kind": "DERIVED_FROM_ACQUISITION_SCRIPT", "source_file": "/evidence/read-original.py",
                "source_code": "# original acquisition reader\n",
                "source_sha256": hashlib.sha256(b"# original acquisition reader\n").hexdigest(),
                "client_contract": "PointHttpSessionClient.get_stock_history"},
        }

    def _original_rows(self):
        return [
            _row(990, "ENTRADA POR PRODUCCIÓN", "2026-08-31T22:44:50.28", 2, 0, 2),
            _row(991, "VENTA", "2026-09-10T18:00:00", 1, 2, 1),
            _row(992, "VENTA", "2026-10-02T18:00:00", 1, 1, 0),
        ]

    def test_original_response_preserves_receipt_provenance_and_is_no_http_idempotent(self):
        rows = self._original_rows()
        evidence = self._original_evidence(rows, limit=5)
        service = AuditStockHistoryService()
        with patch("requests.Session.request", side_effect=AssertionError("HTTP prohibited")):
            result = service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        self.assertEqual(result.coverage_status, "COMPLETE")
        self.assertEqual(result.documentary_opening, Decimal("2"))
        self.assertEqual(result.documentary_closing, Decimal("1"))
        record = service._existing_import(self.branch, self.product)
        self.assertEqual(record.raw_metadata["fetched_at"], evidence["retrieved_at"])
        self.assertEqual(record.raw_metadata["history_limit"], 5)
        proof = record.raw_metadata["response_provenance"][0]
        self.assertEqual(proof["raw_sha256"], evidence["raw_sha256"])
        self.assertEqual(proof["original_locator"], evidence["original_locator"])
        self.assertNotEqual(proof["ingested_at"], proof["retrieved_at"])
        self.assertEqual(record.rows.get(row_number=990).raw_payload, rows[0])
        before_record = PointProductHistoryImport.objects.values().get(pk=record.pk)
        before_rows = list(record.rows.order_by("pk").values())
        with patch("requests.Session.request", side_effect=AssertionError("HTTP prohibited")):
            again = service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        self.assertEqual(again, result)
        self.assertEqual(PointProductHistoryImport.objects.count(), 1)
        self.assertEqual(PointProductHistoryImport.objects.values().get(pk=record.pk), before_record)
        self.assertEqual(list(record.rows.order_by("pk").values()), before_rows)

    def test_original_response_rejects_invalid_envelope_without_writing(self):
        rows = self._original_rows()
        evidence = self._original_evidence(rows)
        mutations = (
            {"raw_sha256": "0" * 64}, {"fetched_rows": 100}, {"history_limit": 0},
            {"history_limit": True}, {"history_limit": 101}, {"response_complete": False}, {"domain": "INSUMO"},
            {"branch_id": self.branch.pk + 1}, {"product_id": self.product.pk + 1},
            {"retrieved_at": "2026-10-04T18:26:15"}, {"retrieved_at": "2099-01-01T00:00:00Z"},
            {"original_locator": {}}, {"source": "OTHER"},
            {"extra": "cannot change replay fingerprint"},
            {"original_locator": {**evidence["original_locator"], "extra": 1}},
            {"request_provenance": {}},
            {"request_provenance": {**evidence["request_provenance"], "source_sha256": "invalid"}},
            {"request_provenance": {**evidence["request_provenance"], "source_code": "tampered"}},
            {"request_provenance": {**evidence["request_provenance"], "kind": "LIVE_HTTP"}},
            {"request": {"path": "/Stock/GetHistorial", "params": {**evidence["request"]["params"], "fecha": "2026-09-01"}}},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaises(AuditStockHistoryError):
                    AuditStockHistoryService().ingest_original_response(
                        self.branch, self.product, date(2026, 9, 1), rows, evidence={**evidence, **mutation})
                self.assertEqual(PointProductHistoryImport.objects.count(), 0)
                self.assertEqual(PointProductHistoryRow.objects.count(), 0)

    def test_original_response_rejects_duplicate_or_malformed_rows(self):
        rows = self._original_rows()
        malformed = [
            [rows[0], {**rows[0], "Cantidad": 3}], [{**rows[0], "Fecha": "invalid"}],
            [{**rows[0], "FK_Movimiento": 0}], [{**rows[0], "Cantidad": None}],
            [{**rows[0], "Cantidad": "NaN"}], [{**rows[0], "Cantidad": True}],
            [{**rows[0], "Cancelado": None}], [{**rows[0], "Cancelado": 0}], [{**rows[0], "Cancelado": 1}],
            [{key: val for key, val in rows[0].items() if key != "Existencia_nueva"}],
            [{**rows[0], "Fecha": "2026-10-05T00:00:00Z"}],
        ]
        for raw in malformed:
            with self.subTest(raw=raw):
                with self.assertRaises(AuditStockHistoryError):
                    AuditStockHistoryService().ingest_original_response(
                        self.branch, self.product, date(2026, 9, 1), raw, evidence=self._original_evidence(raw))
                self.assertEqual(PointProductHistoryImport.objects.count(), 0)

    def test_original_exact_duplicate_keeps_original_count_archive_and_saturation(self):
        rows = [_row(10000, "VENTA", "2026-08-31T22:00:00", 1, 600, 599)]
        rows.extend(_row(10000 + index, "VENTA", "2026-09-10T18:00:00", 1,
            600 - index, 599 - index) for index in range(1, 499))
        original = [rows[0], deepcopy(rows[0]), *rows[1:]]
        evidence = self._original_evidence(original, limit=500)
        service = AuditStockHistoryService()
        result = service.ingest_original_response(self.branch, self.product,
            date(2026, 9, 1), original, evidence=evidence)
        record = service._existing_import(self.branch, self.product)
        self.assertEqual(record.row_count, 499)
        self.assertEqual(record.raw_metadata["fetched_rows"], 500)
        self.assertEqual(len(record.raw_metadata["fetched_movement_ids"]), 500)
        archive = record.raw_metadata["original_responses"][record.raw_metadata["latest_response_fingerprint"]]
        self.assertEqual(json.loads(zlib.decompress(base64.b64decode(archive["raw_zlib_base64"]))), original)
        self.assertEqual(result.coverage_status, "COMPLETE")
        self.assertEqual(result.sales, Decimal("498"))
        self.assertEqual(result.documentary_opening, Decimal("599"))
        self.assertEqual(result.documentary_closing, Decimal("101"))
        self.assertFalse(service._covers_month(record, date(2026, 8, 1)))
        before = list(record.rows.order_by("pk").values())
        again = service.ingest_original_response(self.branch, self.product,
            date(2026, 9, 1), original, evidence=evidence)
        self.assertEqual(again, result)
        self.assertEqual(list(record.rows.order_by("pk").values()), before)

    def test_original_duplicate_type_conflicts_reject_without_writes(self):
        row = self._original_rows()[0]
        for patch in ({"Cancelado": 0}, {"Cantidad": 2.0}, {"Costo_Total": False}):
            original = [row, {**row, **patch}]
            with self.subTest(patch=patch), self.assertRaises(AuditStockHistoryError):
                AuditStockHistoryService().ingest_original_response(self.branch, self.product,
                    date(2026, 9, 1), original, evidence=self._original_evidence(original))
            self.assertEqual(PointProductHistoryImport.objects.count(), 0)
            self.assertEqual(PointProductHistoryRow.objects.count(), 0)

    def test_original_rows_reject_present_domain_or_pair_contradictions_before_writes(self):
        raw = self._original_rows()[0]
        mutations = [{"isInsumo": True}, {"isInsumo": "true"}]
        mutations.extend({field: "different-product"} for field in
            ("FK_Producto", "PK_Producto", "FK_articulo", "FK_Articulo"))
        mutations.extend({field: "different-branch"} for field in ("FK_Sucursal", "PK_Sucursal"))
        for mutation in mutations:
            original = [{**raw, **mutation}]
            with self.subTest(mutation=mutation), self.assertRaises(AuditStockHistoryError):
                AuditStockHistoryService().ingest_original_response(self.branch, self.product,
                    date(2026, 9, 1), original, evidence=self._original_evidence(original))
            self.assertEqual(PointProductHistoryImport.objects.count(), 0)
            self.assertEqual(PointProductHistoryRow.objects.count(), 0)

    def test_original_duplicate_membership_requires_intact_latest_archive(self):
        rows = self._original_rows()
        original = [rows[0], deepcopy(rows[0]), *rows[1:]]
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            original, evidence=self._original_evidence(original))
        record = service._existing_import(self.branch, self.product)
        pristine = deepcopy(record.raw_metadata)
        fingerprint = pristine["latest_response_fingerprint"]
        mutations = (
            lambda metadata: metadata["original_responses"][fingerprint].update(raw_zlib_base64="broken"),
            lambda metadata: metadata["original_responses"][fingerprint].update(raw_sha256="0" * 64),
            lambda metadata: metadata.update(fetched_movement_ids=[990, 991, 992, 992]),
            lambda metadata: metadata["original_responses"][fingerprint]["request"]["params"].update(tipo="true"),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                record.raw_metadata = deepcopy(pristine)
                mutate(record.raw_metadata)
                self.assertFalse(service._covers_month(record, date(2026, 9, 1)))

    def test_original_duplicate_outside_month_gap_is_visible_without_false_unknown(self):
        first = _row(990, "VENTA", "2026-08-20T18:00:00", 1, 5, 4)
        duplicate = _row(991, "VENTA", "2026-08-21T18:00:00", 1, 3, 2)
        september = _row(992, "VENTA", "2026-09-10T18:00:00", 1, 2, 1)
        rows = [first, duplicate, deepcopy(duplicate), september]
        service = AuditStockHistoryService()
        result = service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            rows, evidence=self._original_evidence(rows))
        self.assertEqual(result.documentary_opening, Decimal("2"))
        self.assertEqual(result.documentary_closing, Decimal("1"))
        self.assertEqual(result.unknown_movement_ids, ())
        record = service._existing_import(self.branch, self.product)
        batch = service._original_batch(record)
        self.assertEqual(batch["duplicate_movement_ids"], (991,))
        self.assertEqual(batch["stock_chain_gaps"][0]["movement_id"], 991)
        self.assertEqual(batch["stock_chain_gaps"][0]["previous_movement_id"], 990)
        self.assertEqual(batch["stock_chain_gaps"][0]["movement_at"], "2026-08-21T18:00:00+00:00")
        changed = deepcopy(rows)
        changed[-1]["Existencia_anterior"] = 4
        changed[-1]["Existencia_nueva"] = 3
        latest = self._original_evidence(changed, retrieved="2026-10-04T20:00:00Z")
        result = service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), changed, evidence=latest)
        self.assertIsNone(result.documentary_opening)
        self.assertIsNone(result.documentary_closing)
        self.assertEqual(result.unknown_movement_ids, ())

    def test_original_duplicate_monthly_boundary_carries_archive_signature_and_historical_gap(self):
        from pos_bridge.services.monthly_product_balance_service import documentary_historical_boundary
        rows = [_row(990, "VENTA", "2026-08-20T18:00:00", 1, 5, 4),
                _row(991, "VENTA", "2026-08-21T18:00:00", 1, 3, 2),
                _row(992, "VENTA", "2026-09-10T18:00:00", 1, 2, 1)]
        original = [rows[0], rows[1], deepcopy(rows[1]), rows[2]]
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            original, evidence=self._original_evidence(original))
        closing = PointHistoricalInventoryClosing.objects.create(operational_date=date(2026, 9, 30),
            source=PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY, status="VERIFIED",
            expected_branch_ids=[self.branch.pk], expected_product_ids=[self.product.pk],
            source_fingerprint="unchanged-manifest", retrieved_at=timezone.now(),
            metadata={"method": "point_stock_history_boundary"})
        line = PointHistoricalInventoryClosingLine.objects.create(closing=closing,
            branch=self.branch, product=self.product, stock=9, evidence={})
        def read():
            return documentary_historical_boundary(closing, [line], month=date(2026, 9, 1),
                boundary="closing", cache={})
        values, evidence, missing = read()
        self.assertEqual(values, {line.pk: Decimal("1")})
        self.assertEqual(missing, ())
        proof = evidence[line.pk]["original_batch_evidence"]
        self.assertEqual(proof["stock_chain_gaps"][0]["movement_id"], 991)
        self.assertEqual(proof["original_count"], 4)
        self.assertEqual(proof["unique_count"], 3)
        newer = self._original_evidence(original, retrieved="2026-10-04T20:00:00Z")
        newer["original_locator"]["source_line"] = 2
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), original, evidence=newer)
        again = read()
        self.assertEqual(again[0], values)
        self.assertNotEqual(again[1][line.pk]["original_batch_evidence"]["source_signature"], proof["source_signature"])

    def test_original_duplicate_empty_month_resolves_full_archived_occurrences(self):
        from pos_bridge.services.monthly_product_balance_service import _empty_month_captured_boundaries
        rows = self._original_rows()
        rows[2]["Existencia_anterior"], rows[2]["Existencia_nueva"] = 2, 1
        original = [rows[0], deepcopy(rows[0]), rows[2]]
        service = AuditStockHistoryService()
        history = service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            original, evidence=self._original_evidence(original))
        self.assertEqual(history.movement_ids, ())
        line = SimpleNamespace(branch_id=self.branch.pk, product_id=self.product.pk)
        resolutions = _empty_month_captured_boundaries([line], month=date(2026, 9, 1),
            reconciliations={(line.branch_id, line.product_id): history}, cache={})
        resolved = resolutions[(line.branch_id, line.product_id)]
        self.assertEqual(resolved["opening"].stock, Decimal("2"))
        self.assertEqual(resolved["closing"].stock, Decimal("2"))
        self.assertEqual(resolved["closing"].evidence["history_rows"], 3)
        crossing = deepcopy(original)
        crossing[-1]["Existencia_anterior"] = 4
        crossing[-1]["Existencia_nueva"] = 3
        history = service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), crossing,
            evidence=self._original_evidence(crossing, retrieved="2026-10-04T20:00:00Z"))
        blocked = _empty_month_captured_boundaries([line], month=date(2026, 9, 1),
            reconciliations={(line.branch_id, line.product_id): history}, cache={})
        self.assertFalse(blocked[(line.branch_id, line.product_id)])

    def test_original_batch_rejects_malformed_archive_flag_even_with_consistent_hashes(self):
        rows = self._original_rows()
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            rows, evidence=self._original_evidence(rows))
        record = service._existing_import(self.branch, self.product)
        proof = deepcopy(record.raw_metadata["response_provenance"][0])
        damaged = deepcopy(rows)
        damaged[0]["Cancelado"] = 0
        raw_json = json.dumps(damaged, sort_keys=True, default=str).encode()
        proof["raw_sha256"] = hashlib.sha256(raw_json).hexdigest()
        evidence = {key: value for key, value in proof.items() if key not in {"fingerprint", "ingested_at"}}
        fingerprint = hashlib.sha256(json.dumps(evidence, sort_keys=True, default=str).encode()).hexdigest()
        proof["fingerprint"] = fingerprint
        record.raw_metadata["latest_response_fingerprint"] = fingerprint
        record.raw_metadata["response_provenance"] = [proof]
        record.raw_metadata["original_responses"] = {fingerprint: {**proof,
            "encoding": "zlib-base64-json", "raw_zlib_base64": base64.b64encode(zlib.compress(raw_json)).decode()}}
        self.assertFalse(service._covers_month(record, date(2026, 9, 1)))

    def test_original_batch_corrupt_structures_fail_closed_without_crashing(self):
        rows = self._original_rows()
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            rows, evidence=self._original_evidence(rows))
        record = service._existing_import(self.branch, self.product)
        pristine = deepcopy(record.raw_metadata)
        selected = list(record.rows.filter(row_number=991))
        fingerprint = pristine["latest_response_fingerprint"]
        corrupted = [deepcopy(pristine) for _ in range(3)]
        corrupted[0]["original_responses"][fingerprint] = None
        corrupted[1]["response_provenance"] = [None]
        corrupted[2]["latest_response_fingerprint"] = None
        for metadata in [*corrupted, [1]]:
            with self.subTest(metadata_type=type(metadata).__name__):
                record.raw_metadata = metadata
                self.assertFalse(service._covers_month(record, date(2026, 9, 1)))
                history = service._reconcile_record(record, date(2026, 9, 1), selected)
                self.assertEqual(history.coverage_status, "INCOMPLETE")
                self.assertIsNone(history.documentary_closing)

    def test_original_batch_revalidates_archived_locator_and_script_provenance(self):
        rows = self._original_rows()
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            rows, evidence=self._original_evidence(rows))
        record = service._existing_import(self.branch, self.product)
        pristine = deepcopy(record.raw_metadata)
        for field in ("original_locator", "request_provenance"):
            record.raw_metadata = deepcopy(pristine)
            old_fingerprint = pristine["latest_response_fingerprint"]
            proof = record.raw_metadata["response_provenance"][0]
            if field == "original_locator":
                proof[field] = None
            else:
                proof[field]["source_sha256"] = "0" * 64
            evidence = {key: value for key, value in proof.items() if key not in {"fingerprint", "ingested_at"}}
            fingerprint = hashlib.sha256(json.dumps(evidence, sort_keys=True, default=str).encode()).hexdigest()
            proof["fingerprint"] = fingerprint
            archive = record.raw_metadata["original_responses"].pop(old_fingerprint)
            archive.update(proof)
            record.raw_metadata["original_responses"][fingerprint] = archive
            record.raw_metadata["latest_response_fingerprint"] = fingerprint
            with self.subTest(field=field):
                self.assertFalse(service._covers_month(record, date(2026, 9, 1)))

    def test_original_duplicate_snapshot_rejects_missing_selected_canonical_row(self):
        from datetime import datetime, timezone as utc_timezone
        from pos_bridge.services.monthly_product_balance_service import _snapshot_canonical_consistency
        original = self._original_rows()
        original.insert(0, deepcopy(original[0]))
        service = AuditStockHistoryService()
        history = service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            original, evidence=self._original_evidence(original))
        record = service._existing_import(self.branch, self.product)
        record.rows.filter(row_number=990).delete()
        record.row_count = 2
        record.save()
        line = SimpleNamespace(branch_id=self.branch.pk, product_id=self.product.pk)
        key = (line.branch_id, line.product_id)
        snapshot = {"source_signature": "independent", "captured_at": "2026-10-04T10:00:00+00:00",
            "last_movement_at": "2026-10-02T18:00:00+00:00", "branch_external_id": str(self.branch.external_id),
            "product_external_id": str(self.product.external_id)}
        proofs = _snapshot_canonical_consistency([line], snapshots={key: (Decimal("0"), snapshot)},
            reconciliations={key: history}, cutoff=datetime(2026, 10, 1, 7, tzinfo=utc_timezone.utc),
            month=date(2026, 9, 1), cache={})
        self.assertFalse(proofs[key]["valid"])

    def test_live_membership_preserves_existing_parsed_integer_ids(self):
        rows = self._original_rows()
        rows[0]["FK_Movimiento"] = str(rows[0]["FK_Movimiento"])
        service = AuditStockHistoryService()
        service._persist_response(self.branch, self.product, date(2026, 9, 1), rows,
            fetched_at="2026-10-04T20:00:00Z", history_limit=500)
        record = service._existing_import(self.branch, self.product)
        self.assertTrue(all(type(value) is int for value in record.raw_metadata["fetched_movement_ids"]))
        self.assertTrue(service._covers_month(record, date(2026, 9, 1)))

    def test_live_promotion_removes_old_original_response_pointer(self):
        rows = self._original_rows()
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1),
            rows, evidence=self._original_evidence(rows))
        record = service._existing_import(self.branch, self.product)
        archives = deepcopy(record.raw_metadata["original_responses"])
        service._persist_response(self.branch, self.product, date(2026, 9, 1), rows,
            fetched_at="2026-10-04T20:00:00Z", history_limit=500)
        record.refresh_from_db()
        self.assertNotIn("latest_response_fingerprint", record.raw_metadata)
        self.assertEqual(record.raw_metadata["original_responses"], archives)

    def test_original_partial_pair_cannot_use_original_summary_count_as_full_response(self):
        pair = self._original_rows()[:2]
        evidence = self._original_evidence(pair, limit=100)
        evidence["fetched_rows"] = 100
        with self.assertRaises(AuditStockHistoryError):
            AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), pair, evidence=evidence)
        self.assertEqual(PointProductHistoryImport.objects.count(), 0)

    def test_original_pre_month_receipt_does_not_claim_later_month_coverage(self):
        rows = self._original_rows()[:2]
        evidence = self._original_evidence(rows, retrieved="2026-09-11T00:00:00Z")
        result = AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        self.assertEqual(result.coverage_status, "INCOMPLETE")
        self.assertIsNone(result.documentary_opening)
        record = AuditStockHistoryService()._existing_import(self.branch, self.product)
        self.assertEqual(record.report_date, date(2026, 9, 10))

    def test_older_original_adds_absent_rows_without_downgrading_latest_metadata(self):
        service = AuditStockHistoryService()
        rows = self._original_rows()
        current = self._original_evidence(rows[1:], limit=5, retrieved="2026-10-04T20:00:00Z")
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows[1:], evidence=current)
        record = service._existing_import(self.branch, self.product)
        authority_before = {key: deepcopy(record.raw_metadata[key]) for key in (
            "fetched_at", "history_limit", "fetched_rows", "earliest_movement_at", "fetched_movement_ids")}
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows))
        record.refresh_from_db()
        self.assertEqual({key: record.raw_metadata[key] for key in authority_before}, authority_before)
        self.assertEqual(record.rows.count(), 3)
        self.assertEqual(len(record.raw_metadata["response_provenance"]), 2)

    def test_older_original_conflict_rolls_back_and_does_not_overwrite_newer_row(self):
        service = AuditStockHistoryService()
        rows = self._original_rows()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows))
        record = service._existing_import(self.branch, self.product)
        before = PointProductHistoryImport.objects.values().get(pk=record.pk)
        old_rows = list(record.rows.values())
        conflicting = [{**rows[0], "Cantidad": 99}, _row(993, "VENTA", "2026-09-15T18:00:00Z", 1, 1, 0)]
        with self.assertRaises(AuditStockHistoryError):
            service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), conflicting,
                evidence=self._original_evidence(conflicting, retrieved="2026-10-03T00:00:00Z"))
        self.assertEqual(PointProductHistoryImport.objects.values().get(pk=record.pk), before)
        self.assertEqual(list(record.rows.values()), old_rows)

    def test_original_conflict_without_per_row_freshness_is_fail_closed(self):
        rows = self._original_rows()
        record = self._stored_history(rows)
        changed = [{**rows[0], "Cantidad": 3}]
        with self.assertRaises(AuditStockHistoryError):
            AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), changed, evidence=self._original_evidence(changed))
        self.assertEqual(record.rows.get(row_number=990).raw_payload, rows[0])

    def test_equal_old_original_cannot_assign_unknown_retained_row_a_false_age(self):
        rows = self._original_rows()[:1]
        record = self._stored_history(rows, metadata={"fetched_at": "2026-10-03T18:00:00Z"})
        service = AuditStockHistoryService()
        same = self._original_evidence(rows, retrieved="2026-09-02T18:00:00Z")
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=same)
        changed = [{**rows[0], "Cantidad": 3}]
        intermediate = self._original_evidence(changed, retrieved="2026-09-03T18:00:00Z")
        with self.assertRaises(AuditStockHistoryError):
            service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), changed, evidence=intermediate)
        self.assertEqual(record.rows.get(row_number=990).raw_payload, rows[0])

    def test_promoted_same_original_does_not_forge_membership_age_of_unknown_retained_row(self):
        rows = self._original_rows()[:1]
        record = self._stored_history(rows)
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows))
        changed = [{**rows[0], "Cantidad": 3}]
        with self.assertRaises(AuditStockHistoryError):
            service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), changed,
                evidence=self._original_evidence(changed, retrieved="2026-10-04T20:00:00Z"))
        self.assertEqual(record.rows.get(row_number=990).raw_payload, rows[0])

    def test_first_original_preserves_known_legacy_members_not_selected_by_response(self):
        rows = self._original_rows()
        record = self._stored_history(rows, metadata={"fetched_rows": 2, "fetched_movement_ids": [990, 992]})
        service = AuditStockHistoryService()
        unrelated = [_row(995, "VENTA", "2026-09-03T18:00:00", 1, 2, 1)]
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), unrelated,
            evidence=self._original_evidence(unrelated, retrieved="2026-09-04T18:00:00Z"))
        record.refresh_from_db()
        self.assertEqual(record.raw_metadata["movement_fetched_at"]["990"], "2026-10-02T07:00:00+00:00")
        self.assertNotIn("991", record.raw_metadata["movement_fetched_at"])
        changed = [{**rows[0], "Cantidad": 3}]
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), changed, evidence=self._original_evidence(changed))
        self.assertEqual(record.rows.get(row_number=990).raw_payload, changed[0])

    def test_newer_original_updates_only_rows_with_known_original_freshness(self):
        service = AuditStockHistoryService()
        rows = self._original_rows()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows))
        changed = [{**rows[1], "Cancelado": True}]
        newer = self._original_evidence(changed, retrieved="2026-10-04T20:00:00Z")
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), changed, evidence=newer)
        record = service._existing_import(self.branch, self.product)
        self.assertEqual(record.rows.get(row_number=991).raw_payload, changed[0])
        self.assertEqual(record.rows.get(row_number=990).raw_payload, rows[0])

    def test_original_ids_are_scoped_to_import_and_not_globally_deduplicated(self):
        rows = self._original_rows()
        service = AuditStockHistoryService()
        evidence = self._original_evidence(rows)
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        other = PointProduct.objects.create(external_id="118", sku="0118", name="Bollo")
        evidence = deepcopy(evidence)
        evidence["product_id"] = other.pk
        evidence["request"]["params"]["pkproducto"] = other.external_id
        service.ingest_original_response(self.branch, other, date(2026, 9, 1), rows, evidence=evidence)
        self.assertEqual(PointProductHistoryImport.objects.count(), 2)
        self.assertEqual(PointProductHistoryRow.objects.count(), 6)

    def test_original_and_capture_lock_canonical_import_before_months_and_row_writes(self):
        rows = self._original_rows()
        with CaptureQueriesContext(connection) as queries:
            AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows))
        sql = [entry["sql"] for entry in queries.captured_queries]
        import_lock = next(i for i, statement in enumerate(sql) if "FOR UPDATE" in statement and PointProductHistoryImport._meta.db_table in statement)
        month_lock = next(i for i, statement in enumerate(sql) if "pg_advisory_xact_lock" in statement)
        row_write = next(i for i, statement in enumerate(sql) if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertLess(import_lock, month_lock)
        self.assertLess(month_lock, row_write)

    def test_original_request_requires_every_exact_native_filter(self):
        rows = self._original_rows()
        for key, value in (("tipo", "true"), ("almacen", "99"), ("pkproducto", "999"),
                           ("movimientos", "500"), ("tipoMovimiento", "3")):
            with self.subTest(key=key):
                evidence = self._original_evidence(rows)
                evidence["request"]["params"][key] = value
                with self.assertRaises(AuditStockHistoryError):
                    AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
                self.assertEqual(PointProductHistoryImport.objects.count(), 0)
        evidence = self._original_evidence(rows, limit=3)
        with self.assertRaises(AuditStockHistoryError):
            AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)

    def test_original_rejects_non_product_object_even_with_matching_keys(self):
        rows = self._original_rows()
        evidence = self._original_evidence(rows)
        impostor = SimpleNamespace(pk=self.product.pk, external_id=self.product.external_id, name=self.product.name)
        with self.assertRaises(AuditStockHistoryError):
            AuditStockHistoryService().ingest_original_response(self.branch, impostor, date(2026, 9, 1), rows, evidence=evidence)
        self.assertEqual(PointProductHistoryImport.objects.count(), 0)

    def test_original_locks_retained_audit_and_empty_closure_months_before_writes(self):
        from recetas.models import ProductoMonthClosure
        from reportes.models import ProductInventoryAuditCase, ProductInventoryAuditRun
        old = _row(1200, "VENTA", "2026-07-01T01:00:00", 1, 2, 1)
        self._stored_history([old])
        ProductoMonthClosure.objects.create(month_start=date(2026, 5, 1), month_end=date(2026, 5, 31))
        run = ProductInventoryAuditRun.objects.create(month=date(2026, 4, 1))
        ProductInventoryAuditCase.objects.create(run=run, month=run.month, branch=self.branch, product=self.product, rebuilt_at=timezone.now(),
            **{field: Decimal("0") for field in ("opening_point", "production", "sales", "waste", "transfer_in", "transfer_out",
                "conversion_in", "conversion_out", "identified_adjustment", "expected_closing", "point_closing", "difference")})
        rows = self._original_rows()
        with CaptureQueriesContext(connection) as queries:
            AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows))
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql) if "pg_advisory_xact_lock" in statement]
        for month in ("202604", "202605", "202606", "202607", "202608", "202609", "202610", "202611"):
            self.assertTrue(any(month in statement for _, statement in locks), month)
        first_write = next(i for i, statement in enumerate(sql) if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))

    def test_canonical_identity_collision_is_rejected_without_hijacking_import(self):
        service = AuditStockHistoryService()
        rows = self._original_rows()
        record = service._canonical_import(self.branch, self.product)
        for corruption in ({"raw_metadata": {"source": "OTHER"}}, {"point_product": None}, {"point_branch": None}):
            with self.subTest(corruption=corruption):
                PointProductHistoryImport.objects.filter(pk=record.pk).update(**corruption)
                before = PointProductHistoryImport.objects.values().get(pk=record.pk)
                with self.assertRaises(AuditStockHistoryError):
                    service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows))
                self.assertEqual(PointProductHistoryImport.objects.values().get(pk=record.pk), before)
                self.assertEqual(PointProductHistoryRow.objects.count(), 0)
                PointProductHistoryImport.objects.filter(pk=record.pk).update(raw_metadata={"source": "POINT_STOCK_HISTORY_API"}, point_product=self.product, point_branch=self.branch)

    def test_full_original_archive_survives_later_real_capture_writer_update(self):
        rows = self._original_rows()
        service = AuditStockHistoryService()
        evidence = self._original_evidence(rows)
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        record = service._existing_import(self.branch, self.product)
        archive_before = deepcopy(record.raw_metadata["original_responses"])
        changed = [{**rows[1], "Cancelado": True}]
        AuditStockHistoryService(client=_FakePointClient(changed)).capture(self.branch, self.product, date(2026, 9, 1), force=True)
        record.refresh_from_db()
        self.assertEqual(record.raw_metadata["original_responses"], archive_before)
        archived = next(iter(archive_before.values()))
        raw = zlib.decompress(base64.b64decode(archived["raw_zlib_base64"]))
        self.assertEqual(hashlib.sha256(raw).hexdigest(), evidence["raw_sha256"])
        self.assertEqual(json.loads(raw), rows)
        self.assertTrue(record.rows.get(row_number=991).cancelled)

    def test_original_replay_rejects_tampered_archive_without_writing(self):
        rows = self._original_rows()
        evidence = self._original_evidence(rows)
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        record = service._existing_import(self.branch, self.product)
        next(iter(record.raw_metadata["original_responses"].values()))["raw_zlib_base64"] = "broken"
        record.save()
        before = PointProductHistoryImport.objects.values().get(pk=record.pk)
        with self.assertRaises(AuditStockHistoryError):
            service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        self.assertEqual(PointProductHistoryImport.objects.values().get(pk=record.pk), before)

    def test_original_replay_rejects_extra_proof_fields_instead_of_duplicating_archive(self):
        rows = self._original_rows()
        evidence = self._original_evidence(rows)
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        record = service._existing_import(self.branch, self.product)
        before = PointProductHistoryImport.objects.values().get(pk=record.pk)
        extra = {**evidence, "harmless": "unvalidated label"}
        with self.assertRaises(AuditStockHistoryError):
            service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=extra)
        self.assertEqual(PointProductHistoryImport.objects.values().get(pk=record.pk), before)

    def test_original_replay_checks_archived_acquisition_code_and_identity(self):
        rows = self._original_rows()
        evidence = self._original_evidence(rows)
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        record = service._existing_import(self.branch, self.product)
        original = deepcopy(record.raw_metadata)
        for field, value in (("domain", "INSUMO"), ("fetched_rows", 100),
                ("request_provenance", {**evidence["request_provenance"], "source_code": "corrupt code"})):
            with self.subTest(field=field):
                record.raw_metadata = deepcopy(original)
                next(iter(record.raw_metadata["original_responses"].values()))[field] = value
                record.save()
                before = PointProductHistoryImport.objects.values().get(pk=record.pk)
                with self.assertRaises(AuditStockHistoryError):
                    service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
                self.assertEqual(PointProductHistoryImport.objects.values().get(pk=record.pk), before)

    def test_capture_provenance_marks_live_http_and_receipt_precedes_persistence(self):
        rows = self._original_rows()
        receipt = timezone.datetime(2026, 10, 4, 18, tzinfo=timezone.get_fixed_timezone(0))
        later = timezone.datetime(2026, 10, 4, 20, tzinfo=timezone.get_fixed_timezone(0))
        first_receipt = iter([receipt])
        with patch("pos_bridge.services.audit_stock_history_service.timezone.now", side_effect=lambda: next(first_receipt, later)):
            AuditStockHistoryService(client=_FakePointClient(rows)).capture(self.branch, self.product, date(2026, 9, 1), force=True)
        record = AuditStockHistoryService()._existing_import(self.branch, self.product)
        self.assertEqual(record.raw_metadata["fetched_at"], receipt.isoformat())
        self.assertEqual(record.raw_metadata["response_provenance"][0]["request_provenance"]["kind"], "LIVE_HTTP")

    def test_saturated_original_after_start_is_incomplete_even_with_zero_difference(self):
        rows = [_row(1100 + i, "VENTA", f"2026-09-{i + 2:02d}T18:00:00", 1, 5 - i, 4 - i) for i in range(5)]
        result = AuditStockHistoryService().ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=self._original_evidence(rows, limit=5))
        self.assertEqual(result.coverage_status, "INCOMPLETE")
        self.assertIsNone(result.documentary_opening)

    def _stored_history(self, rows, *, metadata=None):
        service = AuditStockHistoryService()
        record = service._canonical_import(self.branch, self.product)
        record.raw_metadata = {
            "source": "POINT_STOCK_HISTORY_API", "fetched_rows": len(rows),
            "history_limit": 500, "fetched_at": "2026-10-02T07:00:00+00:00",
            **(metadata or {}),
        }
        record.save()
        for raw in rows:
            movement_id, defaults = service._parse_row(raw)
            PointProductHistoryRow.objects.create(
                import_record=record, row_number=movement_id, **defaults,
            )
        return record

    def test_raw_utc_month_cut_is_read_only_and_two_queries(self):
        record = self._stored_history([
            _row(910, "VENTA", "2026-09-01T01:00:00", 9, 10, 1),
            _row(911, "VENTA", "2026-10-01T01:00:00", 2, 2, 0),
            _row(912, "VENTA", "2026-10-01T07:00:00", 3, 3, 0),
        ])
        original = list(record.rows.values())
        line = SimpleNamespace(branch=self.branch, product=self.product, difference=1)
        with self.assertNumQueries(2):
            result = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        history = result[(self.branch.pk, self.product.pk)]
        self.assertEqual(history.sales, Decimal("2"))
        self.assertEqual(history.movement_ids, (911,))
        self.assertEqual(history.coverage_status, "COMPLETE")
        self.assertEqual(list(record.rows.values()), original)
        self.assertEqual(AuditStockHistoryService().reconcile(
            self.branch, self.product, date(2026, 9, 1)), history)

    def test_truncated_coverage_uses_raw_of_exact_last_capture_boundary(self):
        record = self._stored_history([
            _row(920, "VENTA", "2026-09-01T06:00:00", 1, 2, 1),
        ], metadata={
            "fetched_rows": 1, "history_limit": 1,
            "earliest_movement_at": "2026-09-01T13:00:00+00:00",
            "fetched_movement_ids": [920],
        })
        line = SimpleNamespace(branch=self.branch, product=self.product, difference=1)
        with self.assertNumQueries(2):
            history = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        self.assertEqual(history[(self.branch.pk, self.product.pk)].coverage_status, "COMPLETE")
        # An old retained row cannot grant coverage to a newer truncated capture.
        movement_id, defaults = AuditStockHistoryService._parse_row(
            _row(921, "VENTA", "2026-09-02T06:00:00", 1, 2, 1))
        PointProductHistoryRow.objects.create(import_record=record, row_number=movement_id, **defaults)
        record.raw_metadata.update({
            "earliest_movement_at": "2026-09-02T13:00:00+00:00", "fetched_movement_ids": [921],
        })
        record.save()
        with self.assertNumQueries(2):
            history = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        self.assertEqual(history[(self.branch.pk, self.product.pk)].coverage_status, "INCOMPLETE")

    def test_explicit_offset_boundary_is_not_shifted_again(self):
        self._stored_history([
            _row(922, "VENTA", "2026-09-01T08:00:00+00:00", 1, 2, 1),
        ], metadata={"fetched_rows": 500, "earliest_movement_at": "2026-09-01T08:00:00+00:00"})
        result = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertEqual(result.coverage_status, "INCOMPLETE")
        self.assertEqual(result.sales, Decimal("1"))

    def test_malformed_raw_date_cannot_use_persisted_date_as_authority(self):
        record = self._stored_history([_row(923, "VENTA", "2026-09-15T12:00:00", 1, 2, 1)])
        PointProductHistoryRow.objects.filter(import_record=record).update(raw_payload={"Fecha": "invalid"})
        history = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertEqual(history.coverage_status, "INCOMPLETE")
        self.assertEqual(history.sales, Decimal("0"))
        self.assertEqual(history.unknown_movement_ids, (923,))

    def test_capture_locks_effective_and_legacy_month_before_row_write(self):
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(924, "VENTA", "2026-10-01T01:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 9, 1))
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql) if "pg_advisory_xact_lock" in statement]
        self.assertTrue(any("202609" in statement for _, statement in locks))
        self.assertTrue(any("202610" in statement for _, statement in locks))
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))

    def test_capture_also_locks_previous_months_of_an_updated_identity(self):
        record = self._stored_history([
            _row(925, "VENTA", "2026-08-01T01:00:00", 1, 2, 1),
        ])
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(925, "VENTA", "2026-10-01T01:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 9, 1), force=True)
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql) if "pg_advisory_xact_lock" in statement]
        for month in ("202607", "202608", "202609", "202610"):
            self.assertTrue(any(month in statement for _, statement in locks))
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'UPDATE "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))
        self.assertEqual(record.rows.count(), 1)

    def test_truncated_boundary_outside_window_is_prefetched_in_two_queries(self):
        self._stored_history([
            _row(926, "VENTA", "2026-08-01T01:00:00", 1, 2, 1),
            _row(927, "VENTA", "2026-09-10T01:00:00", 1, 2, 1),
        ], metadata={
            "fetched_rows": 2, "history_limit": 2,
            "earliest_movement_at": "2026-08-01T08:00:00+00:00",
            "fetched_movement_ids": [926, 927],
        })
        line = SimpleNamespace(branch=self.branch, product=self.product, difference=1)
        with self.assertNumQueries(2):
            result = AuditStockHistoryService().reconcile_many([line], date(2026, 9, 1))
        history = result[(self.branch.pk, self.product.pk)]
        self.assertEqual(history.coverage_status, "COMPLETE")
        self.assertEqual(history.movement_ids, (927,))

    def test_recapture_metadata_locks_retained_and_empty_months_before_writes(self):
        record = self._stored_history([
            _row(940, "VENTA", "2026-07-15T12:00:00", 1, 2, 1),
        ])
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(1000 + i, "VENTA", "2026-10-03T12:00:00", 1, 2, 1)
            for i in range(500)
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 10, 1), force=True)
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql)
                 if "pg_advisory_xact_lock" in statement]
        for month in ("202607", "202608", "202609", "202610", "202611"):
            self.assertTrue(any(month in statement for _, statement in locks), month)
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))
        self.assertEqual(record.rows.count(), 501)

    def test_recapture_locks_existing_closure_without_history_rows_and_its_successor(self):
        from recetas.models import ProductoMonthClosure
        ProductoMonthClosure.objects.create(
            month_start=date(2026, 5, 1), month_end=date(2026, 5, 31),
        )
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(950, "VENTA", "2026-10-03T12:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 10, 1))
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql)
                 if "pg_advisory_xact_lock" in statement]
        for month in ("202605", "202606"):
            self.assertTrue(any(month in statement for _, statement in locks), month)
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))

    def test_recapture_locks_historical_closing_source_without_case_or_month_closure(self):
        from pos_bridge.models import PointHistoricalInventoryClosing, PointHistoricalInventoryClosingLine
        closing = PointHistoricalInventoryClosing.objects.create(
            operational_date=date(2026, 5, 31), source="POINT_STOCK_HISTORY",
            source_fingerprint="historical-closing-before-first-audit",
        )
        PointHistoricalInventoryClosingLine.objects.create(
            closing=closing, branch=self.branch, product=self.product, stock=1,
        )
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(951, "VENTA", "2026-10-03T12:00:00", 1, 2, 1),
        ]))
        with CaptureQueriesContext(connection) as queries:
            service.capture(self.branch, self.product, date(2026, 10, 1))
        sql = [entry["sql"] for entry in queries.captured_queries]
        locks = [(i, statement) for i, statement in enumerate(sql)
                 if "pg_advisory_xact_lock" in statement]
        for month in ("202605", "202606"):
            self.assertTrue(any(month in statement for _, statement in locks), month)
        first_write = next(i for i, statement in enumerate(sql)
                           if statement.startswith(f'INSERT INTO "{PointProductHistoryRow._meta.db_table}"'))
        self.assertTrue(all(i < first_write for i, _ in locks))

    def test_complete_continuous_utc_history_exposes_documentary_boundaries(self):
        self._stored_history([
            _row(930, "VENTA", "2026-09-01T08:00:00", 1, 4, 3),
            _row(931, "VENTA", "2026-10-01T06:00:00", 2, 3, 1),
        ])
        history = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertEqual(getattr(history, "documentary_opening", None), Decimal("4"))
        self.assertEqual(getattr(history, "documentary_closing", None), Decimal("1"))
        self.assertEqual(getattr(history, "documentary_boundary_movement_ids", ()), (930, 931))
        serialized = history.as_dict(opening=Decimal("4"), point_closing=Decimal("1"))
        self.assertEqual(serialized.get("documentary_opening"), "4.000")
        self.assertEqual(serialized.get("documentary_closing"), "1.000")
        self.assertEqual(serialized.get("documentary_boundary_movement_ids"), [930, 931])

    def test_documentary_boundaries_require_complete_known_continuous_rows(self):
        scenarios = (
            ([], {}),
            ([_row(932, "VENTA", "2026-09-10T08:00:00", 1, 4, 3)],
             {"fetched_at": "2026-10-01T01:00:00+00:00"}),
            ([_row(932, "VENTA", "2026-09-10T08:00:00", 1, 4, 3),
              _row(933, "VENTA", "2026-09-11T08:00:00", 1, 5, 4)], {}),
            ([_row(932, "UNKNOWN", "2026-09-10T08:00:00", 1, 4, 3)], {}),
            ([_row(932, "VENTA", "2026-09-10T08:00:00", 1, 4, 2)], {}),
        )
        for rows, metadata in scenarios:
            with self.subTest(rows=rows, metadata=metadata):
                PointProductHistoryImport.objects.all().delete()
                self._stored_history(rows, metadata=metadata)
                history = AuditStockHistoryService().reconcile(self.branch, self.product, date(2026, 9, 1))
                self.assertIsNone(getattr(history, "documentary_opening", None))
                self.assertIsNone(getattr(history, "documentary_closing", None))
                self.assertEqual(getattr(history, "documentary_boundary_movement_ids", ()), ())

    def test_zero_difference_trace_case_can_review_cached_history(self):
        service = AuditStockHistoryService(client=_FakePointClient([
            _row(900, 'ENTRADA POR PRODUCCIÓN', '2026-08-10T10:00:00-07:00', 1, 0, 1)]))
        service.capture(self.branch, self.product, self.month)
        case = SimpleNamespace(branch=self.branch, product=self.product, difference=Decimal('0'))
        result = AuditStockHistoryService().reconcile_many([case], self.month, include_zero_difference=True)
        self.assertEqual(result[(self.branch.pk, self.product.pk)].production, Decimal('1'))

    def setUp(self):
        self.month = date(2026, 8, 1)
        self.branch = PointBranch.objects.create(external_id="8", name="CEDIS")
        self.product = PointProduct.objects.create(
            external_id="109",
            sku="0109",
            name="Pastel de 3 Pecados Chico",
        )

    def test_cache_must_have_been_fetched_after_the_month_finished(self):
        metadata = {"fetched_rows": 2, "history_limit": 500}
        record = SimpleNamespace(raw_metadata=metadata)
        for fetched_at in (None, "invalid", "2026-09-30T23:59:59-07:00", "2026-10-01T06:59:59+00:00", "2026-10-01T07:00:00"):
            with self.subTest(fetched_at=fetched_at):
                metadata["fetched_at"] = fetched_at
                self.assertFalse(AuditStockHistoryService._covers_month(record, date(2026, 9, 1)))
        metadata["fetched_at"] = "2026-10-01T07:00:00+00:00"
        self.assertTrue(AuditStockHistoryService._covers_month(record, date(2026, 9, 1)))

    def test_impossible_capture_counts_never_grant_month_coverage(self):
        for fetched_rows, history_limit in ((-1, 500), (1, 0), (1, -1), (501, 500)):
            with self.subTest(fetched_rows=fetched_rows, history_limit=history_limit):
                record = SimpleNamespace(raw_metadata={
                    "fetched_rows": fetched_rows, "history_limit": history_limit,
                    "fetched_at": "2026-10-02T07:00:00+00:00",
                    "earliest_movement_at": "2026-08-01T08:00:00+00:00",
                })
                self.assertFalse(AuditStockHistoryService._covers_month(
                    record, date(2026, 9, 1), boundary_rows=[]))

    def test_present_batch_membership_is_strict_and_never_crashes_saturated_coverage(self):
        metadata = {"fetched_rows": 2, "history_limit": 2,
                    "fetched_at": "2026-10-02T07:00:00+00:00",
                    "earliest_movement_at": "2026-08-01T08:00:00+00:00"}
        record = SimpleNamespace(raw_metadata=metadata)
        row = SimpleNamespace(row_number=910,
            movement_at=timezone.datetime.fromisoformat(metadata["earliest_movement_at"]),
            raw_payload=_row(910, "VENTA", "2026-08-01T08:00:00Z", 1, 1, 0))
        for limit in (2, 500):
            metadata["history_limit"] = limit
            for ids in ("invented", None, True, [True, 911], [910, 910],
                        [0, 911], [-1, 911], [910.0, 911], ["910", 911], [910], []):
                with self.subTest(limit=limit, ids=ids):
                    metadata["fetched_movement_ids"] = ids
                    with self.assertNumQueries(0):
                        self.assertFalse(AuditStockHistoryService._covers_month(
                            record, date(2026, 9, 1), boundary_rows=[row]))
            metadata["fetched_movement_ids"] = [910, 911]
            with self.assertNumQueries(0):
                self.assertTrue(AuditStockHistoryService._covers_month(
                    record, date(2026, 9, 1), boundary_rows=[row]))
            metadata.pop("fetched_movement_ids")
            with self.assertNumQueries(0):
                self.assertTrue(AuditStockHistoryService._covers_month(
                    record, date(2026, 9, 1), boundary_rows=[row]))

    def test_adjustment_uses_verified_stock_effect_not_raw_quantity_sign(self):
        scenarios = (
            ("AJUSTE SALIDA INVENTARIO", 10, 20, 10, -10),
            ("AJUSTE SALIDA INVENTARIO", -10, 20, 10, -10),
            ("AJUSTE ENTRADA INVENTARIO", 10, 10, 20, 10),
            ("AJUSTE INVENTARIO", -10, 20, 10, -10),
            ("AJUSTE INVENTARIO", 10, 10, 20, 10),
            ("AJUSTE SALIDA INVENTARIO", 10, 20, 11, None),
            ("AJUSTE SALIDA INVENTARIO", 10, 10, 20, None),
            ("AJUSTE ENTRADA INVENTARIO", 10, 20, 10, None),
            ("AJUSTE SALIDA INVENTARIO", 10, 20, 20, None),
        )
        service = AuditStockHistoryService()
        record = SimpleNamespace(raw_metadata={})
        for movement, quantity, previous, new, expected in scenarios:
            with self.subTest(movement=movement, quantity=quantity, new=new):
                row = SimpleNamespace(
                    row_number=901, movement_type=movement,
                    quantity=Decimal(quantity), previous_existence=Decimal(previous),
                    new_existence=Decimal(new),
                )
                result = service._reconcile_record(record, self.month, [row])
                if expected is None:
                    self.assertEqual(result.identified_adjustment, Decimal("0"))
                    self.assertEqual(result.unknown_movement_ids, (901,))
                    self.assertNotIn("identified_adjustment", result.movement_ids_by_category)
                else:
                    self.assertEqual(result.identified_adjustment, Decimal(expected))
                    self.assertEqual(result.expected_closing(Decimal(previous)), Decimal(new))
                    self.assertEqual(result.unknown_movement_ids, ())
                    self.assertEqual(result.movement_ids_by_category["identified_adjustment"], (901,))

    def test_repeated_capture_upserts_the_same_point_movements(self):
        client = _FakePointClient(
            [
                _row(
                    101,
                    "ENTRADA POR PRODUCCIÓN",
                    "2026-08-02T08:00:00-07:00",
                    2,
                    23,
                    25,
                )
            ]
        )
        service = AuditStockHistoryService(client=client)

        first = service.capture(self.branch, self.product, self.month)
        second = service.capture(self.branch, self.product, self.month, force=True)

        self.assertEqual(PointProductHistoryImport.objects.count(), 1)
        self.assertEqual(PointProductHistoryRow.objects.count(), 1)
        self.assertEqual(first.movement_ids, second.movement_ids)
        self.assertEqual(client.history_calls, [("109", "8", 500), ("109", "8", 500)])

    def test_sale_cancellation_reduces_net_sales_when_stock_is_returned(self):
        rows = [
            _row(301, "VENTA", "2026-08-20T10:00:00-07:00", 1, 2, 1),
            _row(302, "CANCELACION VENTA", "2026-08-20T11:00:00-07:00", 1, 1, 2),
        ]
        result = AuditStockHistoryService(client=_FakePointClient(rows)).capture(self.branch, self.product, self.month)
        self.assertEqual(result.sales, Decimal("0"))
        self.assertEqual(result.expected_closing(Decimal("2")), Decimal("2"))
        self.assertEqual(result.movement_ids_by_category["sales"], (301, 302))
        self.assertEqual(result.unknown_movement_ids, ())

    def _waste_reversal_rows(self, *, quantity=4, previous=4):
        debit = _row(1680267, "MERMA", "2026-09-10T18:00:00Z", quantity,
                     previous, previous - quantity, cancelled=True)
        undo = _row(1680271, "CANCELACION DE MERMA", "2026-09-10T19:00:00Z", quantity,
                    previous - quantity, previous)
        debit.update(FK_Tipo_Movimiento=5, isCargo=True)
        undo.update(FK_Tipo_Movimiento=15, isCargo=False)
        return [debit, undo]

    def test_historical_waste_reversal_preserves_both_scoped_events_and_originals(self):
        service = AuditStockHistoryService()
        month = date(2026, 9, 1)
        for quantity, previous in ((4, 4), (1, 10)):
            product = PointProduct.objects.create(external_id=f"waste-{quantity}", name="Scoped waste")
            rows = self._waste_reversal_rows(quantity=quantity, previous=previous)
            for raw in rows:
                raw.update(FK_Producto=product.external_id, FK_Sucursal=self.branch.external_id, isInsumo=False)
            evidence = self._original_evidence(rows, limit=5)
            evidence["product_id"] = product.pk
            evidence["request"]["params"]["pkproducto"] = product.external_id
            service.ingest_original_response(self.branch, product, month, rows, evidence=evidence)
            record = service._existing_import(self.branch, product)
            originals, metadata = list(record.rows.values()), deepcopy(record.raw_metadata)
            with patch("requests.Session.request", side_effect=AssertionError("HTTP prohibited")):
                result = service.reconcile(self.branch, product, month)
                with self.assertNumQueries(2):
                    again = service.reconcile_many([SimpleNamespace(branch=self.branch, product=product, difference=1)], month)
            self.assertEqual(result.waste, Decimal("0"))
            self.assertEqual(result.movement_ids_by_category.get("waste"), (1680267, 1680271))
            self.assertEqual(result.unknown_movement_ids, ())
            self.assertEqual(result.documentary_opening, Decimal(previous))
            self.assertEqual(result.documentary_closing, Decimal(previous))
            self.assertEqual(again[(self.branch.pk, product.pk)], result)
            record.refresh_from_db()
            self.assertEqual(record.raw_metadata, metadata)
            self.assertEqual(list(record.rows.values()), originals)

    def test_waste_reversal_effects_use_each_original_month_without_pairing(self):
        rows = self._waste_reversal_rows(quantity=1, previous=10)
        rows[0]["Fecha"] = "2026-09-01T06:59:59Z"
        rows[1]["Fecha"] = "2026-09-01T07:00:00Z"
        service = AuditStockHistoryService()
        evidence = self._original_evidence(rows, limit=5)
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows, evidence=evidence)
        august = service.reconcile(self.branch, self.product, date(2026, 8, 1))
        september = service.reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertEqual(august.waste, Decimal("1"))
        self.assertEqual(september.waste, Decimal("-1"))
        self.assertEqual(august.documentary_closing, Decimal("9"))
        self.assertEqual(september.documentary_opening, Decimal("9"))

    def test_malformed_cancelled_waste_is_unknown_not_silently_dropped(self):
        rows = self._waste_reversal_rows()
        rows[0]["isCargo"] = False
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows,
                                         evidence=self._original_evidence(rows, limit=5))
        result = service.reconcile(self.branch, self.product, date(2026, 9, 1))
        self.assertIn(1680267, result.unknown_movement_ids)
        self.assertIsNone(result.documentary_closing)

    def test_raw_mutation_cannot_hide_canonical_historical_waste_debit(self):
        rows = self._waste_reversal_rows()
        service = AuditStockHistoryService()
        service.ingest_original_response(self.branch, self.product, date(2026, 9, 1), rows,
                                         evidence=self._original_evidence(rows, limit=5))
        row = service._existing_import(self.branch, self.product).rows.get(row_number=1680267)
        original = deepcopy(row.raw_payload)
        for raw in ({**original, "Cancelado": False}, None, {},
                    {**original, "FK_Producto": "wrong"}, {**original, "FK_Sucursal": "wrong"},
                    {**original, "Costo_Unitario": 1}, {**original, "FK_Movimiento": 123},
                    {**original, "FK_Movimiento": 1680267.9}, {**original, "FK_Movimiento": "１６８０２６７"}):
            with self.subTest(raw=raw):
                # JSONField disallows SQL NULL; a missing payload is represented
                # here by an empty documentary object, not a second import.
                row.raw_payload = raw if raw is not None else {}
                row.save(update_fields=["raw_payload"])
                result = service.reconcile(self.branch, self.product, date(2026, 9, 1))
                self.assertIn(1680267, result.unknown_movement_ids)

    def test_unproven_cancellation_remains_unknown(self):
        for movement, previous, new in (("CANCELACION VENTA", 1, 0), ("CANCELACION VENTA", 1, 3), ("CANCELACION TRANSFERENCIA", 1, 2)):
            with self.subTest(movement=movement, previous=previous, new=new):
                result = AuditStockHistoryService(client=_FakePointClient([
                    _row(303, movement, "2026-08-20T11:00:00-07:00", 1, previous, new),
                ])).capture(self.branch, self.product, self.month, force=True)
                self.assertEqual(result.unknown_movement_ids, (303,))

    def test_complete_cached_history_avoids_a_second_point_call(self):
        client = _FakePointClient(
            [
                _row(
                    102,
                    "SALIDA POR CONVERSIÓN",
                    "2026-07-31T23:00:00-07:00",
                    -1,
                    24,
                    23,
                ),
                _row(
                    103,
                    "ENTRADA POR CONVERSIÓN",
                    "2026-08-14T10:00:00-07:00",
                    2,
                    4,
                    6,
                ),
            ]
        )
        service = AuditStockHistoryService(client=client)

        service.capture(self.branch, self.product, self.month)
        service.capture(self.branch, self.product, self.month)

        self.assertEqual(len(client.history_calls), 1)

    def test_reconciles_three_sins_and_ignores_cancelled_rows(self):
        rows = [
            _row(1, "ENTRADA POR PRODUCCIÓN", "2026-08-02T08:00:00-07:00", 518, 23, 541),
            _row(2, "ENTRADA POR CONVERSIÓN", "2026-08-14T10:00:00-07:00", 2, 541, 543),
            _row(3, "AJUSTE ENTRADA INVENTARIO", "2026-08-15T10:00:00-07:00", 4, 543, 547),
            _row(4, "RETORNO POR TRANSFERENCIA", "2026-08-16T10:00:00-07:00", 2, 547, 549),
            _row(5, "SALIDA POR CONVERSIÓN", "2026-08-20T10:00:00-07:00", 10, 549, 539),
            _row(6, "SALIDA POR TRANSFERENCIA", "2026-08-31T20:00:00-07:00", 533, 539, 6),
            _row(
                7,
                "SALIDA POR CONVERSIÓN",
                "2026-08-31T21:00:00-07:00",
                -99,
                6,
                -93,
                cancelled=True,
            ),
        ]
        rows[4]["Cancelado"] = "false"
        result = AuditStockHistoryService(client=_FakePointClient(rows)).capture(
            self.branch,
            self.product,
            self.month,
        )

        self.assertEqual(result.coverage_status, "COMPLETE")
        self.assertEqual(result.production, Decimal("518"))
        self.assertEqual(result.conversion_in, Decimal("2"))
        self.assertEqual(result.conversion_out, Decimal("10"))
        self.assertEqual(result.transfer_in, Decimal("2"))
        self.assertEqual(result.transfer_out, Decimal("533"))
        self.assertEqual(result.identified_adjustment, Decimal("4"))
        self.assertEqual(result.expected_closing(Decimal("23")), Decimal("6"))
        self.assertEqual(result.unexplained_remainder(Decimal("23"), Decimal("6")), Decimal("0"))
        self.assertNotIn(7, result.movement_ids)

    def test_history_at_limit_that_does_not_reach_month_start_is_incomplete(self):
        rows = [
            _row(
                movement_id,
                "SALIDA POR TRANSFERENCIA",
                f"2026-08-{(movement_id % 20) + 2:02d}T10:00:00-07:00",
                -1,
                1000 - movement_id,
                999 - movement_id,
            )
            for movement_id in range(1, 501)
        ]

        result = AuditStockHistoryService(client=_FakePointClient(rows)).capture(
            self.branch,
            self.product,
            self.month,
        )

        self.assertEqual(result.coverage_status, "INCOMPLETE")

    def test_reconcile_many_loads_all_cached_histories_in_two_queries(self):
        second_product = PointProduct.objects.create(
            external_id="110",
            sku="0110",
            name="Segundo producto",
        )
        client = _FakePointClient(
            [
                _row(
                    201,
                    "AJUSTE ENTRADA INVENTARIO",
                    "2026-08-10T10:00:00-07:00",
                    1,
                    0,
                    1,
                )
            ]
        )
        service = AuditStockHistoryService(client=client)
        service.capture(self.branch, self.product, self.month)
        service.capture(self.branch, second_product, self.month, force=True)
        lines = [
            SimpleNamespace(
                branch=self.branch,
                product=self.product,
                difference=Decimal("-1"),
            ),
            SimpleNamespace(
                branch=self.branch,
                product=second_product,
                difference=Decimal("-1"),
            ),
        ]

        with self.assertNumQueries(2):
            reconciliations = AuditStockHistoryService().reconcile_many(
                lines,
                self.month,
            )

        self.assertEqual(len(reconciliations), 2)
