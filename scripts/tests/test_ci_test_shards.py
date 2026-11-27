import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.ci_test_runner import ShardedDiscoverRunner, TimedTestSuite
from scripts.ci_test_shards import partition_tests, verify_manifests


def cases(name, count):
    cls = type(name, (unittest.TestCase,), {f"test_{i}": lambda self: None for i in range(count)})
    return list(unittest.defaultTestLoader.loadTestsFromTestCase(cls))


class ShardTests(unittest.TestCase):
    def test_complete_unique_deterministic_balanced_and_preserves_class_order(self):
        tests = cases("Large", 7) + cases("Medium", 4) + cases("Small", 3) + cases("Tiny", 2)
        shards = partition_tests(tests, 3)
        self.assertEqual(sorted(t.id() for shard in shards for t in shard), sorted(t.id() for t in tests))
        self.assertEqual([len(s) for s in shards], [7, 4, 5])
        reverse_shards = partition_tests(list(reversed(tests)), 3)
        for shard, reversed_shard in zip(shards, reverse_shards):
            self.assertEqual(shard, list(reversed(reversed_shard)))
        for cls in {type(t) for t in tests}:
            self.assertEqual(sum(any(type(t) is cls for t in s) for s in shards), 1)

    def test_new_classes_automatically_included(self):
        tests = cases("Existing", 2) + cases("New", 3)
        self.assertEqual(sum(map(len, partition_tests(tests, 4))), 5)

    def test_invalid_count(self):
        with self.assertRaises(ValueError):
            partition_tests([], 0)

    def test_manifest_rejects_missing_duplicate_omitted_and_different_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / f"{i}.json" for i in range(2)]
            baseline = [dict(index=i, count=2, discovered=["a", "b"], selected=[test])
                        for i, test in enumerate(["a", "b"])]
            def save(items):
                for path, item in zip(paths, items):
                    path.write_text(json.dumps(item))
            save(baseline)
            verify_manifests(paths, 2)
            with self.assertRaises(ValueError):
                verify_manifests(paths[:1], 2)
            for change in ({"index": 0}, {"selected": []}, {"selected": ["a"]},
                           {"discovered": ["a", "c"]}, {"count": 3}):
                with self.subTest(change=change):
                    save([baseline[0], baseline[1] | change])
                    with self.assertRaises(ValueError):
                        verify_manifests(paths, 2)

    def test_runner_uses_django_discovery_and_records_full_inventory(self):
        tests = cases("First", 2) + cases("Second", 1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            runner = ShardedDiscoverRunner(shard_count=2, shard_index=1, shard_manifest=path, verbosity=0)
            with patch("django.test.runner.DiscoverRunner.build_suite", return_value=unittest.TestSuite(tests)):
                selected = list(runner.build_suite())
            manifest = json.loads(path.read_text())
            self.assertEqual(manifest["discovered"], sorted(t.id() for t in tests))
            self.assertEqual(manifest["selected"], [t.id() for t in selected])
            self.assertEqual(len(selected), 1)
            with self.assertRaises(ValueError):
                runner.build_suite(["one.app"])

    def test_runner_rejects_nested_parallelism_and_invalid_index(self):
        for options in ({"shard_index": 2}, {"parallel": 2}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                ShardedDiscoverRunner(**(dict(shard_count=2, shard_index=0, shard_manifest="unused") | options))


class TimingTests(unittest.TestCase):
    def test_class_timing_includes_fixtures_and_keeps_failure_and_skip_results(self):
        events = []
        class Example(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                events.append("setup")
                cls.addClassCleanup(events.append, "cleanup")
            @classmethod
            def tearDownClass(cls):
                events.append("teardown")
            def test_failure(self):
                self.fail("expected failure")
            @unittest.skip("expected skip")
            def test_skip(self):
                pass
        suite = TimedTestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(Example))
        result = suite.run(unittest.TestResult())
        self.assertEqual(result.testsRun, 2)
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(len(result.skipped), 1)
        self.assertEqual(events, ["setup", "teardown", "cleanup"])
        name = f"{Example.__module__}.{Example.__qualname__}"
        self.assertGreater(suite.class_seconds[name], 0)
        self.assertGreater(suite.fixture_seconds[name]["setup"], 0)
        self.assertGreater(suite.fixture_seconds[name]["teardown"], 0)

    def test_failed_class_setup_still_reports_time_and_original_error(self):
        class Broken(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                raise RuntimeError("fixture failure")
            def test_unreachable(self):
                self.fail("must not run")
        suite = TimedTestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(Broken))
        result = suite.run(unittest.TestResult())
        self.assertEqual(result.testsRun, 0)
        self.assertEqual(len(result.errors), 1)
        self.assertIn("fixture failure", result.errors[0][1])
        self.assertEqual(len(suite.class_seconds), 1)

    def test_runner_persists_timings_and_real_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(json.dumps({"selected": ["preserved"]}))
            runner = ShardedDiscoverRunner(shard_count=1, shard_index=0, shard_manifest=path, verbosity=0)
            result = runner.run_suite(runner.test_suite(cases("Profiled", 2)))
            manifest = json.loads(path.read_text())
            self.assertTrue(result.wasSuccessful())
            self.assertEqual(manifest["selected"], ["preserved"])
            self.assertEqual(manifest["outcomes"]["run"], 2)
            self.assertEqual(len(manifest["class_seconds"]), 1)
            self.assertEqual(len(manifest["test_seconds"]), 2)
