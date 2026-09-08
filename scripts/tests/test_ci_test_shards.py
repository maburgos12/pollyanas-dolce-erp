import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.ci_test_runner import ShardedDiscoverRunner
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
