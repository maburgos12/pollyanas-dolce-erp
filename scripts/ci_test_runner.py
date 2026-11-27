"""Explicit CI-only runner; application settings and local default stay unchanged."""
import json
import unittest
from time import perf_counter
from pathlib import Path

from django.test.runner import DiscoverRunner, iter_test_cases

from scripts.ci_test_shards import partition_tests


class TimedTestSuite(unittest.TestSuite):
    """Observe class spans including Django pre/post teardown and class cleanups.

    Keep unittest's fixture/error handling intact. These two hooks are exercised
    against setup failure, skips and failures; CI fixes Python to 3.12.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_seconds = {}
        self.fixture_seconds = {}
        self._class_started = {}

    @staticmethod
    def _name(cls):
        return f"{cls.__module__}.{cls.__qualname__}"

    def _handleClassSetUp(self, test, result):
        cls = type(test)
        if cls == getattr(result, "_previousTestClass", None):
            return super()._handleClassSetUp(test, result)
        name = self._name(cls)
        started = perf_counter()
        self._class_started[name] = started
        try:
            return super()._handleClassSetUp(test, result)
        finally:
            self.fixture_seconds.setdefault(name, {})["setup"] = perf_counter() - started

    def _tearDownPreviousClass(self, test, result):
        cls = getattr(result, "_previousTestClass", None)
        if cls is None or cls == type(test):
            return super()._tearDownPreviousClass(test, result)
        name = self._name(cls)
        started = perf_counter()
        try:
            return super()._tearDownPreviousClass(test, result)
        finally:
            finished = perf_counter()
            if name in self._class_started:
                self.fixture_seconds.setdefault(name, {})["teardown"] = finished - started
                self.class_seconds[name] = self.class_seconds.get(name, 0) + finished - self._class_started.pop(name)


class ShardedDiscoverRunner(DiscoverRunner):
    test_suite = TimedTestSuite

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument("--shard-count", type=int, required=True)
        parser.add_argument("--shard-index", type=int, required=True)
        parser.add_argument("--shard-manifest", required=True)

    def __init__(self, *args, shard_count, shard_index, shard_manifest, **kwargs):
        super().__init__(*args, **kwargs)
        if not 0 <= shard_index < shard_count:
            raise ValueError("shard index must be within shard count")
        if self.parallel > 1:
            raise ValueError("use isolated CI jobs, not nested parallel workers")
        self.shard_count = shard_count
        self.shard_index = shard_index
        self.shard_manifest = shard_manifest

    def build_suite(self, test_labels=None, **kwargs):
        if test_labels or self.tags or self.exclude_tags or self.test_name_patterns:
            raise ValueError("CI sharding requires unfiltered full-suite discovery")
        tests = list(iter_test_cases(super().build_suite(test_labels, **kwargs)))
        selected = partition_tests(tests, self.shard_count)[self.shard_index]
        Path(self.shard_manifest).write_text(json.dumps({
            "index": self.shard_index,
            "count": self.shard_count,
            "discovered": sorted(test.id() for test in tests),
            "selected": [test.id() for test in selected],
        }))
        self.log(f"Shard {self.shard_index + 1}/{self.shard_count}: {len(selected)}/{len(tests)} tests")
        return self.test_suite(selected)

    def run_suite(self, suite, **kwargs):
        result = super().run_suite(suite, **kwargs)
        path = Path(self.shard_manifest)
        manifest = json.loads(path.read_text())
        manifest.update({
            "class_seconds": suite.class_seconds,
            "fixture_seconds": suite.fixture_seconds,
            "test_seconds": dict(getattr(result, "collectedDurations", [])),
            "outcomes": {
                "run": result.testsRun, "failures": len(result.failures),
                "errors": len(result.errors), "skipped": len(result.skipped),
                "expected_failures": len(result.expectedFailures),
                "unexpected_successes": len(result.unexpectedSuccesses),
            },
        })
        path.write_text(json.dumps(manifest))
        return result
