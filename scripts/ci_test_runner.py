"""Explicit CI-only runner; application settings and local default stay unchanged."""
import json
from pathlib import Path

from django.test.runner import DiscoverRunner, iter_test_cases

from scripts.ci_test_shards import partition_tests


class ShardedDiscoverRunner(DiscoverRunner):
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
