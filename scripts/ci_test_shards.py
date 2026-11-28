"""Deterministic full-suite partition and cross-job coverage verification."""
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path


def partition_tests(tests, count, class_seconds=None):
    if count < 1:
        raise ValueError("shard count must be positive")
    groups = defaultdict(list)
    for test in tests:
        cls = type(test)
        groups[f"{cls.__module__}.{cls.__qualname__}"].append(test)
    weights = class_seconds or {}
    if any(isinstance(value, bool) or not isinstance(value, (int, float))
           or not math.isfinite(value) or value <= 0 for value in weights.values()):
        raise ValueError("class durations must be finite positive seconds")
    assignments = {}
    sizes = [0] * count
    # Unknown/new classes remain automatically discovered with a count fallback.
    # Measurements change only placement; fixtures and Django order stay intact.
    def weight(name):
        return weights.get(name, len(groups[name]))
    for name in sorted(groups, key=lambda name: (-weight(name), name)):
        index = min(range(count), key=lambda i: (sizes[i], i))
        assignments[name] = index
        sizes[index] += weight(name)
    shards = [[] for _ in range(count)]
    # Preserve Django's TestCase/TransactionTestCase ordering within each shard.
    for test in tests:
        cls = type(test)
        shards[assignments[f"{cls.__module__}.{cls.__qualname__}"]].append(test)
    return shards


def verify_manifests(paths, count):
    manifests = [json.loads(Path(path).read_text()) for path in paths]
    if len(manifests) != count or {m["index"] for m in manifests} != set(range(count)):
        raise ValueError("missing or duplicate shard manifest")
    expected = manifests[0]["discovered"]
    if not expected or len(expected) != len(set(expected)):
        raise ValueError("empty or duplicate test discovery")
    for manifest in manifests:
        if manifest.get("partition") != manifests[0].get("partition"):
            raise ValueError("shards disagree on partition profile")
        if manifest["count"] != count or manifest["discovered"] != expected:
            raise ValueError("shards disagree on full-suite discovery")
    assigned = Counter(test for m in manifests for test in m["selected"])
    if assigned != Counter(expected):
        raise ValueError("tests omitted or assigned more than once")
    print(f"Coverage verified: {len(expected)} tests, {count} shards, no omissions or duplicates.")


if __name__ == "__main__":
    verify_manifests(sorted(Path(sys.argv[1]).glob("*.json")), int(sys.argv[2]))
