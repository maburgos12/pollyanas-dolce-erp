"""Deterministic full-suite partition and cross-job coverage verification."""
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path


def partition_tests(tests, count):
    if count < 1:
        raise ValueError("shard count must be positive")
    groups = defaultdict(list)
    for test in tests:
        cls = type(test)
        groups[f"{cls.__module__}.{cls.__qualname__}"].append(test)
    assignments = {}
    sizes = [0] * count
    # Keep class fixtures together; largest classes first, stable tie breakers.
    for name, cases in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        index = min(range(count), key=lambda i: (sizes[i], i))
        assignments[name] = index
        sizes[index] += len(cases)
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
        if manifest["count"] != count or manifest["discovered"] != expected:
            raise ValueError("shards disagree on full-suite discovery")
    assigned = Counter(test for m in manifests for test in m["selected"])
    if assigned != Counter(expected):
        raise ValueError("tests omitted or assigned more than once")
    print(f"Coverage verified: {len(expected)} tests, {count} shards, no omissions or duplicates.")


if __name__ == "__main__":
    verify_manifests(sorted(Path(sys.argv[1]).glob("*.json")), int(sys.argv[2]))
