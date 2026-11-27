"""Bounded read-only broker capture and addressed worker inspection.

No Django import occurs until CLI execution. The caller must additionally bound
process lifetime: socket/inspect timeouts cannot prove a process was reaped.
"""
import base64
import contextlib
import datetime as dt
import hashlib
import importlib.metadata
import json
import os
import sys
import time

from erp_release_preservation import (
    MAX_UNACKED, MAX_READY, MAX_RAW, RAW_BUDGET, OUTPUT_LIMIT, QUEUES,
    QUEUE_KEYS, canonical, require, unique_object, validate,
)

PHASES = ('ping', 'active', 'reserved', 'scheduled')


def strict_json(raw):
    def invalid_constant(value):
        raise ValueError('Nonfinite JSON constant')
    return json.loads(raw, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def fresh(deadline):
    require(time.monotonic() < deadline, 'Snapshot deadline exceeded')


def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def capture_pending(client, before=None, *, deadline):
    """Capture bounded wire bytes, rejecting incomplete or changing coverage."""
    if before is not None:
        validate(before)
    fresh(deadline)
    count = client.hlen('unacked')
    require(type(count) is int and 0 <= count <= MAX_UNACKED, 'Unacked32 cap exceeded')
    raw_rows, seen = [], {}
    budget = 0
    cursor = 0

    def append(raw, location, **where):
        nonlocal budget
        require(isinstance(raw, bytes) and len(raw) <= MAX_RAW, 'Per-message raw cap exceeded')
        budget += len(raw)
        require(budget <= RAW_BUDGET, 'Combined raw cap exceeded before decode')
        raw_rows.append((raw, location, where))

    for _ in range(8):
        fresh(deadline)
        cursor, batch = client.hscan('unacked', cursor=cursor, count=8)
        require(isinstance(batch, dict) and len(batch) <= MAX_UNACKED, 'HSCAN batch cap exceeded')
        for tag, raw in batch.items():
            if tag in seen:
                require(seen[tag] == raw, 'Reservation changed during scan')
                continue
            require(len(seen) < MAX_UNACKED, 'Unacked32 actual cap exceeded')
            require(isinstance(tag, bytes) and 0 < len(tag) <= 256, 'Invalid reservation tag')
            seen[tag] = raw
            fresh(deadline)
            append(raw, 'unacked', tag=tag.decode('ascii'), score=client.zscore('unacked_index', tag))
        if cursor == 0:
            break
    else:
        raise ValueError('HSCAN8 command cap exceeded')

    lengths, hints = {}, {}
    ready_count = 0
    for queue in sorted(QUEUE_KEYS):
        fresh(deadline)
        hint = client.llen(queue)
        require(type(hint) is int and 0 <= hint <= MAX_READY and ready_count + hint <= MAX_READY,
                'Ready2000 hinted cap exceeded')
        fresh(deadline)
        rows = client.lrange(queue, 0, MAX_READY)
        ready_count += len(rows)
        require(ready_count <= MAX_READY, 'Ready2000 actual cap exceeded')
        lengths[queue], hints[queue] = len(rows), hint
        for raw in rows:
            append(raw, 'ready', queue=queue)

    # Decode only after all byte bounds; bodies remain opaque strings.
    records = []
    for raw, location, where in raw_rows:
        fresh(deadline)
        value = strict_json(raw)
        canonical(value)  # Also rejects numeric overflow to nonfinite float.
        if location == 'unacked':
            require(isinstance(value, list) and len(value) == 3, 'Invalid reservation envelope')
            msg, route = value[0], value[2]
        else:
            msg, route = value, where['queue'].split('\x06\x16')[0]
        require(isinstance(msg, dict) and isinstance(msg.get('headers'), dict), 'Invalid message headers')
        records.append({'location': location, **where,
                        'raw_base64': base64.b64encode(raw).decode(),
                        'sha256': hashlib.sha256(raw).hexdigest(),
                        'id': msg['headers'].get('id'), 'route': route})
    tags = {r['tag'] for r in records if r['location'] == 'unacked'}
    if before:
        tags.update(r['tag'] for r in before['records'] if r['location'] == 'unacked')
    states = {}
    for tag in sorted(tags):
        fresh(deadline)
        present = bool(client.hexists('unacked', tag))
        fresh(deadline)
        states[tag] = {'hash_present': present, 'score': client.zscore('unacked_index', tag)}
    fresh(deadline)
    result = {'records': records, 'ready_lengths': lengths, 'ready_length_hints': hints,
              'tag_states': states, 'unacked_count_hint_before': count,
              'unacked_count_hint_after': client.hlen('unacked'), 'raw_bytes': budget,
              'capture_is_interval_not_atomic': True}
    fresh(deadline)
    validate(result)
    require(len(canonical(result).encode()) <= OUTPUT_LIMIT, 'Snapshot output cap exceeded')
    return result


def validate_nodes(nodes):
    require(isinstance(nodes, list), 'Nodes must be a list')
    if not nodes:
        return
    require(len(nodes) == len(set(nodes)) == len(QUEUES), 'Exactly seven unique nodes required')
    require(all(isinstance(n, str) and len(n) <= 256 and n.count('@') == 1
                and n.split('@')[1] for n in nodes), 'Invalid addressed node')
    require({n.split('@')[0] for n in nodes} == set(QUEUES), 'Node queue identities differ')


def inspect_nodes(app, nodes, *, deadline):
    validate_nodes(nodes)
    require(bool(nodes), 'No addressed worker nodes')
    replies = {}
    for phase in PHASES:
        fresh(deadline)
        inspector = app.control.inspect(destination=list(nodes), timeout=min(2.0, deadline - time.monotonic()))
        reply = getattr(inspector, phase)()
        fresh(deadline)
        require(isinstance(reply, dict) and set(reply) == set(nodes), 'Missing or extra addressed worker response')
        if phase == 'ping':
            require(all(v == {'ok': 'pong'} for v in reply.values()), 'Unexpected ping response')
        else:
            require(all(isinstance(v, list) for v in reply.values()), 'Invalid worker activity response')
        replies[phase] = reply
        require(len(canonical(replies).encode()) <= OUTPUT_LIMIT, 'Worker response cap exceeded')
    return replies


def collect_snapshot(client, before, nodes, *, app=None, deadline, versions=None):
    validate_nodes(nodes)
    result = {'status': 'complete', 'started_at': stamp(), 'versions': versions or {}}
    if nodes:
        require(app is not None, 'Worker app required for inspection')
        result['inspect_started_at'] = stamp()
        result['replies'] = inspect_nodes(app, nodes, deadline=deadline)
        result['inspect_finished_at'] = stamp()
    result.update(capture_pending(client, before, deadline=deadline))
    result['finished_at'] = stamp()
    require(len(canonical(result).encode()) <= OUTPUT_LIMIT, 'Combined snapshot output cap exceeded')
    return result


def connect(url):
    require(isinstance(url, str) and url.startswith(('redis://', 'rediss://')), 'Unsupported broker transport')
    import redis
    return redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)


def main():
    deadline = time.monotonic() + 20
    raw = sys.stdin.buffer.read(OUTPUT_LIMIT + 1)
    require(len(raw) <= OUTPUT_LIMIT, 'Input cap exceeded')
    request = strict_json(raw)
    require(isinstance(request, dict) and set(request) == {'before', 'nodes'}, 'Snapshot request shape differs')
    validate_nodes(request['nodes'])
    require(request['before'] is None or isinstance(request['before'], dict), 'Invalid prior cut')
    # Keep successful stdout a single JSON document even if configuration logs.
    with contextlib.redirect_stdout(sys.stderr):
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
        from django.conf import settings
        from config.celery import app
        versions = {name: importlib.metadata.version(name) for name in ('celery', 'kombu')}
        fresh(deadline)
        client = connect(settings.CELERY_BROKER_URL)
        try:
            result = collect_snapshot(client, request['before'], request['nodes'], app=app,
                                      deadline=deadline, versions=versions)
        finally:
            client.close()
    print(canonical(result), flush=True)


if __name__ == '__main__':
    main()
