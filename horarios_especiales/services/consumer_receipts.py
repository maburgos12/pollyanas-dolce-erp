"""Bounded readback of real public consumers; HTTP success alone is not proof."""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from django.db import transaction
from django.utils import timezone

from horarios_especiales.models import SolicitudHorarioEspecial
from horarios_especiales.services.rules import indexed_branch_rules, rule_applies, resolve_approved_rule
from core.models import Sucursal

STORE_URL = 'https://www.pollyanasdolce.com/api/branch-hours'
MAYA_URL = 'https://api.pollyanasdolce.com/branches/hours-status'
FINALIZED_LABEL = 'Finalizado: lectura Google, tienda y Maya verificada. Google Maps visual no comprobado.'


def read_json(url, params):
    deadline = time.monotonic() + 8
    with requests.Session() as session:
        session.trust_env = False
        with session.get(url, params=params, headers={'User-Agent': 'PollyanasHoursIntegration/1.0'},
                         timeout=(3, 5), allow_redirects=False, stream=True) as response:
            if response.status_code != 200:
                raise ValueError('Consumer unavailable')
            body = bytearray()
            for chunk in response.iter_content(8192):
                body.extend(chunk)
                if len(body) > 65536 or time.monotonic() > deadline:
                    raise ValueError('Invalid consumer response')
    return json.loads(body)


def checked_at(evidence):
    try:
        stamp = datetime.fromisoformat(evidence['checked_at'].replace('Z', '+00:00'))
        return stamp if stamp.tzinfo is not None else None
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def is_fresh(evidence, now):
    stamp = checked_at(evidence)
    return stamp is not None and timedelta(minutes=-2) <= now - stamp <= timedelta(minutes=15)


def comparable(row, expected, now):
    return (is_fresh(row, now)
            and all(row.get(key) == expected[key] for key in ('branch_code', 'target_date'))
            and row.get('timezone') == 'America/Mazatlan'
            and row.get('status') == 'VERIFIED' and row.get('source') == 'ERP_APPROVED_SPECIAL_HOURS')


def matches(row, expected, now):
    return comparable(row, expected, now) and all(
        row.get(key) == expected[key] for key in ('branch_name', 'revision', 'closed_all_day', 'windows')
    )


def check_pair(expected):
    results = {}
    for destination, url in [('store', STORE_URL), ('maya', MAYA_URL)]:
        try:
            params = {'target_date': expected['target_date']}
            if destination == 'maya':
                params['branch_code'] = expected['branch_code']
            payload = read_json(url, params)
            if destination == 'store':
                rows = payload.get('branches', [])
                selected = [r for r in rows if isinstance(r, dict) and r.get('branch_code') == expected['branch_code']]
                row = selected[0] if len(selected) == 1 and payload.get('target_date') == expected['target_date'] and payload.get('timezone') == 'America/Mazatlan' else {}
            else:
                row = payload if payload.get('consumer') == 'MAYA_BRANCH_SERVICE' else {}
            now = timezone.now()
            confirmed = matches(row, expected, now)
            results[destination] = {'confirmed': confirmed, 'checked_at': row.get('checked_at')}
            if not confirmed and comparable(row, expected, now):
                results[destination]['mismatch'] = True
        except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
            results[destination] = {'confirmed': False}
    return results


def consumer_receipts(obj):
    payload = obj.canonical_payload
    pending = ['Tienda: pendiente de verificación.', 'Maya: pendiente de verificación.']
    if not obj.approved_at or obj.cancelled_at or payload.get('schema_version') != 2:
        return pending
    now = timezone.now()
    today = now.astimezone(ZoneInfo('America/Mazatlan')).date()
    cached = obj.execution_summary_json.get('consumer_readback', {})
    wait_for_check = False
    if cached.get('revision') == payload['revision']:
        try:
            if datetime.fromisoformat(cached['next_check_at']) > now:
                wait_for_check = True
        except (KeyError, ValueError, TypeError):
            pass
    # shortcut: readback covers seven days per run; longer/future periods stay programmed.
    dates = [today + timedelta(days=i) for i in range(7)]
    rules = payload['rules']
    programmed = any((r['kind'] == 'weekly' and not r.get('end_date')) or r['start_date'] > today.isoformat() or
                     (r.get('end_date') or (r['start_date'] if r['kind'] == 'date' else today.isoformat())) > dates[-1].isoformat()
                     for r in rules)
    if not any(rule_applies(r, d) for r in rules for d in dates):
        if all(not (r['kind'] == 'weekly' and not r.get('end_date')) and
               (r.get('end_date') or r['start_date']) < today.isoformat() for r in rules):
            return ['Tienda: vigencia concluida; sin lectura vigente.', 'Maya: vigencia concluida; sin lectura vigente.']
        return ['Tienda: programado; sin fecha vigente verificable esta semana.', 'Maya: programado; sin fecha vigente verificable esta semana.']
    pairs = {}
    superseded = False
    for location in payload['locations']:
        branch = Sucursal.objects.get(pk=location['branch_id'])
        branch_rules = indexed_branch_rules(payload, branch.pk)
        for target in dates:
            if not any(rule_applies(r, target) for _, r in branch_rules):
                continue
            effective = resolve_approved_rule(branch, target)
            if not effective or effective.get('revision') != payload['revision']:
                superseded = True
                continue
            key = branch.codigo + ':' + target.isoformat()
            pairs[key] = {'branch_code': branch.codigo, 'branch_name': branch.nombre, 'target_date': target.isoformat(),
                          'revision': payload['revision'], 'closed_all_day': effective['closed_all_day'], 'windows': effective['windows']}
    prior = cached.get('pairs', {}) if cached.get('revision') == payload['revision'] else {}
    keys = list(pairs)
    cursor = cached.get('cursor', 0) if cached.get('revision') == payload['revision'] else 0
    cursor = cursor % len(keys) if keys and type(cursor) is int else 0
    rotated = keys[cursor:] + keys[:cursor]
    destinations = ('store', 'maya')
    batch = [] if wait_for_check else [
        key for key in rotated
        if any(not evidence.get('confirmed') or not is_fresh(evidence, now)
               for evidence in (prior.get(key, {}).get(dest, {}) for dest in destinations))
    ][:4]
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(check_pair, [pairs[key] for key in batch]))
    # Merge new observations into the latest saved evidence after the network reads.
    with transaction.atomic():
        locked = SolicitudHorarioEspecial.objects.select_for_update().get(pk=obj.pk)
        obj.execution_summary_json = locked.execution_summary_json
        if locked.cancelled_at or locked.canonical_payload.get('revision') != payload['revision']:
            obj.status = locked.status
            return pending
        latest = locked.execution_summary_json.get('consumer_readback', {})
        latest_pairs = latest.get('pairs', {}) if latest.get('revision') == payload['revision'] else {}
        evidence_by_pair = {key: dict(latest_pairs.get(key, {})) for key in pairs}
        for key, result in zip(batch, results):
            for dest, evidence in result.items():
                existing = evidence_by_pair[key].get(dest, {})
                previous_stamp = checked_at(existing)
                new_stamp = checked_at(evidence)
                # An unavailable consumer cannot invalidate an earlier observation.
                if ((evidence.get('confirmed') or evidence.get('mismatch')) and new_stamp is not None
                        and (previous_stamp is None or new_stamp >= previous_stamp)):
                    evidence_by_pair[key][dest] = evidence
        if batch:
            cursor = (keys.index(batch[-1]) + 1) % len(keys)
        labels = []
        all_fresh = True
        for dest, name in [('store', 'Tienda'), ('maya', 'Maya')]:
            observations = [pair.get(dest, {}) for pair in evidence_by_pair.values()]
            complete = bool(observations) and all(row.get('confirmed') for row in observations)
            fresh = complete and all(is_fresh(row, now) for row in observations)
            all_fresh = all_fresh and fresh
            if superseded:
                state = 'revisión sustituida o en conflicto; pendiente'
            elif any(row.get('mismatch') for row in observations):
                state = 'discrepancia detectada por lectura del consumidor'
            elif programmed:
                state = 'programado; verificación parcial de fechas'
            elif fresh:
                state = 'confirmado por lectura del consumidor (próximos 7 días)'
            elif complete:
                state = 'confirmado anteriormente; verificación actual pendiente'
            else:
                state = 'pendiente de verificación'
            labels.append(f'{name}: {state}.')
        if wait_for_check:
            next_check_at = latest.get('next_check_at', cached['next_check_at'])
        else:
            delay = 300 if not batch or all_fresh else 30
            next_check_at = (now + timedelta(seconds=delay)).isoformat()
        receipt = {'revision': payload['revision'], 'pairs': evidence_by_pair, 'labels': labels,
                   'cursor': cursor if batch else latest.get('cursor', cursor), 'next_check_at': next_check_at}
        google = locked.execution_summary_json.get('details', [])
        if (not superseded and not programmed and all_fresh
                and len(google) == len(payload['locations'])
                and all(row.get('status') == 'GOOGLE_CONFIRMED' for row in google)):
            labels.append(FINALIZED_LABEL)
        locked.execution_summary_json['consumer_readback'] = receipt
        locked.save(update_fields=['execution_summary_json'])
        obj.execution_summary_json = locked.execution_summary_json
    return labels
