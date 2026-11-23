"""Bounded readback of real public consumers; HTTP success alone is not proof."""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from django.utils import timezone

from horarios_especiales.models import SolicitudHorarioEspecial
from horarios_especiales.services.rules import rule_applies, resolve_approved_rule
from core.models import Sucursal

STORE_URL = 'https://www.pollyanasdolce.com/api/branch-hours'
MAYA_URL = 'https://api.pollyanasdolce.com/branches/hours-status'


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


def matches(row, expected, now):
    try:
        checked = datetime.fromisoformat(row['checked_at'].replace('Z', '+00:00'))
        return (checked.tzinfo is not None and timedelta(minutes=-2) <= now - checked <= timedelta(minutes=15)
            and all(row.get(key) == expected[key] for key in ('branch_code', 'branch_name', 'target_date', 'revision', 'closed_all_day', 'windows'))
            and row.get('timezone') == 'America/Mazatlan' and row.get('status') == 'VERIFIED'
            and row.get('source') == 'ERP_APPROVED_SPECIAL_HOURS')
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


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
            results[destination] = {'confirmed': matches(row, expected, timezone.now()), 'checked_at': row.get('checked_at')}
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
    programmed = any(r['start_date'] > today.isoformat() or
                     (r.get('end_date') or (r['start_date'] if r['kind'] == 'date' else today.isoformat())) > dates[-1].isoformat()
                     for r in rules)
    if not any(rule_applies(r, d) for r in rules for d in dates):
        return ['Tienda: programado; sin fecha vigente verificable esta semana.', 'Maya: programado; sin fecha vigente verificable esta semana.']
    pairs = {}
    superseded = False
    for location in payload['locations']:
        branch = Sucursal.objects.get(pk=location['branch_id'])
        for target in dates:
            if not any(rule_applies(r, target) for r in rules):
                continue
            effective = resolve_approved_rule(branch, target)
            if not effective or effective.get('revision') != payload['revision']:
                superseded = True
                continue
            key = branch.codigo + ':' + target.isoformat()
            pairs[key] = {'branch_code': branch.codigo, 'branch_name': branch.nombre, 'target_date': target.isoformat(),
                          'revision': payload['revision'], 'closed_all_day': effective['closed_all_day'], 'windows': effective['windows']}
    prior = cached.get('pairs', {}) if cached.get('revision') == payload['revision'] else {}
    verified = {}
    for key in pairs:
        evidence = prior.get(key, {})
        try:
            if all(evidence.get(dest, {}).get('confirmed') is True and
                   timedelta(0) <= now - datetime.fromisoformat(evidence[dest]['checked_at']) <= timedelta(minutes=15)
                   for dest in ('store', 'maya')):
                verified[key] = evidence
        except (ValueError, TypeError, KeyError):
            pass
    batch = [] if wait_for_check else [key for key in pairs if key not in verified][:4]
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(check_pair, [pairs[key] for key in batch]))
    verified.update(zip(batch, results))
    labels = []
    for dest, name in [('store', 'Tienda'), ('maya', 'Maya')]:
        complete = bool(pairs) and all(verified.get(key, {}).get(dest, {}).get('confirmed') for key in pairs)
        state = 'revisión sustituida o en conflicto; pendiente' if superseded else (
            'programado; verificación parcial de fechas' if programmed else
            'confirmado por lectura del consumidor (próximos 7 días)' if complete else 'pendiente de verificación')
        labels.append(f'{name}: {state}.')
    google = obj.execution_summary_json.get('details', [])
    if (not superseded and not programmed and len(google) == len(payload['locations']) and
            all(row.get('status') == 'GOOGLE_CONFIRMED' for row in google) and
            all('confirmado por lectura' in label for label in labels)):
        labels.append('Finalizado: lectura Google, tienda y Maya verificada. Google Maps visual no comprobado.')
    receipt = {'revision': payload['revision'], 'pairs': verified, 'labels': labels,
               'next_check_at': cached['next_check_at'] if wait_for_check else (now + timedelta(seconds=300 if not batch or all('confirmado por lectura' in label for label in labels[:2]) else 30)).isoformat()}
    # Update only our summary key so a concurrent Google reconciliation is not overwritten.
    from django.db import transaction
    with transaction.atomic():
        locked = SolicitudHorarioEspecial.objects.select_for_update().get(pk=obj.pk)
        locked.execution_summary_json['consumer_readback'] = receipt
        locked.save(update_fields=['execution_summary_json'])
        obj.execution_summary_json = locked.execution_summary_json
    return labels
