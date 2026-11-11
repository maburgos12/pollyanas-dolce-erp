#!/usr/bin/env python3
"""Run on the NAS: public VPS probes and Telegram, with no VPS dependency."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import socket
import time
import urllib.request
import uuid

RUNBOOK = 'https://github.com/maburgos12/pollyanas-dolce-erp/blob/main/docs/runbooks/nas-vps-monitor.md'


def probe():
    checks = {}
    try:
        with socket.create_connection(('68.183.165.47', 443), timeout=8):
            checks['public_tcp'] = True
    except OSError:
        checks['public_tcp'] = False
    try:
        # Ignore proxy environment variables: this observer must use NAS Internet.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open('https://erp.pollyanasdolce.com/', timeout=12) as response:
            checks['erp_https'] = response.status == 200
    except (OSError, ValueError):
        checks['erp_https'] = False
    return checks


def notify(config, text):
    if not isinstance(config.get('token'), str) or type(config.get('chat_id')) is not int:
        raise ValueError('Invalid Telegram configuration')
    request = urllib.request.Request(
        'https://api.telegram.org/bot' + config['token'] + '/sendMessage',
        data=json.dumps({'chat_id': config['chat_id'], 'text': text}).encode(),
        headers={'Content-Type': 'application/json'}, method='POST')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=15) as response:
        result = json.load(response)
    receipt = result.get('result', {})
    if result.get('ok') is not True or receipt.get('chat', {}).get('id') != config['chat_id'] or type(receipt.get('message_id')) is not int:
        raise ValueError('Telegram did not acknowledge the configured recipient')
    return receipt['message_id']


def advance(previous, checks, now, sender, test=False):
    state = dict(previous)
    healthy = all(checks.values()) and set(checks) == {'public_tcp', 'erp_https'}
    # An interrupted observer cannot treat old samples as consecutive checks.
    consecutive = 0 < now - previous.get('checked_at', 0) <= 120
    state['failures'] = 0 if healthy else min(3, previous.get('failures', 0) + 1) if consecutive else 1
    state['successes'] = min(2, previous.get('successes', 0) + 1) if healthy and consecutive else int(healthy)
    state.update(checked_at=now, checks=checks, message_id=None, error=None)
    down = previous.get('down', False)
    desired = False if state['successes'] >= 2 else True if state['failures'] >= 3 else down
    reminder = desired and state['failures'] >= 3 and now - previous.get('sent_at', 0) >= 6 * 3600
    if desired != down or reminder:
        title = 'PRUEBA SIMULADA · ' if test else ''
        text = title + 'ERP Pollyana · vigilancia externa desde NAS\n'
        if desired:
            failed = ', '.join(key for key, ok in checks.items() if not ok)
            text += 'ALERTA: tres comprobaciones consecutivas sin confirmar acceso al VPS/ERP.\n'
            text += 'Comprobaciones fallidas: ' + failed + '. No distingue caída del servidor de un problema de red o HTTPS.\n'
        else:
            text += 'RECUPERADO: dos comprobaciones consecutivas correctas del IP público y ERP HTTPS.\n'
        text += 'Revisa conectividad y estado del VPS antes de reiniciar servicios.\n' + RUNBOOK
        try:
            receipt = sender(text)
        except Exception as error:
            # Never print exception URLs: Telegram URLs contain the bot token.
            state['error'] = type(error).__name__
        else:
            state.update(down=desired, sent_at=now, message_id=receipt)
    return state


def save(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2) + '\n')
    tmp.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--entry-point', choices=['manual', 'cron', 'test'], default='manual')
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    os.umask(0o077)
    state_dir = base / 'state'
    state_dir.mkdir(mode=0o700, exist_ok=True)
    run_id = uuid.uuid4().hex
    with (state_dir / 'monitor.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        report = dict(event='external_vps_check', run_id=run_id, entry_point=args.entry_point)
        try:
            config = json.loads((base / 'telegram.json').read_text())
            sender = lambda text: notify(config, text)
            now = time.time()
            if args.entry_point == 'test':
                state = {}
                receipts = []
                for offset, ok in enumerate([False, False, False, True, True]):
                    state = advance(state, dict(public_tcp=ok, erp_https=ok), now + offset * 60, sender, test=True)
                    if state['error']:
                        raise RuntimeError('Test notification failed')
                    if state['message_id'] is not None:
                        receipts.append(state['message_id'])
                if len(receipts) != 2:
                    raise RuntimeError('Missing test receipts')
                report.update(message_ids=receipts, success=True)
            else:
                path = state_dir / 'delivery.json'
                try:
                    previous = json.loads(path.read_text())
                except FileNotFoundError:
                    previous = {}
                state = advance(previous, probe(), now, sender)
                save(path, state)
                report.update(state, success=state['error'] is None)
            save(state_dir / ('test-latest.json' if args.entry_point == 'test' else 'latest.json'), report)
        except Exception as error:
            report.update(success=False, error=type(error).__name__)
            save(state_dir / ('test-latest.json' if args.entry_point == 'test' else 'latest.json'), report)
        print(json.dumps(report))
        return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
