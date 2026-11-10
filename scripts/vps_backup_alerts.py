#!/usr/bin/env python3
"""Read-only VPS/NAS monitoring, with durable Telegram delivery receipts."""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

REPORT = Path('/var/lib/erp-backup-restore/health-latest.json')
TELEGRAM_HELPER = '/usr/local/libexec/erp-mac-ia-watchdog/vigilar_revision.py'
RUNBOOK = 'https://github.com/maburgos12/pollyanas-dolce-erp/blob/main/docs/runbooks/vps-backup-alerts.md'
REPEAT_SECONDS = 6 * 3600


def issues(percent, nas_ok, report, service_result, now):
    found = {}
    if percent is not None and percent >= 85:
        found['disk_urgent'] = f'URGENTE: disco raíz al {percent}% (umbral 85%).'
    elif percent is not None and percent >= 75:
        found['disk_warning'] = f'ALERTA: disco raíz al {percent}% (umbral 75%).'
    if not nas_ok:
        found['nas'] = 'No se pudo comprobar el acceso a los respaldos del NAS.'
    if report is None:
        found['verification'] = 'No hay un informe de recuperación válido disponible.'
    elif report.get('success') is not True:
        found['verification'] = 'La última verificación de recuperación no confirmó éxito. Revisa health-latest.json.'
    elif not isinstance(report.get('finished_epoch'), (int, float)) or not 0 <= now - report['finished_epoch'] <= 36 * 3600:
        found['verification'] = 'La verificación de recuperación está ausente, desactualizada o tiene una fecha inválida.'
    if service_result != 'success':
        found['verification'] = 'El servicio de recuperación no confirmó éxito: fallo, tiempo límite o estado no disponible. Revisa su journal.'
    return found


def read_command(args, timeout):
    return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL, timeout=timeout)


def collect(now):
    failures = {}
    try:
        percent = int(read_command(['df', '-P', '/'], 5).splitlines()[-1].split()[4].rstrip('%'))
        if not 0 <= percent <= 100:
            raise ValueError('Invalid percentage')
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        percent = None
        failures['monitor'] = 'No se pudo medir el uso del disco. Revisa erp-backup-alerts.service.'
    try:
        output = read_command(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                               '-o', 'ConnectTimeout=15', '-o', 'ConnectionAttempts=1',
                               'qnap-nasdolce-archive',
                               'test -d /share/ERP_BACKUPS && test -r /share/ERP_BACKUPS && '
                               'ls /share/ERP_BACKUPS/backup_????????_??????.manifest >/dev/null 2>&1 && echo NAS_BACKUP_READABLE'], 25)
        nas_ok = output.strip() == 'NAS_BACKUP_READABLE'
    except (OSError, subprocess.SubprocessError):
        nas_ok = False
    try:
        report = json.loads(REPORT.read_text())
        if not isinstance(report, dict):
            report = None
    except (OSError, ValueError):
        report = None
    try:
        status = dict(line.split('=', 1) for line in read_command(
            ['systemctl', 'show', 'erp-backup-health.service', '-p', 'LoadState', '-p', 'Result'], 5).splitlines())
        result = status.get('Result', 'unknown') if status.get('LoadState') == 'loaded' else 'missing'
    except (OSError, ValueError, subprocess.SubprocessError):
        result = 'unknown'
    failures.update(issues(percent, nas_ok, report, result, now))
    return failures, percent


def notify(config, text):
    # Reuse the VPS's existing transport and credential contract; never log tokens.
    spec = importlib.util.spec_from_file_location('erp_telegram', TELEGRAM_HELPER)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    result = helper.telegram(config, 'sendMessage', {'chat_id': config['chat_id'], 'text': text})
    if result.get('chat', {}).get('id') != config['chat_id'] or type(result.get('message_id')) is not int:
        raise ValueError('Delivery was not acknowledged for the configured recipient')
    return result['message_id']


def message(current, previous, percent, test=False):
    title = 'PRUEBA · ERP respaldos y disco' if test else 'ERP Pollyana · respaldos y disco'
    lines = [title, 'Servidor: VPS ERP']
    lines.extend(current[key] for key in sorted(current))
    cleared = set(previous) - set(current)
    if {'disk_warning', 'disk_urgent'} & set(current):
        cleared -= {'disk_warning', 'disk_urgent'}
    if cleared:
        labels = dict(nas='acceso al NAS', verification='verificación de recuperación',
                      disk_warning='disco desde 75%', disk_urgent='disco desde 85%', monitor='lectura de disco')
        lines.append('Condiciones resueltas: ' + ', '.join(labels.get(key, 'monitor') for key in sorted(cleared)) + '.')
    if not current:
        lines.append(f'Recuperado: NAS accesible, verificación vigente y disco al {percent}% (menor a 75%).')
    lines.append('Este monitor no borra datos. La limpieza sigue exigiendo recuperación verificada y conservar el respaldo más reciente.')
    if test:
        lines.append('Condición simulada para probar el aviso; no se provocó un fallo real.')
    lines.append('Guía: ' + RUNBOOK)
    return '\n'.join(lines)


def deliver(current, state, percent, now, sender):
    previous = state.get('active', [])
    changed = set(current) != set(previous)
    reminder = bool(current) and now - state.get('sent_at', 0) >= REPEAT_SECONDS
    if not changed and not reminder:
        return state, None
    receipt = sender(message(current, previous, percent))
    # A failed/ambiguous delivery never advances the deduplication state.
    return dict(active=sorted(current), sent_at=now, message_id=receipt), receipt


def main(run_id):
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', choices=('nas', 'verification', 'disk'))
    args = parser.parse_args()
    entry = 'test' if args.test else 'monitor'
    os.umask(0o077)
    state_dir = Path(os.environ['STATE_DIRECTORY'])
    config = json.loads((Path(os.environ['CREDENTIALS_DIRECTORY']) / 'telegram.json').read_text())
    with (state_dir / 'monitor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        now = time.time()
        if args.test:
            current = issues(75 if args.test == 'disk' else 74, args.test != 'nas',
                             dict(success=args.test != 'verification', finished_epoch=now), 'success', now)
            receipt = notify(config, message(current, [], 75 if args.test == 'disk' else 74, test=True))
            print(json.dumps(dict(event='test_alert_accepted', run_id=run_id, entry_point=entry,
                                  condition=args.test, message_id=receipt)), flush=True)
            return
        current, percent = collect(now)
        path = state_dir / 'delivery.json'
        try:
            state = json.loads(path.read_text())
            if (not isinstance(state, dict) or not isinstance(state.get('active'), list)
                    or not all(isinstance(key, str) for key in state['active'])
                    or not isinstance(state.get('sent_at'), (int, float))
                    or not 0 <= state['sent_at'] <= now + 60):
                raise ValueError('Invalid delivery state')
        except FileNotFoundError:
            state = {}
        except (ValueError, TypeError):
            # Re-alert rather than silently suppressing incidents after corrupt state.
            state = {}
        state, receipt = deliver(current, state, percent, now, lambda text: notify(config, text))
        if receipt is not None:
            temp = path.with_suffix('.next')
            with temp.open('w') as stream:
                json.dump(state, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temp.chmod(0o600)
            temp.replace(path)
        print(json.dumps(dict(event='backup_alert_check', run_id=run_id, entry_point=entry,
                              active=sorted(current), used_percent=percent, message_id=receipt)), flush=True)


if __name__ == '__main__':
    run_id = uuid.uuid4().hex
    try:
        main(run_id)
    except Exception as error:
        print(json.dumps(dict(event='backup_alert_monitor_failed', run_id=run_id,
                              entry_point='test' if '--test' in sys.argv else 'monitor',
                              error_type=type(error).__name__)), flush=True)
        raise SystemExit(1)
