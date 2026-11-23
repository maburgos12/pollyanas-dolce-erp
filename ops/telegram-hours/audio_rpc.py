#!/usr/bin/python3
"""Install root:root 0755. SSH_ORIGINAL_COMMAND and all caller arguments are ignored."""
import json
import os
import signal
import subprocess
import sys


def main():
    signal.alarm(45)
    try:
        raw = sys.stdin.buffer.read(8193)
        if len(raw) > 8192:
            raise ValueError()
        payload = json.loads(raw)
        if not isinstance(payload, dict) or not (payload == {'action':'claim'} or
                payload.get('action') == 'finish' and set(payload) == {'action','id','lease','sha256','text'}):
            raise ValueError()
        result = subprocess.run(['/usr/bin/docker', 'compose', 'exec', '-T', 'web', 'python', 'manage.py', 'hours_audio_rpc'],
            cwd='/opt/pastelerias-erp', input=raw, capture_output=True, timeout=40,
            env={'PATH':'/usr/bin:/bin', 'HOME':'/root'}, check=True)
        if len(result.stdout) > 7_700_000:
            raise ValueError()
        value = json.loads(result.stdout)
        if not isinstance(value, dict):
            raise ValueError()
        sys.stdout.write(json.dumps(value))
    except Exception:
        sys.stdout.write('{"error":"unavailable"}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
