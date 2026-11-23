#!/usr/local/bin/python3
"""Outbound-only Mac worker. Cached Whisper base; no Telegram/OpenAI credentials."""
import base64
import hashlib
import json
import re
from pathlib import Path
import subprocess
import sys
import time

MAX_PCM = 180 * 16000 * 2
ROOT = Path.home() / '.local/share/pollyanas-hours'


def validate_job(job):
    if (not isinstance(job, dict) or set(job) != {'id', 'lease', 'sha256', 'pcm'} or
            type(job['id']) is not int or not 0 < job['id'] < 2**63 or
            not isinstance(job['lease'], str) or not re.fullmatch('[a-f0-9]{32}', job['lease']) or
            not isinstance(job['pcm'], str) or len(job['pcm']) > 7_680_000):
        raise ValueError('invalid_job')
    pcm = base64.b64decode(job['pcm'], validate=True)
    if not pcm or len(pcm) > MAX_PCM or len(pcm) % 2 or hashlib.sha256(pcm).hexdigest() != job['sha256']:
        raise ValueError('invalid_job')
    return pcm


def rpc(payload):
    result = subprocess.run(['/usr/bin/ssh', '-T', '-F', str(ROOT / 'ssh_config'), 'hours-audio'],
        input=json.dumps(payload).encode(), capture_output=True, timeout=45, check=True,
        env={'PATH':'/usr/bin:/bin', 'LANG':'C'})
    if len(result.stdout) > 7_700_000:
        raise ValueError('invalid_output')
    value = json.loads(result.stdout)
    if not isinstance(value, dict) or 'error' in value:
        raise ValueError('unavailable')
    return value


def transcribe(job):
    pcm = validate_job(job)
    import resource
    resource.setrlimit(resource.RLIMIT_CPU, (55, 60))
    import numpy as np
    import torch
    import whisper
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)
    model_path = Path.home() / '.cache/whisper/base.pt'
    if not model_path.is_file():
        raise ValueError('cached_model_missing')
    model = whisper.load_model(str(model_path), device='cpu')
    result = model.transcribe(np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0,
        language='es', fp16=False, condition_on_previous_text=False, verbose=None)
    text = result['text'].strip()
    if not text or len(text) > 2000:
        raise ValueError('invalid_text')
    return text


def work():
    job = rpc({'action':'claim'})
    if not job:
        return
    validate_job(job)
    child = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--transcribe'],
        input=json.dumps(job).encode(), capture_output=True, timeout=60, check=True,
        env={'PATH':'/usr/bin:/bin', 'LANG':'C', 'HOME':str(Path.home())})
    if len(child.stdout) > 8192:
        raise ValueError('invalid_text')
    text = json.loads(child.stdout)['text']
    if not isinstance(text, str) or not text.strip() or len(text) > 2000:
        raise ValueError('invalid_text')
    payload = {key:job[key] for key in ('id','lease','sha256')}
    payload.update(action='finish', text=text)
    for _ in range(3):
        result = rpc(payload)
        if not result.get('busy'):
            return
        time.sleep(2)


def main():
    if sys.argv[1:] == ['--transcribe']:
        try:
            raw = sys.stdin.buffer.read(7_700_001)
            if len(raw) > 7_700_000:
                raise ValueError()
            print(json.dumps({'text': transcribe(json.loads(raw))}))
            return 0
        except Exception:
            return 1
    if sys.argv[1:]:
        return 1
    while True:
        try:
            work()
        except Exception:
            # No stdout/stderr, recording, transcript or credentials reach logs.
            pass
        time.sleep(5)


if __name__ == '__main__':
    sys.exit(main())
