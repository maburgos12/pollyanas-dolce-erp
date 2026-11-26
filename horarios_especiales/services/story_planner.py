"""Signed transport for the private Marketing Stories endpoint."""
import hashlib
import hmac
import json
import os
import re
import time

import requests

ORIGIN = 'https://ads.pollyanasdolce.com/api'
ROOT = '/social-scheduler/hours-story-drafts'
DIGEST = r'[a-f0-9]{64}'
DRAFT = rf'hours:[1-9][0-9]{{0,18}}:{DIGEST}:[1-9][0-9]{{0,18}}'
PATH = re.compile(rf'{ROOT}(?:/assets(?:/template|/{DIGEST}/preview)?|/invalidate|/{DRAFT}(?:/approve)?)?')


class MarketingError(Exception):
    def __init__(self, code, *, retryable=False):
        super().__init__(code)
        self.retryable = retryable


def encode(payload):
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


class MarketingClient:
    def __init__(self, secret):
        if len(secret) < 16:
            raise ValueError('Falta el secreto de servicio de historias.')
        self.secret = secret.encode()

    @classmethod
    def configured(cls, *, for_revocation=False):
        secret = os.environ.get('HOURS_BOT_SIGNING_SECRET', '')
        if (for_revocation or os.environ.get('HOURS_STORIES_ENABLED') == 'true') and len(secret) >= 16:
            return cls(secret)
        return None

    def request(self, method, path, body=b'', *, image=False):
        if not PATH.fullmatch(path) or method not in ('GET', 'POST') or (method == 'GET' and body):
            raise ValueError('Ruta de historias inválida.')
        timestamp = str(int(time.time()))
        signed = b'\n'.join((timestamp.encode(), method.encode(), path.encode(), body))
        headers = {
            'X-Hours-Timestamp': timestamp,
            'X-Hours-Signature': hmac.new(self.secret, signed, hashlib.sha256).hexdigest(),
        }
        if method == 'POST':
            headers['Content-Type'] = 'application/json'
        deadline = time.monotonic() + 10
        try:
            with requests.Session() as session:
                session.trust_env = False
                with session.request(method, ORIGIN + path, data=body, headers=headers, stream=True,
                                     allow_redirects=False, timeout=(3, 7)) as response:
                    if 300 <= response.status_code < 400:
                        raise MarketingError('unexpected_redirect')
                    limit = 6_000_000 if image and response.status_code == 200 else 131_072
                    output = bytearray()
                    for chunk in response.iter_content(8192):
                        output.extend(chunk)
                        if len(output) > limit or time.monotonic() > deadline:
                            raise MarketingError('invalid_response', retryable=True)
                    if response.status_code in (200, 201):
                        if image:
                            if response.headers.get('Content-Type', '').split(';')[0] != 'image/jpeg':
                                raise MarketingError('invalid_image')
                            return bytes(output)
                        payload = json.loads(output)
                        if isinstance(payload, dict):
                            return payload
                        raise MarketingError('invalid_response', retryable=True)
                    try:
                        code = json.loads(output).get('detail', {}).get('code', '')
                    except (ValueError, AttributeError):
                        code = ''
                    if not isinstance(code, str) or not re.fullmatch(r'[a-z0-9_]{1,100}', code):
                        code = f'http_{response.status_code}'
                    raise MarketingError(code, retryable=(
                        code == 'previous_story_retirement_pending' or response.status_code in (429, 502, 504)
                        or (response.status_code >= 500 and code != 'hours_publishing_disabled')
                    ))
        except (requests.RequestException, ValueError):
            raise MarketingError('transport_unavailable', retryable=True) from None
