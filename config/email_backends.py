from __future__ import annotations

import base64
import json
import logging
import urllib.error
import urllib.request

from django.conf import settings
from django.core.mail.backends.base import BaseEmailBackend


logger = logging.getLogger(__name__)


class ResendEmailBackend(BaseEmailBackend):
    """Django email backend using Resend's HTTPS API.

    The VPS blocks outbound SMTP ports, while HTTPS works reliably. This keeps
    Django's regular send_mail/EmailMessage contract and changes only transport.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = getattr(settings, "RESEND_API_KEY", "")
        self.base_url = getattr(settings, "RESEND_BASE_URL", "https://api.resend.com").rstrip("/")
        self.timeout = float(getattr(settings, "RESEND_TIMEOUT_SECONDS", 30))

    def send_messages(self, email_messages) -> int:
        if not email_messages:
            return 0
        if not self.api_key:
            if self.fail_silently:
                logger.warning("RESEND_API_KEY no configurado; correo omitido.")
                return 0
            raise RuntimeError("RESEND_API_KEY no configurado.")

        sent_count = 0
        for message in email_messages:
            try:
                self._send(message)
            except Exception:
                if not self.fail_silently:
                    raise
                logger.exception("Fallo silencioso enviando correo por Resend.")
            else:
                sent_count += 1
        return sent_count

    def _send(self, message) -> None:
        payload = self._payload_for_message(message)
        request = urllib.request.Request(
            f"{self.base_url}/emails",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "PollyanasERP/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status_code = response.getcode()
                body = response.read().decode("utf-8", errors="replace")
                if status_code >= 400:
                    raise RuntimeError(f"Resend API error ({status_code}): {body[:300]}")
                resend_response = json.loads(body or "{}")
                message.resend_response = resend_response
                message.resend_email_id = resend_response.get("id")
                message.resend_status_code = status_code
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Resend API error ({exc.code}): {body[:300]}") from exc

    def _payload_for_message(self, message) -> dict:
        from_email = getattr(settings, "RESEND_FROM", "") or message.from_email or settings.DEFAULT_FROM_EMAIL
        payload = {
            "from": from_email,
            "to": list(message.to or []),
            "subject": message.subject or "",
            "text": message.body or "",
        }
        cc = list(getattr(message, "cc", []) or [])
        bcc = list(getattr(message, "bcc", []) or [])
        reply_to = list(getattr(message, "reply_to", []) or [])
        if cc:
            payload["cc"] = cc
        if bcc:
            payload["bcc"] = bcc
        if reply_to:
            payload["reply_to"] = reply_to

        html_body = self._html_body(message)
        if html_body:
            payload["html"] = html_body

        attachments = self._attachments(message)
        if attachments:
            payload["attachments"] = attachments
        return payload

    def _html_body(self, message) -> str:
        alternatives = getattr(message, "alternatives", []) or []
        for content, mimetype in alternatives:
            if mimetype == "text/html":
                return content
        return ""

    def _attachments(self, message) -> list[dict]:
        attachments: list[dict] = []
        for attachment in getattr(message, "attachments", []) or []:
            filename = getattr(attachment, "filename", None)
            content = getattr(attachment, "content", None)
            if filename is None and isinstance(attachment, tuple) and len(attachment) >= 2:
                filename = attachment[0]
                content = attachment[1]
            if not filename or content is None:
                continue
            if isinstance(content, str):
                content = content.encode("utf-8")
            attachments.append(
                {
                    "filename": str(filename),
                    "content": base64.b64encode(bytes(content)).decode("ascii"),
                }
            )
        return attachments


def retrieve_resend_email(email_id: str) -> dict:
    api_key = getattr(settings, "RESEND_API_KEY", "")
    if not api_key:
        raise RuntimeError("RESEND_API_KEY no configurado.")
    base_url = getattr(settings, "RESEND_BASE_URL", "https://api.resend.com").rstrip("/")
    timeout = float(getattr(settings, "RESEND_TIMEOUT_SECONDS", 30))
    request = urllib.request.Request(
        f"{base_url}/emails/{email_id}",
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "PollyanasERP/1.0",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            return json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Resend API error ({exc.code}): {body[:300]}") from exc
