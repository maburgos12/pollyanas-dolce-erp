from __future__ import annotations

from requests import exceptions as requests_exceptions

TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


def source_error_metadata(exc: Exception) -> dict[str, object]:
    retryable = isinstance(
        exc,
        (TimeoutError, ConnectionError, requests_exceptions.Timeout, requests_exceptions.ConnectionError),
    )
    if isinstance(exc, requests_exceptions.HTTPError):
        status = getattr(getattr(exc, "response", None), "status_code", None)
        retryable = status in TRANSIENT_HTTP_STATUSES
    return {"retryable": retryable, "error_type": type(exc).__name__}
