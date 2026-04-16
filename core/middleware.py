from __future__ import annotations

import logging
import time
from contextlib import ExitStack
from dataclasses import dataclass

from django.conf import settings
from django.db import connections
from django.middleware.csrf import get_token
from django.shortcuts import redirect

from core.access import is_branch_capture_only


ERP_BUILD_TAG = "2026.03.13-enterprise-01"
performance_logger = logging.getLogger("erp.performance")


@dataclass
class _QuerySample:
    alias: str
    duration_ms: float
    sql: str


class _ConnectionTimingWrapper:
    def __init__(self, alias: str, slow_query_ms: float, collector: list[_QuerySample]):
        self.alias = alias
        self.slow_query_ms = float(slow_query_ms)
        self.collector = collector

    def __call__(self, execute, sql, params, many, context):
        started_at = time.perf_counter()
        try:
            return execute(sql, params, many, context)
        finally:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            if duration_ms >= self.slow_query_ms:
                self.collector.append(
                    _QuerySample(
                        alias=self.alias,
                        duration_ms=duration_ms,
                        sql=" ".join((sql or "").split())[:800],
                    )
                )


class CanonicalLocalHostMiddleware:
    """
    Fuerza un solo host local para evitar sesiones, caché y cookies divergentes
    entre aliases locales durante desarrollo.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        host = request.get_host()
        canonical = getattr(settings, "CANONICAL_LOCAL_HOST", "localhost:8011")
        hostname = host.split(":", 1)[0]

        if hostname in {"localhost", "127.0.0.1"} and host != canonical:
            query = request.META.get("QUERY_STRING", "")
            target = f"{request.scheme}://{canonical}{request.path}"
            if query:
                target = f"{target}?{query}"
            return redirect(target, permanent=False)

        return self.get_response(request)


class BranchCaptureOnlyMiddleware:
    """
    Restringe usuarios en modo captura sucursal a su módulo operativo mínimo.
    """

    ALLOWED_PREFIXES = (
        "/recetas/reabasto-cedis/",
        "/logout/",
        "/login/",
        "/static/",
        "/favicon.ico",
    )

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        user = getattr(request, "user", None)
        path = request.path or "/"

        if (
            user
            and user.is_authenticated
            and is_branch_capture_only(user)
            and not any(path.startswith(prefix) for prefix in self.ALLOWED_PREFIXES)
        ):
            return redirect("/recetas/reabasto-cedis/captura/")

        return self.get_response(request)


class EnsureCSRFCookieOnHtmlMiddleware:
    """
    Fuerza la emisión de cookie CSRF en vistas HTML seguras para evitar
    sesiones que navegan bien pero fallan en el primer POST por falta de cookie.
    """

    SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        accept = (request.headers.get("Accept") or "").lower()
        wants_html = "text/html" in accept or "*/*" in accept or not accept

        if request.method in self.SAFE_METHODS and wants_html:
            get_token(request)

        response = self.get_response(request)
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type:
            response["X-ERP-Build"] = ERP_BUILD_TAG
        return response


class PerformanceLoggingMiddleware:
    """
    Observabilidad ligera y reversible para detectar endpoints y SQL lentos.
    """

    def __init__(self, get_response):
        self.get_response = get_response
        self.enabled = getattr(settings, "ERP_PERF_LOGGING_ENABLED", False)
        self.endpoint_threshold_ms = float(getattr(settings, "ERP_SLOW_ENDPOINT_MS", 1000))
        self.query_threshold_ms = float(getattr(settings, "ERP_SLOW_QUERY_MS", 200))

    def __call__(self, request):
        if not self.enabled:
            return self.get_response(request)

        started_at = time.perf_counter()
        slow_queries: list[_QuerySample] = []

        with ExitStack() as stack:
            for alias in connections:
                wrapper = _ConnectionTimingWrapper(
                    alias=alias,
                    slow_query_ms=self.query_threshold_ms,
                    collector=slow_queries,
                )
                stack.enter_context(connections[alias].execute_wrapper(wrapper))
            response = self.get_response(request)

        total_ms = (time.perf_counter() - started_at) * 1000.0
        if total_ms >= self.endpoint_threshold_ms:
            performance_logger.warning(
                "slow_endpoint path=%s method=%s status=%s total_ms=%.2f slow_queries=%s",
                request.path,
                request.method,
                getattr(response, "status_code", "n/a"),
                total_ms,
                len(slow_queries),
            )

        for sample in slow_queries:
            performance_logger.warning(
                "slow_query path=%s db=%s duration_ms=%.2f sql=%s",
                request.path,
                sample.alias,
                sample.duration_ms,
                sample.sql,
            )
        return response
