from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping

from django.conf import settings
from django.core.cache import cache


DEFAULT_SCOPE_VERSION = 1
DEFAULT_VERSIONED_CACHE_TTL = int(getattr(settings, "ERP_VERSIONED_CACHE_TTL_SECONDS", 900) or 900)


def _scope_key(scope: str) -> str:
    return f"erp:version:{(scope or 'default').strip().lower()}"


def get_cache_scope_version(scope: str) -> int:
    key = _scope_key(scope)
    try:
        version = cache.get(key)
        if version is None:
            cache.add(key, DEFAULT_SCOPE_VERSION, timeout=None)
            version = cache.get(key, DEFAULT_SCOPE_VERSION)
        return int(version or DEFAULT_SCOPE_VERSION)
    except Exception:
        return DEFAULT_SCOPE_VERSION


def bump_cache_scopes(*scopes: str) -> dict[str, int]:
    bumped: dict[str, int] = {}
    for raw_scope in scopes:
        scope = (raw_scope or "").strip().lower()
        if not scope:
            continue
        key = _scope_key(scope)
        try:
            cache.add(key, DEFAULT_SCOPE_VERSION, timeout=None)
            bumped[scope] = int(cache.incr(key))
        except ValueError:
            cache.set(key, DEFAULT_SCOPE_VERSION + 1, timeout=None)
            bumped[scope] = DEFAULT_SCOPE_VERSION + 1
        except Exception:
            bumped[scope] = DEFAULT_SCOPE_VERSION
    return bumped


def versioned_cache_key(*parts: object, scopes: Iterable[str]) -> str:
    normalized_parts = [str(part).strip(":") for part in parts if str(part or "").strip(":")]
    version_token = ":".join(
        f"{scope}:v{get_cache_scope_version(scope)}"
        for scope in [str(scope or "").strip().lower() for scope in scopes]
        if scope
    )
    if version_token:
        normalized_parts.append(version_token)
    return ":".join(normalized_parts)


def get_or_set_versioned_cache(
    *,
    key_parts: Iterable[object],
    scopes: Iterable[str],
    builder: Callable[[], object],
    timeout: int | None = None,
    runtime_cache: dict[str, object] | None = None,
):
    key = versioned_cache_key(*list(key_parts), scopes=scopes)
    if runtime_cache is not None and key in runtime_cache:
        return runtime_cache[key]
    try:
        cached_value = cache.get(key)
    except Exception:
        cached_value = None
    if cached_value is not None:
        if runtime_cache is not None:
            runtime_cache[key] = cached_value
        return cached_value

    value = builder()
    try:
        cache.set(key, value, timeout=timeout or DEFAULT_VERSIONED_CACHE_TTL)
    except Exception:
        pass
    if runtime_cache is not None:
        runtime_cache[key] = value
    return value
