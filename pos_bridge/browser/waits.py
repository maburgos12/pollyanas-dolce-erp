from __future__ import annotations

from pos_bridge.utils.exceptions import NavigationError


def find_first(page, selectors: list[str], timeout_ms: int = 500):
    for selector in selectors:
        try:
            locators = page.locator(selector)
            count = locators.count()
        except Exception:
            continue
        for index in range(count):
            candidate = locators.nth(index)
            try:
                candidate.wait_for(state="visible", timeout=timeout_ms)
                return candidate
            except Exception:
                continue
    return None


def require_first(page, selectors: list[str], description: str, timeout_ms: int = 1500):
    locator = find_first(page, selectors, timeout_ms=timeout_ms)
    if locator is None:
        raise NavigationError(f"No se encontró {description}", context={"selectors": selectors})
    return locator


def fill_first(page, selectors: list[str], value: str, description: str, timeout_ms: int = 1500):
    locator = require_first(page, selectors, description, timeout_ms=timeout_ms)
    locator.fill(value)
    return locator


def click_first(page, selectors: list[str], description: str, timeout_ms: int = 1500):
    locator = require_first(page, selectors, description, timeout_ms=timeout_ms)
    locator.wait_for(state="visible", timeout=timeout_ms)
    locator.click()
    return locator


def wait_for_any(page, selectors: list[str], description: str, timeout_ms: int = 5000):
    locator = require_first(page, selectors, description, timeout_ms=timeout_ms)
    locator.wait_for(state="visible", timeout=timeout_ms)
    return locator
