from __future__ import annotations

import logging

from django.apps import apps
from django.conf import settings
from django.core.checks import Error, Warning, register
from django.core.exceptions import ImproperlyConfigured
from django.db import connection
from django.db.utils import OperationalError, ProgrammingError

from reportes.product_business_rules import (
    CRITICAL_FIXED_REVENTA_PRODUCT_NAMES,
    normalize_product_name,
)

logger = logging.getLogger(__name__)


def _critical_rule_expectations() -> dict[str, tuple[str, bool]]:
    return {
        normalize_product_name(product_name): ("REVENTA", True)
        for product_name in CRITICAL_FIXED_REVENTA_PRODUCT_NAMES
    }


def collect_critical_product_business_rule_issues() -> list[str]:
    ProductBusinessRule = apps.get_model("reportes", "ProductBusinessRule")
    table_name = ProductBusinessRule._meta.db_table

    try:
        if table_name not in connection.introspection.table_names():
            return [
                "La tabla reportes_productbusinessrule no existe; aplica las migraciones 0009-0011 antes de retirar el fallback."
            ]

        expected = _critical_rule_expectations()
        rows = ProductBusinessRule.objects.filter(normalized_name__in=expected.keys()).only(
            "normalized_name",
            "classification",
            "is_fixed",
        )
        existing = {
            row.normalized_name: (row.classification, row.is_fixed)
            for row in rows
            if row.normalized_name
        }
    except (OperationalError, ProgrammingError) as exc:
        return [f"No fue posible validar ProductBusinessRule: {exc}"]

    issues: list[str] = []
    for normalized_name, (expected_classification, expected_is_fixed) in expected.items():
        current = existing.get(normalized_name)
        if current is None:
            issues.append(f"Falta regla crítica ProductBusinessRule para '{normalized_name}'.")
            continue
        classification, is_fixed = current
        if classification != expected_classification or is_fixed is not expected_is_fixed:
            issues.append(
                f"Regla crítica ProductBusinessRule inconsistente para '{normalized_name}': "
                f"classification={classification}, is_fixed={is_fixed}."
            )
    return issues


def assert_critical_product_business_rules_present() -> None:
    issues = collect_critical_product_business_rule_issues()
    if not issues:
        return
    message = " | ".join(issues)
    logger.error("critical ProductBusinessRule validation failed: %s", message)
    raise ImproperlyConfigured(message)


@register()
def check_critical_product_business_rules(app_configs, **kwargs):
    issues = collect_critical_product_business_rule_issues()
    if not issues:
        return []

    message = " | ".join(issues)
    logger.error("critical ProductBusinessRule validation failed: %s", message)
    issue_class = Error if getattr(settings, "PRODUCT_BUSINESS_RULES_ENFORCE_ON_STARTUP", False) else Warning
    return [
        issue_class(
            "Faltan reglas críticas en ProductBusinessRule.",
            hint=message,
            id="reportes.E001" if issue_class is Error else "reportes.W001",
        )
    ]
