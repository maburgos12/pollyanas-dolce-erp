from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.checks import Error, register

from core.hallmark_ui_audit import BASELINE_PATH, new_issues_against_baseline


@register()
def hallmark_ui_guardrail_check(app_configs, **kwargs):
    base_dir = Path(settings.BASE_DIR)
    baseline = base_dir / BASELINE_PATH
    if not baseline.exists():
        return [
            Error(
                "Falta baseline Hallmark UI.",
                hint=f"Ejecuta python manage.py check_hallmark_ui --update-baseline y revisa {BASELINE_PATH}.",
                id="hallmark.E001",
            )
        ]

    issues = new_issues_against_baseline(base_dir)
    if not issues:
        return []

    detail = "; ".join(f"{issue.rule} en {issue.path}" for issue in issues[:5])
    return [
        Error(
            f"Hallmark UI detecto {len(issues)} regresion(es) visual(es) nueva(s).",
            hint=f"{detail}. Ejecuta python manage.py check_hallmark_ui para ver detalles.",
            id="hallmark.E002",
        )
    ]
