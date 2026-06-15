from __future__ import annotations

from pathlib import Path
import re

from django.conf import settings
from django.core.checks import Error, register

from core.hallmark_ui_audit import BASELINE_PATH, new_issues_against_baseline


ASSIGNABLE_GUARDRAIL_GLOBS = ("**/views.py", "**/api_views.py", "**/serializers.py", "**/forms.py")
ASSIGNABLE_GUARDRAIL_SKIP_PARTS = {".venv", "migrations", "tests", "management"}
ALLOW_INACTIVE_MARKER = "rrhh-allow-inactive-history"
ASSIGNABLE_PATTERNS = (
    (re.compile(r"queryset\s*=\s*(?:get_user_model\(\)|User)\.objects\.all\("), "usa filter(is_active=True)"),
    (re.compile(r"(?:get_user_model\(\)|User)\.objects\.get\(pk="), "usa get(pk=..., is_active=True)"),
    (re.compile(r"(?:get_user_model\(\)|User)\.objects\.all\("), "usa filter(is_active=True)"),
    (re.compile(r"Empleado\.objects\.all\("), "usa filter(activo=True) o marca historial explicito"),
    (re.compile(r"(?:empleados|empleados_qs)\s*=\s*Empleado\.objects\.filter\((?![^#\n]*activo=True)"), "usa filter(activo=True)"),
)


def _iter_assignable_source_files(base_dir: Path):
    seen = set()
    for pattern in ASSIGNABLE_GUARDRAIL_GLOBS:
        for path in base_dir.glob(pattern):
            if path in seen or any(part in ASSIGNABLE_GUARDRAIL_SKIP_PARTS for part in path.parts):
                continue
            seen.add(path)
            yield path


@register()
def active_personnel_assignment_guardrail(app_configs, **kwargs):
    base_dir = Path(settings.BASE_DIR)
    errors = []
    for path in _iter_assignable_source_files(base_dir):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        rel = path.relative_to(base_dir)
        for lineno, line in enumerate(lines, start=1):
            if ALLOW_INACTIVE_MARKER in line:
                continue
            if ("User" in line or "get_user_model" in line) and "is_active=True" in line:
                continue
            if "Empleado" in line and "activo=True" in line:
                continue
            for pattern, hint in ASSIGNABLE_PATTERNS:
                if pattern.search(line):
                    errors.append(
                        Error(
                            f"Lista/asignacion de personal sin filtro activo en {rel}:{lineno}.",
                            hint=f"{hint}. Si es historial, agrega comentario {ALLOW_INACTIVE_MARKER}.",
                            id="rrhh.E901",
                        )
                    )
                    break
    return errors


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
