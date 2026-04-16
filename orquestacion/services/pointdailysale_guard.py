from __future__ import annotations

import ast
import fnmatch
from dataclasses import dataclass
from pathlib import Path


POINT_DAILY_SALE_SYMBOL = "PointDailySale"
POINT_DAILY_SALE_ALLOWED_GLOBS = (
    "core/signals.py",
    "integraciones/views.py",
    "pos_bridge/admin.py",
    "pos_bridge/api/*.py",
    "pos_bridge/api/**/*.py",
    "pos_bridge/management/**/*.py",
    "pos_bridge/models/*.py",
    "pos_bridge/models/**/*.py",
    "pos_bridge/models.py",
    "pos_bridge/services/official_sales_backfill_service.py",
    "pos_bridge/services/product_month_closure_service.py",
    "pos_bridge/services/sales_cutoff_service.py",
    "pos_bridge/services/sales_materialization_repair_service.py",
    "pos_bridge/services/sales_report_reconciliation_service.py",
    "pos_bridge/services/sync_service.py",
    "pos_bridge/services/sales_pipeline/rebuild_service.py",
    "pos_bridge/tasks/*.py",
    "pos_bridge/tasks/**/*.py",
    "recetas/utils/*.py",
    "recetas/utils/**/*.py",
    "recetas/views.py",
    "reportes/analytics_service.py",
    "reportes/daily_operational_closure_service.py",
    "reportes/executive_panels.py",
    "reportes/management/**/*.py",
    "reportes/sales_dashboard_freshness.py",
    "reportes/services_investment_projects.py",
    "reportes/services_operating_finance.py",
    "reportes/signals.py",
    "orquestacion/services/sales_publication_guard.py",
    "ventas/services/financials.py",
    "ventas/services/forecasting.py",
    "ventas/services/point_reconciliation.py",
    "ventas/services/sales_canonical_source.py",
    "ventas/services/sales_read_service.py",
    "ventas/services/sales_truth.py",
)
POINT_DAILY_SALE_IGNORED_GLOBS = (
    ".git/**",
    ".venv/**",
    "docs/**",
    "**/__pycache__/**",
    "**/migrations/*.py",
    "**/tests.py",
    "**/tests_*.py",
    "**/test_*.py",
)
POINT_DAILY_SALE_SUGGESTION = (
    "Usa ventas/services/sales_read_service.py o ventas/services/sales_canonical_source.py "
    "en lugar de leer PointDailySale directo desde capas visibles, gateway o consultas operativas."
)


@dataclass(frozen=True)
class GuardViolation:
    relative_path: str
    line_number: int
    reason: str
    suggestion: str


@dataclass(frozen=True)
class GuardScanResult:
    checked_files: int
    violations: tuple[GuardViolation, ...]

    @property
    def has_violations(self) -> bool:
        return bool(self.violations)


def scan_pointdailysale_usage(*, base_dir: str | Path) -> GuardScanResult:
    root = Path(base_dir).resolve()
    violations: list[GuardViolation] = []
    checked_files = 0

    for path in sorted(root.rglob("*.py")):
        relative_path = path.relative_to(root).as_posix()
        if _matches_any(relative_path, POINT_DAILY_SALE_IGNORED_GLOBS):
            continue
        checked_files += 1
        if _matches_any(relative_path, POINT_DAILY_SALE_ALLOWED_GLOBS):
            continue

        source = path.read_text(encoding="utf-8")
        if POINT_DAILY_SALE_SYMBOL not in source:
            continue

        for line_number in _detect_symbol_lines(source):
            violations.append(
                GuardViolation(
                    relative_path=relative_path,
                    line_number=line_number,
                    reason=(
                        "Uso directo no autorizado de PointDailySale fuera de la allowlist "
                        "canónica del ERP."
                    ),
                    suggestion=POINT_DAILY_SALE_SUGGESTION,
                )
            )

    return GuardScanResult(checked_files=checked_files, violations=tuple(violations))


def is_allowed_pointdailysale_path(relative_path: str) -> bool:
    return _matches_any(relative_path, POINT_DAILY_SALE_ALLOWED_GLOBS)


def _matches_any(relative_path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(relative_path, pattern) for pattern in patterns)


def _detect_symbol_lines(source: str) -> list[int]:
    tree = ast.parse(source)
    lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if (node.module or "").startswith("pos_bridge.models"):
                if any(alias.name == POINT_DAILY_SALE_SYMBOL for alias in node.names):
                    lines.add(node.lineno)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.endswith(".PointDailySale"):
                    lines.add(node.lineno)
        elif isinstance(node, ast.Name) and node.id == POINT_DAILY_SALE_SYMBOL:
            lines.add(node.lineno)
    return sorted(lines)
