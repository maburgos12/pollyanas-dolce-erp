from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path


BASELINE_PATH = Path("docs/hallmark_ui_audit_baseline.json")


@dataclass(frozen=True)
class HallmarkIssue:
    rule: str
    path: str
    snippet: str
    message: str

    @property
    def key(self) -> str:
        return f"{self.rule}|{self.path}|{self.snippet}"


SKIP_DIR_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    "staticfiles",
    "storage",
}

TEMPLATE_EXEMPTIONS = (
    "pwa_",
    "_imprimir",
    "imprimir",
    "print",
    "email",
    "pdf",
)

INLINE_STYLE_RE = re.compile(r'style=["\'][^"\']*(?:min-width|max-width|width)\s*:', re.IGNORECASE)
WIDE_GRID_RE = re.compile(r"grid-template-columns\s*:\s*repeat\(\s*([5-9]|\d{2,})\s*,", re.IGNORECASE)
HARD_OVERFLOW_RE = re.compile(r"overflow-x\s*:\s*hidden", re.IGNORECASE)
NOWRAP_RE = re.compile(r"white-space\s*:\s*nowrap", re.IGNORECASE)
LOCAL_MODULE_TABS_LAYOUT_RE = re.compile(
    r"\.module-tabs[^{\n]*\{[^}\n]*(?:grid-template-columns|width\s*:|max-width\s*:)|"
    r"\.module-tab[^{\n]*\{[^}\n]*(?:flex-wrap\s*:\s*nowrap|overflow\s*:\s*hidden|text-overflow\s*:\s*ellipsis)",
    re.IGNORECASE,
)


def _is_skipped(path: Path) -> bool:
    return any(part in SKIP_DIR_PARTS for part in path.parts)


def _relative(base_dir: Path, path: Path) -> str:
    return path.relative_to(base_dir).as_posix()


def _clean_snippet(text: str, *, limit: int = 140) -> str:
    snippet = " ".join(text.strip().split())
    return snippet[:limit]


def _line_matches(pattern: re.Pattern[str], text: str) -> list[str]:
    return [line for line in text.splitlines() if pattern.search(line)]


def iter_ui_files(base_dir: Path) -> list[Path]:
    suffixes = {".html", ".css"}
    files: list[Path] = []
    for path in base_dir.rglob("*"):
        if _is_skipped(path) or not path.is_file() or path.suffix not in suffixes:
            continue
        if path.suffix == ".html" and "/templates/" not in path.as_posix() and path.parent.name != "templates":
            continue
        files.append(path)
    return sorted(files)


def scan_hallmark_ui(base_dir: Path) -> list[HallmarkIssue]:
    issues: list[HallmarkIssue] = []
    for path in iter_ui_files(base_dir):
        rel_path = _relative(base_dir, path)
        text = path.read_text(encoding="utf-8", errors="ignore")
        lower_name = path.name.lower()
        is_template = path.suffix == ".html"
        is_base_template = "{% extends \"base.html\" %}" in text or "{% extends 'base.html' %}" in text
        is_exempt_template = any(token in lower_name for token in TEMPLATE_EXEMPTIONS)

        if is_template and is_base_template and "<table" in text and "table-responsive" not in text and not is_exempt_template:
            issues.append(
                HallmarkIssue(
                    "table-without-responsive-wrapper",
                    rel_path,
                    "<table",
                    "Las tablas de vistas ERP deben vivir dentro de .table-responsive para que el scroll sea interno.",
                )
            )

        if (
            is_template
            and "module-tabs" in text
            and "rrhh-tabs" not in text
            and "report-tabs" not in text
            and "mant-tabs" not in text
            and not is_exempt_template
        ):
            issues.append(
                HallmarkIssue(
                    "module-tabs-without-responsive-family",
                    rel_path,
                    "module-tabs",
                    "Los tabs operativos deben usar una familia responsive conocida, por ejemplo .module-tabs.rrhh-tabs o .module-tabs.report-tabs.",
                )
            )

        for line in _line_matches(WIDE_GRID_RE, text):
            issues.append(
                HallmarkIssue(
                    "rigid-wide-grid",
                    rel_path,
                    _clean_snippet(line),
                    "Evita repeat(5+) fijo; usa auto-fit/minmax para que los cards no se aplasten.",
                )
            )

        for line in _line_matches(INLINE_STYLE_RE, text):
            issues.append(
                HallmarkIssue(
                    "inline-fixed-width",
                    rel_path,
                    _clean_snippet(line),
                    "No fijes anchos inline en templates; usa clases cubiertas por guardrails.",
                )
            )

        for line in _line_matches(HARD_OVERFLOW_RE, text):
            if "hallmark_guardrails.css" in rel_path:
                continue
            issues.append(
                HallmarkIssue(
                    "overflow-hidden-x",
                    rel_path,
                    _clean_snippet(line),
                    "No ocultes overflow horizontal para tapar errores; usa contencion real o overflow-x: clip global.",
                )
            )

        for line in _line_matches(NOWRAP_RE, text):
            if "hallmark_guardrails.css" in rel_path:
                continue
            issues.append(
                HallmarkIssue(
                    "local-nowrap",
                    rel_path,
                    _clean_snippet(line),
                    "No fuerces nowrap local en tabs/chips/botones sin una regla responsive global.",
                )
            )

        for line in _line_matches(LOCAL_MODULE_TABS_LAYOUT_RE, text):
            if "hallmark_guardrails.css" in rel_path:
                continue
            issues.append(
                HallmarkIssue(
                    "local-module-tabs-layout",
                    rel_path,
                    _clean_snippet(line),
                    "No redefinas ancho o truncado de .module-tabs/.module-tab fuera de Hallmark; mueve esa distribucion a hallmark_guardrails.css.",
                )
            )

    return sorted(issues, key=lambda issue: issue.key)


def load_baseline(base_dir: Path) -> set[str]:
    path = base_dir / BASELINE_PATH
    if not path.exists():
        return set()
    data = json.loads(path.read_text(encoding="utf-8"))
    return {item["key"] for item in data.get("accepted_issues", [])}


def write_baseline(base_dir: Path, issues: list[HallmarkIssue]) -> None:
    path = base_dir / BASELINE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "description": (
            "Baseline de deuda visual Hallmark existente. No se debe ampliar salvo "
            "revision explicita; el test falla cuando aparecen issues nuevos."
        ),
        "accepted_issues": [
            {
                "key": issue.key,
                **asdict(issue),
            }
            for issue in issues
        ],
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def new_issues_against_baseline(base_dir: Path) -> list[HallmarkIssue]:
    baseline = load_baseline(base_dir)
    return [issue for issue in scan_hallmark_ui(base_dir) if issue.key not in baseline]
