#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATHS = [
    ROOT / "templates",
    *[path / "templates" for path in ROOT.iterdir() if path.is_dir() and (path / "templates").exists()],
]
SKIP_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    "staticfiles",
    "media",
}
FLOAT_RE = re.compile(r"\|floatformat:(?:0|2)(?!\|intcomma)\b")
VARIABLE_RE = re.compile(r"{{.*?}}", re.DOTALL)
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?</\1>", re.IGNORECASE | re.DOTALL)
LOAD_RE = re.compile(r"{%\s*load\s+([^%]+?)\s*%}")
EXTENDS_RE = re.compile(r"({%\s*extends\s+[^%]+?%\}\s*)")


@dataclass(frozen=True)
class Finding:
    file: str
    line: int
    token: str
    reason: str


def iter_template_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for base in paths:
        if not base.exists():
            continue
        for path in base.rglob("*.html"):
            if not path.is_file():
                continue
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            files.append(path)
    return sorted(set(files))


def protected_ranges(text: str) -> list[range]:
    return [range(match.start(), match.end()) for match in SCRIPT_STYLE_RE.finditer(text)]


def in_ranges(pos: int, ranges: list[range]) -> bool:
    return any(pos in protected for protected in ranges)


def in_html_tag(text: str, pos: int) -> bool:
    last_open = text.rfind("<", 0, pos)
    last_close = text.rfind(">", 0, pos)
    return last_open > last_close


def line_number(text: str, pos: int) -> int:
    return text.count("\n", 0, pos) + 1


def token_context(text: str, match: re.Match[str], protected: list[range]) -> str:
    if in_ranges(match.start(), protected):
        return "script_or_style"
    if in_html_tag(text, match.start()):
        return "html_attribute"
    return "visible_text"


def add_humanize_load(text: str) -> str:
    load_match = LOAD_RE.search(text)
    if load_match:
        libraries = load_match.group(1).split()
        if "humanize" in libraries:
            return text
        updated = " ".join([*libraries, "humanize"])
        return text[: load_match.start(1)] + updated + text[load_match.end(1) :]

    extends_match = EXTENDS_RE.search(text)
    insert = "{% load humanize %}\n"
    if extends_match:
        return text[: extends_match.end()] + insert + text[extends_match.end() :]
    return insert + text


def analyze_file(path: Path) -> tuple[list[Finding], list[Finding]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    protected = protected_ranges(text)
    safe: list[Finding] = []
    skipped: list[Finding] = []

    for variable in VARIABLE_RE.finditer(text):
        if not FLOAT_RE.search(variable.group(0)):
            continue
        reason = token_context(text, variable, protected)
        finding = Finding(
            file=str(path.relative_to(ROOT)),
            line=line_number(text, variable.start()),
            token=" ".join(variable.group(0).split()),
            reason=reason,
        )
        if reason == "visible_text":
            safe.append(finding)
        else:
            skipped.append(finding)
    return safe, skipped


def fix_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8", errors="ignore")
    protected = protected_ranges(text)
    replacements = 0
    parts: list[str] = []
    cursor = 0

    for variable in VARIABLE_RE.finditer(text):
        if token_context(text, variable, protected) != "visible_text":
            continue
        updated, count = FLOAT_RE.subn(lambda match: f"{match.group(0)}|intcomma", variable.group(0))
        if count == 0:
            continue
        parts.append(text[cursor : variable.start()])
        parts.append(updated)
        cursor = variable.end()
        replacements += count

    if replacements == 0:
        return 0

    parts.append(text[cursor:])
    fixed = add_humanize_load("".join(parts))
    path.write_text(fixed, encoding="utf-8")
    return replacements


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audita numeros visibles sin separador de miles en templates Django."
    )
    parser.add_argument("--json", action="store_true", help="Imprime el resultado en JSON.")
    parser.add_argument("--fix", action="store_true", help="Corrige solo tokens visibles seguros.")
    args = parser.parse_args()

    files = iter_template_files(DEFAULT_PATHS)
    safe: list[Finding] = []
    skipped: list[Finding] = []
    fixed: dict[str, int] = {}

    for path in files:
        file_safe, file_skipped = analyze_file(path)
        safe.extend(file_safe)
        skipped.extend(file_skipped)
        if args.fix and file_safe:
            count = fix_file(path)
            if count:
                fixed[str(path.relative_to(ROOT))] = count

    result = {
        "files_scanned": len(files),
        "safe_visible_missing_intcomma": len(safe),
        "skipped_technical_context": len(skipped),
        "fixed": fixed,
        "safe_findings": [finding.__dict__ for finding in safe[:200]],
        "skipped_findings": [finding.__dict__ for finding in skipped[:80]],
    }

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"files={result['files_scanned']} "
            f"safe_visible_missing_intcomma={result['safe_visible_missing_intcomma']} "
            f"skipped_technical_context={result['skipped_technical_context']}"
        )
        if fixed:
            print("Fixed:")
            for file, count in sorted(fixed.items()):
                print(f"- {file}: {count}")
        elif safe:
            print("Visible findings:")
            for finding in safe[:40]:
                print(f"- {finding.file}:{finding.line} {finding.token}")
        if skipped:
            print("Skipped technical contexts:")
            for finding in skipped[:20]:
                print(f"- {finding.file}:{finding.line} [{finding.reason}] {finding.token}")

    return 1 if safe and not args.fix else 0


if __name__ == "__main__":
    raise SystemExit(main())
