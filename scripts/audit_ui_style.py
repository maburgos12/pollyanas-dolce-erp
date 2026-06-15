#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATHS = [
    ROOT / "templates",
    ROOT / "static",
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
TEXT_EXTENSIONS = {".html", ".css", ".js"}
INLINE_STYLE_RE = re.compile(r"\bstyle\s*=")
REACT_INLINE_STYLE_RE = re.compile(r"\bstyle\s*:\s*\{")
STYLE_BLOCK_RE = re.compile(r"<style\b", re.IGNORECASE)
HEX_RE = re.compile(r"#[0-9a-fA-F]{3,8}\b")
RGB_RE = re.compile(r"\brgba?\(")
HSL_RE = re.compile(r"\bhsla?\(")
OKLCH_RE = re.compile(r"\boklch\(")
TRANSITION_ALL_RE = re.compile(r"transition\s*:\s*all\b")
EMOJI_ICON_RE = re.compile(r"[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE0F]")


def is_email_template(path: Path) -> bool:
    rel_parts = path.relative_to(ROOT).parts
    return path.suffix == ".html" and "emails" in rel_parts


def iter_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for base in paths:
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix not in TEXT_EXTENSIONS:
                continue
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            files.append(path)
    return sorted(set(files))


def count_pattern(pattern: re.Pattern[str], text: str) -> int:
    return len(pattern.findall(text))


def main() -> int:
    parser = argparse.ArgumentParser(description="Audita deuda visual del ERP.")
    parser.add_argument("--json", action="store_true", help="Imprime solo JSON.")
    parser.add_argument("--max-inline-styles", type=int, default=None)
    parser.add_argument("--max-style-blocks", type=int, default=None)
    parser.add_argument("--max-react-inline-styles", type=int, default=None)
    parser.add_argument("--max-transition-all", type=int, default=None)
    parser.add_argument("--max-emoji-icons", type=int, default=None)
    parser.add_argument("--max-email-inline-styles", type=int, default=None)
    parser.add_argument("--max-email-style-blocks", type=int, default=None)
    parser.add_argument("--max-email-react-inline-styles", type=int, default=None)
    parser.add_argument("--max-email-transition-all", type=int, default=None)
    parser.add_argument("--max-email-emoji-icons", type=int, default=None)
    args = parser.parse_args()

    totals = {
        "files": 0,
        "screen_files": 0,
        "email_files": 0,
        "inline_styles": 0,
        "react_inline_styles": 0,
        "style_blocks": 0,
        "hex_colors": 0,
        "rgb_colors": 0,
        "hsl_colors": 0,
        "oklch_colors": 0,
        "transition_all": 0,
        "emoji_icons": 0,
        "email_inline_styles": 0,
        "email_react_inline_styles": 0,
        "email_style_blocks": 0,
        "email_hex_colors": 0,
        "email_rgb_colors": 0,
        "email_hsl_colors": 0,
        "email_oklch_colors": 0,
        "email_transition_all": 0,
        "email_emoji_icons": 0,
    }
    offenders: dict[str, dict[str, int]] = {}
    email_offenders: dict[str, dict[str, int]] = {}

    for path in iter_files(DEFAULT_PATHS):
        text = path.read_text(encoding="utf-8", errors="ignore")
        counts = {
            "inline_styles": count_pattern(INLINE_STYLE_RE, text),
            "react_inline_styles": count_pattern(REACT_INLINE_STYLE_RE, text),
            "style_blocks": count_pattern(STYLE_BLOCK_RE, text),
            "hex_colors": count_pattern(HEX_RE, text),
            "rgb_colors": count_pattern(RGB_RE, text),
            "hsl_colors": count_pattern(HSL_RE, text),
            "oklch_colors": count_pattern(OKLCH_RE, text),
            "transition_all": count_pattern(TRANSITION_ALL_RE, text),
            "emoji_icons": count_pattern(EMOJI_ICON_RE, text),
        }
        totals["files"] += 1
        if is_email_template(path):
            totals["email_files"] += 1
            for key, value in counts.items():
                totals[f"email_{key}"] += value
            if any(counts.values()):
                email_offenders[str(path.relative_to(ROOT))] = counts
        else:
            totals["screen_files"] += 1
            for key, value in counts.items():
                totals[key] += value
            if any(counts.values()):
                offenders[str(path.relative_to(ROOT))] = counts

    top_offenders = sorted(
        offenders.items(),
        key=lambda item: (
            item[1]["inline_styles"],
            item[1]["react_inline_styles"],
            item[1]["style_blocks"],
            item[1]["hex_colors"] + item[1]["rgb_colors"],
            item[1]["transition_all"],
            item[1]["emoji_icons"],
        ),
        reverse=True,
    )[:25]

    result = {
        "totals": totals,
        "top_offenders": [{"file": file, **counts} for file, counts in top_offenders],
        "email_exceptions": [
            {"file": file, **counts}
            for file, counts in sorted(
                email_offenders.items(),
                key=lambda item: (item[1]["inline_styles"], item[1]["style_blocks"]),
                reverse=True,
            )
        ],
    }

    failures = []
    thresholds = {
        "inline_styles": args.max_inline_styles,
        "react_inline_styles": args.max_react_inline_styles,
        "style_blocks": args.max_style_blocks,
        "transition_all": args.max_transition_all,
        "emoji_icons": args.max_emoji_icons,
        "email_inline_styles": args.max_email_inline_styles,
        "email_react_inline_styles": args.max_email_react_inline_styles,
        "email_style_blocks": args.max_email_style_blocks,
        "email_transition_all": args.max_email_transition_all,
        "email_emoji_icons": args.max_email_emoji_icons,
    }
    for key, limit in thresholds.items():
        if limit is not None and totals[key] > limit:
            failures.append(f"{key}: {totals[key]} > {limit}")

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(result["totals"], indent=2, ensure_ascii=False))
        print("\nTop offenders:")
        for offender in result["top_offenders"]:
            print(
                "- {file}: inline={inline_styles}, style_blocks={style_blocks}, "
                "react_inline={react_inline_styles}, colors={colors}, "
                "transition_all={transition_all}, emoji={emoji_icons}".format(
                    colors=offender["hex_colors"] + offender["rgb_colors"] + offender["hsl_colors"] + offender["oklch_colors"],
                    **offender,
                )
            )
        if result["email_exceptions"]:
            print("\nEmail source offenders:")
            for offender in result["email_exceptions"]:
                print(
                    "- {file}: inline={inline_styles}, style_blocks={style_blocks}, "
                    "react_inline={react_inline_styles}, colors={colors}".format(
                        colors=offender["hex_colors"] + offender["rgb_colors"] + offender["hsl_colors"] + offender["oklch_colors"],
                        **offender,
                    )
                )

    if failures:
        print("\nFAIL:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
