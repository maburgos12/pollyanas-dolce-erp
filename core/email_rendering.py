from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

from django.conf import settings
from django.contrib.staticfiles import finders
from django.template.loader import render_to_string
from lxml import html as lxml_html


EMAIL_CSS_STATIC_PATH = "css/pollyana_email.css"
CSS_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
CSS_RULE_RE = re.compile(r"(?P<selectors>[^{}]+)\{(?P<declarations>[^{}]+)\}", re.DOTALL)
TAG_RE = re.compile(r"^[a-zA-Z][\w-]*$")
TAG_CLASS_RE = re.compile(r"^(?P<tag>[a-zA-Z][\w-]*)\.(?P<class_name>[\w-]+)$")


def _parse_declarations(block: str) -> list[tuple[str, str]]:
    declarations: list[tuple[str, str]] = []
    for chunk in block.split(";"):
        if ":" not in chunk:
            continue
        property_name, value = chunk.split(":", 1)
        property_name = property_name.strip().lower()
        value = value.strip()
        if property_name and value:
            declarations.append((property_name, value))
    return declarations


def _parse_rules(css_text: str) -> list[tuple[str, list[tuple[str, str]]]]:
    css_text = CSS_COMMENT_RE.sub("", css_text)
    rules: list[tuple[str, list[tuple[str, str]]]] = []
    for match in CSS_RULE_RE.finditer(css_text):
        declarations = _parse_declarations(match.group("declarations"))
        if not declarations:
            continue
        for selector in match.group("selectors").split(","):
            selector = selector.strip()
            if selector:
                rules.append((selector, declarations))
    return rules


def _class_xpath(class_name: str, tag: str = "*") -> str:
    return f"//{tag}[contains(concat(' ', normalize-space(@class), ' '), ' {class_name} ')]"


def _select(document, selector: str):
    if selector.startswith("."):
        return document.xpath(_class_xpath(selector[1:]))
    tag_class_match = TAG_CLASS_RE.match(selector)
    if tag_class_match:
        return document.xpath(
            _class_xpath(
                tag_class_match.group("class_name"),
                tag=tag_class_match.group("tag").lower(),
            )
        )
    if TAG_RE.match(selector):
        return document.xpath(f"//{selector.lower()}")
    return []


def _merge_style(existing_style: str | None, declarations: list[tuple[str, str]]) -> str:
    merged: dict[str, str] = {}
    if existing_style:
        for property_name, value in _parse_declarations(existing_style):
            merged[property_name] = value
    for property_name, value in declarations:
        merged[property_name] = value
    return "; ".join(f"{property_name}: {value}" for property_name, value in merged.items()) + ";"


@lru_cache(maxsize=1)
def _load_email_css() -> str:
    static_path = finders.find(EMAIL_CSS_STATIC_PATH)
    if static_path:
        return Path(static_path).read_text(encoding="utf-8")
    return (Path(settings.BASE_DIR) / "static" / EMAIL_CSS_STATIC_PATH).read_text(encoding="utf-8")


def inline_email_styles(html_message: str, css_text: str | None = None) -> str:
    document = lxml_html.document_fromstring(html_message)
    for selector, declarations in _parse_rules(css_text or _load_email_css()):
        for element in _select(document, selector):
            element.set("style", _merge_style(element.get("style"), declarations))
    return "<!doctype html>\n" + lxml_html.tostring(document, encoding="unicode", method="html")


def render_email_to_string(template_name: str, context: dict | None = None, request=None, using=None) -> str:
    html_message = render_to_string(template_name, context=context, request=request, using=using)
    return inline_email_styles(html_message)
