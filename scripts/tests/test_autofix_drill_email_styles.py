from __future__ import annotations

import unittest

from lxml import html as lxml_html

from core.email_rendering import inline_email_styles


class InlineEmailStylesTests(unittest.TestCase):
    def test_existing_inline_style_overrides_stylesheet_declaration(self):
        rendered = inline_email_styles(
            '<html><body><p class="notice" style="color: blue">Hello</p></body></html>',
            css_text=".notice { color: red; font-weight: bold; }",
        )

        document = lxml_html.document_fromstring(rendered)
        notice = document.xpath("//p[@class='notice']")[0]

        self.assertEqual(
            notice.get("style"),
            "color: blue; font-weight: bold;",
        )
