from unittest import TestCase

from lxml import html as lxml_html

from core.email_rendering import inline_email_styles


class InlineEmailStylesTests(TestCase):
    def test_existing_inline_declaration_takes_precedence_over_stylesheet(self):
        rendered = inline_email_styles(
            '<html><body><p style="color: red;">Message</p></body></html>',
            css_text="p { color: blue; padding: 4px; }",
        )

        document = lxml_html.document_fromstring(rendered)
        paragraph = document.xpath("//p")[0]
        declarations = {
            property_name.strip(): value.strip()
            for declaration in paragraph.get("style").split(";")
            if ":" in declaration
            for property_name, value in [declaration.split(":", 1)]
        }

        self.assertEqual(declarations["color"], "red")
        self.assertEqual(declarations["padding"], "4px")
