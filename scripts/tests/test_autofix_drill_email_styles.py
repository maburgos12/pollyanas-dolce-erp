from __future__ import annotations

import unittest

from core.email_rendering import inline_email_styles


class InlineEmailStylesTests(unittest.TestCase):
    def test_existing_inline_declaration_overrides_stylesheet_declaration(self):
        rendered = inline_email_styles(
            '<html><body><p class="notice" style="color: red">Alert</p></body></html>',
            ".notice { color: blue; font-weight: bold; }",
        )

        self.assertIn(
            'style="color: red; font-weight: bold;"',
            rendered,
        )
        self.assertNotIn("color: blue", rendered)


if __name__ == "__main__":
    unittest.main()
