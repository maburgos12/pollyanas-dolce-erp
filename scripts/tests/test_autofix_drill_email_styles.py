import unittest

from core.email_rendering import _parse_declarations


class EmailStyleDeclarationParserTests(unittest.TestCase):
    def test_parses_valid_declarations_and_preserves_colons_in_values(self):
        declarations = _parse_declarations(
            " COLOR : red ; background-image: url(https://example.com/a.png); "
            "invalid; empty: ; : missing-name; font-weight: bold"
        )

        self.assertEqual(
            declarations,
            [
                ("color", "red"),
                ("background-image", "url(https://example.com/a.png)"),
                ("font-weight", "bold"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
