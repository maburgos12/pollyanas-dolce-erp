"""Check executable JavaScript, not just the presence of chart canvases."""

import re
import shutil
import subprocess
from unittest import skipUnless

from django.contrib.auth.models import AnonymousUser
from django.template.loader import render_to_string
from django.test import RequestFactory, SimpleTestCase


@skipUnless(shutil.which("node"), "Node.js is required to validate chart JavaScript")
class ReportChartJavaScriptTests(SimpleTestCase):
    def test_dashboard_scripts_parse_after_template_rendering(self):
        request = RequestFactory().get("/reportes/")
        request.user = AnonymousUser()
        for template in ("reportes/ventas.html", "reportes/bi.html"):
            with self.subTest(template=template):
                html = render_to_string(template, {"current_year": 2026, "request": request})
                scripts = re.findall(r"<script>(.*?)</script>", html, flags=re.DOTALL)
                self.assertTrue(scripts)
                self.assertTrue(any("new Chart(" in script for script in scripts))
                for index, script in enumerate(scripts):
                    with self.subTest(script=index):
                        result = subprocess.run(
                            [shutil.which("node"), "--check"],
                            input=script,
                            text=True,
                            capture_output=True,
                            timeout=10,
                            check=False,
                        )
                        self.assertEqual(result.returncode, 0, result.stderr)
