from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase


class DashboardCutRefreshPollingTests(SimpleTestCase):
    def test_pending_cut_refresh_polls_without_reloading_page(self):
        template = (
            Path(settings.BASE_DIR) / "core" / "templates" / "core" / "dashboard_executive.html"
        ).read_text(encoding="utf-8")
        script_path = (
            Path(settings.BASE_DIR) / "static" / "js" / "dashboard_cut_refresh.js"
        )

        sales_template = (Path(settings.BASE_DIR) / "core/templates/core/partials/dashboard_cut_sales.html").read_text()
        self.assertIn("data-cut-refresh-poll", sales_template)
        self.assertIn("js/dashboard_cut_refresh.js", template)
        self.assertTrue(script_path.exists())

        script = script_path.read_text(encoding="utf-8")
        self.assertIn('form[data-cut-refresh-poll]', script)
        self.assertIn('[aria-busy="true"]', script)
        self.assertIn("window.setTimeout", script)
        self.assertNotIn("window.location.reload()", script)
        self.assertIn("visibilitychange", script)
        self.assertIn("cut_refresh", script)
