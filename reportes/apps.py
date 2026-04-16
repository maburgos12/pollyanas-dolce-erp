from django.apps import AppConfig
from django.conf import settings


SKIP_STRICT_STARTUP_COMMANDS = {
    "check",
    "collectstatic",
    "dbshell",
    "dumpdata",
    "flush",
    "loaddata",
    "makemigrations",
    "migrate",
    "shell",
    "showmigrations",
    "sqlmigrate",
    "test",
}


class ReportesConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "reportes"

    def ready(self):
        import sys

        from reportes.checks import assert_critical_product_business_rules_present

        # Register Django system checks for `manage.py check` and runserver.
        from . import checks  # noqa: F401
        from . import signals  # noqa: F401

        if not getattr(settings, "PRODUCT_BUSINESS_RULES_ENFORCE_ON_STARTUP", False):
            return
        current_command = sys.argv[1] if len(sys.argv) > 1 else ""
        if current_command in SKIP_STRICT_STARTUP_COMMANDS:
            return
        assert_critical_product_business_rules_present()
