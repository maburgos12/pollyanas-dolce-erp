from django.apps import AppConfig


class LogisticaConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "logistica"

    def ready(self):
        import logistica.checks  # noqa: F401
        import logistica.signals  # noqa: F401
