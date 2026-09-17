from django.apps import AppConfig


class RrhhConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "rrhh"
    verbose_name = "RRHH"

    def ready(self):
        from . import signals_extra  # noqa: F401
