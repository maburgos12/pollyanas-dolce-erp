from django.apps import AppConfig

class RecetasConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "recetas"
    verbose_name = "Recetas"

    def ready(self):
        from . import signals  # noqa: F401
