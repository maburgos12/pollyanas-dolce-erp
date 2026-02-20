from .settings import *  # noqa: F401,F403


DEBUG = True
SECRET_KEY = SECRET_KEY or "test-key"

# Entorno de pruebas local: evita dependencia de whitenoise en la venv local.
MIDDLEWARE = [m for m in MIDDLEWARE if m != "whitenoise.middleware.WhiteNoiseMiddleware"]
if "STATICFILES_STORAGE" in globals():
    del STATICFILES_STORAGE
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"},
}
