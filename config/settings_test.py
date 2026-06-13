from .settings import *  # noqa: F401,F403

import os


DEBUG = True
SECRET_KEY = SECRET_KEY or "test-key"
GOOGLE_SERVER_API_KEY = ""
# Ruta oficial de pruebas:
# este proyecto incluye migraciones y SQL de PostgreSQL (por ejemplo MATERIALIZED VIEW en reportes),
# por lo que SQLite no es un backend honesto para la suite general.
#
# settings_test debe reutilizar PostgreSQL cuando exista DATABASE_URL o DB_HOST.
TEST_DATABASE_URL = (os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()

if TEST_DATABASE_URL:
    DATABASES = {
        "default": dj_database_url.config(
            default=TEST_DATABASE_URL,
            conn_max_age=0,
            ssl_require=False,
        )
    }
    if DATABASES["default"].get("ENGINE") == "django.db.backends.postgresql":
        DATABASES["default"]["TEST"] = {
            "NAME": os.getenv("TEST_DB_NAME", "test_pastelerias_erp"),
        }
elif os.getenv("DB_HOST"):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.getenv("DB_NAME", "pastelerias_erp"),
            "USER": os.getenv("DB_USER", "postgres"),
            "PASSWORD": os.getenv("DB_PASSWORD", "postgres"),
            "HOST": os.getenv("DB_HOST", "localhost"),
            "PORT": os.getenv("DB_PORT", "5432"),
            "TEST": {
                "NAME": os.getenv("TEST_DB_NAME", "test_pastelerias_erp"),
            },
        }
    }
else:
    raise ValueError(
        "config.settings_test requiere PostgreSQL via TEST_DATABASE_URL, DATABASE_URL o DB_HOST. "
        "SQLite ya no es una ruta valida para la suite de pruebas del ERP."
    )

# Entorno de pruebas local: evita dependencia de whitenoise en la venv local.
MIDDLEWARE = [m for m in MIDDLEWARE if m != "whitenoise.middleware.WhiteNoiseMiddleware"]
if "STATICFILES_STORAGE" in globals():
    del STATICFILES_STORAGE
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"},
}
