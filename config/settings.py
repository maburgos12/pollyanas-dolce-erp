"""Django settings for pastelerias_erp_sprint1 - Production Ready."""
from pathlib import Path
import os
from decouple import config as env_config
import dj_database_url

BASE_DIR = Path(__file__).resolve().parent.parent

# ============================================================================
# SECURITY SETTINGS
# ============================================================================
SECRET_KEY = env_config("SECRET_KEY", default="django-insecure-dev-key-change-me").strip()
DEBUG = env_config("DEBUG", default="False").lower() in ("true", "1", "yes")

# Parse ALLOWED_HOSTS safely
ALLOWED_HOSTS_STR = env_config("ALLOWED_HOSTS", default="localhost,127.0.0.1,0.0.0.0").strip()
ALLOWED_HOSTS = [h.strip() for h in ALLOWED_HOSTS_STR.split(",") if h.strip()]

# ============================================================================
# INSTALLED APPS
# ============================================================================
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "django_filters",
    "core",
    "maestros",
    "recetas",
    "api",
]

# ============================================================================
# MIDDLEWARE
# ============================================================================
MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"

# ============================================================================
# TEMPLATES
# ============================================================================
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

# ============================================================================
# DATABASE - Smart routing for Railway and Local
# ============================================================================
DATABASE_URL = env_config("DATABASE_URL", default="").strip()

if DATABASE_URL:
    # Railway mode: use DATABASE_URL
    DATABASES = {
        "default": dj_database_url.config(
            default=DATABASE_URL,
            conn_max_age=600,
            conn_health_checks=True,
        )
    }
else:
    # Local/Docker mode: use individual env vars
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": env_config("DB_NAME", default="pastelerias_erp").strip(),
            "USER": env_config("DB_USER", default="postgres").strip(),
            "PASSWORD": env_config("DB_PASSWORD", default="postgres").strip(),
            "HOST": env_config("DB_HOST", default="localhost").strip(),
            "PORT": env_config("DB_PORT", default="5432").strip(),
        }
    }

# ============================================================================
# PASSWORD VALIDATION
# ============================================================================
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# ============================================================================
# INTERNATIONALIZATION
# ============================================================================
LANGUAGE_CODE = "es-mx"
# Strip whitespace from timezone to prevent "Incorrect timezone setting" error
TIME_ZONE = env_config("TIME_ZONE", default="UTC").strip()
if not TIME_ZONE or TIME_ZONE.isspace():
    TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# ============================================================================
# STATIC FILES
# ============================================================================
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = [BASE_DIR / "static"] if os.path.exists(BASE_DIR / "static") else []
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ============================================================================
# CORS & API
# ============================================================================
CORS_ALLOW_ALL_ORIGINS = True

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.SessionAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    "DEFAULT_FILTER_BACKENDS": [
        "django_filters.rest_framework.DjangoFilterBackend",
    ],
}

# ============================================================================
# LOGGING - Console only in production
# ============================================================================
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {"format": "{levelname} {asctime} {name} {message}", "style": "{"},
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "root": {"handlers": ["console"], "level": "INFO"},
}
