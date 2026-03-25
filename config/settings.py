"""Django settings."""
import os
import socket
import sys

import dj_database_url

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_local_env_file(filepath: str) -> None:
    if not os.path.exists(filepath):
        return

    with open(filepath, encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ.setdefault(key, value)


load_local_env_file(os.path.join(BASE_DIR, ".env"))


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        return int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        return int(default)


def env_list(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


RUNNING_ON_RAILWAY = any(
    os.getenv(name)
    for name in (
        "RAILWAY_PROJECT_ID",
        "RAILWAY_SERVICE_ID",
        "RAILWAY_ENVIRONMENT_ID",
        "RAILWAY_PUBLIC_DOMAIN",
    )
)
DEBUG = env_bool("DEBUG", default=not RUNNING_ON_RAILWAY)
SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-dev-key-change-me")
if not DEBUG and SECRET_KEY == "django-insecure-dev-key-change-me":
    raise ValueError("SECRET_KEY must be set when DEBUG=False")

CANONICAL_LOCAL_HOST = os.getenv("CANONICAL_LOCAL_HOST", "127.0.0.1:8002")

ALLOWED_HOSTS = env_list(
    "ALLOWED_HOSTS",
    "localhost,127.0.0.1,0.0.0.0,healthcheck.railway.app,.up.railway.app",
)
railway_public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN")
if railway_public_domain and railway_public_domain not in ALLOWED_HOSTS:
    ALLOWED_HOSTS.append(railway_public_domain)

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.humanize",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "rest_framework.authtoken",
    "corsheaders",
    "django_filters",
    "django_celery_beat",
    "core",
    "maestros",
    "recetas",
    "compras",
    "inventario",
    "activos",
    "control",
    "crm",
    "rrhh",
    "logistica",
    "integraciones",
    "pos_bridge",
    "reportes",
    "api",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "core.middleware.CanonicalLocalHostMiddleware",
    "core.middleware.EnsureCSRFCookieOnHtmlMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "core.middleware.BranchCaptureOnlyMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"

TEMPLATES = [{
    "BACKEND": "django.template.backends.django.DjangoTemplates",
    "DIRS": [os.path.join(BASE_DIR, "templates")],
    "APP_DIRS": True,
    "OPTIONS": {
        "context_processors": [
            "django.template.context_processors.debug",
            "django.template.context_processors.request",
            "django.contrib.auth.context_processors.auth",
            "django.contrib.messages.context_processors.messages",
            "core.context_processors.ui_access",
        ],
    },
}]

# Database
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    DATABASES = {
        "default": dj_database_url.config(
            default=DATABASE_URL,
            conn_max_age=600,
            ssl_require=not DEBUG,
        )
    }
elif os.getenv("DB_HOST"):
    db_host = os.getenv("DB_HOST", "localhost")
    if DEBUG:
        try:
            socket.getaddrinfo(db_host, None)
        except OSError:
            DATABASES = {
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": os.path.join(BASE_DIR, "db.sqlite3"),
                }
            }
        else:
            DATABASES = {
                "default": {
                    "ENGINE": "django.db.backends.postgresql",
                    "NAME": os.getenv("DB_NAME", "pastelerias_erp"),
                    "USER": os.getenv("DB_USER", "postgres"),
                    "PASSWORD": os.getenv("DB_PASSWORD", "postgres"),
                    "HOST": db_host,
                    "PORT": os.getenv("DB_PORT", "5432"),
                }
            }
    else:
        DATABASES = {
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": os.getenv("DB_NAME", "pastelerias_erp"),
                "USER": os.getenv("DB_USER", "postgres"),
                "PASSWORD": os.getenv("DB_PASSWORD", "postgres"),
                "HOST": db_host,
                "PORT": os.getenv("DB_PORT", "5432"),
            }
        }
elif DEBUG:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": os.path.join(BASE_DIR, "db.sqlite3"),
        }
    }
else:
    raise ValueError("DATABASE_URL or DB_HOST must be set when DEBUG=False")

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "es-mx"
TIME_ZONE = os.getenv("TIME_ZONE", "America/Mazatlan")
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = os.path.join(BASE_DIR, "staticfiles")
STATICFILES_DIRS = [os.path.join(BASE_DIR, "static")]
if "test" in sys.argv:
    # Evita dependencia de manifest/collectstatic en la suite de tests.
    STATICFILES_STORAGE = "django.contrib.staticfiles.storage.StaticFilesStorage"
else:
    STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

PICKUP_AVAILABILITY_FRESHNESS_MINUTES = env_int("PICKUP_AVAILABILITY_FRESHNESS_MINUTES", 20)
PICKUP_STOCK_BUFFER_DEFAULT = os.getenv("PICKUP_STOCK_BUFFER_DEFAULT", "1")
PICKUP_LOW_STOCK_THRESHOLD = os.getenv("PICKUP_LOW_STOCK_THRESHOLD", "3")
PICKUP_RESERVATION_TTL_MINUTES = env_int("PICKUP_RESERVATION_TTL_MINUTES", 15)

CORS_ALLOW_ALL_ORIGINS = env_bool("CORS_ALLOW_ALL_ORIGINS", default=False)
CORS_ALLOWED_ORIGINS = env_list("CORS_ALLOWED_ORIGINS", "")
CSRF_TRUSTED_ORIGINS = env_list("CSRF_TRUSTED_ORIGINS", "")
if railway_public_domain:
    trusted_origin = f"https://{railway_public_domain}"
    if trusted_origin not in CSRF_TRUSTED_ORIGINS:
        CSRF_TRUSTED_ORIGINS.append(trusted_origin)

CSRF_FAILURE_VIEW = "core.views.csrf_failure"
for trusted_origin in (
    "http://localhost",
    "http://127.0.0.1",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "https://localhost",
    "https://127.0.0.1",
    "https://localhost:8000",
    "https://127.0.0.1:8000",
):
    if trusted_origin not in CSRF_TRUSTED_ORIGINS:
        CSRF_TRUSTED_ORIGINS.append(trusted_origin)
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.TokenAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.IsAuthenticated"],
    "DEFAULT_FILTER_BACKENDS": ["django_filters.rest_framework.DjangoFilterBackend"],
}

CELERY_BROKER_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
POS_BRIDGE_AGENT_MODEL = os.getenv("POS_BRIDGE_AGENT_MODEL", "gpt-4o-mini")

LOGIN_URL = "/login/"
LOGIN_REDIRECT_URL = "/dashboard/"
LOGOUT_REDIRECT_URL = "/login/"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {
        "core": {"handlers": ["console"], "level": "INFO"},
    },
    "root": {"handlers": ["console"], "level": "INFO"},
}

SECURE_HSTS_SECONDS = int(os.getenv("SECURE_HSTS_SECONDS", "31536000" if not DEBUG else "0"))
SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool("SECURE_HSTS_INCLUDE_SUBDOMAINS", default=not DEBUG)
SECURE_HSTS_PRELOAD = env_bool("SECURE_HSTS_PRELOAD", default=not DEBUG)
SECURE_SSL_REDIRECT = False
SESSION_COOKIE_SECURE = env_bool("SESSION_COOKIE_SECURE", default=(not DEBUG and RUNNING_ON_RAILWAY))
CSRF_COOKIE_SECURE = env_bool("CSRF_COOKIE_SECURE", default=(not DEBUG and RUNNING_ON_RAILWAY))
X_FRAME_OPTIONS = os.getenv("X_FRAME_OPTIONS", "DENY")

# Inventario: fórmula de punto de reorden.
# - excel_legacy: (dias_llegada + consumo_diario_promedio) * stock_minimo
# - leadtime_plus_safety: (dias_llegada * consumo_diario_promedio) + stock_minimo
INVENTARIO_REORDER_FORMULA = os.getenv("INVENTARIO_REORDER_FORMULA", "excel_legacy").strip().lower()
# Inventario: diferencia máxima permitida (%) para capturar manualmente el punto de reorden.
INVENTARIO_REORDER_MAX_DIFF_PCT = float(os.getenv("INVENTARIO_REORDER_MAX_DIFF_PCT", "10"))

# API pública: límite de requests por cliente por minuto.
# 0 o negativo = sin límite.
PUBLIC_API_RATE_LIMIT_PER_MINUTE = int(os.getenv("PUBLIC_API_RATE_LIMIT_PER_MINUTE", "120"))

POINT_BRIDGE_STORAGE_ROOT = os.getenv(
    "POINT_BRIDGE_STORAGE_ROOT",
    os.path.join(BASE_DIR, "storage", "pos_bridge"),
)
POINT_BRIDGE_SYNC_INTERVAL_HOURS = int(os.getenv("POINT_BRIDGE_SYNC_INTERVAL_HOURS", "24"))
POINT_BRIDGE_RETRY_ATTEMPTS = int(os.getenv("POINT_BRIDGE_RETRY_ATTEMPTS", "3"))
