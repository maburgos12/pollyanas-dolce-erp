"""Django settings."""
import os
import socket
import sys
from urllib.parse import urlparse

import dj_database_url
from celery.schedules import crontab

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


def resolve_effective_database_url() -> str:
    """
    Fuera de Railway, permite usar DATABASE_PUBLIC_URL cuando DATABASE_URL
    apunta al host privado (*.railway.internal) y ese DNS no resuelve localmente.
    """
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    public_url = (os.getenv("DATABASE_PUBLIC_URL") or "").strip()
    if not db_url:
        return ""
    if RUNNING_ON_RAILWAY or not public_url or "railway.internal" not in db_url:
        return db_url

    try:
        host = (urlparse(db_url).hostname or "").strip()
    except Exception:
        host = ""
    if not host:
        return db_url

    try:
        socket.getaddrinfo(host, None)
    except OSError:
        os.environ["DATABASE_URL"] = public_url
        return public_url

    return db_url


RUNNING_ON_RAILWAY = any(
    os.getenv(name)
    for name in (
        "RAILWAY_PROJECT_ID",
        "RAILWAY_SERVICE_ID",
        "RAILWAY_ENVIRONMENT_ID",
        "RAILWAY_PUBLIC_DOMAIN",
    )
)
APP_ENV = os.getenv("APP_ENV", "production" if RUNNING_ON_RAILWAY else "development").strip().lower()
IS_DEVELOPMENT_ENV = APP_ENV in {"development", "dev", "local", "test"}
RUNNING_TESTS = "test" in sys.argv or bool(os.getenv("PYTEST_CURRENT_TEST"))
DEBUG = env_bool("DEBUG", default=(IS_DEVELOPMENT_ENV and not RUNNING_ON_RAILWAY))

if RUNNING_ON_RAILWAY and DEBUG:
    raise ValueError("DEBUG must remain disabled when RUNNING_ON_RAILWAY is detected.")

if APP_ENV in {"production", "staging"} and DEBUG:
    raise ValueError("DEBUG must remain disabled when APP_ENV is production or staging.")

ALLOW_INSECURE_LOCAL_SECRET_KEY = env_bool("ALLOW_INSECURE_LOCAL_SECRET_KEY", default=False)
SECRET_KEY = (os.getenv("SECRET_KEY") or "").strip()
if not SECRET_KEY:
    if RUNNING_TESTS:
        SECRET_KEY = "test-secret-key"
    elif DEBUG and IS_DEVELOPMENT_ENV and ALLOW_INSECURE_LOCAL_SECRET_KEY:
        SECRET_KEY = "django-insecure-local-dev-key-change-me"
    else:
        raise ValueError(
            "SECRET_KEY must be set. "
            "Use ALLOW_INSECURE_LOCAL_SECRET_KEY=1 only for temporary local development."
        )

LOCAL_DEV_HOST_PORT = os.getenv("WEB_HOST_PORT", "8011")
CANONICAL_LOCAL_HOST = os.getenv("CANONICAL_LOCAL_HOST", f"localhost:{LOCAL_DEV_HOST_PORT}")
AI_GATEWAY_OPENAPI_SERVER_URL = os.getenv("AI_GATEWAY_OPENAPI_SERVER_URL", "").strip()
ONYX_PORTAL_URL = os.getenv("ONYX_PORTAL_URL", "https://ai.pollyanasdolce.com").strip()

ALLOWED_HOSTS = env_list(
    "ALLOWED_HOSTS",
    "localhost,127.0.0.1,healthcheck.railway.app,.up.railway.app",
)
railway_public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN")
railway_private_domain = os.getenv("RAILWAY_PRIVATE_DOMAIN")
if railway_public_domain and railway_public_domain not in ALLOWED_HOSTS:
    ALLOWED_HOSTS.append(railway_public_domain)
if railway_private_domain and railway_private_domain not in ALLOWED_HOSTS:
    ALLOWED_HOSTS.append(railway_private_domain)
if RUNNING_ON_RAILWAY:
    for railway_host in ("healthcheck.railway.app", ".up.railway.app", ".railway.internal"):
        if railway_host not in ALLOWED_HOSTS:
            ALLOWED_HOSTS.append(railway_host)

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
    "ventas",
    "rrhh",
    "logistica",
    "fallas",
    "integraciones",
    "horarios_especiales",
    "pos_bridge",
    "reportes",
    "proyecciones",
    "orquestacion",
    "rentabilidad",
    "api",
]

MIDDLEWARE = [
    "config.middleware.HealthCheckSecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "core.middleware.CanonicalLocalHostMiddleware",
    "core.middleware.EnsureCSRFCookieOnHtmlMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "core.middleware.BranchCaptureOnlyMiddleware",
    "core.middleware.RepartidorOnlyMiddleware",
    "core.middleware.PerformanceLoggingMiddleware",
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

# Database priority:
# 1. DATABASE_URL
# 2. DB_HOST + DB_NAME/DB_USER/DB_PASSWORD/DB_PORT
# SQLite no es una ruta operativa valida para este ERP.
DATABASE_URL = resolve_effective_database_url()
if DATABASE_URL:
    DATABASES = {
        "default": dj_database_url.config(
            default=DATABASE_URL,
            conn_max_age=600,
            ssl_require=False,
        )
    }
elif os.getenv("DB_HOST"):
    db_host = os.getenv("DB_HOST", "localhost")
    if DEBUG:
        try:
            socket.getaddrinfo(db_host, None)
        except OSError:
            raise ValueError(
                "DB_HOST está configurado pero no resolvió a una base PostgreSQL válida. "
                "Corrige la configuración de PostgreSQL; SQLite ya no es una ruta soportada para este ERP."
            )
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
    raise ValueError(
        "Configuración de base de datos faltante. Para operar el ERP usa PostgreSQL con DATABASE_URL o DB_HOST. "
        "SQLite ya no es una ruta soportada para este ERP."
    )
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
MEDIA_URL = os.getenv("MEDIA_URL", "/media/")
MEDIA_ROOT = os.getenv("MEDIA_ROOT", os.path.join(BASE_DIR, "storage", "media"))
if "test" in sys.argv:
    # Evita dependencia de manifest/collectstatic en la suite de tests.
    STATICFILES_STORAGE = "django.contrib.staticfiles.storage.StaticFilesStorage"
else:
    STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

PICKUP_AVAILABILITY_FRESHNESS_MINUTES = env_int("PICKUP_AVAILABILITY_FRESHNESS_MINUTES", 20)
PICKUP_AVAILABILITY_RESPONSE_CACHE_SECONDS = env_int("PICKUP_AVAILABILITY_RESPONSE_CACHE_SECONDS", 3)
PICKUP_RESERVATION_EXPIRY_SWEEP_DEBOUNCE_SECONDS = env_int(
    "PICKUP_RESERVATION_EXPIRY_SWEEP_DEBOUNCE_SECONDS",
    30,
)
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
    f"http://{CANONICAL_LOCAL_HOST}",
    f"http://localhost:{LOCAL_DEV_HOST_PORT}",
    f"http://127.0.0.1:{LOCAL_DEV_HOST_PORT}",
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

CACHE_DEFAULT_TIMEOUT = env_int("CACHE_DEFAULT_TIMEOUT", 300)
CACHE_KEY_PREFIX = os.getenv("CACHE_KEY_PREFIX", "pastelerias_erp")
CACHE_URL = (os.getenv("CACHE_URL") or os.getenv("REDIS_URL") or "").strip()
if CACHE_URL:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.redis.RedisCache",
            "LOCATION": CACHE_URL,
            "TIMEOUT": CACHE_DEFAULT_TIMEOUT,
            "KEY_PREFIX": CACHE_KEY_PREFIX,
        }
    }
else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "pastelerias-erp-local-cache",
            "TIMEOUT": CACHE_DEFAULT_TIMEOUT,
            "KEY_PREFIX": CACHE_KEY_PREFIX,
        }
    }

CELERY_BROKER_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BEAT_SCHEDULE = {
    "logistica-alertar-documentos-por-vencer": {
        "task": "logistica.tasks.alertar_documentos_por_vencer",
        "schedule": crontab(hour=8, minute=0),
    },
    "logistica-alertar-servicios-proximos": {
        "task": "logistica.tasks.alertar_servicios_proximos",
        "schedule": crontab(hour=8, minute=5),
    },
    "logistica-alertar-lavados-pendientes": {
        "task": "logistica.tasks.alertar_lavados_pendientes",
        "schedule": crontab(hour=8, minute=10),
    },
    "logistica-escalar-tickets-sin-respuesta-cada-60-min": {
        "task": "logistica.tasks.escalar_tickets_sin_respuesta",
        "schedule": 60 * 60,
    },
    # --- Sync diario de ventas Point ---
    "pos_bridge: sync ventas diario": {
        "task": "pos_bridge.daily_sales_sync",
        "schedule": crontab(hour=3, minute=0),
        "kwargs": {"days": 3, "lag_days": 0},
    },
    # --- Cierre nocturno de producción ---
    "reportes: cierre produccion nocturno": {
        "task": "reportes.cierre_produccion_nocturno",
        "schedule": crontab(hour=3, minute=15),
    },
}

EMAIL_HOST = os.getenv("EMAIL_HOST", "localhost")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "25"))
EMAIL_USE_TLS = env_bool("EMAIL_USE_TLS", default=False)
EMAIL_USE_SSL = env_bool("EMAIL_USE_SSL", default=False)
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD", "")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL", EMAIL_HOST_USER or "webmaster@localhost")
DIRECTOR_EMAIL = os.getenv("DIRECTOR_EMAIL", DEFAULT_FROM_EMAIL)

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
        "erp.performance": {"handlers": ["console"], "level": "INFO", "propagate": False},
    },
    "root": {"handlers": ["console"], "level": "INFO"},
}

ERP_PERF_LOGGING_ENABLED = env_bool("ERP_PERF_LOGGING_ENABLED", default=DEBUG and not RUNNING_TESTS)
ERP_SLOW_ENDPOINT_MS = env_int("ERP_SLOW_ENDPOINT_MS", 1000)
ERP_SLOW_QUERY_MS = env_int("ERP_SLOW_QUERY_MS", 200)
ERP_AUTO_PURCHASE_ENABLED = env_bool("ERP_AUTO_PURCHASE_ENABLED", default=True)
ERP_AUTO_PURCHASE_MIN_SHORTAGE = os.getenv("ERP_AUTO_PURCHASE_MIN_SHORTAGE", "0.001")
ERP_OPERATION_ALERTS_ENABLED = env_bool("ERP_OPERATION_ALERTS_ENABLED", default=True)

IS_SECURE_ENV = not DEBUG and (RUNNING_ON_RAILWAY or APP_ENV in {"staging", "production"})
if RUNNING_ON_RAILWAY:
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
    USE_X_FORWARDED_HOST = True
    USE_X_FORWARDED_PORT = True
SECURE_HSTS_SECONDS = int(os.getenv("SECURE_HSTS_SECONDS", "31536000" if not DEBUG else "0"))
SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool("SECURE_HSTS_INCLUDE_SUBDOMAINS", default=not DEBUG)
SECURE_HSTS_PRELOAD = env_bool("SECURE_HSTS_PRELOAD", default=not DEBUG)
SECURE_SSL_REDIRECT = env_bool("SECURE_SSL_REDIRECT", default=IS_SECURE_ENV)
SESSION_COOKIE_SECURE = env_bool("SESSION_COOKIE_SECURE", default=IS_SECURE_ENV)
CSRF_COOKIE_SECURE = env_bool("CSRF_COOKIE_SECURE", default=IS_SECURE_ENV)
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

ORQUESTACION_POINTDAILYSALE_GUARD_ENABLED = env_bool("ORQUESTACION_POINTDAILYSALE_GUARD_ENABLED", default=True)

# Umbrales de rentabilidad para el agente IA
RENT_MARGEN_BRUTO_MIN = 55.0
RENT_MARGEN_NETO_MIN = 15.0
RENT_ROI_OBJETIVO = 25.0
RENT_PAYBACK_MAX_MESES = 36
