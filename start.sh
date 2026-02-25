#!/bin/bash
set -e

echo "Running migrations..."
python manage.py migrate

echo "Bootstrapping roles..."
python manage.py bootstrap_roles

echo "Bootstrapping Point branches..."
python manage.py bootstrap_sucursales_point --only-missing

echo "Collecting static files..."
python manage.py collectstatic --noinput

echo "Creating legacy CSS aliases for cached clients..."
LATEST_STYLES_CSS="$(ls -1 /app/staticfiles/css/styles.*.css 2>/dev/null | grep -v '\\.map$' | sort | tail -n 1 || true)"
if [ -n "${LATEST_STYLES_CSS}" ]; then
  for LEGACY_HASH in e01c8c2bd142 c81c46f9d131 5f87aa237771; do
    cp -f "${LATEST_STYLES_CSS}" "/app/staticfiles/css/styles.${LEGACY_HASH}.css" || true
  done
  cp -f "${LATEST_STYLES_CSS}" "/app/staticfiles/css/styles.css" || true
  echo "Legacy CSS aliases ready -> ${LATEST_STYLES_CSS}"
else
  echo "No hashed styles CSS file found in /app/staticfiles/css"
fi

echo "DEBUG: CREATE_SUPERUSER=${CREATE_SUPERUSER}"
echo "DEBUG: USERNAME=${DJANGO_SUPERUSER_USERNAME:-EMPTY}"
echo "DEBUG: ENABLE_AUTO_SYNC_ALMACEN=${ENABLE_AUTO_SYNC_ALMACEN:-0}"
echo "DEBUG: ENABLE_AUTO_MAINT_INTEGRACIONES=${ENABLE_AUTO_MAINT_INTEGRACIONES:-0}"

if [ "${CREATE_SUPERUSER:-0}" = "1" ]; then
  echo "Creating superuser..."
  
  if [ -n "${DJANGO_SUPERUSER_USERNAME:-}" ] && [ -n "${DJANGO_SUPERUSER_EMAIL:-}" ] && [ -n "${DJANGO_SUPERUSER_PASSWORD:-}" ]; then
    python manage.py shell << 'PY'
from django.contrib.auth import get_user_model
import os

User = get_user_model()
username = os.environ.get("DJANGO_SUPERUSER_USERNAME", "")
email = os.environ.get("DJANGO_SUPERUSER_EMAIL", "")
password = os.environ.get("DJANGO_SUPERUSER_PASSWORD", "")

if not username or not email or not password:
    print("ERROR: Missing superuser credentials")
    exit(1)

try:
    user, created = User.objects.get_or_create(
        username=username,
        defaults={"email": email, "is_staff": True, "is_superuser": True},
    )
    if not created:
        user.email = email
        user.is_staff = True
        user.is_superuser = True
    user.set_password(password)
    user.save()
    print(f"SUPERUSER_READY: username={username} created={created}")
except Exception as e:
    print(f"ERROR: {e}")
    raise
PY
  fi
fi

if [ "${ENABLE_AUTO_SYNC_ALMACEN:-0}" = "1" ]; then
  echo "Starting auto sync worker..."
  ./scripts/auto_sync_almacen.sh &
fi

if [ "${ENABLE_AUTO_MAINT_INTEGRACIONES:-0}" = "1" ]; then
  echo "Starting auto maintenance worker (integraciones)..."
  ./scripts/auto_maintenance_integraciones.sh &
fi

echo "Starting Gunicorn..."
exec gunicorn config.wsgi:application --bind 0.0.0.0:${PORT:-8000} --workers ${WEB_CONCURRENCY:-2} --timeout 60
