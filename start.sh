#!/bin/bash
set -e

echo "Running migrations..."
python manage.py migrate

echo "Collecting static files..."
python manage.py collectstatic --noinput

echo "DEBUG: CREATE_SUPERUSER=${CREATE_SUPERUSER}"
echo "DEBUG: USERNAME=${DJANGO_SUPERUSER_USERNAME:-EMPTY}"
echo "DEBUG: EMAIL=${DJANGO_SUPERUSER_EMAIL:-EMPTY}"
echo "DEBUG: PASSWORD=${DJANGO_SUPERUSER_PASSWORD:-EMPTY}"

if [ "${CREATE_SUPERUSER:-0}" = "1" ]; then
  echo "Creating superuser from environment variables..."
  
  if [ -n "${DJANGO_SUPERUSER_USERNAME:-}" ] && [ -n "${DJANGO_SUPERUSER_EMAIL:-}" ] && [ -n "${DJANGO_SUPERUSER_PASSWORD:-}" ]; then
    python manage.py shell << 'PY'
from django.contrib.auth import get_user_model
import os

User = get_user_model()
username = os.environ.get("DJANGO_SUPERUSER_USERNAME", "")
email = os.environ.get("DJANGO_SUPERUSER_EMAIL", "")
password = os.environ.get("DJANGO_SUPERUSER_PASSWORD", "")

if not username or not email or not password:
    print("ERROR: Missing superuser credentials in environment")
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
    print(f"ERROR creating superuser: {e}")
    raise
PY
  else
    echo "ERROR: Skipping superuser creation - missing DJANGO_SUPERUSER_* variables"
  fi
fi

echo "Starting Gunicorn..."
exec gunicorn config.wsgi:application --bind 0.0.0.0:${PORT:-8000} --workers ${WEB_CONCURRENCY:-2} --timeout 60
