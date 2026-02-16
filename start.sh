#!/bin/bash
set -e

echo "Running migrations..."
python manage.py migrate

echo "Collecting static files..."
python manage.py collectstatic --noinput

if [ "${CREATE_SUPERUSER:-0}" = "1" ]; then
  echo "Creating superuser from environment variables..."
  if [ -n "${DJANGO_SUPERUSER_USERNAME:-}" ] && [ -n "${DJANGO_SUPERUSER_EMAIL:-}" ] && [ -n "${DJANGO_SUPERUSER_PASSWORD:-}" ]; then
    python manage.py createsuperuser --noinput || true
  else
    echo "Skipping superuser creation: missing DJANGO_SUPERUSER_* variables"
  fi
fi

echo "Starting Gunicorn..."
exec gunicorn config.wsgi:application --bind 0.0.0.0:${PORT:-8000} --workers ${WEB_CONCURRENCY:-2} --timeout 60
