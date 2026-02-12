#!/bin/bash
set -e

echo "Running migrations..."
python manage.py migrate || true

echo "Collecting static files..."
python manage.py collectstatic --noinput || true

echo "Starting Gunicorn..."
exec gunicorn config.wsgi:application --bind 0.0.0.0:${PORT:-8000} --workers 1 --timeout 60
