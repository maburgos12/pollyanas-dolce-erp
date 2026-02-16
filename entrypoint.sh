#!/bin/bash
set -e

echo "=== Starting Django Application ==="
echo "PORT: ${PORT:-8000}"
echo "DEBUG: ${DEBUG:-False}"
echo "ALLOWED_HOSTS: ${ALLOWED_HOSTS}"

echo ""
echo "=== Step 1: Running migrations ==="
python manage.py migrate

echo ""
echo "=== Step 2: Collecting static files ==="
python manage.py collectstatic --noinput --clear

echo ""
echo "=== Step 3: Starting Gunicorn ==="
gunicorn config.wsgi:application \
  --bind 0.0.0.0:${PORT:-8000} \
  --workers 4 \
  --worker-class sync \
  --timeout 120 \
  --access-logfile - \
  --error-logfile - \
  --log-level info
