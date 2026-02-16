import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django_app = get_wsgi_application()

def application(environ, start_response):
    # Simple ping endpoint that doesn't require Django
    if environ.get('PATH_INFO') == '/ping':
        status = '200 OK'
        response_headers = [('Content-Type', 'text/plain')]
        start_response(status, response_headers)
        return [b'pong']
    
    # Route everything else to Django
    return django_app(environ, start_response)
