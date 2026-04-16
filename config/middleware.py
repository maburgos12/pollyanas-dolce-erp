from django.http import HttpRequest, HttpResponse
from django.middleware.security import SecurityMiddleware as DjangoSecurityMiddleware


class HealthCheckSecurityMiddleware(DjangoSecurityMiddleware):
    """Security middleware that excludes /ping and /health/ from SSL redirect."""
    
    def process_request(self, request: HttpRequest) -> HttpResponse | None:
        if request.path in ("/ping", "/health/"):
            return None
        
        return super().process_request(request)
