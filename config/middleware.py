from django.http import HttpRequest, HttpResponse
from django.middleware.security import SecurityMiddleware as DjangoSecurityMiddleware


class HealthCheckSecurityMiddleware(DjangoSecurityMiddleware):
    """Security middleware that excludes /ping and /health/ from SSL redirect."""
    
    def process_request(self, request: HttpRequest) -> HttpResponse | None:
        # Skip SSL redirect for health checks
        if request.path in ("/ping", "/health/"):
            # Temporarily disable SSL redirect for these paths
            original_redirect = self.redirect_to_https
            self.redirect_to_https = False
            response = super().process_request(request)
            self.redirect_to_https = original_redirect
            return response
        
        return super().process_request(request)
