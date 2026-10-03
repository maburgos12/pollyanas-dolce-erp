"""Limit marked sales credentials before any view can read or write operations."""

from django.http import JsonResponse
from rest_framework.authtoken.models import Token
from rest_framework.exceptions import AuthenticationFailed
from rest_framework_simplejwt.authentication import JWTAuthentication

from integraciones.models import PublicApiClient
from integraciones.sales_read_policy import SALES_GROUP, allows_sales_read


class SalesReadBoundaryMiddleware:
    MAX_HEADER_LENGTH = 4096

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        authorization = request.META.get("HTTP_AUTHORIZATION", "")
        api_key = request.META.get("HTTP_X_API_KEY", "")
        if any(not isinstance(header, str) or len(header) > self.MAX_HEADER_LENGTH
               for header in (authorization, api_key)):
            return self._denied()

        principals = []
        session_user = getattr(request, "user", None)
        if session_user is not None and session_user.is_authenticated:
            principals.append(session_user)

        # Mirror DRF's case-insensitive Token scheme and ASCII whitespace split.
        # This lookup only identifies restrictions; downstream auth still decides
        # validity and request.user is never replaced here.
        try:
            parts = authorization.encode("ascii").split()
        except UnicodeEncodeError:
            parts = []
        if len(parts) == 2 and parts[0].lower() == b"token":
            token = Token.objects.select_related("user").filter(key=parts[1].decode("ascii")).first()
            if token is not None:
                principals.append(token.user)

        # Logistics also accepts JWT. Reuse its verifier to identify the same
        # principal, without granting authentication to other endpoints.
        if authorization:
            try:
                jwt_identity = JWTAuthentication().authenticate(request)
            except (AuthenticationFailed, UnicodeEncodeError):
                jwt_identity = None
            if jwt_identity is not None:
                principals.append(jwt_identity[0])

        kinds = set()
        for principal in principals:
            if principal.groups.filter(name=SALES_GROUP).exists():
                if not principal.is_active or principal.is_staff or principal.is_superuser:
                    return self._denied()
                kinds.add("token")

        raw_key = api_key.strip()
        if raw_key:
            client = PublicApiClient.objects.filter(clave_prefijo=raw_key[:12], activo=True).first()
            if (client is not None and client.validate(raw_key)
                    and client.has_capability(PublicApiClient.CAPABILITY_SALES_READ_ONLY)):
                kinds.add("public_key")

        if any(not allows_sales_read(kind, request.method, request.path) for kind in kinds):
            return self._denied()
        return self.get_response(request)

    @staticmethod
    def _denied():
        return JsonResponse({"detail": "Acceso no autorizado"}, status=403)
