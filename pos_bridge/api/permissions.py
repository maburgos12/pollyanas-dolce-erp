from rest_framework.permissions import BasePermission


class IsPosAdminUser(BasePermission):
    """Restrict operational controls to staff or users with pos_bridge permissions."""

    def has_permission(self, request, view):
        user = request.user
        if not user or not user.is_authenticated:
            return False
        if user.is_staff:
            return True
        return user.has_perm("pos_bridge.view_pointsyncjob")
