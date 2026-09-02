from rest_framework.permissions import BasePermission

from core.access import (
    can_build_product_closure,
    can_lock_product_closure,
    can_rebuild_product_closure,
    can_view_product_closure,
)


class IsPosAdminUser(BasePermission):
    """Restrict operational controls to staff or users with pos_bridge permissions."""

    def has_permission(self, request, view):
        user = request.user
        if not user or not user.is_authenticated:
            return False
        if user.is_staff:
            return True
        return user.has_perm("pos_bridge.view_pointsyncjob")


class IsProductClosureUser(BasePermission):
    def has_permission(self, request, view):
        user = request.user
        if not user or not user.is_authenticated:
            return False
        if user.is_staff:
            return True

        action = getattr(view, "action", "")
        if action in {"list", "retrieve"}:
            return can_view_product_closure(user)
        if action == "build":
            payload = request.data or {}
            may_build = (
                can_rebuild_product_closure(user)
                if bool(payload.get("rebuild"))
                else can_build_product_closure(user)
            )
            if bool(payload.get("lock_after_build")):
                return may_build and can_lock_product_closure(user)
            return may_build
        if action == "lock":
            return can_lock_product_closure(user)
        return can_view_product_closure(user)
