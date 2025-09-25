from rest_framework.permissions import BasePermission


class HasValidVeevaSession(BasePermission):
    def has_permission(self, request, view):
        return hasattr(request, 'veeva_user_id') and request.veeva_user_id is not None