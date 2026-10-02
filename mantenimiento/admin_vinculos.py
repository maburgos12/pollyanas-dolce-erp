"""Conserva la confirmación Django y revierte también su log si aparece un vínculo."""
from django.contrib import admin, messages
from django.contrib.admin.actions import delete_selected
from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.urls import reverse

from mantenimiento.services_vinculos import PROTECTED_MESSAGE


def delete_selected_protegido(modeladmin, request, queryset):
    try:
        with transaction.atomic():
            return delete_selected(modeladmin, request, queryset)
    except ProtectedError:
        return modeladmin.protected_response(request)


class DocumentosAtencionAdmin(admin.ModelAdmin):
    def protected_response(self, request):
        self.message_user(request, PROTECTED_MESSAGE, level=messages.ERROR)
        opts = self.model._meta
        return HttpResponseRedirect(reverse(f'admin:{opts.app_label}_{opts.model_name}_changelist'))

    def delete_view(self, request, object_id, extra_context=None):
        try:
            with transaction.atomic():
                return super().delete_view(request, object_id, extra_context)
        except ProtectedError:
            return self.protected_response(request)

    def delete_model(self, request, obj):
        with transaction.atomic():
            obj = get_object_or_404(self.get_queryset(request).select_for_update(of=('self',)), pk=obj.pk)
            return super().delete_model(request, obj)

    def delete_queryset(self, request, queryset):
        with transaction.atomic():
            list(queryset.select_for_update(of=('self',)).order_by('pk').values_list('pk', flat=True))
            return super().delete_queryset(request, queryset)

    def get_actions(self, request):
        actions = super().get_actions(request)
        if 'delete_selected' in actions:
            actions['delete_selected'] = (delete_selected_protegido, 'delete_selected', actions['delete_selected'][2])
        return actions
