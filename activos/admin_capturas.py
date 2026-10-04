"""Adaptador de altas nativas; Django conserva validación y persistencia de inlines."""
import hashlib
import logging
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

from django import forms
from django.contrib import messages
from django.contrib.admin import helpers
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.db import models, router, transaction
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_protect

from mantenimiento.models import ComprobanteCapturaEquipo
from mantenimiento.services_capturas_equipos import CapturaEquipoError, capturar_equipo_autorizado
from .models import OrdenMantenimiento

logger = logging.getLogger(__name__)
CONTEXT = '_orden_admin_captura'
KEY = 'clave_captura'


class OrdenCapturaAdminForm(forms.ModelForm):
    clave_captura = forms.UUIDField(widget=forms.HiddenInput, initial=uuid4, required=False)

    class Meta:
        model = OrdenMantenimiento
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if KEY in self.fields:
            self.fields[KEY].required = self.instance.pk is None

    def validate_unique(self):
        # A receipt owns the original attempt even if its result changed/deleted.
        # Only this technical replay bypasses folio's uniqueness; the common
        # engine still checks the complete original hash before any write.
        request = getattr(self, 'capture_request', None)
        key = self.cleaned_data.get(KEY)
        receipt = None
        if request is not None and self.instance.pk is None and key:
            receipt = ComprobanteCapturaEquipo.objects.filter(usuario_id=request.user.pk,
                operacion='activos_admin_orden', clave=key)
        replay = receipt is not None and receipt.exists()
        folio = self.cleaned_data.get('folio')
        field = self.fields.pop('folio', None) if replay else None
        try:
            super().validate_unique()
        finally:
            if field is not None:
                self.fields['folio'] = field
        # Another valid native attempt can commit between receipt lookup and
        # unique validation. Remove only that receipt-owned uniqueness error;
        # never revalidate, reserve a key early, or suppress other form errors.
        errors = self._errors.get('folio')
        if not replay and receipt is not None and errors:
            data = errors.as_data()
            if any(error.code == 'unique' for error in data) and receipt.exists():
                remaining = [error for error in data if error.code != 'unique']
                if remaining:
                    self._errors['folio'] = self.error_class(remaining)
                else:
                    del self._errors['folio']
                    self.cleaned_data['folio'] = folio


class ReintentoAdmin(Exception):
    def __init__(self, orden):
        self.orden = orden


def contenido_admin(request):
    """Conserva multivalores y gestión inline; los botones sólo afectan respuesta."""
    excluded = {'csrfmiddlewaretoken', KEY, '_save', '_continue', '_addanother',
                '_saveasnew', '_popup', '_to_field', '_changelist_filters'}
    post = {key: request.POST.getlist(key) for key in request.POST if key not in excluded}
    files = {}
    for key in request.FILES:
        files[key] = []
        for archivo in request.FILES.getlist(key):
            position = archivo.tell()
            try:
                archivo.seek(0)
                digest = hashlib.sha256()
                for chunk in archivo.chunks():
                    digest.update(chunk)
                files[key].append({'nombre': archivo.name, 'size': archivo.size, 'sha256': digest.hexdigest()})
            finally:
                archivo.seek(position)
    return {'post': post, 'files': files}


@contextmanager
def observar_archivos(instance, contexto):
    """Observar sólo FieldFiles nuevos de esta instancia, antes de su INSERT.

    Referencias ya comprometidas (incluidas copias save-as-new) nunca se borran.
    Un storage que escribe y falla sin devolver nombre no permite descubrir ese
    archivo. Tampoco se borra una ruta preexistente que un storage sobrescriba.
    """
    restorations = []
    for model_field in instance._meta.fields:
        if not isinstance(model_field, models.FileField):
            continue
        field = getattr(instance, model_field.name)
        if not field or field._committed:
            continue
        # Snapshot persisted references before this save (pk=None does not prove
        # that a name is new, e.g. a clone can share its original document).
        referenced = set(type(instance)._default_manager.exclude(
            **{model_field.name: ''}).values_list(model_field.name, flat=True))
        original = field.save
        def save(name, content, save=True, *, field=field, original=original, referenced=referenced):
            candidate = field.field.generate_filename(instance, name)
            existed = field.storage.exists(candidate)
            try:
                return original(name, content, save=save)
            finally:
                if (field._committed and field.name and field.name not in referenced
                        and (field.name != candidate or not existed)):
                    contexto.archivos[(id(field.storage), field.name)] = (field.storage, field.name)
        field.save = save
        restorations.append((field, original))
    try:
        yield
    finally:
        for field, original in restorations:
            field.save = original


class CapturaOrdenAdminMixin:
    form = OrdenCapturaAdminForm

    def get_form(self, request, obj=None, **kwargs):
        native_form = super().get_form(request, obj, **kwargs)
        class RequestForm(native_form):
            capture_request = request
        return RequestForm

    def get_fields(self, request, obj=None):
        fields = list(super().get_fields(request, obj))
        if obj is not None and KEY in fields:
            fields.remove(KEY)
        return fields

    def save_form(self, request, form, change):
        context = getattr(request, CONTEXT, None)
        if context is not None:
            context.form = form
        return super().save_form(request, form, change)

    def get_formsets_with_inlines(self, request, obj=None):
        context = getattr(request, CONTEXT, None)
        for FormSet, inline in super().get_formsets_with_inlines(request, obj):
            if context is None:
                yield FormSet, inline
                continue
            def make_formset(base, inline_instance):
                class CaptureFormSet(base):
                    def __init__(self, *args, **kwargs):
                        super().__init__(*args, **kwargs)
                        context.formsets.append(self)
                        context.inlines.append(inline_instance)
                return CaptureFormSet
            yield make_formset(FormSet, inline), inline

    def _usuario_vigente(self, request):
        try:
            user = get_user_model().objects.get(pk=request.user.pk)
        except get_user_model().DoesNotExist:
            raise PermissionDenied('El usuario ya no está disponible.')
        request.user = user  # Fresh permission caches for every current native gate.
        if not user.is_active or not user.is_staff or not self.has_add_permission(request):
            raise PermissionDenied('Ya no tienes permiso para crear órdenes de mantenimiento.')
        return user

    def save_model(self, request, obj, form, change):
        if change:
            return super().save_model(request, obj, form, change)
        context = getattr(request, CONTEXT)
        user = self._usuario_vigente(request)
        def validar(usuario, activo):
            self._usuario_vigente(request)
        def ordenes(usuario):
            return self.get_queryset(request)
        def crear(archivos):
            with observar_archivos(obj, context):
                super(CapturaOrdenAdminMixin, self).save_model(request, obj, form, change)
            return obj
        orden, replay = capturar_equipo_autorizado(
            usuario=user, activo=obj.activo_ref, operacion='activos_admin_orden',
            clave=form.cleaned_data[KEY], contenido=contenido_admin(request),
            crear=crear, validar=validar, ordenes=ordenes,
        )
        if replay:
            if not self.has_view_or_change_permission(request, orden):
                raise PermissionDenied('Ya no tienes permiso para consultar esta orden.')
            raise ReintentoAdmin(orden)

    def save_formset(self, request, form, formset, change):
        context = getattr(request, CONTEXT, None)
        if context is None:
            return super().save_formset(request, form, formset, change)
        # Wrap every inline instance before native formset.save, including new rows.
        from contextlib import ExitStack
        with ExitStack() as stack:
            for inline_form in formset.forms:
                stack.enter_context(observar_archivos(inline_form.instance, context))
            return super().save_formset(request, form, formset, change)

    def message_user(self, request, message, level=messages.INFO, extra_tags='', fail_silently=False):
        context = getattr(request, CONTEXT, None)
        if context is not None and getattr(context, 'replay', False) and level == messages.SUCCESS:
            message = 'Esta captura ya se había guardado. Se recuperó la orden existente.'
        return super().message_user(request, message, level, extra_tags, fail_silently)

    def _error_captura(self, request, context, error, form_url, extra_context):
        form = context.form
        form.add_error(None, str(error.detail))
        admin_form = helpers.AdminForm(form, list(self.get_fieldsets(request, None)),
            self.get_prepopulated_fields(request, None), self.get_readonly_fields(request, None), model_admin=self)
        inline_formsets = self.get_inline_formsets(request, context.formsets, context.inlines, None)
        media = self.media + admin_form.media
        for inline in inline_formsets:
            media += inline.media
        data = {**self.admin_site.each_context(request), 'title': 'Agregar orden de mantenimiento',
            'subtitle': None, 'adminform': admin_form, 'object_id': None, 'original': None,
            'is_popup': '_popup' in request.POST or '_popup' in request.GET,
            'to_field': request.POST.get('_to_field', request.GET.get('_to_field')),
            'media': media, 'inline_admin_formsets': inline_formsets,
            'errors': helpers.AdminErrorList(form, context.formsets),
            'preserved_filters': self.get_preserved_filters(request), **(extra_context or {})}
        response = self.render_change_form(request, data, add=True, change=False, obj=None, form_url=form_url)
        response.status_code = error.status_code
        return response

    @method_decorator(csrf_protect)
    def changeform_view(self, request, object_id=None, form_url='', extra_context=None):
        creating = object_id is None or (request.method == 'POST' and '_saveasnew' in request.POST)
        if not creating:
            return super().changeform_view(request, object_id, form_url, extra_context)
        context = SimpleNamespace(archivos={}, formsets=[], inlines=[], form=None)
        previous = getattr(request, CONTEXT, None)
        setattr(request, CONTEXT, context)
        succeeded = False
        try:
            self._usuario_vigente(request)
            try:
                with transaction.atomic(using=router.db_for_write(self.model)):
                    response = super().changeform_view(request, object_id, form_url, extra_context)
                succeeded = True
                return response
            except ReintentoAdmin as replay:
                # Native popup/continue/filter behavior, without another log or inline.
                self._usuario_vigente(request)
                orden = self.get_queryset(request).filter(pk=replay.orden.pk).first()
                if orden is None or not self.has_view_or_change_permission(request, orden):
                    raise PermissionDenied('Ya no tienes permiso para consultar esta orden.')
                context.replay = True
                return self.response_add(request, orden)
            except CapturaEquipoError as error:
                # Conflict/tombstone details are also reads of an owned attempt.
                # The engine can raise them before its result visibility gate.
                user = self._usuario_vigente(request)
                receipt = ComprobanteCapturaEquipo.objects.filter(
                    usuario=user, operacion='activos_admin_orden',
                    clave=context.form.cleaned_data.get(KEY)).first()
                if receipt is not None:
                    orden = None
                    if receipt.orden_id is not None:
                        orden = self.get_queryset(request).filter(pk=receipt.orden_id).first()
                        if orden is None:
                            raise PermissionDenied('Ya no tienes permiso para consultar esta orden.')
                    if not self.has_view_or_change_permission(request, orden):
                        raise PermissionDenied('Ya no tienes permiso para consultar esta orden.')
                return self._error_captura(request, context, error, form_url, extra_context)
        finally:
            if not succeeded:
                for storage, name in context.archivos.values():
                    try:
                        storage.delete(name)
                    except Exception:
                        logger.exception('No se pudo limpiar archivo de alta Admin revertida: %s', name)
            if previous is None:
                delattr(request, CONTEXT)
            else:
                setattr(request, CONTEXT, previous)
