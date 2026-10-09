"""Configuración móvil de planes: ámbito fresco y recibos específicos de intento."""
from uuid import UUID
from collections.abc import Mapping
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.http import Http404
from activos.models import Activo, PlanMantenimiento
from core.audit import log_event
from mantenimiento.models import ComprobanteConfiguracionPlan
from mantenimiento.services_access import authorized_branch_ids, can_write_mantenimiento
from mantenimiento.services_capturas_equipos import CapturaEquipoError, huella_captura


def planes_autorizados(usuario):
    qs = PlanMantenimiento.objects.select_related('activo_ref', 'activo_ref__sucursal')
    branches = authorized_branch_ids(usuario)
    return qs if branches is None else qs.filter(activo_ref__sucursal_id__in=branches)


def activos_autorizados(usuario):
    qs = Activo.objects.filter(activo=True).select_related('sucursal')
    branches = authorized_branch_ids(usuario)
    return qs if branches is None else qs.filter(sucursal_id__in=branches)


def _validar(actor_id, asset):
    # Refetch also clears cached module/group/profile decisions on request.user.
    actor = get_user_model().objects.get(pk=actor_id)
    if not actor.is_active or not can_write_mantenimiento(actor):
        raise PermissionDenied('No tienes permisos para configurar planes.')
    asset.refresh_from_db(fields=["activo", "sucursal_id"])
    branches = authorized_branch_ids(actor)
    if not asset.activo or (branches is not None and asset.sucursal_id not in branches):
        raise Http404
    return actor


@transaction.atomic
def configurar_plan(*, usuario, operacion, data, guardar, plan_id=None):
    if not isinstance(data, Mapping):
        raise CapturaEquipoError('Envía los datos del plan como objeto.', 400)
    actor = get_user_model().objects.get(pk=usuario.pk)
    if not actor.is_active or not can_write_mantenimiento(actor):
        raise PermissionDenied('No tienes permisos para configurar planes.')
    key = data.get('clave_captura')
    if key is not None:
        try:
            key = UUID(str(key))
        except (ValueError, TypeError, AttributeError):
            raise CapturaEquipoError('La clave del intento debe ser un UUID válido.', 400)
    # Find asset without locking Plan first: existing order closing locks Order → Plan.
    prior = ComprobanteConfiguracionPlan.objects.filter(usuario=actor, operacion=operacion, clave=key).first() if key else None
    if plan_id:
        asset_id = planes_autorizados(actor).filter(pk=plan_id).values_list('activo_ref_id', flat=True).first()
        if asset_id is None and prior and prior.objeto_id == plan_id:
            asset_id = prior.activo_ref_id
    else:
        try:
            asset_id = int(data.get('activo_id'))
        except (ValueError, TypeError):
            raise Http404
    asset = Activo.objects.select_for_update(no_key=True).filter(pk=asset_id).first()
    if asset is None:
        raise Http404
    actor = _validar(actor.pk, asset)
    fingerprint = huella_captura({'plan_id': plan_id, 'datos': {k: v for k, v in data.items() if k != 'clave_captura'}})
    receipt = None
    if key:
        receipt, new = ComprobanteConfiguracionPlan.objects.get_or_create(usuario=actor, operacion=operacion, clave=key,
            defaults={'huella': fingerprint, 'activo_ref': asset, 'objeto_id': plan_id})
        if not new:
            if receipt.huella != fingerprint:
                raise CapturaEquipoError('Este intento ya se envió con otros datos. Revisa el conflicto sin reenviarlo como nuevo.')
            if receipt.activo_ref_id != asset.pk:
                raise Http404
            result = PlanMantenimiento.objects.select_for_update().filter(pk=receipt.plan_id).first()
            actor = _validar(actor.pk, asset)
            if result is None or (not result.activo and operacion != 'plan_delete'):
                raise CapturaEquipoError('El plan de este intento fue retirado; el reintento no puede recrearlo.', 410)
            if result.activo_ref_id != asset.pk:
                raise Http404
            return result, True
    if plan_id:
        plan = PlanMantenimiento.objects.select_for_update().filter(pk=plan_id, activo_ref=asset).first()
        if plan is None:
            raise Http404
        if not plan.activo:
            raise CapturaEquipoError('Este plan fue retirado.', 410)
        revision = data.get('revision_en')
        if revision is not None and revision != plan.actualizado_en.isoformat():
            raise CapturaEquipoError({'error': 'El plan cambió desde que abriste el formulario. Revisa la configuración actual antes de guardar.',
                'error_code': 'plan_revision_conflict'}, 409)
    else:
        plan = PlanMantenimiento(activo_ref=asset)
    if operacion == 'plan_delete':
        plan.activo = False
    else:
        try:
            error = guardar(plan, data)
            if error:
                raise CapturaEquipoError(error, 400)
            plan.full_clean()
        except (ValueError, TypeError, AttributeError, ValidationError):
            raise CapturaEquipoError('Revisa el nombre, las fechas y los valores del plan.', 400)
    # Permissions may have changed while waiting on the plan/receipt lock.
    actor = _validar(actor.pk, asset)
    plan.save()
    log_event(actor, 'DELETE' if operacion == 'plan_delete' else ('CREATE' if plan_id is None else 'UPDATE'),
        'activos.PlanMantenimiento', plan.pk, {'origen': 'configuracion_plan_movil', 'operacion': operacion})
    if receipt:
        receipt.plan = plan
        receipt.objeto_id = plan.pk
        receipt.save(update_fields=['plan', 'objeto_id'])
    return plan, False
