"""Agenda y generación serializada por plan; cada plan conserva su propio commit."""
from datetime import timedelta
from decimal import Decimal
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.utils import timezone
from core.access import can_manage_inventario
from core.audit import log_event
from activos.models import PlanMantenimiento, OrdenMantenimiento, BitacoraMantenimiento


def actualizar_agenda_plan(*, usuario, plan_id, fecha):
    with transaction.atomic():
        plan = PlanMantenimiento.objects.select_for_update(of=('self',)).get(pk=plan_id)
        actor = get_user_model().objects.get(pk=usuario.pk)
        if not actor.is_active or not can_manage_inventario(actor):
            raise PermissionDenied('No tienes permisos para gestionar planes.')
        plan.ultima_ejecucion = fecha
        plan.recompute_next_date()
        plan.save(update_fields=['ultima_ejecucion', 'proxima_ejecucion', 'actualizado_en'])
        log_event(usuario, 'UPDATE', 'activos.PlanMantenimiento', plan.pk,
            {'ultima_ejecucion': str(fecha), 'proxima_ejecucion': str(plan.proxima_ejecucion or '')})
        return plan


def _eligible(plan, today, scope):
    return (plan.activo and plan.estatus == PlanMantenimiento.ESTATUS_ACTIVO and plan.activo_ref.activo
            and plan.proxima_ejecucion is not None and (today <= plan.proxima_ejecucion <= today + timedelta(days=7)
            if scope == 'week' else plan.proxima_ejecucion <= today))


def generar_ordenes_programadas(*, usuario, today=None, scope='overdue', dry_run=False):
    today = today or timezone.localdate()
    if not usuario.is_active or not can_manage_inventario(usuario):
        raise PermissionDenied('No tienes permisos para gestionar planes.')
    plans = PlanMantenimiento.objects.filter(activo=True, estatus=PlanMantenimiento.ESTATUS_ACTIVO, proxima_ejecucion__isnull=False)
    plans = plans.filter(proxima_ejecucion__gte=today, proxima_ejecucion__lte=today + timedelta(days=7)) if scope == 'week' else plans.filter(proxima_ejecucion__lte=today)
    result = {'created': 0, 'skipped': 0, 'failed_plan': None}
    for plan_id in list(plans.order_by('proxima_ejecucion', 'pk').values_list('pk', flat=True)):
        try:
            with transaction.atomic():
                qs = PlanMantenimiento.objects.select_related('activo_ref')
                plan = (qs if dry_run else qs.select_for_update(of=('self',))).get(pk=plan_id)
                actor = get_user_model().objects.get(pk=usuario.pk)
                if not actor.is_active or not can_manage_inventario(actor):
                    raise PermissionDenied('No tienes permisos para gestionar planes.')
                if not _eligible(plan, today, scope):
                    result['skipped'] += 1
                    continue
                if OrdenMantenimiento.objects.filter(plan_ref=plan, fecha_programada=plan.proxima_ejecucion).exclude(estatus=OrdenMantenimiento.ESTATUS_CANCELADA).exists():
                    result['skipped'] += 1
                    continue
                if not dry_run:
                    order = OrdenMantenimiento.objects.create(activo_ref=plan.activo_ref, plan_ref=plan,
                        tipo=OrdenMantenimiento.TIPO_PREVENTIVO, prioridad=({'ALTA': OrdenMantenimiento.PRIORIDAD_ALTA, 'BAJA': OrdenMantenimiento.PRIORIDAD_BAJA}.get(plan.activo_ref.criticidad, OrdenMantenimiento.PRIORIDAD_MEDIA)),
                        estatus=OrdenMantenimiento.ESTATUS_PENDIENTE, fecha_programada=plan.proxima_ejecucion,
                        responsable=plan.responsable or '', descripcion=f'Orden preventiva automática desde plan: {plan.nombre}', creado_por=usuario)
                    BitacoraMantenimiento.objects.create(orden=order, accion='AUTO_PLAN', comentario='Generada automáticamente desde plan activo', usuario=usuario, costo_adicional=Decimal('0'))
                    log_event(usuario, 'CREATE', 'activos.OrdenMantenimiento', order.pk,
                        {'origen': 'plan_auto', 'plan_id': plan.pk, 'fecha_programada': str(plan.proxima_ejecucion), 'folio': order.folio})
            result['created'] += 1
        except Exception:
            result['failed_plan'] = plan_id
            break
    return result
