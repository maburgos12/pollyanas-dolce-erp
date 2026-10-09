"""Ejecución real de planes: identidad del intento y agenda en una transacción."""
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.urls import reverse
from django.utils import timezone
from activos.models import Activo, PlanMantenimiento, OrdenMantenimiento, BitacoraMantenimiento
from mantenimiento.services_access import can_access_mantenimiento, can_write_mantenimiento, authorized_branch_ids, authorized_orders
from mantenimiento.services_capturas_equipos import capturar_equipo_autorizado, CapturaEquipoError


class OrdenesPlanAbiertas(CapturaEquipoError):
    def __init__(self, ordenes):
        super().__init__('El plan ya tiene órdenes abiertas. Abre la orden actual o declara una intervención adicional con motivo.', 409)
        self.ordenes = [{'id': order.pk, 'folio': order.folio, 'url': reverse('mantenimiento:dashboard') + f'?open=orden:{order.pk}#tab-seguimiento'} for order in ordenes]


def registrar_ejecucion_plan(*, usuario, plan_id, clave, fecha=None, notas='', adicional=False, motivo_adicional='', movil=False):
    plan = PlanMantenimiento.objects.select_related('activo_ref').get(pk=plan_id)
    asset_id = plan.activo_ref_id
    def validar(actor, asset):
        fresh_actor = get_user_model().objects.get(pk=actor.pk)
        fresh_plan = PlanMantenimiento.objects.select_related('activo_ref').get(pk=plan_id)
        gate = can_write_mantenimiento if movil else can_access_mantenimiento
        if not fresh_actor.is_active or not gate(fresh_actor):
            raise PermissionDenied('No tienes permisos para ejecutar planes.')
        if (not fresh_plan.activo
                or fresh_plan.activo_ref_id != asset_id or not fresh_plan.activo_ref.activo):
            raise PermissionDenied('El plan o equipo ya no está disponible.')
        branches = authorized_branch_ids(fresh_actor) if movil else None
        if branches is not None and fresh_plan.activo_ref.sucursal_id not in branches:
            raise PermissionDenied('El plan ya no está en tu ámbito autorizado.')
    def crear(files):
        locked = PlanMantenimiento.objects.select_for_update(of=('self',)).select_related('activo_ref').get(pk=plan_id)
        validar(usuario, locked.activo_ref)
        # Nunca bloquear una orden después del plan: el cierre toma orden -> plan.
        opened = OrdenMantenimiento.objects.filter(plan_ref=locked, estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO])
        if opened.exists() and not adicional:
            raise OrdenesPlanAbiertas(list(opened))
        if adicional and not motivo_adicional.strip():
            raise CapturaEquipoError('Describe el motivo de la intervención adicional.', 400)
        execution_date = fecha or timezone.localdate()
        locked.ultima_ejecucion = execution_date
        locked.recompute_next_date()
        locked.save(update_fields=['ultima_ejecucion', 'proxima_ejecucion', 'actualizado_en'])
        order = OrdenMantenimiento.objects.create(activo_ref=locked.activo_ref, plan_ref=locked,
            tipo=OrdenMantenimiento.TIPO_PREVENTIVO, prioridad=OrdenMantenimiento.PRIORIDAD_BAJA,
            estatus=OrdenMantenimiento.ESTATUS_CERRADA, fecha_programada=execution_date,
            fecha_inicio=execution_date, fecha_cierre=execution_date,
            responsable=usuario.get_full_name() or usuario.username,
            descripcion=notas or f'Ejecución de plan: {locked.nombre}', origen=OrdenMantenimiento.ORIGEN_PLAN, creado_por=usuario)
        comment = notas or f'Registrado desde bandeja de mantenimiento. Plan: {locked.nombre}'
        if adicional:
            comment += f'\nIntervención adicional: {motivo_adicional}'
        BitacoraMantenimiento.objects.create(orden=order, usuario=usuario, accion='Ejecución registrada', comentario=comment)
        return order
    def ordenes_autorizadas(actor):
        # El recibo pudo esperar otro commit: validar otra vez antes de replay.
        validar(actor, plan.activo_ref)
        fresh_actor = get_user_model().objects.get(pk=actor.pk)
        return authorized_orders(fresh_actor) if movil else OrdenMantenimiento.objects.all()
    return capturar_equipo_autorizado(usuario=usuario, activo=plan.activo_ref, operacion='ejecucion_plan',
        clave=clave, contenido={'plan_id': plan.pk, 'activo_id': asset_id, 'fecha': fecha, 'notas': notas,
            'adicional': adicional, 'motivo_adicional': motivo_adicional}, crear=crear, validar=validar,
        ordenes=ordenes_autorizadas,
        audit_metadata=lambda order: {'origen': 'ejecucion_plan', 'plan_id': plan.pk, 'adicional': adicional, 'motivo_adicional': motivo_adicional})
