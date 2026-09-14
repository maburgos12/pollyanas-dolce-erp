"""Generación del último aniversario cumplido; nunca reconstruye el historial."""
import calendar
import logging
from datetime import date

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from core.models import AuditLog
from .models import (
    AplicacionGoceVacaciones, Empleado, MovimientoVacaciones,
    PeriodoVacacional, PoliticaVacaciones, SolicitudVacaciones,
)

logger = logging.getLogger(__name__)


def ultimo_aniversario_cumplido(empleado, al=None):
    corte = min(al or timezone.localdate(), timezone.localdate())
    ingreso = empleado.fecha_ingreso
    if not empleado.activo or not ingreso or ingreso > corte:
        return None

    def aniversario(anio):
        # Conserva la antigüedad usada por servicios_vacaciones: quien ingresó
        # el 29/02 cumple el año al llegar marzo en años no bisiestos.
        if ingreso.month == 2 and ingreso.day == 29 and not calendar.isleap(anio):
            return date(anio, 3, 1)
        return date(anio, ingreso.month, ingreso.day)

    actual = aniversario(corte.year)
    if actual > corte:
        actual = aniversario(corte.year - 1)
    return actual if actual.year > ingreso.year else None


def plan_periodo_actual(empleado, al=None):
    """Simulación genuinamente de lectura: no inserta ni avanza secuencias."""
    from .services_vacaciones import goce_vacacional_fifo_activo

    plan = {'empleado_id': empleado.pk, 'nombre': empleado.nombre, 'estado': 'omitido'}
    aniversario = ultimo_aniversario_cumplido(empleado, al)
    if not goce_vacacional_fifo_activo() or aniversario is None:
        return plan
    # Una captura retroactiva no autoriza reconstruir periodos anteriores.
    if aniversario != ultimo_aniversario_cumplido(empleado):
        return plan
    plan['aniversario'] = aniversario.isoformat()
    existente = PeriodoVacacional.objects.filter(empleado=empleado, aniversario=aniversario).first()
    if existente:
        return {**plan, 'estado': 'existente', 'periodo_id': existente.pk}

    if PeriodoVacacional.objects.filter(empleado=empleado, aniversario__year=aniversario.year).exists():
        return {**plan, 'estado': 'revision', 'motivo': 'Se requiere conciliar un periodo del mismo año con otra fecha de aniversario.'}

    anios = aniversario.year - empleado.fecha_ingreso.year
    politicas = PoliticaVacaciones.objects.filter(activo=True, antiguedad_desde__lte=anios)
    politicas = [p for p in politicas if p.antiguedad_hasta is None or p.antiguedad_hasta >= anios]
    if len(politicas) != 1 or politicas[0].dias_laborables <= 0:
        return {**plan, 'estado': 'revision', 'motivo': 'Se requiere conciliar la política vacacional aplicable.'}

    pendientes = SolicitudVacaciones.objects.filter(
        empleado=empleado, estado__in=['solicitada', 'preautorizada', 'aprobada'],
    ).exclude(aplicaciones_goce__estado__in=[
        AplicacionGoceVacaciones.ESTADO_RESERVADA, AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
    ])
    if pendientes.exists():
        return {**plan, 'estado': 'revision', 'motivo': 'Se requiere conciliar solicitudes sin aplicación de goce antes de generar el aniversario.'}
    movimientos = MovimientoVacaciones.objects.filter(empleado=empleado)
    ajuste_actual = movimientos.filter(tipo='ajuste', creado_en__date__gte=aniversario).exclude(
        descripcion__startswith='[saldo-inicial-vacaciones-20260616]',
    ).exists()
    consumo_sin_solicitud = movimientos.filter(
        tipo='consumido', solicitud__isnull=True, periodo_anio__gte=aniversario.year,
    ).exists()
    if ajuste_actual or consumo_sin_solicitud:
        return {**plan, 'estado': 'revision', 'motivo': 'Se requiere conciliar movimientos manuales del aniversario.'}
    mes = aniversario.month + 6
    anio_limite = aniversario.year + (mes - 1) // 12
    mes = (mes - 1) % 12 + 1
    limite = date(anio_limite, mes, min(aniversario.day, calendar.monthrange(anio_limite, mes)[1]))
    return {
        **plan, 'estado': 'propuesta', 'dias_generados': str(politicas[0].dias_laborables),
        'antiguedad_anios': anios, 'fecha_limite': limite.isoformat(),
    }


@transaction.atomic
def asegurar_periodo_actual(empleado_id, *, al=None, actor=None, referencia='automatico'):
    # Comparte este bloqueo con POST; la restricción única de la tabla es la
    # última defensa ante otros procesos que no usen este servicio.
    empleado = Empleado.objects.select_for_update().get(pk=empleado_id)
    plan = plan_periodo_actual(empleado, al)
    if plan['estado'] == 'revision':
        raise ValidationError(plan['motivo'])
    if plan['estado'] != 'propuesta':
        return plan
    periodo, creado = PeriodoVacacional.objects.get_or_create(
        empleado=empleado, aniversario=plan['aniversario'],
        defaults={
            'fecha_limite': plan['fecha_limite'], 'antiguedad_anios': plan['antiguedad_anios'],
            'dias_generados': plan['dias_generados'], 'origen': 'calculado',
            'notas': f'Generación de aniversario. Referencia: {referencia}',
        },
    )
    if creado:
        AuditLog.objects.create(
            user=actor if getattr(actor, 'is_authenticated', False) else None,
            action='VACACIONES_ANIVERSARIO', model='rrhh.PeriodoVacacional',
            object_id=str(periodo.pk), payload={**plan, 'referencia': referencia},
        )
    return {**plan, 'estado': 'creado' if creado else 'existente', 'periodo_id': periodo.pk}


def asegurar_periodos_actuales(*, empleado_ids=None, ejecutar=True, actor=None, referencia='diario'):
    empleados = Empleado.objects.filter(activo=True).order_by('pk')
    if empleado_ids is not None:
        empleados = empleados.filter(pk__in=empleado_ids)
    resultados = []
    for empleado in empleados.iterator():
        try:
            resultado = (
                asegurar_periodo_actual(empleado.pk, actor=actor, referencia=referencia)
                if ejecutar else plan_periodo_actual(empleado)
            )
        except ValidationError as exc:
            resultado = {'empleado_id': empleado.pk, 'nombre': empleado.nombre, 'estado': 'revision', 'motivo': '; '.join(exc.messages)}
        if resultado['estado'] == 'revision' and ejecutar:
            logger.warning('Aniversario vacacional por conciliar: empleado=%s motivo=%s', empleado.pk, resultado['motivo'])
        resultados.append(resultado)
    return resultados
