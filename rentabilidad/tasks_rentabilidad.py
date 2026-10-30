"""
=============================================================
sucursales/tasks.py  — Celery Beat tasks de rentabilidad
=============================================================
Configuración en settings.py / celery_app.py:

CELERY_BEAT_SCHEDULE = {
    "recalcular_rentabilidad_mensual": {
        "task": "sucursales.tasks.recalcular_rentabilidad_mensual",
        "schedule": crontab(minute=0, hour=6, day_of_month=1),  # día 1 de cada mes, 6am
    },
    "recalcular_rentabilidad_diario": {
        "task": "sucursales.tasks.recalcular_rentabilidad_periodo_actual",
        "schedule": crontab(minute=30, hour=23),  # cada noche 23:30 actualiza el mes en curso
    },
}
"""

import logging
from datetime import date
from decimal import Decimal
from celery import shared_task
from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def recalcular_rentabilidad_mensual(self, year=None, month=None):
    """
    Genera/actualiza los registros SucursalRentabilidad para un mes completo.
    Jala datos de: Ventas (pos_bridge), Nómina (HR), Gastos fijos (Gastos).

    Si year/month son None usa el mes anterior al actual (mes cerrado).
    """
    from .models_rentabilidad import SucursalRentabilidad
    hoy = timezone.localdate()
    if year is None or month is None:
        # Mes anterior (cerrado)
        if hoy.month == 1:
            year, month = hoy.year - 1, 12
        else:
            year, month = hoy.year, hoy.month - 1

    periodo = date(year, month, 1)
    logger.info(f"[RentabilidadTask] Recalculando periodo {periodo.strftime('%B %Y')}")

    # ---------------------------------------------------------- #
    # Aquí integras con los apps reales del ERP:
    # ---------------------------------------------------------- #
    from django.db.models import Sum, Q
    from core.models import Sucursal
    from reportes.models import (
        ProyectoInversion,
        ProyectoInversionGasto,
    )
    from reportes.services_rentabilidad_mensual import leer_costos_mensuales, aplicar_costos_en_memoria
    from rentabilidad.models_rentabilidad import SucursalRentabilidad

    import calendar

    # Rango del mes completo
    _, ultimo_dia = calendar.monthrange(year, month)
    fecha_inicio_mes = date(year, month, 1)
    fecha_fin_mes = date(year, month, ultimo_dia)

    costos_mensuales = leer_costos_mensuales(periodo)
    sucursales = Sucursal.objects.filter(Q(activa=True) | Q(pk__in=[sid for sid in costos_mensuales["resultado"]["sucursales"] if sid]))

    for suc in sucursales:

        # ---- INVERSIÓN INICIAL Y FECHA DE APERTURA DESDE PROYECTOS ----
        inversion_total = ProyectoInversionGasto.objects.filter(
            proyecto__sucursal_relacionada=suc,
            proyecto__estatus__in=[
                ProyectoInversion.ESTATUS_ACTIVO,
                ProyectoInversion.ESTATUS_EN_RECUPERACION,
                ProyectoInversion.ESTATUS_CERRADO,
                ProyectoInversion.ESTATUS_EJECUCION,
            ]
        ).aggregate(t=Sum("monto_total"))["t"] or Decimal("0")

        proyecto_apertura = ProyectoInversion.objects.filter(
            sucursal_relacionada=suc,
            tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
        ).order_by("fecha_inicio").first()

        fecha_apertura_suc = proyecto_apertura.fecha_inicio if proyecto_apertura else suc.fecha_apertura

        # ---- GUARDAR ----
        defaults = {
            "inversion_inicial": inversion_total,
            "fecha_apertura": fecha_apertura_suc,
        }
        with transaction.atomic():
            obj, created = SucursalRentabilidad.objects.update_or_create(
                sucursal=suc, periodo=fecha_inicio_mes, defaults=defaults,
            )
            resumen_gastos = aplicar_costos_en_memoria(obj, costos_mensuales)
            campos = ["ventas_brutas", "descuentos", "devoluciones", "costo_materia_prima",
                      "costo_reventa", "empaque", "otros_costos_variables", "estado", "alerta_nivel", "calculado_en"]
            obj.save(update_fields=campos + list(resumen_gastos["campos"]))
        if not resumen_gastos["completo"]:
            logger.warning(
                "[Rentabilidad] %s %s: cobertura %s%%; PE parcial, falta %s",
                suc.nombre, periodo, resumen_gastos["cobertura_pct"],
                ", ".join(resumen_gastos["familias_faltantes"]) or "confirmar importes",
            )
        logger.info(f"[Rentabilidad] {suc.nombre} {periodo} — {obj.estado}")

    logger.info(f"[RentabilidadTask] Completado periodo {periodo.strftime('%B %Y')}")
    return {"periodo": str(periodo), "ok": True}


@shared_task
def recalcular_rentabilidad_periodo_actual():
    """Actualiza el mes en curso (datos parciales) cada noche."""
    hoy = timezone.localdate()
    return recalcular_rentabilidad_mensual(year=hoy.year, month=hoy.month)


@shared_task
def analizar_sucursal_con_ia(rent_pk):
    """Tarea individual para analizar una sucursal con el agente IA."""
    from .models_rentabilidad import SucursalRentabilidad
    from .agente_rentabilidad import analizar_sucursal
    try:
        rent = SucursalRentabilidad.objects.get(pk=rent_pk)
        resultado = analizar_sucursal(rent, guardar=True)
        return {"ok": True, "sucursal": str(rent.sucursal), "estado": rent.estado}
    except SucursalRentabilidad.DoesNotExist:
        return {"ok": False, "error": f"No existe SucursalRentabilidad pk={rent_pk}"}
    except Exception as e:
        logger.exception(f"[AnalizarSucursal] Error pk={rent_pk}: {e}")
        return {"ok": False, "error": str(e)}
