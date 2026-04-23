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

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def recalcular_rentabilidad_mensual(self, year=None, month=None):
    """
    Genera/actualiza los registros SucursalRentabilidad para un mes completo.
    Jala datos de: Ventas (pos_bridge), Nómina (HR), Gastos fijos (Gastos).

    Si year/month son None usa el mes anterior al actual (mes cerrado).
    """
    from .models_rentabilidad import SucursalRentabilidad
    # Importa tus modelos reales aquí:
    # from sucursales.models import Sucursal
    # from ventas.models import Venta
    # from gastos.models import GastoFijo
    # from hr.models import NominaDetalle

    hoy = date.today()
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
    from pos_bridge.models.sales import PointDailySale
    from recetas.models import RecetaCostoVersion
    from reportes.models import (
        GastoOperativoMensual,
        CentroCosto,
        CategoriaGasto,
        ProyectoInversion,
        ProyectoInversionGasto,
    )
    from rrhh.models import NominaLinea, NominaPeriodo
    from rentabilidad.models_rentabilidad import SucursalRentabilidad

    import calendar

    # Rango del mes completo
    _, ultimo_dia = calendar.monthrange(year, month)
    fecha_inicio_mes = date(year, month, 1)
    fecha_fin_mes = date(year, month, ultimo_dia)

    sucursales = Sucursal.objects.filter(activa=True)

    for suc in sucursales:

        # ---- VENTAS ----
        ventas_qs = PointDailySale.objects.filter(
            branch__erp_branch=suc,
            sale_date__year=year,
            sale_date__month=month,
        )
        ventas_brutas = ventas_qs.aggregate(t=Sum("gross_amount"))["t"] or Decimal("0")
        descuentos = ventas_qs.aggregate(t=Sum("discount_amount"))["t"] or Decimal("0")

        # ---- GASTOS OPERATIVOS (renta, servicios, mantenimiento) ----
        # Gastos del centro de costo de esta sucursal, tipo REAL
        centros_suc = CentroCosto.objects.filter(sucursal=suc)
        gastos_qs = GastoOperativoMensual.objects.filter(
            centro_costo__in=centros_suc,
            periodo__year=year,
            periodo__month=month,
            tipo_dato="REAL",
        )

        # Clasificar por categoría (ajusta los nombres según tus CategoriaGasto reales)
        def suma_categoria(keyword):
            return gastos_qs.filter(
                categoria_gasto__nombre__icontains=keyword
            ).aggregate(t=Sum("monto"))["t"] or Decimal("0")

        renta = suma_categoria("renta")
        servicios = suma_categoria("servicio") + suma_categoria("luz") + suma_categoria("agua")
        mantenimiento = suma_categoria("mantenimiento")
        otros_fijos = gastos_qs.exclude(
            Q(categoria_gasto__nombre__icontains="renta") |
            Q(categoria_gasto__nombre__icontains="servicio") |
            Q(categoria_gasto__nombre__icontains="luz") |
            Q(categoria_gasto__nombre__icontains="agua") |
            Q(categoria_gasto__nombre__icontains="mantenimiento") |
            Q(categoria_gasto__nombre__icontains="nomina") |
            Q(categoria_gasto__nombre__icontains="nómina")
        ).aggregate(t=Sum("monto"))["t"] or Decimal("0")

        # ---- NÓMINA desde GastoOperativoMensual categoría NOMINA ----
        nomina_directa = gastos_qs.filter(
            categoria_gasto__codigo="NOMINA"
        ).aggregate(t=Sum("monto"))["t"] or Decimal("0")

        # ---- COSTO MATERIA PRIMA (real desde RecetaCostoVersion) ----
        receta_ids = list(
            ventas_qs.filter(receta__isnull=False).values_list("receta_id", flat=True).distinct()
        )

        from django.db.models import Case, IntegerField, When

        versiones = {}
        for rv in RecetaCostoVersion.objects.filter(
            receta_id__in=receta_ids
        ).annotate(
            fuente_prioridad=Case(
                When(fuente="POINT_PRODUCTION_REPORT", then=0),
                When(fuente="POINT_COST_CAPTURE", then=1),
                When(fuente="POINT_COST_CAPTURE_FIX", then=2),
                default=3,
                output_field=IntegerField(),
            )
        ).order_by("receta_id", "fuente_prioridad", "-version_num"):
            if rv.receta_id not in versiones:
                versiones[rv.receta_id] = rv.costo_total

        costo_mp_real = Decimal("0")
        for venta in ventas_qs.filter(receta__isnull=False):
            costo = versiones.get(venta.receta_id, Decimal("0"))
            costo_mp_real += costo * venta.quantity

        costo_mp_real = costo_mp_real.quantize(Decimal("0.01"))

        # Si no se pudo calcular costo real (0 recetas vinculadas), usar estimado 30%
        if costo_mp_real == Decimal("0") and ventas_brutas > 0:
            costo_mp_real = (ventas_brutas * Decimal("0.30")).quantize(Decimal("0.01"))
        # Cap de seguridad: si CMV supera 80% de ventas, el costeo está mal escalado
        # Usar fallback 30% hasta que el costeo real se corrija
        elif ventas_brutas > 0 and costo_mp_real > (ventas_brutas * Decimal("0.80")):
            costo_mp_real = (ventas_brutas * Decimal("0.30")).quantize(Decimal("0.01"))

        # ---- GASTOS CORPORATIVOS PRORRATEADOS ----
        # Tomamos gastos de centros de costo CORPORATIVO y los dividimos entre 9 sucursales
        centros_corp = CentroCosto.objects.filter(tipo="CORPORATIVO")
        gasto_corp_total = GastoOperativoMensual.objects.filter(
            centro_costo__in=centros_corp,
            periodo__year=year,
            periodo__month=month,
            tipo_dato="REAL",
        ).aggregate(t=Sum("monto"))["t"] or Decimal("0")
        admin_prorrateado = (gasto_corp_total / Decimal("9")).quantize(Decimal("0.01"))

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
        obj, created = SucursalRentabilidad.objects.update_or_create(
            sucursal=suc,
            periodo=fecha_inicio_mes,
            defaults={
                "ventas_brutas": ventas_brutas,
                "descuentos": descuentos,
                "devoluciones": Decimal("0"),
                "costo_materia_prima": costo_mp_real,
                "empaque": Decimal("0"),
                "otros_costos_variables": Decimal("0"),
                "renta": renta,
                "nomina_directa": nomina_directa,
                "servicios_luz_agua": servicios,
                "mantenimiento": mantenimiento,
                "gastos_admin_prorrateados": admin_prorrateado,
                "otros_gastos_fijos": otros_fijos,
                "inversion_inicial": inversion_total,
                "fecha_apertura": fecha_apertura_suc,
            }
        )
        obj.calcular_estado()
        obj.save()
        logger.info(f"[Rentabilidad] {suc.nombre} {periodo} — {obj.estado}")

    logger.info(f"[RentabilidadTask] Completado periodo {periodo.strftime('%B %Y')}")
    return {"periodo": str(periodo), "ok": True}


@shared_task
def recalcular_rentabilidad_periodo_actual():
    """Actualiza el mes en curso (datos parciales) cada noche."""
    hoy = date.today()
    return recalcular_rentabilidad_mensual.delay(hoy.year, hoy.month)


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


"""
=============================================================
MIGRACIÓN  — Agregar al final de tu migration más reciente
o crear nueva: python manage.py makemigrations sucursales
=============================================================

from django.db import migrations, models
import django.db.models.deletion

class Migration(migrations.Migration):
    dependencies = [
        ('sucursales', '000X_anterior'),
    ]
    operations = [
        migrations.CreateModel(
            name='SucursalRentabilidad',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True)),
                ('sucursal', models.ForeignKey('sucursales.Sucursal', on_delete=django.db.models.deletion.CASCADE, related_name='rentabilidad_mensual')),
                ('periodo', models.DateField()),
                ('ventas_brutas', models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ('descuentos', models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ('devoluciones', models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ('costo_materia_prima', models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ('empaque', models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ('otros_costos_variables', models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ('renta', models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ('nomina_directa', models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ('servicios_luz_agua', models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ('mantenimiento', models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ('gastos_admin_prorrateados', models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ('otros_gastos_fijos', models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ('inversion_inicial', models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ('fecha_apertura', models.DateField(blank=True, null=True)),
                ('subsidio_recibido', models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ('estado', models.CharField(choices=[...], db_index=True, default='SIN_DATOS', max_length=20)),
                ('diagnostico_ia', models.TextField(blank=True, default='')),
                ('recomendaciones_ia', models.JSONField(default=list)),
                ('alerta_nivel', models.IntegerField(default=0)),
                ('calculado_en', models.DateTimeField(auto_now=True)),
                ('calculado_por_agente', models.BooleanField(default=False)),
                ('notas_manuales', models.TextField(blank=True, default='')),
            ],
            options={'ordering': ['-periodo', 'sucursal'], 'unique_together': {('sucursal', 'periodo')}},
        ),
    ]
"""

"""
=============================================================
urls.py  — Agregar en sucursales/urls.py
=============================================================

from django.urls import path
from . import views_rentabilidad

app_name = 'sucursales'

urlpatterns += [
    path('rentabilidad/',                          views_rentabilidad.dashboard_rentabilidad, name='rentabilidad_dashboard'),
    path('rentabilidad/<int:pk>/',                 views_rentabilidad.detalle_sucursal,       name='rentabilidad_detalle'),
    path('rentabilidad/<int:pk>/analizar/',        views_rentabilidad.analizar_con_ia,        name='rentabilidad_analizar'),
    path('rentabilidad/analizar-todas/',           views_rentabilidad.analizar_todas,         name='rentabilidad_analizar_todas'),
]
"""
