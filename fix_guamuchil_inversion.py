import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.db import transaction
from django.db.models import Sum
from decimal import Decimal
from datetime import date
from reportes.models import (
    GastoOperativoMensual, CentroCosto,
    ProyectoInversion, ProyectoInversionGasto,
)
from core.models import Sucursal

suc = Sucursal.objects.get(codigo='GUAMUCHIL')
centro = CentroCosto.objects.get(codigo='GUAMUCHIL')

with transaction.atomic():

    eliminados, _ = GastoOperativoMensual.objects.filter(
        centro_costo=centro,
        categoria_gasto__codigo='INVERSION',
    ).delete()
    print(f'Registros INVERSION eliminados: {eliminados}')

    proyecto, created = ProyectoInversion.objects.get_or_create(
        nombre_proyecto='Apertura Guamuchil 2026',
        defaults={
            'sucursal_relacionada': suc,
            'tipo_proyecto': ProyectoInversion.TIPO_APERTURA_SUCURSAL,
            'estatus': ProyectoInversion.ESTATUS_ACTIVO,
            'fecha_inicio': suc.fecha_apertura or date(2026, 1, 1),
        }
    )
    print(f'ProyectoInversion creado={created}: {proyecto.nombre_proyecto}')

    gasto, gcreated = ProyectoInversionGasto.objects.get_or_create(
        proyecto=proyecto,
        descripcion='Inversion apertura importado presupuesto 2026',
        defaults={
            'fecha': suc.fecha_apertura or date(2026, 1, 1),
            'categoria': ProyectoInversionGasto.CATEGORIA_OTROS,
            'monto': Decimal('492343.00'),
            'iva': Decimal('0'),
            'monto_total': Decimal('492343.00'),
        }
    )
    print(f'ProyectoInversionGasto creado={gcreated}: ${gasto.monto_total:,.2f}')

total_op = GastoOperativoMensual.objects.filter(
    centro_costo=centro, tipo_dato='REAL'
).aggregate(t=Sum('monto'))['t'] or 0
print(f'Gasto operativo Guamuchil despues: ${total_op:,.2f}')

from rentabilidad.tasks_rentabilidad import recalcular_rentabilidad_mensual
recalcular_rentabilidad_mensual(2026, 3)

from rentabilidad.models_rentabilidad import SucursalRentabilidad
r = SucursalRentabilidad.objects.get(
    sucursal__codigo='GUAMUCHIL',
    periodo__year=2026,
    periodo__month=3
)
print(f'Estado: {r.estado}')
print(f'Ventas: ${r.ventas_netas:,.0f}')
print(f'Gasto fijo: ${r.gasto_fijo_total:,.0f}')
print(f'Inversion inicial: ${r.inversion_inicial:,.0f}')
print(f'Utilidad: ${r.utilidad_operativa:,.0f}')
