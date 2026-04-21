import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from pos_bridge.models.sales import PointDailySale
from recetas.models import RecetaCostoVersion
from django.db.models import Sum, Max
from decimal import Decimal
import calendar
from datetime import date

year, month = 2026, 3
fecha_inicio = date(year, month, 1)
fecha_fin = date(year, month, calendar.monthrange(year, month)[1])

from core.models import Sucursal
suc = Sucursal.objects.get(codigo='MATRIZ')

ventas = PointDailySale.objects.filter(
    branch__erp_branch=suc,
    sale_date__range=(fecha_inicio, fecha_fin),
    receta__isnull=False,
)
print(f'Ventas con receta vinculada: {ventas.count()}')
print(f'Ventas sin receta: {PointDailySale.objects.filter(branch__erp_branch=suc, sale_date__range=(fecha_inicio, fecha_fin), receta__isnull=True).count()}')

costo_total = Decimal('0')
sin_costo = 0
con_costo = 0

for venta in ventas:
    version = RecetaCostoVersion.objects.filter(
        receta=venta.receta
    ).order_by('-version_num').first()
    
    if version and version.costo_total > 0:
        costo_total += version.costo_total * venta.quantity
        con_costo += 1
    else:
        sin_costo += 1

ventas_netas = PointDailySale.objects.filter(
    branch__erp_branch=suc,
    sale_date__range=(fecha_inicio, fecha_fin),
).aggregate(t=Sum('net_amount'))['t'] or Decimal('0')

print(f'Ventas netas Matriz marzo: ${ventas_netas:,.2f}')
print(f'Costo MP calculado: ${costo_total:,.2f}')
print(f'CMV real: {(costo_total/ventas_netas*100):.1f}%' if ventas_netas else 'N/A')
print(f'Productos con costo: {con_costo}')
print(f'Productos sin costo: {sin_costo}')
