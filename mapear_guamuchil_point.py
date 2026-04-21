import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from pos_bridge.models.branch import PointBranch
from core.models import Sucursal

suc = Sucursal.objects.get(codigo='GUAMUCHIL')

print('=== POINT BRANCHES SIN MAPEO ===')
for b in PointBranch.objects.filter(erp_branch=None).order_by('name'):
    from pos_bridge.models.sales import PointDailySale
    from django.db.models import Sum, Max
    agg = PointDailySale.objects.filter(branch=b).aggregate(
        total=Sum('net_amount'), ultimo=Max('sale_date')
    )
    print(f'  [{b.external_id}] {b.name} -> ventas={agg["total"]} ultimo={agg["ultimo"]}')

print()
print('Para mapear Guamuchil ejecuta:')
print('PointBranch.objects.filter(external_id="AQUI_EL_ID_DE_GUAMUCHIL").update(erp_branch=suc)')
print()
print('Sustituye AQUI_EL_ID_DE_GUAMUCHIL con el external_id')
print('que aparezca arriba con ventas de Guamuchil en produccion.')
