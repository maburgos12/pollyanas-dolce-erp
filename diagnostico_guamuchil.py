import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from pos_bridge.models.branch import PointBranch
from pos_bridge.models.sales import PointDailySale
from django.db.models import Sum, Max

print('=== POINT BRANCHES CON NOMBRE GUAMUCHIL ===')
for b in PointBranch.objects.all():
    if 'gua' in b.name.lower() or 'gua' in str(b.external_id).lower():
        print(f'  [{b.external_id}] {b.name} -> erp_branch={b.erp_branch}')

print()
print('=== TODOS LOS BRANCHES SIN MAPEO ===')
for b in PointBranch.objects.filter(erp_branch=None):
    agg = PointDailySale.objects.filter(branch=b).aggregate(
        total=Sum('net_amount'), ultimo=Max('sale_date')
    )
    print(f'  [{b.external_id}] {b.name} -> ventas={agg["total"]} ultimo={agg["ultimo"]}')

print()
print('=== RECETAS CON COSTO ===')
from recetas.models import Receta
campos_costo = [f.name for f in Receta._meta.fields 
                if 'cost' in f.name.lower() or 'costo' in f.name.lower()]
print(f'Campos de costo en Receta: {campos_costo}')
total_recetas = Receta.objects.count()
print(f'Total recetas: {total_recetas}')
if campos_costo:
    campo = campos_costo[0]
    con_costo = Receta.objects.filter(**{f'{campo}__gt': 0}).count()
    print(f'Con {campo} > 0: {con_costo}')
    ejemplo = Receta.objects.filter(**{f'{campo}__gt': 0}).first()
    if ejemplo:
        print(f'Ejemplo: {ejemplo.nombre} -> {campo}={getattr(ejemplo, campo)}')
