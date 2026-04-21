import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from pos_bridge.models.branch import PointBranch
from core.models import Sucursal

print('=== TODOS LOS POINT BRANCHES ===')
for b in PointBranch.objects.all().order_by('name'):
    erp = b.erp_branch.nombre if b.erp_branch else 'SIN MAPEO'
    print(f'  [{b.external_id}] {b.name} -> {erp}')

print()
print('=== SUCURSALES ERP SIN POINT BRANCH ===')
sucursales_mapeadas = set(
    PointBranch.objects.exclude(erp_branch=None)
    .values_list('erp_branch_id', flat=True)
)
for suc in Sucursal.objects.filter(activa=True):
    if suc.id not in sucursales_mapeadas:
        print(f'  {suc.nombre} (codigo={suc.codigo})')
