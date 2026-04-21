import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from rentabilidad.tasks_rentabilidad import recalcular_rentabilidad_mensual

for year, month in [(2026, 1), (2026, 2)]:
    print(f'Recalculando {year}-{month:02d}...')
    recalcular_rentabilidad_mensual(year, month)

from rentabilidad.models_rentabilidad import SucursalRentabilidad
print()
print('=== HISTORIAL 3 MESES ===')
for r in SucursalRentabilidad.objects.filter(
    periodo__year=2026
).select_related('sucursal').order_by('sucursal__nombre', 'periodo'):
    print(f'{r.sucursal.nombre} {r.periodo.strftime("%b")}: ventas={r.ventas_netas:,.0f} utilidad={r.utilidad_operativa:,.0f} margen={r.porcentaje_utilidad_operativa}%')
