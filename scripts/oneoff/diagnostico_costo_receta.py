import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from recetas.models import RecetaCostoVersion, RecetaCostoSemanal

print('=== CAMPOS RecetaCostoVersion ===')
for f in RecetaCostoVersion._meta.fields:
    print(f'  {f.name} ({f.get_internal_type()})')

print()
print('=== PRIMERA RecetaCostoVersion ===')
v = RecetaCostoVersion.objects.order_by('-id').first()
if v:
    for f in RecetaCostoVersion._meta.fields:
        val = getattr(v, f.name)
        if val not in [None, '', 0, False]:
            print(f'  {f.name} = {val}')

print()
print('=== CAMPOS RecetaCostoSemanal ===')
for f in RecetaCostoSemanal._meta.fields:
    print(f'  {f.name} ({f.get_internal_type()})')

print()
print('=== PRIMERA RecetaCostoSemanal ===')
s = RecetaCostoSemanal.objects.order_by('-id').first()
if s:
    for f in RecetaCostoSemanal._meta.fields:
        val = getattr(s, f.name)
        if val not in [None, '', 0, False]:
            print(f'  {f.name} = {val}')

print()
print('Totales:')
print(f'  RecetaCostoVersion: {RecetaCostoVersion.objects.count()}')
print(f'  RecetaCostoSemanal: {RecetaCostoSemanal.objects.count()}')
