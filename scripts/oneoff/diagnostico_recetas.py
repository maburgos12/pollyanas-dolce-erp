import os, sys, django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from recetas.models import Receta

print('=== TODOS LOS CAMPOS DE RECETA ===')
for f in Receta._meta.fields:
    print(f'  {f.name} ({f.get_internal_type()})')

print()
print('=== PRIMERA RECETA CON DATOS ===')
r = Receta.objects.first()
if r:
    print(f'Nombre: {r.nombre}')
    for f in Receta._meta.fields:
        val = getattr(r, f.name)
        if val not in [None, '', 0, False]:
            print(f'  {f.name} = {val}')

print()
print('=== MODELOS RELACIONADOS A RECETA ===')
for rel in Receta._meta.related_objects:
    print(f'  {rel.related_model.__name__} via {rel.field.name}')
