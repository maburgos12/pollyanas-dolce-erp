# TASK: Reemplazar CMV estimado con costo real de recetas

## Contexto
- Archivo a modificar: rentabilidad/tasks_rentabilidad.py
- Reemplazar el estimado del 30% de CMV con costo real desde RecetaCostoVersion
- NO modificar ningún otro archivo

## Lógica confirmada
- PointDailySale.receta → FK a recetas.Receta (puede ser null)
- RecetaCostoVersion.receta → FK a recetas.Receta
- RecetaCostoVersion.costo_total → costo de producción por lote
- PointDailySale.quantity → unidades vendidas
- CMV real = SUM(RecetaCostoVersion.costo_total × PointDailySale.quantity)
  para todas las ventas del mes que tienen receta vinculada

## Cambio a implementar

En recalcular_rentabilidad_mensual, dentro del loop for suc in sucursales,
REEMPLAZA este bloque:

```python
# ---- COSTO MATERIA PRIMA ----
# VentaAutoritativaPoint no tiene costo directo.
# Usamos estimado: 30% de ventas brutas como proxy hasta conectar recetas.
# TODO: reemplazar con costo real desde recetas cuando esté disponible.
costo_mp_estimado = (ventas_brutas * Decimal("0.30")).quantize(Decimal("0.01"))
```

POR este bloque:

```python
# ---- COSTO MATERIA PRIMA (real desde RecetaCostoVersion) ----
from recetas.models import RecetaCostoVersion

costo_mp_real = Decimal("0")
ventas_con_receta = ventas_qs.filter(receta__isnull=False).select_related("receta")

for venta in ventas_con_receta:
    version = (
        RecetaCostoVersion.objects
        .filter(receta=venta.receta)
        .order_by("-version_num")
        .first()
    )
    if version and version.costo_total > 0:
        costo_mp_real += version.costo_total * venta.quantity

costo_mp_real = costo_mp_real.quantize(Decimal("0.01"))

# Si no se pudo calcular costo real (0 recetas vinculadas), usar estimado 30%
if costo_mp_real == Decimal("0") and ventas_brutas > 0:
    costo_mp_real = (ventas_brutas * Decimal("0.30")).quantize(Decimal("0.01"))
```

Luego en el bloque defaults del update_or_create, cambiar:

```python
"costo_materia_prima": costo_mp_estimado,
```

POR:

```python
"costo_materia_prima": costo_mp_real,
```

## NOTA DE RENDIMIENTO
El loop por venta puede ser lento si hay miles de registros.
Si el tiempo de ejecución supera 30 segundos para una sucursal,
optimizar con un dict de versiones en memoria:

```python
from recetas.models import RecetaCostoVersion
from django.db.models import Max
from collections import defaultdict

# Pre-cargar la versión más reciente de cada receta en un dict
receta_ids = ventas_qs.filter(
    receta__isnull=False
).values_list("receta_id", flat=True).distinct()

versiones = {}
for rv in RecetaCostoVersion.objects.filter(
    receta_id__in=receta_ids
).order_by("receta_id", "-version_num"):
    if rv.receta_id not in versiones:
        versiones[rv.receta_id] = rv.costo_total

costo_mp_real = Decimal("0")
for venta in ventas_qs.filter(receta__isnull=False):
    costo = versiones.get(venta.receta_id, Decimal("0"))
    costo_mp_real += costo * venta.quantity

costo_mp_real = costo_mp_real.quantize(Decimal("0.01"))

if costo_mp_real == Decimal("0") and ventas_brutas > 0:
    costo_mp_real = (ventas_brutas * Decimal("0.30")).quantize(Decimal("0.01"))
```

Usa la versión optimizada con dict directamente — es más eficiente 
para 9 sucursales con miles de registros cada una.

## Validación después del cambio

```bash
./.venv/bin/python manage.py shell -c "
from rentabilidad.tasks_rentabilidad import recalcular_rentabilidad_mensual
recalcular_rentabilidad_mensual(2026, 3)
from rentabilidad.models_rentabilidad import SucursalRentabilidad
for r in SucursalRentabilidad.objects.filter(periodo__year=2026, periodo__month=3).select_related('sucursal').order_by('-ventas_brutas'):
    print(r.sucursal.nombre, 'CMV%:', float(r.costo_materia_prima/r.ventas_netas*100) if r.ventas_netas else 0, 'Margen%:', float(r.porcentaje_margen_bruto), 'Estado:', r.estado)
"
```

Reporta el output completo incluyendo tiempos si son lentos.

## LO QUE NO DEBES TOCAR
- El bloque de ventas (PointDailySale)
- El bloque de gastos (GastoOperativoMensual)
- El bloque de nómina
- El bloque de inversiones (ProyectoInversion)
- Ningún archivo fuera de tasks_rentabilidad.py
