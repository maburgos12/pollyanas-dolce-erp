# TASK: Conectar ProyectoInversion con SucursalRentabilidad

## Contexto
- Archivo a modificar: rentabilidad/tasks_rentabilidad.py
- Solo modificar el bloque de inversión dentro de recalcular_rentabilidad_mensual
- NO tocar ningún otro archivo

## Qué hacer

En la función `recalcular_rentabilidad_mensual`, dentro del loop `for suc in sucursales`,
ANTES del bloque `update_or_create`, agregar este código:

```python
from reportes.models import ProyectoInversion, ProyectoInversionGasto
from django.db.models import Sum

# Sumar inversión total de todos los proyectos de esta sucursal
inversion_total = ProyectoInversionGasto.objects.filter(
    proyecto__sucursal_relacionada=suc,
    proyecto__estatus__in=[
        ProyectoInversion.ESTATUS_ACTIVO,
        ProyectoInversion.ESTATUS_EN_RECUPERACION,
        ProyectoInversion.ESTATUS_CERRADO,
        ProyectoInversion.ESTATUS_EJECUCION,
    ]
).aggregate(t=Sum("monto_real"))["t"] or Decimal("0")

# Fecha de apertura desde el proyecto tipo APERTURA_SUCURSAL si existe
proyecto_apertura = ProyectoInversion.objects.filter(
    sucursal_relacionada=suc,
    tipo_proyecto=ProyectoInversion.TIPO_APERTURA_SUCURSAL,
).order_by("fecha_inicio").first()

fecha_apertura_suc = proyecto_apertura.fecha_inicio if proyecto_apertura else suc.fecha_apertura
```

Luego en el bloque `defaults` del `update_or_create`, agregar estos dos campos:

```python
"inversion_inicial": inversion_total,
"fecha_apertura":    fecha_apertura_suc,
```

## Verificación después del cambio

```bash
./.venv/bin/python manage.py shell -c "
from reportes.models import ProyectoInversion, ProyectoInversionGasto
from core.models import Sucursal
from django.db.models import Sum

print('=== PROYECTOS DE INVERSIÓN POR SUCURSAL ===')
for suc in Sucursal.objects.filter(activa=True).order_by('nombre'):
    total = ProyectoInversionGasto.objects.filter(
        proyecto__sucursal_relacionada=suc
    ).aggregate(t=Sum('monto_real'))['t'] or 0
    proyectos = ProyectoInversion.objects.filter(sucursal_relacionada=suc).count()
    print(f'  {suc.nombre}: {proyectos} proyectos — inversion=\${total:,.0f}')
"
```

Reporta el output completo.

## LO QUE NO DEBES TOCAR
- El bloque de ventas (PointDailySale)
- El bloque de gastos (GastoOperativoMensual)
- El bloque de nómina
- El estimado de CMV 30%
- Ningún archivo fuera de tasks_rentabilidad.py

## NOTA
Si ProyectoInversionGasto no tiene campo `monto_real` sino otro nombre
(por ejemplo `monto`, `importe`, `costo_real`), reportar el error
y buscar el nombre correcto con:
grep "monto" reportes/models.py | grep -i "inversion\|gasto"
