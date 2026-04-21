# TASK: Actualizar integración de ventas en recalcular_rentabilidad_mensual

## Contexto
- Archivo a modificar: rentabilidad/tasks_rentabilidad.py
- Función a modificar: recalcular_rentabilidad_mensual
- NO modificar ningún otro archivo

## Problema actual
La integración usa `VentaAutoritativaPoint` que está vacía en producción.
El modelo correcto es `PointDailySale` que se llena por el pipeline operativo de pos_bridge.

## Mapa de modelos confirmado

### Ventas
```python
# pos_bridge/models/sales.py
class PointDailySale(models.Model):
    branch = ForeignKey("pos_bridge.PointBranch")
    sale_date = DateField()
    gross_amount = DecimalField()
    discount_amount = DecimalField()
    net_amount = DecimalField()

# pos_bridge/models/branch.py  
class PointBranch(models.Model):
    erp_branch = ForeignKey("core.Sucursal")  # ← cruce directo con Sucursal
```

### Gastos (sin cambio)
```python
# reportes/models.py
GastoOperativoMensual → centro_costo → CentroCosto.sucursal → core.Sucursal
```

### Nómina
Empleado.sucursal es CharField sin datos — dejar en Decimal("0") por ahora con un TODO.

## Cambio a implementar

En `recalcular_rentabilidad_mensual`, reemplaza el bloque de ventas:

### ANTES (quitar esto):
```python
from ventas.models import VentaAutoritativaPoint
ventas_qs = VentaAutoritativaPoint.objects.filter(
    branch=suc,
    sale_date__year=year,
    sale_date__month=month,
)
ventas_brutas = ventas_qs.aggregate(t=Sum("gross_amount"))["t"] or Decimal("0")
descuentos    = ventas_qs.aggregate(t=Sum("discount_amount"))["t"] or Decimal("0")
```

### DESPUÉS (poner esto):
```python
from pos_bridge.models.sales import PointDailySale
ventas_qs = PointDailySale.objects.filter(
    branch__erp_branch=suc,
    sale_date__year=year,
    sale_date__month=month,
)
ventas_brutas = ventas_qs.aggregate(t=Sum("gross_amount"))["t"] or Decimal("0")
descuentos    = ventas_qs.aggregate(t=Sum("discount_amount"))["t"] or Decimal("0")
```

## Validación después del cambio

```bash
./.venv/bin/python manage.py check
./.venv/bin/python manage.py shell -c "
from pos_bridge.models.sales import PointDailySale
from core.models import Sucursal
suc = Sucursal.objects.filter(activa=True).first()
if suc:
    from django.db.models import Sum
    total = PointDailySale.objects.filter(
        branch__erp_branch=suc
    ).aggregate(t=Sum('net_amount'))['t']
    print(f'{suc.nombre}: \${total}')
else:
    print('Sin sucursales activas')
"
```

Reporta el output completo.

## LO QUE NO DEBES TOCAR
- El bloque de gastos (GastoOperativoMensual) — no cambia
- El bloque de nómina — dejar como está con el TODO
- El costo de materia prima estimado (30%) — no cambia aún
- Ningún otro archivo fuera de tasks_rentabilidad.py
