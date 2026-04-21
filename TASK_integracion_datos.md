# TASK: Integrar datos reales en recalcular_rentabilidad_mensual

## Contexto
- Repo: pastelerias_erp_sprint1
- Archivo a modificar: rentabilidad/tasks_rentabilidad.py
- Función a completar: recalcular_rentabilidad_mensual
- NO modificar ningún otro archivo

## Modelos disponibles (ya existen en el proyecto)

### Ventas
```python
# ventas/models.py
class VentaAutoritativaPoint(models.Model):
    branch = ForeignKey("core.Sucursal", related_name="ventas_autoritativas_point")
    sale_date = DateField()
    gross_amount = DecimalField()   # venta bruta
    discount_amount = DecimalField() # descuentos
    net_amount = DecimalField()     # venta neta
    # No hay campo de costo directo en este modelo
```

### Gastos operativos
```python
# reportes/models.py
class CentroCosto(models.Model):
    sucursal = ForeignKey("core.Sucursal")  # puede ser null si es corporativo
    tipo = CharField()  # "SUCURSAL_VENTA", "CORPORATIVO", etc.

class CategoriaGasto(models.Model):
    nombre = CharField()
    # Buscar categorías que contengan: RENTA, NOMINA, SERVICIOS, MANTENIMIENTO

class GastoOperativoMensual(models.Model):
    periodo = DateField()           # primer día del mes
    centro_costo = ForeignKey(CentroCosto)
    categoria_gasto = ForeignKey(CategoriaGasto)
    monto = DecimalField()
    tipo_dato = CharField()         # "REAL" o "PRESUPUESTO"
```

### Nómina
```python
# rrhh/models.py
class NominaPeriodo(models.Model):
    fecha_inicio = DateField()
    fecha_fin = DateField()
    tipo_periodo = CharField()      # SEMANAL, QUINCENAL, MENSUAL
    estatus = CharField()           # BORRADOR, CERRADA, PAGADA

class NominaLinea(models.Model):
    periodo = ForeignKey(NominaPeriodo)
    empleado = ForeignKey(Empleado)
    neto_calculado = DecimalField()

class Empleado(models.Model):
    sucursal = CharField(max_length=120)  # nombre de la sucursal como texto, no FK
    nombre = CharField()
```

---

## Lógica a implementar

Reemplaza el bloque comentado dentro de `recalcular_rentabilidad_mensual` con este código real:

```python
from django.db.models import Sum, Q
from decimal import Decimal
from core.models import Sucursal
from ventas.models import VentaAutoritativaPoint
from reportes.models import GastoOperativoMensual, CentroCosto, CategoriaGasto
from rrhh.models import NominaLinea, NominaPeriodo
from rentabilidad.models_rentabilidad import SucursalRentabilidad

import calendar

# Rango del mes completo
_, ultimo_dia = calendar.monthrange(year, month)
fecha_inicio_mes = date(year, month, 1)
fecha_fin_mes = date(year, month, ultimo_dia)

sucursales = Sucursal.objects.filter(activa=True)

for suc in sucursales:

    # ---- VENTAS ----
    ventas_qs = VentaAutoritativaPoint.objects.filter(
        branch=suc,
        sale_date__year=year,
        sale_date__month=month,
    )
    ventas_brutas    = ventas_qs.aggregate(t=Sum("gross_amount"))["t"] or Decimal("0")
    descuentos       = ventas_qs.aggregate(t=Sum("discount_amount"))["t"] or Decimal("0")

    # ---- GASTOS OPERATIVOS (renta, servicios, mantenimiento) ----
    # Gastos del centro de costo de esta sucursal, tipo REAL
    centros_suc = CentroCosto.objects.filter(sucursal=suc)
    gastos_qs = GastoOperativoMensual.objects.filter(
        centro_costo__in=centros_suc,
        periodo__year=year,
        periodo__month=month,
        tipo_dato="REAL",
    )

    # Clasificar por categoría (ajusta los nombres según tus CategoriaGasto reales)
    def suma_categoria(keyword):
        return gastos_qs.filter(
            categoria_gasto__nombre__icontains=keyword
        ).aggregate(t=Sum("monto"))["t"] or Decimal("0")

    renta         = suma_categoria("renta")
    servicios     = suma_categoria("servicio") + suma_categoria("luz") + suma_categoria("agua")
    mantenimiento = suma_categoria("mantenimiento")
    otros_fijos   = gastos_qs.exclude(
        Q(categoria_gasto__nombre__icontains="renta") |
        Q(categoria_gasto__nombre__icontains="servicio") |
        Q(categoria_gasto__nombre__icontains="luz") |
        Q(categoria_gasto__nombre__icontains="agua") |
        Q(categoria_gasto__nombre__icontains="mantenimiento") |
        Q(categoria_gasto__nombre__icontains="nomina") |
        Q(categoria_gasto__nombre__icontains="nómina")
    ).aggregate(t=Sum("monto"))["t"] or Decimal("0")

    # ---- NÓMINA ----
    # NominaLinea no tiene FK a sucursal, solo Empleado.sucursal como CharField
    # Cruzamos por nombre de sucursal (normalizado)
    periodos_nomina = NominaPeriodo.objects.filter(
        fecha_inicio__lte=fecha_fin_mes,
        fecha_fin__gte=fecha_inicio_mes,
        estatus__in=["CERRADA", "PAGADA"],
    )
    nomina_directa = NominaLinea.objects.filter(
        periodo__in=periodos_nomina,
        empleado__sucursal__icontains=suc.nombre,
    ).aggregate(t=Sum("neto_calculado"))["t"] or Decimal("0")

    # ---- COSTO MATERIA PRIMA ----
    # VentaAutoritativaPoint no tiene costo directo.
    # Usamos estimado: 30% de ventas brutas como proxy hasta conectar recetas.
    # TODO: reemplazar con costo real desde recetas cuando esté disponible.
    costo_mp_estimado = (ventas_brutas * Decimal("0.30")).quantize(Decimal("0.01"))

    # ---- GASTOS CORPORATIVOS PRORRATEADOS ----
    # Tomamos gastos de centros de costo CORPORATIVO y los dividimos entre 9 sucursales
    centros_corp = CentroCosto.objects.filter(tipo="CORPORATIVO")
    gasto_corp_total = GastoOperativoMensual.objects.filter(
        centro_costo__in=centros_corp,
        periodo__year=year,
        periodo__month=month,
        tipo_dato="REAL",
    ).aggregate(t=Sum("monto"))["t"] or Decimal("0")
    admin_prorrateado = (gasto_corp_total / Decimal("9")).quantize(Decimal("0.01"))

    # ---- GUARDAR ----
    obj, created = SucursalRentabilidad.objects.update_or_create(
        sucursal=suc,
        periodo=fecha_inicio_mes,
        defaults={
            "ventas_brutas":               ventas_brutas,
            "descuentos":                  descuentos,
            "devoluciones":                Decimal("0"),
            "costo_materia_prima":         costo_mp_estimado,
            "empaque":                     Decimal("0"),
            "otros_costos_variables":      Decimal("0"),
            "renta":                       renta,
            "nomina_directa":              nomina_directa,
            "servicios_luz_agua":          servicios,
            "mantenimiento":               mantenimiento,
            "gastos_admin_prorrateados":   admin_prorrateado,
            "otros_gastos_fijos":          otros_fijos,
            "fecha_apertura":              suc.fecha_apertura,
        }
    )
    obj.calcular_estado()
    obj.save()
    logger.info(f"[Rentabilidad] {suc.nombre} {periodo} — {obj.estado}")
```

---

## Instrucciones para Codex

1. Abre `rentabilidad/tasks_rentabilidad.py`
2. Localiza el comentario `# Aquí integras con los apps reales del ERP`
3. Reemplaza ese bloque comentado completo con el código de arriba
4. Mantén intacto todo lo demás del archivo
5. Ejecuta:
   ```bash
   ./.venv/bin/python manage.py check
   ./.venv/bin/python manage.py shell -c "
   from rentabilidad.tasks_rentabilidad import recalcular_rentabilidad_mensual
   print('Import OK')
   "
   ```
6. Reporta cualquier ImportError con el nombre exacto del modelo que no encontró

## IMPORTANTE
- El costo de materia prima es un estimado del 30% por ahora — hay un TODO en el código
- La nómina usa `icontains` sobre el nombre de sucursal porque `Empleado.sucursal` es CharField
- Si hay ImportError en `CategoriaGasto` o `CentroCosto`, verificar que `reportes` está en INSTALLED_APPS
