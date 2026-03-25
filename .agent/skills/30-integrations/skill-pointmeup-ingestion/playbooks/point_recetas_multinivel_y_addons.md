# Playbook - PointMeUp Recetas Multinivel y Add-ons

## Objetivo
Estandarizar cómo el agente debe extraer desde PointMeUp:
- receta de producto final,
- subrecetas de insumos preparados,
- rendimiento base,
- y códigos `sabor/topping/addon` que se venden con ingreso cero pero sí descuentan inventario y deben costearse.

## Fuente de verdad
- `PointMeUp` es la fuente primaria.
- El agente no debe inventar recetas ni toppings.
- El ERP solo materializa lo confirmado desde Point o lo aprobado explícitamente por Dirección General.

## Modelo conceptual
1. **Producto final**
   - vive en `Configuración -> Productos`
   - puede traer BOM propio vía `getBomsByProducts`
2. **Insumo producido / preparación**
   - vive en `Catálogos -> Insumos`
   - puede traer receta propia
   - ejemplos: betunes, mezclas, panes
3. **Add-on Point sin ingreso**
   - aparece como código separado en ventas
   - `total_amount = 0`
   - sí trae receta/BOM y sí descuenta inventario
   - ejemplos: `TOPPING FRESA`, `Sabor Oreo`, `Sabor Brownie`

## Orden de extracción obligatorio
1. login Point
2. seleccionar workspace válido
3. catálogo de productos
4. detalle de producto final
5. BOM de producto final
6. para cada línea:
   - si es hoja directa, se conserva
   - si es insumo preparado, bajar su receta desde `Catálogos -> Insumos`
7. repetir recursivamente hasta llegar a hojas
8. detectar add-ons de venta cero en histórico de ventas
9. sincronizar receta del add-on desde Point aunque no venga en el catálogo normal
10. dejar agrupación `base + addon` en estado:
   - `DETECTED`
   - `APPROVED`
   - `REJECTED`

## Identidad y matching
- No confiar solo en nombre libre.
- Prioridad:
  1. `codigo_point`
  2. alias homologado ERP
  3. nombre normalizado
- Si Point reutiliza el mismo `sku` para más de un producto, el agente debe bloquear aprobación automática.
- En esos casos se requiere identidad por `external_id` o corrección en Point.

## Rendimiento
Guardar siempre:
- `yield_mode`
  - `YIELD_WEIGHT`
  - `YIELD_VOLUME`
  - `YIELD_UNIT`
- `yield_quantity`
- `yield_unit`

Ejemplos:
- `Betún Dream Whip Pastel` -> `1 KG`
- `Mezcla 3 Leches` -> `1 LT`
- `Pan Vainilla Dawn Chico` -> normalmente `1 PZA`

## Regla especial Add-ons
- Un add-on Point no sustituye la receta base.
- Se costea aparte y luego se agrupa:
  - `costo_agrupado = costo_base + costo_addon`
- La agrupación debe guardar evidencia:
  - `cooccurrence_days`
  - `cooccurrence_branches`
  - `cooccurrence_qty`
  - `confidence_score`

## Casos aprobados como patrón institucional
- `Pay de Queso Grande + Sabor Fresa Grande Pay`
- `Pay de Queso Grande + Sabor Oreo Grande`
- `Pay de Queso Mediano + Sabor Oreo Mediano`
- `Pastel de Fresas Con Crema Chico + TOPPING FRESA C`
- `Pastel de Fresas Con Crema Mediano + TOPPING FRESA M`
- `Pastel de Fresas Con Crema Grande + TOPPING FRESA G`
- `Pastel de Snickers (C/M/G) + topping correspondiente`
- `Pastel de Crunch (C/M/G) + topping correspondiente`
- `Pastel de Zanahoria (M/G) + topping correspondiente`
- `Pastel de 3 Leches Mediano + topping correspondiente`

## Casos bloqueados
- si el `sku` Point está duplicado
- si el addon tiene más de un candidato de base con conflicto semántico
- si la receta del addon existe pero no tiene costo resuelto todavía

## Salidas esperadas del agente
- receta base
- subrecetas
- BOM completo
- add-ons aprobados
- add-ons detectados pendientes
- evidencia de confianza
- advertencia de ambigüedad cuando aplique

## Checklist operativo
- Confirmar que el BOM sale de Point y no de inferencia manual.
- Confirmar que cada preparación tenga rendimiento.
- Confirmar que el addon tenga receta propia.
- Confirmar que la agrupación sea semánticamente correcta, no solo estadística.
- Bloquear autoaprobación si el `sku` de Point es ambiguo.
