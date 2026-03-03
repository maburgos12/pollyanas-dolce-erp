# Playbook - Modelo Insumo -> Subinsumo -> Producto Final

## Modelo conceptual
1. **Materia prima:** compra directa a proveedor.
2. **Insumo producido (batida/mezcla):** se fabrica con materia prima y tiene rendimiento.
3. **Subinsumo/presentación derivada:** se deriva del insumo producido (ej. pan chico/mediano/grande).
4. **Producto final de venta:** armado con subinsumos + insumos + empaques.

## Reglas de costeo
- Costo insumo producido = suma componentes de receta.
- Costo por unidad base = costo total / rendimiento.
- Costo subinsumo = costo unidad base * cantidad por presentación.
- Costo producto final = suma de cada componente seleccionado por cantidad.

## Reglas de unidad
- La unidad del subinsumo se fija al crearlo y no se cambia en armado final.
- Cantidad en armado final debe respetar unidad del componente.
- Catálogo de unidades debe ser único y estandarizado (sin duplicados por mayúsculas/minúsculas).

## Validaciones obligatorias
- No permitir costo con cantidad nula en líneas activas.
- No permitir rendimiento cero en insumos producidos activos.
- Mostrar pendientes de match y obligar homologación en insumos críticos.
