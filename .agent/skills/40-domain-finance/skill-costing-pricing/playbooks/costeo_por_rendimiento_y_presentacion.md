# Playbook - Costeo por Rendimiento y Presentación

## Objetivo
Asegurar costeo consistente para mezclas/batidas y presentaciones derivadas.

## Datos mínimos requeridos
- Ingrediente, cantidad, unidad, costo unitario.
- Rendimiento total de receta (kg/lt/pza según aplique).
- Tabla de presentaciones con cantidad por presentación.

## Cálculo
1. `costo_total_batida = sum(cantidad_i * costo_unitario_i)`
2. `costo_por_unidad_base = costo_total_batida / rendimiento_total`
3. Para cada presentación j:
   - `costo_presentacion_j = costo_por_unidad_base * cantidad_presentacion_j`

## Reglas de visualización
- Montos monetarios con máximo 2 decimales.
- Cantidades técnicas: hasta 2 decimales en UI (interno puede conservar mayor precisión).
- Encabezados y columnas alineados por campo semántico.

## Auditoría
- Cada recalculo guarda versión de costo con fecha, fuente y usuario/job.
