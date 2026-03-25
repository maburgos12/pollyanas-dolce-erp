# Mapeo de Costeo Point

## Objetivo
Definir de forma determinística de dónde sale el costo unitario que alimenta:
- `CostoInsumo`
- `LineaReceta.costo_unitario_snapshot`
- costeo recursivo de preparaciones y productos finales

## Fuente de verdad operativa
La fuente primaria es **Pointmeup**.

## Jerarquía de captura
1. `Inventario -> Existencias -> Existencias por sucursal -> ALMACEN`
2. `Reportes -> Mis Reportes -> Crear Reportes -> Inventario -> Movimientos de inventario -> Sucursal ALMACEN -> Movimiento COMPRA`
3. `Inventario -> Compras -> Buscar por proveedor`

## Regla por fuente

### 1. Existencias por sucursal / ALMACEN
Uso:
- costo unitario operativo vigente del artículo en almacén

Interpretación:
- la tabla ya muestra el `costo unitario` de la unidad presente en esa vista
- esta fuente es la mejor para snapshot operativo rápido

Persistencia sugerida:
- `source = POINT_EXISTENCIA_ALMACEN`
- `branch = ALMACEN`
- `unit_cost = costo unitario mostrado`

### 2. Movimientos de inventario / COMPRA / ALMACEN
Uso:
- corroborar costo de compra real por rango
- obtener costo unitario calculado con evidencia documental

Interpretación:
- Point devuelve cantidad total comprada y total en dinero
- el costo unitario se calcula como:
  - `costo_unitario = total_importe / cantidad_total`

Persistencia sugerida:
- `source = POINT_MOVIMIENTO_COMPRA`
- `branch = ALMACEN`
- `quantity_total`
- `amount_total`
- `unit_cost_calculated`
- guardar rango de fechas auditado

### 3. Compras por proveedor
Uso:
- auditoría de proveedor y revisión de trazabilidad
- respaldo cuando se requiere entender variación por proveedor

Interpretación:
- es más compleja y no debe ser la primera fuente para snapshot masivo
- sirve para:
  - proveedor principal
  - historial de compra
  - diferencia de costo entre proveedores

Persistencia sugerida:
- `source = POINT_COMPRA_PROVEEDOR`
- `supplier_name`
- `unit_cost`
- `document reference`

## Política de resolución de costo

### Materia prima y empaque
Prioridad:
1. `POINT_EXISTENCIA_ALMACEN`
2. `POINT_MOVIMIENTO_COMPRA`
3. `POINT_COMPRA_PROVEEDOR`
4. costo canónico vigente ya persistido

### Insumo interno / preparación
Prioridad:
1. costo recursivo de la `Receta` de preparación
2. costo Point vigente persistido del insumo interno
3. costo canónico vigente ya persistido

## Regla de unidad
- el costo debe guardarse con la unidad fuente real
- al pasar a `LineaReceta`, se convierte a la unidad de la línea
- ejemplos:
  - `kg -> g`
  - `lt -> ml`
  - `pza -> unidad`

## Regla de actualización de recetas
1. capturar o recalcular costo canónico del insumo
2. actualizar `LineaReceta.costo_unitario_snapshot`
3. recalcular `RecetaCostoVersion`
4. sincronizar insumos derivados/preparaciones

## Guardrails
- no usar costo de Point sin registrar `raw` y `source_hash`
- no mezclar transferencias con costo de compra
- no usar costo de producto final para costear materia prima
- no cerrar dashboard financiero si faltan snapshots de costo
