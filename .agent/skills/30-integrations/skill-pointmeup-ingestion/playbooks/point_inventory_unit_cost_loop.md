# Playbook - Loop de costo unitario desde Point/Existencias

## Objetivo
Estandarizar el loop oficial para obtener y persistir costos unitarios de insumos desde `Point -> Existencias -> ALMACEN`, de forma auditable, idempotente y compatible con costeo/abasto del ERP.

## Fuente de verdad
- `Point -> Existencias -> ALMACEN` es la fuente primaria del costo unitario capturado por este loop.
- El ERP publica y consume ese costo a través de `maestros.CostoInsumo`.
- Pantallas operativas como compras automáticas no consultan Point en vivo; leen el último `CostoInsumo` vigente.

## Contrato mínimo del loop
1. ejecutar sync oficial de inventario Point
2. si es corrida completa y no realtime, abrir `Existencias`
3. fijar sucursal `ALMACEN`
4. recorrer categorías de insumos
5. extraer filas con:
   - `codigo_point`
   - `nombre`
   - `categoria`
   - `cantidad`
   - `unidad`
   - `costo_unitario`
   - `costo_total`
   - `last_movement`
6. intentar match contra `Insumo` ERP por:
   - `codigo_point`
   - alias homologado
   - nombre normalizado
7. persistir en `CostoInsumo` solo si:
   - hay match ERP
   - `costo_unitario > 0`
8. registrar evidencia y resumen del job
9. si el subloop de costos falla, no borrar snapshots de inventario ya persistidos; marcar la corrida como parcial

## Cuándo debe correr
- Dentro del `run_inventory_sync` diario completo.
- No correr en:
  - sync realtime
  - corridas filtradas por sucursal
  - corridas manuales donde se pida explícitamente `--without-cost-capture`

## Estados operativos esperados
- `SUCCESS`
  - inventario persistido
  - costos persistidos o revisados
- `PARTIAL`
  - inventario persistido
  - subloop de costos falló
- `FAILED`
  - falló el inventario principal

## Resumen mínimo obligatorio en PointSyncJob
- `inventory_cost_capture_enabled`
- `inventory_cost_capture_branch`
- `inventory_cost_status`
- `inventory_cost_rows_seen`
- `inventory_cost_costs_created`
- `inventory_cost_costs_existing`
- `inventory_cost_unresolved_matches`
- `inventory_cost_zero_cost_matches`
- `inventory_cost_unresolved_samples`
- `inventory_cost_zero_cost_samples`

## Reglas de honestidad
- No reportar `costo estimado` como “leído en vivo de Point” si proviene de `CostoInsumo`.
- Si un insumo sale con costo `0` en compras automáticas, primero verificar si existe `CostoInsumo` vigente antes de culpar a Point.
- No autoasumir match por nombre libre ambiguo.

## Checklist operativo
- Confirmar que el sync diario completo corrió en `SUCCESS` o `PARTIAL`.
- Confirmar que el subloop de costos apuntó a `ALMACEN`.
- Confirmar que `inventory_cost_status` no quedó en `FAILED`.
- Auditar muestras `NO_MATCH_ERP` antes de crear aliases masivos.
- Auditar filas con `unit_cost = 0` para distinguir empaque sin costo vs gap real en Point.

## Rollback
- Desactivar `POINT_INVENTORY_COST_CAPTURE_ENABLED=0` si el loop introduce ruido operativo.
- Correr `run_inventory_sync --without-cost-capture` para aislar el inventario principal.
- Si hace falta revertir costos capturados, usar `source_hash`/`raw.source = POINT_EXISTENCIA_ALMACEN` para identificar los registros afectados en `CostoInsumo`.
