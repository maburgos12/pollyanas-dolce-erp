# Diseño: cierre mensual automático de producto

## Objetivo

El mes anterior se reconstruye y bloquea sin intervención cuando las fuentes Point, los conteos físicos de las sucursales y la auditoría visible están completos. Una excepción conserva el cierre como `BUILT`, informa el motivo exacto y se reintenta automáticamente.

## Flujo

1. La tarea diaria selecciona el mes anterior y usa el mutex PostgreSQL existente.
2. La conciliación nocturna procesa conteos enviados de la fecha final del mes.
3. Un conteo compuesto sólo por productos pasa a `ACEPTADO` si todas sus líneas están calculadas y la diferencia física contra Point es cero.
4. Si una línea calculada tiene diferencia, el conteo pasa a `RECONTEO` y abre una nueva lectura sólo para esas líneas.
5. Se sincronizan ventas, producción, mermas y conversiones para el rango exacto del mes.
6. `ProductMonthClosureService` construye el cierre y conserva la huella de fuentes.
7. El candado físico exige una evidencia de producto lista para cada sucursal de venta operativa del último día del mes. Los conteos mixtos pueden acreditar la parte de productos aunque sus insumos sigan un flujo independiente.
8. Se reconstruye `ProductInventoryAuditRun`; sólo `READY` permite bloquear.
9. El orquestador agrega la evidencia física y del auditor a `metadata.validation`. Si no hay bloqueos, ejecuta el lock canónico existente.

## Reglas

- Fecha operativa: último día calendario del mes en `America/Mazatlan`, sin desplazarla por la hora de llegada.
- Un vacío físico nunca equivale a cero.
- La diferencia física no genera ajustes de inventario.
- `PickupReservation` permanece fuera de venta y de la ecuación contable mensual; su función es disponibilidad comprometida.
- Los reintentos reutilizan `source_hash`, constraints, mutex y el mismo cierre mensual.
- Un cierre `LOCKED` es inmutable y los siguientes intentos terminan sin consultar Point.

## Programación

`pos_bridge.monthly_product_closure` se ejecuta diariamente a las 05:00, después de inventario, mermas, transferencias y conciliación nocturna. Se habilita por defecto con `lock_after_build=true`. Mientras el mes permanezca abierto, cada día vuelve a intentar; después del lock, la ejecución es una lectura rápida sin mutaciones.

## Verificación

- Tests de transición automática `ENVIADO -> ACEPTADO` y `ENVIADO -> RECONTEO`.
- Tests del candado para sucursales faltantes, líneas pendientes y diferencias.
- Tests del orquestador para impedir lock con auditoría incompleta y permitirlo con auditoría `READY`.
- Test de schedule habilitado, diario y con lock.
- Checks Django, migraciones, ejecución controlada de octubre, despliegue y validación autenticada de Producido vs Vendido.
