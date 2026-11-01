# Ficha de fuentes — correcciones documentadas del costeo

Fecha: 2026-10-31. Producción, consultas de solo lectura.

## Necesidad y unidad de análisis
Corregir el consumo y costo por insumo/mes sin reescribir movimientos ni existencias. Evitar que el recálculo reincorpore una captura equivocada confirmada por Dirección.

## Fuentes candidatas
| Concepto | Modelo/tabla | Fuente | Identificador | Evidencia | Consumidores |
|---|---|---|---|---|---|
| Producción | PointProductionLine | Point | producción/detalle/sucursal | 22880/85994, 840 KG; error confirmado por Mauricio | BOM, auditoría |
| Movimiento | MovimientoInventario | consumo BOM/sync Point | PK y source_hash | 35511–35515; ajustes POINT_ALMACEN_BASELINE | auditoría, stock |
| Costo mensual | ConsumoInsumoMensual | auditoría | periodo/insumo único | 729 filas julio/agosto | presupuesto y reportes |
| Resultado | LineaPresupuestoMensual | consolidación | rubro/periodo/versión | fuente AUTO:CONSUMO_MP | Estado de resultados |
| Historial | AuditLog | core.audit.log_event | hash del plan | antes/después completos | trazabilidad y recuperación |

## Alias y equivalencias
No se crean equivalencias. Cada exclusión se fija a un movimiento exacto, hash y cantidad observada. La captura errónea fue confirmada por Mauricio; las demás recetas del folio se conservan.

## Decisión
Reutilizar trazabilidad JSON de movimientos para exclusiones documentadas y AuditLog para respaldo antes/después; sin tablas, migraciones ni modificaciones de existencias. El generador BOM ya conserva trazabilidad al actualizar movimientos. Si cambia la cantidad fuente, la auditoría exige revisar la exclusión. Descontar del diferencial el efecto de POINT_ALMACEN_BASELINE; conservar el modelo histórico de otras salidas. Unificar lectura individual con la lectura masiva.

El comando recibe un plan por stdin y simula por defecto con rollback. --apply hace una única transacción; contrasta cantidades con movimientos y precios históricos almacenados. Actualiza exclusivamente costos indicados y líneas automáticas de consumo de materia prima de la versión ORIGINAL. Conserva importes manuales y otros rubros. La aplicación se identifica por hash en AuditLog.

Procedimiento: inventario_fuentes_datos --term consumo --term produccion; consultas acotadas de movimientos, producción, costos y reglas; simulación, pruebas, aplicación y revisión en Estado de resultados. No se interpreta un saldo de apertura como producción ni se inventa una cantidad de reemplazo.
