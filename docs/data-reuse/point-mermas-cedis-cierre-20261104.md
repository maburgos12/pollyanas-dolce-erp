# Ficha de fuentes — mermas CEDIS y cierre histórico mensual Point

Fecha y ambiente consultado: 2026-11-04; PostgreSQL local aislado y producción en modo de solo lectura.

## Necesidad y unidad de análisis

Recuperar para el cierre mensual todas las líneas de merma que Point registra por movimiento y producto, incluidas las de CEDIS, sin interpretar una justificación temporalmente ilegible como ausencia de la merma.

Materializar automáticamente el saldo final exacto del último día del mes desde el historial oficial de existencias Point, sin relabelar snapshots corrientes como cierres históricos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Merma Point | `pos_bridge.PointWasteLine` | `PointWasteExtractor` y `PointMovementSyncService` | `source_hash`; movimiento, producto y renglón | Octubre conservaba cuatro líneas CEDIS que el listado general omitía | cierre mensual, balance y auditoría |
| Espejo operativo | `control.MermaPOS` | `PointMovementSyncService` | mismo `source_hash` | refleja cantidad, producto y motivo de la línea Point | reportes operativos |
| Sucursal Point | `pos_bridge.PointBranch` | sincronizadores Point | `external_id` | existen `8` canónica y `CEDIS` legada | matching de sucursal |
| Cierre histórico Point | `pos_bridge.PointHistoricalInventoryClosing` y `PointHistoricalInventoryClosingLine` | `HistoricalPointInventoryClosingCapture` | fecha operativa, sucursal y producto Point | producción tiene cierres verificados del 31/08 y 30/09; el 31/10 no existía | balance mensual y auditoría Producido vs Vendido |
| Historial oficial de existencias | endpoint Stock leído por `PointHttpSessionClient` | Point | sucursal, producto, movimiento y fecha UTC | es la evidencia usada por los cierres verificados existentes | capturador histórico |
| Snapshot operativo | `pos_bridge.PointInventorySnapshot` | sincronización diaria de inventario | job, sucursal, producto y hora de captura | octubre tiene snapshots del 31/10, pero no acreditan por sí solos el corte histórico exacto | disponibilidad y balance preliminar |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| filtro Point `8` y nombre devuelto `CEDIS` | confirmada para consultar mermas | el filtro `8` devuelve movimientos cuyo campo `Sucursal` es `CEDIS`; el listado general no incluye cuatro líneas persistidas | ninguna para la consulta |
| `PointBranch.external_id=8` y `PointBranch.external_id=CEDIS` | candidata, no fusionada | representan CEDIS en contextos distintos, pero esta corrección no cambia llaves ni datos maestros | revisión separada antes de fusionar |

## Decisión de diseño

Extender el extractor existente con una consulta complementaria al filtro Point `8` sólo cuando se sincroniza el conjunto completo y deduplicar con el `source_hash` ya existente. Si el listado mensual omite un movimiento ya acreditado, releer su detalle por `PK_Movimiento` y conservarlo únicamente cuando todos sus hashes coincidan con el historial; cualquier diferencia sigue bloqueando la sincronización. Si únicamente `/Mermas/get_justificacion` devuelve el error explícito de Point, conservar cantidad y detalle, registrar la advertencia en `raw_payload` y preservar cualquier justificación anterior. Los listados, fechas, detalles y cantidades mantienen validación estricta. No se crea una segunda captura ni se modifica inventario.

Antes de materializar la auditoría mensual, reutilizar `HistoricalPointInventoryClosingCapture` para el último día exacto del mes. Si ya existe un cierre `VERIFIED`, se reutiliza sin consultar Point. Si la captura queda `DRAFT`, el cierre mensual permanece en revisión y la ejecución diaria vuelve a intentar; sólo un cierre histórico `VERIFIED` habilita la auditoría. No se convierte `PointInventorySnapshot` en evidencia histórica ni se crea otra tabla.

Consultas reproducibles: `python manage.py inventario_fuentes_datos --term merma --term CEDIS`, `--term 'cierre histórico Point'` y `--term 'inventario Point'`; consultas acotadas de `PointWasteLine`, `PointBranch`, `PointHistoricalInventoryClosing` y jobs de octubre en producción.

Riesgos y pendientes: la duplicidad histórica de `PointBranch` queda documentada y fuera de este cambio; fusionarla requiere una revisión de identidades independiente.
