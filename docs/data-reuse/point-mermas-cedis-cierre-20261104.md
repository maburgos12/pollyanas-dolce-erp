# Ficha de fuentes — mermas CEDIS en cierre mensual

Fecha y ambiente consultado: 2026-11-04; PostgreSQL local aislado y producción en modo de solo lectura.

## Necesidad y unidad de análisis

Recuperar para el cierre mensual todas las líneas de merma que Point registra por movimiento y producto, incluidas las de CEDIS, sin interpretar una justificación temporalmente ilegible como ausencia de la merma.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Merma Point | `pos_bridge.PointWasteLine` | `PointWasteExtractor` y `PointMovementSyncService` | `source_hash`; movimiento, producto y renglón | Octubre conservaba cuatro líneas CEDIS que el listado general omitía | cierre mensual, balance y auditoría |
| Espejo operativo | `control.MermaPOS` | `PointMovementSyncService` | mismo `source_hash` | refleja cantidad, producto y motivo de la línea Point | reportes operativos |
| Sucursal Point | `pos_bridge.PointBranch` | sincronizadores Point | `external_id` | existen `8` canónica y `CEDIS` legada | matching de sucursal |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| filtro Point `8` y nombre devuelto `CEDIS` | confirmada para consultar mermas | el filtro `8` devuelve movimientos cuyo campo `Sucursal` es `CEDIS`; el listado general no incluye cuatro líneas persistidas | ninguna para la consulta |
| `PointBranch.external_id=8` y `PointBranch.external_id=CEDIS` | candidata, no fusionada | representan CEDIS en contextos distintos, pero esta corrección no cambia llaves ni datos maestros | revisión separada antes de fusionar |

## Decisión de diseño

Extender el extractor existente con una consulta complementaria al filtro Point `8` sólo cuando se sincroniza el conjunto completo y deduplicar con el `source_hash` ya existente. Si únicamente `/Mermas/get_justificacion` devuelve el error explícito de Point, conservar cantidad y detalle, registrar la advertencia en `raw_payload` y preservar cualquier justificación anterior. Los listados, fechas, detalles y cantidades mantienen validación estricta. No se crea una segunda captura ni se modifica inventario.

Consultas reproducibles: `python manage.py inventario_fuentes_datos --term merma --term CEDIS`; consultas acotadas de `PointWasteLine`, `PointBranch` y jobs de octubre en producción.

Riesgos y pendientes: la duplicidad histórica de `PointBranch` queda documentada y fuera de este cambio; fusionarla requiere una revisión de identidades independiente.
