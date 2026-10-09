# Ficha de fuentes — merma corroborada por historial original

Fecha: 8oct2026. Unidad: producto/sucursal/mes y movimiento de merma.
PostgreSQL16 propio `erp_auditor_merma_original_corroborada`, puerto55603.

## Fuentes, escritores y consumidores

| Concepto | Fuente existente | Escritor | Identidad | Consumidores |
| --- | --- | --- | --- | --- |
| Merma comercial | PointWasteLine | Sincronizador Point autorizado | PK, sucursal, movimiento externo, cantidad, unidad, hash y header/detalles originales | Traza mensual, agente y cierre |
| Identidad documental de producto | PointProductHistoryImport y archivo original íntegro | Captura/ingreso oficial | Petición producto/insumo, sucursal, SHA, receipt, membresía completa y FK_Movimiento | AuditStockHistoryService |
| Efecto neto mensual | PointHistoryReconciliation | Reconciliador existente | Par/mes, categoría waste, cantidades y cortes | Cierre individual |
| Decisión inmutable | ProductInventoryDocumentaryEvent | ProductDocumentaryCloseService | Par/mes, actor y huella de evidencia | Expediente autenticado |

Inventario de fuentes ejecutado con `--term merma --term historial --term cierre`.
Los candidatos léxicos no acreditan equivalencia. Diagnóstico acotado read-only:
`corroboracion-grupo-mermas-originales-20261008.json`: 94 referencias/57 pares
coinciden por sucursal, movimiento y cantidad; no equivalen a 57 cierres.
Un header de merma puede incluir varios productos: no usar sólo su PK.

## Decisión y límites

Reutilizar `_existing_import` (sin crear registros), `_original_batch` y la
reconciliación canónica. Extender sólo cierre individual, no el guard mensual.
Ante la advertencia secundaria de waste, exigir fuente trazada, producto final,
sucursal exacta, fecha dentro del mes, header PK entero coincidente y movimiento
incluido en waste del historial COMPLETE. Validar nuevamente el archivo íntegro;
su evidencia debe coincidir con la usada por la reconciliación. Exigir un único
detalle de nombre exacto y cantidad/unidad coincidentes con la fila y original.
Esto acredita identidad mediante la petición original por producto, no mediante
una nueva equivalencia de nombre ni una receta/maestro modificado.

Conservar advertencia y hashes en `corroborated_waste`. Mantener todos los vetos
de cortes, cadena, unknown, remanente y cada rubro. No aceptar producción/SKU por
esta excepción, archivo ausente/corrupto, detalle ambiguo, cantidad/dominio/fecha
contradictoria ni canon incompleto. No cambiar cancelaciones ni inferir que toda
merma es prueba, pérdida o conteo físico. No nueva tabla, captura ni HTTP.

## Verificación

TDD con ingreso oficial de archivo real: fallo previo y cierre/idempotencia
posteriores; negativos de cantidad, unidad, fecha, hash, header, ambigüedad,
procedencia original, cobertura y rubros. Regresiones de retorno y conversión.
Publicación requiere CI completo, deploy oficial, comparación de fuentes,
segunda ejecución y ecuación visible autenticada. Pruebas locales no son cierre.
