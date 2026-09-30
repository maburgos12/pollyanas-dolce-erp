# Ficha de fuentes — historial Point para auditoría de inventario

Fecha y ambiente consultado: 2026-09-30; código `origin/main`, PostgreSQL local aislado y PostgreSQL de producción en solo lectura.

## Necesidad y unidad de análisis

Resolver diferencias mensuales de inventario con movimientos transaccionales ya existentes en Point. La unidad de análisis permanece `mes + sucursal Point canónica + producto Point`; una fila histórica representa un movimiento Point identificado por `FK_Movimiento` dentro de ese producto y sucursal.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Expediente mensual | `reportes.ProductInventoryAuditCase` / `reportes_productinventoryauditcase` | Materializador mensual | Único por mes, sucursal y producto | Producción: 2,026 casos de agosto; 881 con diferencia | Auditoría y agente |
| Historial persistido | `pos_bridge.PointProductHistoryImport` y `PointProductHistoryRow` | Importador histórico existente | `file_hash`; importación + `row_number` | Producción: 0 importaciones y 0 filas antes de esta ampliación | Costos y nueva resolución de auditoría |
| Historial vivo | `PointHttpSessionClient.get_stock_history` | Cuenta Point configurada en el ERP | sucursal externa, producto externo, `FK_Movimiento` | Para CEDIS/109 devuelve el libro que acredita el cierre de agosto | Cierres históricos y nueva captura por excepción |
| Producción agregada | `PointProductionLine` | Reporte Point persistido | fecha, sucursal y producto | Caso 79: 518 unidades | Producido vs Vendido y auditoría |
| Transferencias y retornos | `PointTransferLine` y Logística | Point y flujo de entrega | transferencia, detalle, origen, destino | Caso 79: balance agregado neto de -527 | Logística y auditoría |
| Conversiones agregadas | `PointConversionLine` | Reporte Point persistido | movimiento y producto destino | Caso 79: entrada 2, salida 0 por falta de origen | Auditoría |
| Cierres protegidos | `PointHistoricalInventoryClosing` y líneas | Captura histórica verificada | fecha operativa, sucursal, producto | Caso 79: apertura 23 y cierre 6 | Balance mensual |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `FK_Movimiento` + sucursal + producto | confirmada | Identidad estable del movimiento en el historial solicitado | Usar para idempotencia |
| `SALIDA POR CONVERSIÓN` ↔ conversión de salida del producto origen | confirmada | Point reduce directamente la existencia del producto | Incluir en el balance |
| Entradas de rebanadas con la misma fecha ↔ destino de un pastel convertido | candidata | Fecha y cantidades son compatibles, pero no existe clave compartida observada | Mostrar como probable, no confirmar |
| Historial Point ↔ nuevo inventario operativo | distinta | El historial es evidencia de auditoría; no reemplaza ni modifica existencias | Solo lectura |
| Diferencia de reporte ↔ merma física | distinta | Una omisión del agregado puede explicar el saldo sin existir merma | No declarar pérdida automática |

## Decisión de diseño

Extender la captura sobre `PointProductHistoryImport` y `PointProductHistoryRow`, sin crear una segunda tabla. Cada sucursal-producto usa una importación canónica con huella determinista y cada movimiento usa `FK_Movimiento` como número de fila. El agente consulta Point solo cuando el expediente conserva una diferencia y el historial local no cubre el mes; después reutiliza los registros persistidos.

El reconciliador conserva las fuentes agregadas como contraste y registra sus diferencias en `investigation_summary`. Solo una cobertura mensual completa puede cerrar el remanente. No se modifica ninguna tabla operativa de inventario ni se emite una escritura a Point.

Consultas o procedimiento reproducible:

```bash
python3 manage.py inventario_fuentes_datos \
  --term historial --term movimiento --term conversion --term produccion \
  --term transferencia --term merma --term ajuste --term auditoria
python3 manage.py inventario_fuentes_datos \
  --term pointproducthistory --term historial_producto_point --term stock_history
```

Consulta de producción acotada a conteos de tablas de historial, casos de agosto y expediente 79; sin modificar registros.

Riesgos y pendientes: Point limita el historial solicitado. Si la ventana no acredita los límites del mes, el agente debe conservar el caso pendiente. Las coincidencias temporales entre productos no sustituyen una relación explícita de conversión.
