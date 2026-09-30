# Ficha de fuentes — retornos parciales de transferencias Point

Fecha y ambiente consultado: 2026-09-29; producción, solo lectura.

## Necesidad y unidad de análisis

Conciliar cada línea de transferencia de producto entre su almacén Point de origen y destino. Una línea representa un producto enviado, recibido y, cuando la recepción es parcial, retornado automáticamente por Point al origen.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Transferencia | `pos_bridge.PointTransferLine` / `pos_bridge_transfer_lines` | Sincronización de `/Transfer/GetTransfer` y `/Transfer/GetDetalle` | `source_hash`; origen, destino y detalle Point | Línea 38860: transferencia 37934, detalle 532992, CEDIS a Bamoa, enviado 2 y recibido 1 | Auditoría mensual, Logística |
| Historial oficial de existencia | `/Stock/GetHistorial` | Point | sucursal Point, producto Point y `FK_Movimiento` | CEDIS: salida 1646643 por 2 y retorno 1646764 por 1; Bamoa: entrada 1646763 por 1 | Cierres históricos Point |
| Relación logística | `logistica.RutaCargaChecklistLinea` y `logistica.DiscrepanciaLogistica` | Ruta y revisión de recepción | `point_transfer_line_id`, ruta y parada | Discrepancia 300, ruta 122, `RUT-202608-0029` | Revisiones de entrega, agente auditor |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `sent_quantity` / SALIDA POR TRANSFERENCIA | confirmada | Point redujo CEDIS de 10 a 8 por 2 unidades | No |
| `received_quantity` / ENTRADA POR TRANSFERENCIA | confirmada | Point aumentó Bamoa de 1 a 2 por 1 unidad | No |
| `sent_quantity - received_quantity` / RETORNO POR TRANSFERENCIA | confirmada para transferencia finalizada parcial | Point reintegró 1 unidad a CEDIS tres segundos después de la recepción parcial | Validar con pruebas de casos abierto, parcial, completo y cancelado |
| `creado_por` / repartidor de ruta | distinta | `creado_por` identifica al actor de sincronización; el repartidor real está en `ruta.repartidor.user` | Corregir solo presentación |

## Decisión de diseño

Reutilizar `PointTransferLine` y derivar el retorno comprobado en transferencias finalizadas: `retorno = enviado - recibido`, siempre que la diferencia sea positiva. El saldo neto del origen será `enviado - retorno`; el destino conserva `recibido`. Las transferencias abiertas mantienen la salida enviada y quedan en tránsito. No se crea una segunda tabla ni se reingresa información. La pantalla de revisiones mostrará `ruta.repartidor.user` en la columna Repartidor.

Consultas o procedimiento reproducible: `inventario_fuentes_datos` con “retorno transferencia” e “historial inventario Point”; consulta acotada de `PointTransferLine` 38860; lecturas directas de `/Stock/GetHistorial` para producto Point 109 en sucursales 8 y 2, límite 500.

Riesgos y pendientes: no derivar retornos para transferencias abiertas, recibidas por encima de lo enviado o sin estado final verificable. No modificar Point ni los movimientos históricos.
