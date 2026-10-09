# Ficha de fuentes — identidad de producción Point, septiembre 2026

Fecha y ambiente consultado: 2026-10-07, producción ERP (solo lectura); pruebas en PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Asignar cada línea de producción Point al producto Point exacto, por línea y sucursal, antes de calcular entradas y decidir un cierre documental. El SKU y el nombre no son identidad cuando hay colisiones.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Producción original | `PointProductionLine` / `pos_bridge_pointproductionline` | Sincronización Point, sin reescritura en esta tarea | `raw_payload.detail.PK_Producto`, `IsInsumo`, línea, sucursal y fecha | Filas 11715 y 11895: FK 818/Bollo Lotus y 112/Ciruela; 19 líneas de CEDIS con SKU ambiguo | Trazabilidad, cierre documental, auditoría mensual |
| Catálogo Point guardado | `PointProduct` / `pos_bridge_pointproduct` | Sincronización de catálogo Point | `external_id` de producto, dominio producto | FK 818 corresponde a producto 110; FK 112 a producto 541 | Mismos consumidores |
| Saldo inicial/final | `PointHistoricalInventoryClosingLine` / tabla existente | Cierres Point guardados | Producto, sucursal y corte | Se conserva como control independiente; no se deduce del SKU | Trazabilidad y cierre |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `detail.PK_Producto=818` → `PointProduct.external_id=818` | Confirmada para la línea original 11715 | El mismo detalle declara `IsInsumo=false`; SKU 0160 también pertenece a otro producto | No, salvo contradicción de dominio o código único |
| `detail.PK_Producto=112` → `PointProduct.external_id=112` | Confirmada para la línea original 11895 | El mismo detalle declara `IsInsumo=false`; SKU 0112 tiene otros candidatos | No, salvo contradicción |
| SKU 0160/0112 o nombre como identidad | No resuelta | Hay colisiones; no se promueve una coincidencia textual a FK | Sí, si falta la clave original |

## Decisión de diseño

Extender exclusivamente el lector existente de producción: cuando el detalle original contiene `PK_Producto`, resolver contra `PointProduct.external_id` solo con dominio producto confirmado. Clave inválida/desconocida o dominio/SKU único contradictorio falla cerrado; clave ausente mantiene el contrato anterior. No crear otra captura, tabla, alias, equivalencia o modificación de Point. Conservar saldo, ventas, mermas y cortes como pruebas independientes. La prueba cubre tanto resolución como propagación a la ecuación del par.

Consultas reproducibles: `inventario_fuentes_datos --term 'producción Point' --term 'producto Point' --term 'PK_Producto'` propone metadatos, no identidad; consulta acotada de las 19 filas y sus `raw_payload` originales, seguida por `PointProduct.external_id` 818/112. Una comprobación solo-lectura de las 786 líneas de producto de septiembre halló 767 identidades idénticas a las del lector previo y exactamente 19 ambigüedades resueltas por FK; cero FK inválidas, desconocidas, de dominio contradictorio o contrarias a un SKU único.

Riesgos y pendientes: ninguna resolución autoriza por sí sola cerrar los demás pares de CEDIS ni el mes. Validar en producción que desaparezcan solo las ambigüedades de estas líneas y que los controles independientes sigan vigentes.
