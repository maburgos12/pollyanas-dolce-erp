# Ficha de fuentes — cero histórico Point, caso 2354

Fecha y ambiente: 5 de octubre de 2026; lectura de producción y PostgreSQL 16 local aislado (puerto 5485). Ninguna escritura en Point.

## Necesidad y unidad de análisis

Probar, o rechazar, el saldo documental de apertura del 31 de agosto para un par producto/sucursal. Una respuesta de historial, un snapshot, una venta comercial y un conteo físico son unidades distintas.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Apertura conservada | `PointHistoricalInventoryClosingLine` | captura histórica Point | cierre 6, línea 8867, sucursal 4/producto 235 | stock 0; dos intentos de historial vacío y existencia actual 0, sin prueba temporal | balance mensual, cierre y auditoría |
| Existencia original | `PointInventorySnapshot` | extracción Point inventario | FK sucursal 4/producto 235, dominio producto | snapshots 28602633 antes y 28605628 después de 2026-09-01 07:00 UTC; ambos 0, jobs 34842/35042 SUCCESS, `Ult_Mov` vacío | lector documental |
| Historial transaccional original | respuesta `/Stock/GetHistorial`, artefacto preservado | consulta protegida de solo lectura | sucursal externa 5/producto externo 832, dominio producto, límite 500 | HTTP 200, `[]`, 2026-10-05 23:58:03 UTC, SHA de wire `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945` | nueva prueba de frontera, no importación canónica |
| Venta comercial | `PointDailySale` | reporte oficial Point | fila 923543, mismo par | 1 pieza el 1 de septiembre, fuente `/Report/PrintReportes?idreporte=3` | balance y expediente; no equivale a movimiento Stock |
| Nota comercial original | Point `/Ventas/getVentasByProducto` y `PointNoteDetailService` | Point | PK_Nota 897829, folio 65207, Colosio, 1 de septiembre | dos líneas en el mismo documento: `0002` Pay de Queso Mediano 1 pieza por $380.01 y `SGALLETACAJETAM` 1 línea de complemento a $0; cabecera FK_Sucursal 5 | identifica la venta como complemento asociado al pay de esa nota; no es salida de Stock del complemento |
| Relación de complemento | `RecetaAgrupacionAddon` | curación ERP | ID 3, código `SGALLETACAJETAM` | activa y `APPROVED`, base receta 70 y addon 107; no contiene FK del ticket de venta | filtro de consumo compartido; no acredita descuento 1:1 de base |
| Importación canónica | `PointProductHistoryImport` | `AuditStockHistoryService` | hash canónico por par | ninguna para este par; ingresar `[]` como canónica marcaría cobertura COMPLETE aunque la venta no consta en Stock | reconciliación y cierre |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| ERP sucursal 4/producto 235 y Point externo 5/832 | confirmada | FK canónica de caso 2354 y snapshots raw del mismo par | ninguna |
| venta comercial y movimiento Stock | distinta | reporte de 1 línea de complemento, historial Stock vacío; no hay FK movimiento | conservar fuentes separadas y explicar la línea comercial |
| complemento aprobado y venta de su base | documento transaccional exacto | nota 897829 contiene `0002` 1 pieza y `SGALLETACAJETAM` 1 línea a $0; relación 3 activa/APPROVED enlaza recetas 70/107 | no descontar dos veces la base ni fabricar salida de Stock del complemento |
| saldo Point y conteo físico | distinta | snapshots son documentales; `ConteoSucursal/Linea/Lectura` sigue 0 | conteo humano independiente |

## Decisión de diseño

Reutilizar `PointProductHistoryImport` para archivar la respuesta original en un registro de **evidencia de frontera**, separado por fuente y hash del import canónico. No crear maestro ni promover cobertura histórica: `reconcile_many` sólo utiliza imports canónicos. El lector compartido aceptará cero únicamente si verifica la respuesta original íntegra, petición/recibo/SHA/dominio, ambos snapshots exactos que rodean el corte con raw/selección/job válidos, y ausencia de contradicción documental. Preservar la línea 8867 y los snapshots sin reescribirlos. La diferencia comercial de una línea se explica documentalmente como complemento de la nota 897829; no es una pérdida ni error humano acreditado. La ecuación guardada del caso no se altera por esa explicación.

Consultas reproducibles: `inventario_fuentes_datos --term historial`, `--term inventario` en PG16 local; inspección de caso 2354 y dos snapshots vecinos en producción mediante consulta acotada solo lectura; una respuesta Point 500 ya capturada y no repetible. Artefactos `faltante-apertura-2354-20261005.md` y `consulta-apertura-2354-limite500-resultado-20261005.jsonl` en el expediente del hilo.

Riesgos: `[]` no acredita inventario físico; snapshots cero aislados tampoco prueban el instante del corte. El contrato compuesto debe fallar si faltan procedencia, ventana temporal, stock bruto, identidad, recibo original o aparecen movimientos contradictorios. Aceptación exige segunda lectura idéntica y cero escrituras operativas. La nota original explica la línea comercial, pero no reescribe el caso ni convierte automáticamente el complemento en consumo adicional de la base 70. No atribuir la diferencia a una persona.
