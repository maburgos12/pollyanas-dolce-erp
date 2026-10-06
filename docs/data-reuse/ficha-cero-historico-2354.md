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
| Relación de complemento | `RecetaAgrupacionAddon` | curación ERP | ID 3, código `SGALLETACAJETAM` | activa y `APPROVED`, base receta 70 y addon 107; no contiene FK del ticket de venta | filtro de consumo compartido; no acredita descuento 1:1 de base |
| Importación canónica | `PointProductHistoryImport` | `AuditStockHistoryService` | hash canónico por par | ninguna para este par; ingresar `[]` como canónica marcaría cobertura COMPLETE aunque la venta no consta en Stock | reconciliación y cierre |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| ERP sucursal 4/producto 235 y Point externo 5/832 | confirmada | FK canónica de caso 2354 y snapshots raw del mismo par | ninguna |
| venta comercial y movimiento Stock | distinta | reporte de 1 pieza, historial Stock vacío; no hay FK movimiento | conservar diferencia sin atribuir actor |
| complemento aprobado y venta de su base | candidata, no equivalencia transaccional | relación 3 está activa/APPROVED, pero no enlaza el ticket concreto de 923543 | no descontar base por analogía |
| saldo Point y conteo físico | distinta | snapshots son documentales; `ConteoSucursal/Linea/Lectura` sigue 0 | conteo humano independiente |

## Decisión de diseño

Reutilizar `PointProductHistoryImport` para archivar la respuesta original en un registro de **evidencia de frontera**, separado por fuente y hash del import canónico. No crear maestro ni promover cobertura histórica: `reconcile_many` sólo utiliza imports canónicos. El lector compartido aceptará cero únicamente si verifica la respuesta original íntegra, petición/recibo/SHA/dominio, ambos snapshots exactos que rodean el corte con raw/selección/job válidos, y ausencia de contradicción documental. Preservar la línea 8867 y los snapshots sin reescribirlos. La venta de 1 pieza queda como diferencia comercial no explicada.

Consultas reproducibles: `inventario_fuentes_datos --term historial`, `--term inventario` en PG16 local; inspección de caso 2354 y dos snapshots vecinos en producción mediante consulta acotada solo lectura; una respuesta Point 500 ya capturada y no repetible. Artefactos `faltante-apertura-2354-20261005.md` y `consulta-apertura-2354-limite500-resultado-20261005.jsonl` en el expediente del hilo.

Riesgos: `[]` no explica la venta comercial del complemento ni acredita inventario físico; snapshots cero aislados tampoco prueban el instante del corte. El contrato compuesto debe fallar si faltan procedencia, ventana temporal, stock bruto, identidad, recibo original o aparecen movimientos contradictorios. Aceptación exige segunda lectura idéntica y cero escrituras operativas. La diferencia de una pieza sigue en expediente: no se atribuye a una persona ni se convierte automáticamente en consumo de la base 70.
