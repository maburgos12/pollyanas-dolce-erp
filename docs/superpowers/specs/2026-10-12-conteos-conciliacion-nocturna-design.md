# Conciliación diaria de conteos físicos de sucursal

Alcance autorizado por Mauricio en la conversación lateral y trasladado a este hilo: captura antes del cierre, hora física por artículo, conciliación diaria a las 02:00 America/Mazatlan, evidencia y cobertura Point, pendientes con motivo, conservación de cantidades e historial. Esta ampliación es independiente del PR #1557.

## Captura

Añadir hora declarada del conteo a LecturaConteoSucursal. actualizado_en continúa siendo última edición; no se usa para completar horas históricas. La UI propone hora al capturar cantidad usando reloj del servidor y permite corregirla cuando se contó antes. Notas, guardado, agregado y envío no cambian automáticamente una hora ya declarada. Fecha/hora se presentan en America/Mazatlan.

El campo es opcional en el contrato para conservar clientes/borradores anteriores. Una lectura sin hora comprobable puede guardarse y enviarse, pero su conciliación queda pendiente. Cantidades e incidencias conservan validación, UUID de intención, versión, permisos, sucursal, unidades y duplicados existentes. Ninguna acción de captura consulta Point ni espera un resultado de conciliación.

## Cierre y cálculo

Cierre del día operativo: comienzo del día siguiente a ConteoSucursal.fecha, a las 00:00 America/Mazatlan, consistente con los límites diarios de Point. Ventas de medianoche en adelante pertenecen al día siguiente.

Físico al cierre = cantidad declarada + movimientos físicos comprobados entre su hora y el cierre. Una lectura posterior al cierre se retrocede por el mismo intervalo. Point al cierre se reconstruye desde un snapshot comprobado sumando/restando el historial según su posición temporal. Movimientos posteriores al cierre se muestran separados y no se mezclan con la proyección física.

Solo se calcula con identidad canónica de sucursal/producto, unidad compatible, snapshot con cantidad cruda consistente, ciclo SUCCESS y respuesta original de Stock completa, conservada y validada. Se exige cobertura hasta todos los instantes usados, límite de respuesta suficiente y cadena de saldos continua. Si hay movimientos dentro de la ventana de extracción del snapshot, orden temporal ambiguo, cantidades sin prueba, cancelaciones/ajustes no acreditados o resultados físicos negativos, la línea queda pendiente con motivo. Un insumo sin historial de dominio acreditado queda pendiente. Ausencias jamás son cero.

## Fuentes y ejecución

Reutilizar PointInventorySnapshot, PointSyncJob, PointProductHistoryImport/Row, AuditStockHistoryService, point_stock_history_instant, historical_waste_effect y misma_unidad. La consulta Stock mantiene el contrato PointHttpSessionClient.get_stock_history, dominio producto, almacén/producto explícitos y todos los tipos de movimiento. Conservar la respuesta original en los exports de Point y en su archivo canónico con SHA, localizador, hora de recepción y código del lector ejecutado. No relajar contratos de archivos originales ni agregar catálogos maestros.

Un cliente y una sesión Point por lote, con point_account_session_lock en modo no bloqueante. No contactar Point para líneas sin cantidad/hora o ámbito resuelto. Consultas y archivo Point fuera de la transacción del conteo; publicación breve bajo control de versión/estado y huella de lecturas. Si el empleado/revisor cambia el conteo durante una consulta, descartar el resultado para esa versión y reintentar.

Celery inicia a las 02:00; reintentos de 30 minutos ante disponibilidad/sincronización pendiente, acotados a ocho horas. Los pendientes de datos físicos requieren revisión, no consultas repetidas. Resultados se guardan aparte de referencia manual; eventos automáticos identifican Sistema. No alterar cantidades, existencias, merma, estado de captura ni Point. La regla manual de 15 minutos y su validación permanecen.

## Consumidores y aceptación

Captura móvil, borradores, FormData/JSON y fallback tradicional; revisión ERP/App, historial, Excel; tarea/Beat. Mantener captura ciega: ningún saldo Point o evento comparativo aparece en CAPTURA/RECONTEO, ni en DOM oculto. Un reconteo conserva las lecturas anteriores e invalida únicamente la conciliación actual.

Pruebas: captura sin Point/referencia; conservación de hora al editar notas; clientes antiguos; guardado/agregado/envío; movimientos antes/después del cierre; sin movimientos con cero explícito; fuente ausente/parcial/truncada/corrupta, horas ambiguas, unidades/identidad incorrectas, sincronización tardía, cancelaciones y reconteo; carreras y reintentos; exportación y permisos. Validar formularios en navegador local con usuario empleado y producción mediante vista de empleado sin inventar actividad operativa.
