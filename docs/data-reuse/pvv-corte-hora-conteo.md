# Cruce al momento registrado

Fecha y ambiente: 19/11/2026; producción en transacción repetible de solo lectura, red externa bloqueada; PG16 aislado para regresión.

## Necesidad y unidad de análisis

Una comparación por mes, producto Point y sucursal, con lectura enviada y unidad explícita.

Necesidad autorizada: mostrar qué diferencia persiste al comparar físico y Point a la hora enviada, manteniendo los pendientes de movimientos y la referencia del cierre.

Fuentes existentes: EventoConteoSucursal de envío inmutable, Conteo/Linea para fecha, producto/sucursal y unidad. ProductInventoryAuditCase canónico del reporte para cierre acreditado. PointProductHistoryImport original validado mediante AuditStockHistoryService._original_batch, conciliación mensual canónica y PointProductHistoryRow. Los timestamps raw de Stock se interpretan con point_stock_history_instant, sin cambiar America/Mazatlan. Cálculo intervalos existente inventario.conteos_conciliacion._intervalo.

## Fuentes candidatas

| Concepto | Modelo / tabla | Crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Cantidad y hora enviadas | EventoConteoSucursal, LineaConteoSucursal, LecturaConteoSucursal | Envío ERP y archivo histórico conservado | Folio, línea, ronda realmente enviada, mes y sucursal | 651 lecturas sin cambios; cinco estados automáticos históricos | Conteos y Producido vs Vendido |
| Cierre Point acreditado | ProductInventoryAuditCase | Materializador oficial existente | Mes, branch_id, product_id | Cierre y source_trace existentes | Reporte y auditor por caso |
| Historial Stock original | PointProductHistoryImport, PointProductHistoryRow | Captura/ingreso oficial existente; sin nueva captura | file_hash canónico, claves exactas, receipt, SHA y membresía | Nueve discrepancias con hora; ocho acreditadas y una con discontinuidad previa | Conciliación y servicios mensuales existentes |

## Alias y equivalencias

| Términos | Estado | Evidencia y límite | Revisión requerida |
| --- | --- | --- | --- |
| Producto/sucursal del caso e historial | Confirmada por claves canónicas | Mismas FK; rechazo por cambio de identidad | Sin equivalencia nueva |
| U y PZA | Distintas salvo identidad explícita existente | misma_unidad; sin conversión añadida | Ninguna regla nueva |
| Coincide con cierre / coincide al momento | Conceptos distintos | El primero no acredita igualdad temporal | Mantener rótulos separados |

## Decisión de diseño

Reutilizar originales y servicios publicados, extender solo la lectura HTML y sus pruebas; sin segunda captura ni nuevo modelo. API/CSV y consumidores mensuales conservados.

Identidad: mismas claves producto Point y sucursal ERP ya acreditadas por el reporte. No crear equivalencias por nombre/SKU. Permisos de revisión y capturas ciegas conservados.

Reglas del lector: solo discrepancias contra cierre con lecturas enviadas con cantidad/hora. Las coincidencias con cierre conservan referencia fechada explícita, sin certificar coincidencia temporal ni descartar diferencias a la hora; no se omiten filas. Hora aware dentro del mes declarado, anterior al cierre y no futura. Unidad acreditada por Unidad explícita de filas originales de Stock del mes y el ancla anterior al conteo, cotejada con misma_unidad existente. La ausencia o diferencia impide certificar; U no equivale por defecto a PZA. Cierre visible y documental coincidente con historia canónica; cobertura COMPLETE y unknown vacío. Original íntegro validado por SHA/receipt/membership, recibido después del corte, límite que cruza inicio, cadena sin salto y sin empate de movimiento/conteo en mismo segundo. Antes de cruzar resultados de consultas, comprobar mismo fingerprint de respuesta. No basta llamar resolve_stock_at_close.

Saldo Point al momento = cierre Point − delta Point entre hora enviada y cierre. Diferencia al momento = cantidad enviada − saldo Point al momento. _intervalo valida magnitud, dirección, ajustes y cancelaciones de merma. No usar ventas diarias ni inferir movimiento físico a partir de ajuste administrativo.

Presentación: columna Físico vs Point usa Coincide al momento registrado o Diferencia al momento registrado cuando hay prueba suficiente, muestra hora local, cantidades/delta e identificación de movimiento posterior en detalle útil. Para historia sin hora, contradicción del cierre o ausencia de cobertura, comparación de referencia contra cierre con fecha explícita (30/09 o31/10 según periodo) y Mazatlán, sin inventar hora, junto con el motivo específico. Conservar condición de conteo enviado pendiente de aprobación. El estado automático histórico no es una tarea de segunda ronda.

Alcance: modificar ensamblador de evidencia y template con un helper de lectura si es necesario. Lecturas en lote; evitar consultas por producto/sucursal. No alterar modelos, estados, API/export, movimientos, snapshots ni ingreso. Mismo total de651físicos antes/después.

Prueba de fuentes realizada19/11: octubre378lecturas,218conhora;217conhora acreditadas (209coinciden/8difieren),5814sin cierre canónico por salto previo de cadena;160sinhora. Septiembre273sinhora. Evidencia matriz-horas.jsonl y preview4507. Estas cifras son corte de consulta, nunca IDs hardcodeados. La implementación valida al momento solo las nueve discrepancias con hora: ocho comprobadas y una sin cierre acreditado. Las otras209 coincidencias con cierre mantienen solo referencia, incluso si un movimiento posterior pudiera causar diferencia al momento. Los intentos de validar218 originales agregaron20.89/14.42/12.39/8.64 segundos; se retiraron íntegramente las optimizaciones compartidas propias y se conserva el servicio publicado. No se amplía el framework para esta pantalla.

Pruebas negativas: original ausente/corrupto, identidad/unidad/receipt/fingerprint distintos, truncación, hueco de cadena, desconocido, empate temporal, fecha inválida, cierre contradictorio, ausencia de permisos, cero explícito, movimientos posteriores y distintas unidades sin suma. Aceptación: hashes lecturas y cifras mensuales/API/export, medición de carga completa/repetida y consola/red autenticadas. Entorno propio con PG16, registro, revisión independiente, CI cabeza/base integrada, deploysafe coordinado y respaldo/retiro.

Orden acordado:1754avisos validado antes de implementar; Finanzas17k integrado antes de rebase único y CI del complemento. La lectura se incorpora en el ensamblador visible del reporte; no ejecuta cierre mensual.

Procedimiento reproducible: inventario_fuentes_datos --term "conteo fisico" --term "historial point" y consultas acotadas por los casos del reporte; fuentes.log, matriz-horas-unidad.jsonl, preview-candidate-focused.jsonl y preview-full-get-focused.jsonl en el expediente de tarea. Validación real en READ ONLY y con socket externo prohibido; no HTTP Point ni writes.

Riesgos y pendientes: las referencias sin hora y las coincidencias contra cierre no certifican igualdad al momento;5814 conserva su salto previo sin cierre acreditado. Falta aceptar SHA integrado, CI y pantalla productiva antes de entregar.

Medición definitiva: `benchmark-isolated-samples.jsonl`, proceso Django contra PG de producción en solo lectura; no es HTTP/navegador ni una medición con PG frío. Cada muestra usa User nuevo fuera del reloj, contexto/view nuevos, LocMem y Engine privados vacíos y una transacción RR/READ ONLY independiente. Se cronometra exclusivamente view.get + response.render. Huellas de originales/filas/casos/conteos/lecturas/eventos, cifras mensuales y CSV se calculan después del reloj en el mismo snapshot; la transacción termina antes de otra petición. API se verifica después, en transacciones separadas. La comparación se rechaza si cambia cualquier huella entre muestras. Sin Point HTTP, nueva sesión, archivos productivos ni escrituras.

Octubre, tres pares alternados: base19.674/8.312/8.687s; candidato9.875/10.615/10.430s. Medianas8.687→10.430s (+1.743s), costo adicional, no mejora. La primera muestra base es un valor alto; no se fuerza limpieza de caché PG/OS ni se promete un porcentaje. Smoke septiembre4.091→4.264s. Todas las fuentes y las651lecturas, cifras mensuales/API/CSV se conservaron; ocho diferencias se acreditan al momento, demás referencias explícitas. Validar navegación real después del despliegue.

Los arneses previos `run-full-get-focused` y `benchmark-independent-requests` son evidencia limitada: el primero reutilizaba contexto y precalentaba la caché con API; el segundo reutilizaba User y mantenía una sola transacción entre peticiones. Sus tiempos no certifican carga completa independiente. La segunda ejecución terminó naturalmente y liberó sus locks; no se interrumpieron jobs ni se liberaron candados ajenos. Se conserva la evidencia y la rectificación.
