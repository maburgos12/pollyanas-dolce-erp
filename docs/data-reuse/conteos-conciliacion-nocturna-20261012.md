# Ficha de fuentes — conciliación nocturna de conteos de sucursal
Fecha: 2026-10-12. Ambiente: producción, consultas de solo lectura. La inspección fundamenta la implementación aditiva; las consultas de esta ficha no cambiaron datos.

Necesidad: preservar observación física por artículo y ronda con hora declarada propia, trasladarla al cierre operativo mediante movimientos posteriores, comparar contra Point en el mismo instante, y dejar pendientes con motivo cuando falte evidencia. No modificar existencias ni Point. Ningún estado de conciliación condiciona preparar/iniciar/guardar/agregar/enviar.

Fuentes:
- inventario.ConteoSucursal / LineaConteoSucursal / LecturaConteoSucursal / EventoConteoSucursal: fuente física propia; identidad conteo, línea producto/insumo protegido y ronda. CF26 Plaza Nío ENVIADO, observación conservada «Reb zanahoria 1», 45 líneas. actualizado_en de las 45 lecturas corresponde al último envío, 2026-10-12 03:57:57 UTC; no prueba hora física por producto.
- PointInventorySnapshot + PointSyncJob: existencia extraída, cantidad cruda, unidad y ciclo. Producto265 SKU0068 en PointBranch12 último snapshot2026-10-12 09:23:53 UTC, stock1, job94218. Hora de extracción no certifica cierre atómico. Excluir jobs de inventario canónico de insumos cuando no cubren esta sucursal/producto.
- PointProductHistoryImport/Row: movimientos por producto y sucursal con hora, cantidades, existencia anterior/nueva y raw_payload. 954 imports /178105 filas. Import636 para producto265, branch12:500 filas, último movimiento2026-09-28 05:26:56 UTC, consulta2026-10-01 01:55:45 UTC, límite500. Metadatos legados sin original_responses ni latest_response_fingerprint: insuficiente para acreditar cobertura actual y miembros de respuesta original.
- AuditStockHistoryService: reutilizar validación de lote original, identidad, hash, timestamps Point UTC, deduplicación por FK_Movimiento, cancelados y huecos de cadena. _covers_month no corresponde a ventana intradía y no se debe reutilizar como prueba de cierre diario. La captura live existente guarda metadatos legados si no se proporciona evidencia original; no presentar ese guardado como cobertura probada.
- PointDailySale/PointSalesNormalized: fuente diaria por producto/sucursal; no contiene hora de venta por artículo y no permite recortar después del conteo. 407032 registros hasta2026-10-12. Solo cobertura auxiliar; no sumarla nuevamente al historial Stock.
- PointWasteLine/PointTransferLine/PointConversionLine: fuentes de movimientos con hora y unidades; no combinarlas sin acreditar cobertura y sin duplicar movimientos ya representados en Stock. PointProductionLine es diaria, no prueba hora de producción.
- PointHistoricalInventoryClosing/Line: reutilizar cuando existe cierre exacto VERIFIED. Últimos cierres comprobados septiembre30, agosto31, julio31; no extrapolarlos a diario.

Identidades/equivalencias:
- Confirmada: línea de producto apunta a PointProduct.pk;0068=producto265 external_id68 con unidadPZA oficial.
- Confirmada por lógica actual de referencia y consulta: Plaza Nío tiene PointBranch12 external_id7 y alias textual19. Resolver único ID numérico activo, conservar ambigüedad cuando no sea único.
- Distinta: lectura física vs snapshot Point vs cierre histórico vs última edición.
- No resuelta: unidades o insumos sin historial Stock de dominio correspondiente; pendiente, nunca equivalencia por nombre o cero.

Horarios: CELERY_TIMEZONE America/Mazatlan. Ventas diarias03:00, conversiones04:00; jobsSUCCESS de otro ámbito no acreditan sucursal/producto. Ejecutar02:00 exige evaluar cobertura real y reintentar pendientes; no adelantar ni cambiar sincronizaciones existentes por intuición.

Decisión: extender lectura con hora propia sin backfill desde actualizado_en; mantener fuente física e historial. Separar resultado de conciliación de referencia manual anterior al inicio y conservar regla15min del flujo previo. Reutilizar consulta/lock de sesión Point y evidencia completa de historial, sin segundo catálogo/tabla maestra ni ajustes automáticos. Pendientes deben mostrar razón por artículo.

Evidencia reproducible: manage.py inventario_fuentes_datos --term conteo --term movimiento --term inventario --term sincronizacion (38 candidatos léxicos,15 mostrados); consultas acotadas sobre CF26, producto265 y branch12, agregados de cobertura, dos jobs recientes por tipo y revisión de schedules de settings. No se abrió otra sesión Point ni se capturaron datos en producción.


Implementación: hora nullable por lectura, resultado JSON independiente y evento de sistema protegido por constraint. Se reutiliza la resolución actual de sucursal numérica única; no se agregan equivalencias. El lector nocturno conserva respuesta completa y código realmente ejecutado con SHA en raw_exports y archivo canónico. Valida dominio/identidad, membresía, duplicados, fecha UTC Point, cantidad/unidad, ciclo/snapshot y cadena. Una respuesta saturada necesita frontera estrictamente anterior al inicio; extracción con movimientos es ambigua. Hora física ausente, insumo sin historial de dominio, ajustes físicos no acreditados, fuente parcial o ambigua quedan pendientes con motivo. Captura no consulta Point. Resultado se publica con control de versión/huella y conciliación previa para impedir carreras entre trabajos. Una consulta Point usa el bloqueo de cuenta no bloqueante y un cliente por lote.

El cierre es 00:00 del día siguiente en America/Mazatlan. Se ejecuta 02:00, reintentos de30min hasta ocho horas si falta disponibilidad/cobertura temporal. No se cambian los horarios de otros sincronizadores. La regla manual15min permanece separada. No se completan horas históricas desde actualizado_en ni se convierten notas en cantidades.
