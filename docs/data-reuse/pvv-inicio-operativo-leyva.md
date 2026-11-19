# Ficha de fuentes — inicio operativo observado de Leyva

Fecha y ambiente: 18/11/2026, producción consultada en solo lectura; pruebas PG16 sintéticas aisladas.

## Necesidad y unidad de análisis
Mostrar la cantidad observada por producto y sucursal al inicio operativo autorizado del 03/10. La observación de las 10:48 no acredita el saldo al 30/09 ni el cierre mensual.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Caso mensual | ProductInventoryAuditCase | Conciliación mensual existente | Caso + producto + sucursal + mes | Octubre, apertura desconocida, sucursal Point28/ERP18 | Producido vs Vendido |
| Stock observado | PointInventorySnapshot / pos_bridge_inventory_snapshots | Extractor Point de inventario | Job81086 + captura03/10/2026 17:48:28.494613UTC + productoFK + sucursal28 externa14 | 44 productos del reporte; raw original y stock se cotejan | Detalle HTML suplementario |
| Procedencia de sucursal | PointExtractionLog / pos_bridge_extraction_logs | Mismo extractor | Log302317, job81086, branch_id28, branch_external_id14 | «Sucursal procesada 14.» | Verificación de la captura |

## Alias y equivalencias

| Identificadores | Estado | Evidencia | Revisión requerida |
| --- | --- | --- | --- |
| Point28 / externa14 / ERP18 | Identidad ya existente | FK actual y selección archivada del job | No se crea equivalencia |
| Captura03/10 / saldo30/09 | Distintos | Fecha y hora originales; autorización operativa del usuario | Saldo mensual sigue pendiente |

## Decisión de diseño
Reutilizar el snapshot existente exclusivamente como evidencia visible después de resolver la salida HTML. Cantidad exacta y unidad original, fecha/hora Mazatlán y referencias snapshot/job/log. No recalcular el mes ni escribir casos, no modificar cantidades mensuales, confianza, API, JSON ni exportaciones. No promover el stock observado a apertura a medianoche. La venta disponible del03/10 tiene granularidad diaria y todavía impide conciliar un intervalo operativo completo sin evidencia horaria.

Procedimiento: `inventario_fuentes_datos --term saldo --term inventario --term snapshot` con PG16 configurado; fuentes léxicas no equivalen a identidad. Consulta acotada de producción por job81086/sucursal28 y log302317; no llamadas nuevas a Point para renderizar.

Pendientes: primera nota y conversiones/ajustes previos a la captura; 30/09 sigue sin soporte. La vista conserva todo pendiente mensual que corresponda.


## Conciliación suplementaria del tramo operativo

La validación por producto acredita 42 de las 44 observaciones: 41 sin actividad y Bollo Mini Vainilla con 18 entradas y 18 ventas, final 0. Se revalidan en cada HTML las familias autoritativas, movimientos comerciales/Stock, frontera final y ausencia de actividad anterior a la captura. La ecuación es `PointHistoryReconciliation.expected_closing`; las reglas de identidad, coberturas y final vienen de los lectores existentes. No hay una lista fija de 42 casos aprobados ni se promueve el estado mensual o la red. Si cambia un flujo, el historial, la cobertura, el cierre o la fecha, se retira la acreditación y permanece la captura observada.

`BranchInventoryTraceabilityService._load_direct_movements` acepta una selección interna opcional de ubicaciones. Conserva los contadores y cobertura mensual; restringe eventos y mapeos al ámbito solicitado. Su llamada sin parámetro conserva el contrato global. Se retiraron joins no usados a productos/jobs que repetían JSON por fila; se conserva `receta.tipo` y la sucursal de ventas. El lector operativo tampoco repite metadata del import de Stock en cada movimiento.

Las 41 etiquetas sin actividad exigen totales en cero y ausencia de movimientos Stock. Las devoluciones que se compensan no se presentan automáticamente como ausencia de actividad. Producción toma exclusivamente lo realmente producido; un solicitado distinto no reemplaza un producido 0.

### Medición y límites

Servidor productivo, lectura aislada REPEATABLE READ READ ONLY, sin escrituras ni callbacks. Helper global: 15.19→4.25 s al retirar joins; mapas, autoridades y hash idénticos. Scope Leyva: 2.12 s/32 consultas, mismos mapas locales y autoridades. GET completo con Python nuevo y LocMem privado vacío: base 10.83 s/509 consultas; candidato 13.66 s/563 consultas. El incremento aproximado de 2.83 s corresponde a evidencia nueva revalidada, **no es una mejora de la carga completa**. La caché productiva no se borró ni modificó. La medición HTTP/Safari y validación visual móvil/escritorio son comprobaciones distintas y quedan para la revisión del consumidor tras despliegue.

El primer sondeo con DummyCache no representa caché inicialmente vacía que puede llenarse durante el request y no se usa para afirmar tiempos causales de usuario. Los resultados de consultas de producción adicional 5942/5943 se conservan en expediente separado; este PR no cambia su procedencia ni proyecciones.
