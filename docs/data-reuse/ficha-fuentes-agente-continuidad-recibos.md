# Ficha de fuentes — continuidad y recibos del agente

Fecha / ambiente: 2026-10-09; PostgreSQL 16.11 local aislado,
`erp_ai_continuidad_20261009`, puerto 56685. Sin extracción de producción.

## Necesidad y unidad de análisis

Un intercambio completo propio aporta contexto conversacional. Un ChatToolCall
incident.prepare aporta la identidad/versionado del borrador. Un ReporteFalla
confirmado aporta el folio operacional. Son unidades distintas, no tablas duplicadas.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creador y actualización | Identidad y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conversación | ChatConversation / orquestacion_chatconversation | chat_service | UUID, owner, active | 0 filas iniciales | API web, runtime |
| Mensaje | ChatMessage / orquestacion_chatmessage | chat_service / runtime | UUID, conversation + sequence únicos, rol/status | 0 filas iniciales | historial web, continuación |
| Borrador/recibo | ChatToolCall / orquestacion_chattoolcall | agent_incidents.invoke/confirm | UUID, conversación propia, versión/hash | 0 filas iniciales | runtime, tarjetas, confirmación |
| Resultado | ChatToolResult / orquestacion_chattoolresult | runtime / confirm | tool_call uno a uno | esquema y servicios existentes | tarjetas |
| Falla real | ReporteFalla / fallas_reportefalla | crear_reporte_falla, confirm humano | PK, equipo, sucursal, reportado_por | 0 filas iniciales | Fallas / Mantenimiento |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Recibo / falla ejecutada | Confirmada por relación explícita | metadata report_id + EXECUTED + confirmed_by, fila real coincidente; preparar no crea falla | No se fusionan registros |
| Chat / workflow / borrador | Distintas | mensaje, consulta técnica y propuesta poseen identidad y estados diferentes | No se equiparan por palabras |
| SolicitudFalla / ReporteFalla | Distintas | No se utiliza SolicitudFalla como folio de incident.prepare | Conserva fuente actual |

## Decisión

Reutilizar mensajes, borradores, resultados y fallas; extender sólo contexto,
metadatos de presentación y serialización. Ninguna tabla, captura o equivalencia
nueva. Revisar permisos también al proyectar citas del usuario vinculadas a una
respuesta revocada y los recibos al reabrir un chat.

Procedimiento: `manage.py inventario_fuentes_datos --term conversación --term
mensaje --term falla --term comprobante` (28 candidatos léxicos); consulta acotada
COUNT de las cuatro tablas iniciales en la base aislada (todos 0). Las pruebas
crean datos ficticios locales y verifican las relaciones; no representan volumen
ni cobertura temporal de producción. El comando léxico no prueba equivalencia.

Riesgos: prosa generada puede interpretar mal una evidencia; los recibos y estados
se muestran desde el servidor. Historial acotado no equivale a archivo completo;
reportes fuera de los diez recientes requieren una futura búsqueda autorizada,
no deben inventarse ni suponerse inexistentes.
