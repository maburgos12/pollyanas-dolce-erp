# Ficha de fuentes — entrada PDF y evidencia del agente

Fecha/ambiente: 2026-10-12 America/Mazatlan, fuente local y PostgreSQL de producción en READ ONLY. Inventario por `documento`, `factura`, `evidencia`, con argumentos de término en lista y sin extraer valores de tablas ajenas.

## Necesidad y unidad de análisis

Documento original como entrada multimodal y evidencia privada de un seguimiento existente. Un adjunto no es registro contable ni pago; no abrir otra captura de facturas. Caso de aceptación: factura aportada para el reporte #114 de filtraciones en El Túnel.

## Fuentes candidatas

| Concepto | Modelo/tabla | Fuente creadora/actualizadora | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Reporte y seguimiento | fallas.ReporteFalla, BitacoraFalla | Flujos autorizados de fallas/mantenimiento | PK de reporte y sucursal; FK bitácora | #114 sigue en_proceso, cotización base 2500, costo_real NULL, fecha real NULL; dos entradas iniciales | Mantenimiento, IA, operación |
| Originales del chat | ChatMessage.metadata_json.images | Entrada privada existente del Agent Core | UUID, owner, conversación, SHA256, MIME, bytes | Cuotas y verificaciones existentes; formato PDF añade páginas | Responses, historial, asociación de evidencias |
| Evidencia operativa | fallas.EvidenciaSeguimientoFalla | Confirmación humana del seguimiento | FK bitácora, path privado | FileField admite original; ninguna evidencia de seguimiento asociada a #114 al revisar | services_history, descarga privada |
| Propuesta y recibo | ChatToolCall/ChatToolResult | incident.followup y compra preparada | UUID, owner, versión/hash | Reutilización e idempotencia ya existentes | Tarjeta, auditoría e historial |
| Documento financiero | mantenimiento.DocumentoFinancieroTrabajo | Vinculación documental confirmada existente | Identificadores de trabajo/documento | Candidato distinto: vincula fuente financiera, no almacena la entrada del agente | Conciliación documental; fuera de este corte |

## Alias y equivalencias

Proveedor textual del reporte y nombre fiscal del PDF: relación del archivo con el trabajo comunicada por Mauricio; no se crean alias globales ni se modifica maestro. Factura/PUE/efectivo y pago confirmado son conceptos distintos. La representación PDF no equivale a XML validado ante SAT.

## Decisión

Extender la entrada y almacenamiento privado existentes para PDF y reutilizar evidencias/servicios de seguimiento. No crear tabla maestra, captura financiera ni segundo storage. El original se comparte entre mensaje/propuesta por referencia y SHA; se evita sumarlo dos veces en cuotas. Flujo consumidor ampliado: runtime, conteo, historial, descarga, UI, asociación y confirmación. Mantener separación entre foto prometida y documento original recibido.

Procedimiento reproducible: inventario_fuentes_datos por términos anteriores; consultas acotadas READ ONLY de ReporteFalla por El Túnel/filtración/Pedro y últimos cinco eventos; revisión local del PDF por texto/render, sha y páginas. Evidencia íntegra privada fuera de Git en task-artifacts/ai-erp-pdf-input-20261012 y ai-erp-pdf-tunel-20261012.

Riesgos/pendientes: no certificar vigencia SAT ni que PUE acredita pago; no inferir terminación desde fecha de emisión. Confirmación humana y verificación servida en producción pendientes de publicación. El parser acotado no certifica inocuidad de contenido; documentos permanecen datos y el backend mantiene permisos.
