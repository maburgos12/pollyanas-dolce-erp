# Ficha de fuentes — imágenes del Agente ERP

Fecha: 2026-10-11. Código de origin/main c2f0a6f; consultas de producción de solo lectura y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Una imagen original aportada por el propietario de un mensaje. El mismo original puede ser evidencia de una propuesta. Una observación del modelo no es factura, pago, recepción, cierre técnico ni autorización.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creación y actualización | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conversación y mensaje | ChatConversation, ChatMessage / orquestacion_chatmessage | chat_service, propietario autenticado | UUID de conversación/mensaje, usuario y secuencia | Caso de compras: 8 mensajes del usuario 2 | UI privada, historial y Agent Core |
| Propuesta y evidencia privada | ChatToolCall / orquestacion_chattoolcall, metadata_json.files | agent_purchases y agent_followups | UUID, versión, hash, propietario y permisos frescos | Propuesta f75e297d: un PNG de 74590 bytes, SHA verificado en el corte anterior | Tarjeta de aprobación, endpoints privados, confirmación |
| Evidencia operativa de mantenimiento | EvidenciaSeguimientoFalla / fallas_evidenciaseguimientofalla | Confirmación existente del seguimiento | Reporte, bitácora y ámbito autorizado | Candidata obtenida por inventario de fuentes; no se creó actividad de prueba | Fallas y mantenimiento |
| Compra y comprobante | SolicitudCompraDepartamental y EventoCompraDepartamental | Servicio nativo de compras | Folio, solicitante, área | Propuesta del caso aún sin solicitud formal, compra financiera ni recepción | Compras departamentales |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Foto, imagen, captura | Mismo tipo de entrada; no identidad entre documentos | SHA distingue contenido; nombre o vendedor visible no prueban pertenencia al artículo | No se crean equivalencias de proveedores |
| Captura de compra, factura, pago, recepción | Distintos | Captura dice que se compró; no incluye importes, factura ni entrega realizada | Continúan los controles humanos del ERP |
| Fuente del mensaje y archivo de propuesta | Mismo original solamente cuando el usuario lo vincula en su conversación | Se conserva path, UUID y SHA sin guardar otra copia | Versión y permisos del borrador se revalidan |

## Decisión

Extender metadata_json de ChatMessage con referencias de imágenes privadas. Reutilizar Pillow, validación de evidencia, almacenamiento ya protegido, Responses, historial, ledger, confirmación, RBAC y servicios existentes. No añadir tablas, proveedor ni nuevo canal. El prefijo `compras/departamentales/agente/inputs/` hereda la denegación pública existente; la lectura ocurre exclusivamente mediante el endpoint del mensaje autenticado.

Procedimiento reproducible: `inventario_fuentes_datos --term evidencia`, `--term archivo`, `--term ChatMessage`, `--term ChatToolCall`; candidatos léxicos revisados contra servicios reales y consulta acotada del caso existente. Evidencia íntegra en `.codex/task-artifacts/ai-erp-multimodal-preflight-20261011/`.

Riesgos: legibilidad y extracción imperfectas, instrucciones maliciosas en imágenes, doble uso de una evidencia, revocación de acceso, transporte grande y gasto. Los límites y permisos se validan en servidor; ninguna imagen habilita escritura. Sin equivalencias nuevas ni duplicación de datos maestros.
