# Ficha de fuentes — notas de voz del Agente ERP

Fecha: 2026-10-11. Código main 46bcfaf, PostgreSQL 16 local aislado y lectura acotada de producción.

## Necesidad y unidad de análisis

Una grabación del propietario como entrada de un mensaje; su transcripción automática no acredita identidad, autorización, pago, entrega ni recepción. Continúa la misma conversación y sus procesos existentes.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creación y actualización | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conversación y mensaje | ChatConversation, ChatMessage / orquestacion_chatmessage | chat_service y usuario autenticado | UUID, secuencia y propietario | Producción: 54 mensajes propios del usuario 2, uno con imágenes, ninguno con audio antes del corte | UI privada, historial, Agent Core |
| Archivo de entrada | metadata_json.images, almacenamiento privado ya existente | agent_images | UUID de mensaje, path privado, SHA y bytes | Original del caso de compras conservado en el corte previo | Lectura multimodal y propuestas |
| Reserva de gasto | core.AuditLog, AI_AGENT_PILOT | agent_pilot | Piloto, UUID del turno y ciclo | Lectura fresca: 19/20 turnos, USD 2.87934350 reservados de USD 5 | Admisión del participante y llamadas OpenAI |
| Propuestas y procesos | ChatToolCall, AgentWorkflow | Servicios actuales de compras/mantenimiento | UUID, versión, hash, permisos actuales | Fuentes del inventario y código revisadas; no se crean transacciones operativas para probar voz | Tarjetas, confirmación humana, auditoría |

## Alias y equivalencias

Voz y texto son modalidades de entrada del mismo mensaje. Audio y foto son archivos distintos, aunque compartan mensaje. Una voz no equivale a credenciales ni a una confirmación operativa. No se crean equivalencias entre personas, productos o proveedores.

## Decisión

Extender metadata_json de ChatMessage con un original WAV privado y estado/transcripción. Reutilizar almacenamiento, cuota de 100 MB, conversaciones, Responses, servicios, historial, permisos y confirmaciones. Audio y fotos juntos comparten un mensaje y un límite total de 20 MB. Reutilizar AuditLog para reserva previa de transcripción (ciclo 0); ciclos 1–6 del Agent Core y límites de 20 turnos/USD 5 permanecen. Sin tablas, migraciones, credenciales ni dependencias nuevas.

Inventario reproducible: `inventario_fuentes_datos --term conversación --term audio --term transcripción`, PostgreSQL local con conectividad verificada. Cuatro modelos candidatos léxicos; no prueba equivalencia semántica. El grafo fue indexado pero no devolvió los servicios de audio/agente; se inspeccionó código y consumidores como fallback. Lectura SQL acotada READ ONLY: COUNT de mensajes del usuario 2 y metadata images/audio, sin contenido ni datos personales. Evidencia en `.codex/task-artifacts/ai-erp-voice-20261011/`.

Riesgos: errores de transcripción, voz de terceros, reenvío de audio, permisos revocados y gasto por fallos del proveedor. Se conserva original y reserva comprometida incluso si falla la llamada. La misma grabación no se ingresa dos veces en la misma conversación; correcciones se aportan por texto. Todo acceso al original exige propietario y permisos actuales.
