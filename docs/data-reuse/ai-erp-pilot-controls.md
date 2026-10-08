# Ficha de fuentes — controles del piloto READ

Fecha y ambiente consultado: 2026-10-08; PostgreSQL 16 local aislado y producción, solo lectura para el inventario.

## Necesidad y unidad de análisis

Participante: identidad autenticada existente, sin crear roles ni empleados.
Gasto: una reserva técnica por respuesta de OpenAI, identificada por UUID del mensaje asistente y ciclo. Un turno puede consumir hasta seis reservas; el piloto completo tiene un techo acumulado de 20 turnos y USD1.00, independiente de conversaciones y reinicios.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Identidad | auth.User / auth_user | Autenticación existente | PK; estado activo | Dos cuentas activas comparten el nombre Mauricio. Las tres conversaciones visibles pertenecen al usuario 2; este es el participante, no la cuenta 24 | Auth, ACL, Gateway |
| Conversaciones | orquestacion.ChatConversation / orquestacion_chatconversation | Chat actual | UUID y propietario | Producción: 3; todos owner_id=2, títulos coincidentes con navegador autenticado | UI, runtime |
| Turnos y ejecución | ChatMessage / orquestacion_chatmessage | Servicio de chat | UUID; secuencia única por conversación | Producción: 16 mensajes | Historial, tool calls, runtime |
| Estado conversacional | ChatConversationState / orquestacion_chatconversationstate | Chat y referencias READ | OneToOne con conversación | Producción: 3 | Contexto; puede eliminarse con chat y sobreescribirse por legado |
| Auditoría técnica | core.AuditLog / core_auditlog | Backend y servicios | PK; model/action/object_id | Producción: 34,815; reservas AI_AGENT_PILOT: 0 | Bitácora existente y nuevo control de cuota |
| Reservas de producto | crm.PickupReservation | CRM/tienda | Token y producto/sucursal | Candidato léxico de inventario, dominio diferente | Stock; no reutilizar para dinero del proveedor |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Mismo nombre Mauricio vs misma cuenta | Distinta | PK2 y PK24 son identidades distintas; solo PK2 coincide con las conversaciones observadas | Ninguna fusión ni cambio de identidad |
| Reserva de stock vs reserva de consumo OpenAI | Distinta | Entidades, unidades y consumidores diferentes | No conectar |
| Mensaje asistente vs turno presupuestario | Confirmada para este piloto | UUID existente identifica el turno y todos sus ciclos; recibos guardan copia escalar, sin FK CASCADE | No fusiona registros |
| Estado chat vs presupuesto acumulado | Distinta | El estado es borrable con chat; no puede sostener el límite global | Usar AuditLog independiente |

## Decisión de diseño

Reutilizar auth.User, permisos y ámbito existentes. Reutilizar AuditLog para recibos append-only identificados por model=AI_AGENT_PILOT, object_id=read-assets-20261008 y action=AI_AGENT_PILOT_RESERVE. No crear tabla, campo, migración, captura maestra o nueva dependencia. Un bloqueo transaccional PostgreSQL serializa reservas globales; no se mantiene durante OpenAI. Rechazar llamadas dentro de transacciones externas para asegurar el commit previo. No descontar ni reembolsar reservas, incluso con timeout. Cambiar participante no renueva el presupuesto. El ID fijo del piloto no es una opción de usuario ni variable que rote con el día.

Procedimiento reproducible: `inventario_fuentes_datos --term chat --term audit --term reserva --term cuota --presence --limit 30`; local previamente migrado. Producción: transaction READ ONLY, statement_timeout 5s, conteos acotados a cinco modelos y máximo cinco filas de cuentas/conversaciones. Evidencia privada en task-artifacts/ai-erp-pilot-controls-20261008; sin claves, cookies o datos operativos copiados.

Riesgos: AuditLog no es almacenamiento inmutable contra un administrador de DB; no se autoriza purgar ni alterar recibos. No se encontraron jobs de retención que los eliminen. El control protege el camino READ aprobado, no la facturación total de la cuenta OpenAI, otras aplicaciones ni el chat legado. Datos extraídos de activos siguen siendo DTOs sujetos a permisos existentes.
