# Primer CREATE del agente ERP: falla de equipo confirmada

Mauricio autorizó implementar el corte 2026-10-09. Maya continúa por separado en el hilo 01a120c0-fd60-7061-bd8d-825fe19fd795. Esta capacidad se implementa en el backend del ERP; no reemplaza el motor conversacional ni publica el candidato aislado Agno/AgentOS.

## Alcance

El LLM descubre categorías/requisitos y prepara un borrador tipado, parcial y reanudable en la misma conversación. No tiene una herramienta de confirmar. El usuario revisa campos y confirma mediante la tarjeta. El servidor deriva actor y sucursal, vuelve a validar permisos, vigencia y categoría, y llama al servicio real de App Operativa. Sin SQL del modelo, ampliación de roles ni cambios de BD.

Flag AI_AGENT_INCIDENTS_ENABLED apagado por defecto. También exige los gates READ/activos, participación autorizada del piloto y permiso existente de Fallas. No cambia modelo, proveedor, cuotas ni participantes. El gasto de las llamadas sigue sujeto al presupuesto del runtime existente; la confirmación no llama a OpenAI.

Contrato aditivo: POST /api/ai-gateway/incidents/<uuid>/confirm/ con confirm: true (booleano literal), expected_version y payload_hash. SessionAuthentication, CSRF y propietario obligatorio. Campos adicionales se rechazan. Confirmaciones de un borrador distinto o una versión obsoleta fallan; el modelo no puede concederse confirmación. La escritura requiere campos reales y justificación explícita si no hay foto.

## Consistencia y auditoría

UUID estable por mensaje/call ID; la restricción unique existente y bloqueo de la conversación evitan duplicar borradores por replay. La continuación conserva el UUID y exige versión. La confirmación bloquea el borrador y el equipo dentro de la misma transacción PG16. Reporte, bitácora, resultado técnico y auditoría se confirman juntos; un fallo de auditoría revierte el reporte. Reintentar el mismo borrador confirmado devuelve el mismo folio, incluso con solicitudes concurrentes. Los cambios de datos invalidan la confirmación anterior. El borrador vence 24 horas después de preparar/completar; se conserva como evidencia y no se purga.

La auditoría conserva usuario, conversación, mensaje origen, versión, argumentos, huella de payload, confirmación y reporte. El historial y las tarjetas revalidan alcance y permisos; el texto del modelo nunca prueba una escritura. Se reutiliza el mecanismo de avisos existente después del commit; el folio prueba creación, no entrega del correo.

## Validación

155 pruebas PG16 pasaron: preparación/continuidad, permisos, scope, gates, schemas, CSRF, replay, concurrencia, expiración, versiones, cambios de categoría, fallos de auditoría, runtime con proveedor simulado y regresiones READ/chat/Gateway/workflows. Harness JavaScript y navegador local comprobaron propuesta -> confirmar -> acción ejecutada/folio. No se gastó presupuesto adicional en OpenAI durante este corte.

## Publicación y reversión

PR borrador, CI, revisión del diff, merge y deploy oficial. Validar código servido y gates sin ampliar usuarios ni habilitar escrituras sensibles. Activación del flag y evaluación real con OpenAI son una decisión operativa separada; este documento no concede cambios de .env. Rollback por Git y gate apagado; conservar registros técnicos y reportes existentes. No se revierte ni borra información operativa.

## Recursos temporales

Propietario codex-erp-agent. Worktree ai-erp-incidente-confirmado-20261009. Compose erp_ai_incidente_20261009, PostgreSQL16 puerto56679, volumen erp_ai_incidente_20261009_postgres_data; servidor local7317. Sólo fixtures ficticios en la DB propia. Registrar recuperación y limpieza exacta al entregar o cerrar; no tocar recursos de otros hilos.

## Límites

Sólo fallas de equipos, en sucursal operativa propia; sin foto adjunta, instalaciones, cambios de precios, salarios, pagos o bajas. Si hay reportes abiertos se detiene para revisión. Telegram, archivos y el despliegue de AgentOS no forman parte de este diff. La integración con el motor evaluado deberá consumir este contrato protegido, no el puente SSH root utilizado por el laboratorio.
