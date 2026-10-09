# Piloto READ — participantes y gasto

## Alcance aprobado

Solo la cuenta ERP de Mauricio verificada en el navegador (PK2), GPT-6.1 Sol, consultas de activos/mantenimiento y continuidad técnica de consultas. Sin crear órdenes, pagos, empleados, permisos ni datos operativos. El piloto utiliza el mismo Gateway, políticas de sucursal, DTOs y workflows ya probados. Telegram y otras modalidades quedan fuera de este corte.

## Contrato

- Feature flags READ, Gateway y workflows apagadas por defecto. `AI_AGENT_PILOT_USER_ID=0` deniega la admisión por defecto; no basta ser superusuario. Identidad y estado activo se consultan nuevamente en el servidor.
- `AI_AGENT_READ_MODEL` independiente del modelo del chat legado; exclusivamente `gpt-6.1-sol` para este piloto. Endpoint OpenAI oficial, service_tier=default, store=false, output máximo 1400, retries=0.
- 20 turnos admitidos como máximo, globales entre chats, y USD1.00 de reservas acumuladas. No hay renovación diaria, reset en sesión, reembolso por timeout, ni ampliación desde argumentos del modelo o navegador.
- Reservar antes de cada llamada. PostgreSQL advisory_xact_lock serializa lectura y recibo; el commit ocurre antes del acceso a red. Un recibo repetido no autoriza reenviar. Cuota agotada, ledger corrupto, configuración inválida, falta de acceso o transacción externa: no enviar la llamada.
- Los recibos independientes en AuditLog sobreviven al borrado de conversación y cambio de participante. El historial técnico conserva modelo/tier observados del proveedor y usage separado; usage no devuelve dinero reservado.
- No transmitir prompts ni salidas a la bitácora de presupuesto: solo UUID, ciclo, modelo, tamaño e importe reservado. El resto de ejecución conserva la auditoría existente.
- Revocar acceso a una conversación READ no habilita herramientas legadas como alternativa. Las conversaciones legadas y usuarios fuera del piloto conservan su comportamiento preexistente.

## Reserva de gasto

Tarifa verificada el 2026-10-08 en [documentación oficial GPT-6.1 Sol](https://developers.openai.com/api/docs/models/gpt-6.1-sol): Standard USD2 por millón input, USD2.50 cache write, USD10 output; regional +10%. Reserva con techo input USD2.75/output USD11 por millón, sin descuento de caché, para cubrir la tarifa máxima de input/cache write y regional. Cota conservadora de entrada: bytes UTF-8 del payload JSON más 4096 de envoltura; payload máximo 60,000 bytes, output máximo 1400. Redondeo Decimal hacia arriba a ocho decimales. No alcanza umbral de entrada larga de 272k.

Es una reserva conservadora para este camino, no conciliación de factura, impuestos ni techo de gasto de otras funciones de la misma cuenta. No usar Fast/Ultrafast, hosted tools ni endpoint alternativo. Revalidar tarifas antes de ampliar/reanudar un piloto; nunca borrar recibos para renovarlo. Para extender a cuotas permanentes o múltiples modelos se requiere un diseño revisado, no cambiar estos límites silenciosamente.

## Archivos y pruebas

`config/settings.py`, `api/ai_gateway_assets.py`, servicios `agent_pilot`, `agent_read_runtime`, `chat_service`, caller `chat_views`; aviso de piloto detenido en template, versión de caché PWA y pruebas del hub; fixtures READ existentes; `tests_agent_pilot` en PostgreSQL TransactionTestCase. Se verifican activo/otro superusuario/config inválida; READ directo y runtime; 20 turnos y última continuación; USD1; dos conexiones disputando último turno y saldo; duplicados; commit previo a proveedor; timeout; borrado chat/reconexión; corrupción; modelo/tier/tamaño inválidos; replay/revocación sin fallback y aislamiento del modelo legado. Las pruebas anteriores aíslan admisión y presupuesto nuevos para seguir probando dominio, permisos y separación de varios propietarios sin depender de una cuota acumulada; nuevas pruebas usan recibos reales.

## Despliegue y rollback

Primero publicar código con gates apagadas por flujo Git/CI y script oficial, validar PostgreSQL/migraciones y navegador. Activación limitada a cinco valores aprobados: AI_AGENT_READ_ENABLED, AI_GATEWAY_ASSETS_ENABLED, AI_AGENT_WORKFLOWS_ENABLED, AI_AGENT_READ_MODEL, AI_AGENT_PILOT_USER_ID. Conservar copia de configuración con permisos root, sin imprimir credenciales. Reiniciar servicios para cargar configuración; validar usuario exacto, gates, módulo visible y consulta real. Rollback: apagar las tres flags y conservar recibos y estado; no revertir ni limpiar datos operativos. El despliegue no crea migraciones. El techo es persistente aunque se revierta código y después se vuelva a habilitar.

## Criterios de aceptación

Pruebas y checks sin errores, cero migraciones nuevas, revisión independiente y CI correctos. En producción: solo PK2 admite READ; otros usuarios denegados por el Gateway; modelo/tier de respuesta verificados; recibo comprometido para cada llamada; total menor o igual USD1; resultado respaldado por tools; ningún registro operativo creado. Registrar diferencia entre pruebas sintéticas y consulta real; limpieza de recursos locales de esta tarea con respaldo verificado y ownership registrado.
