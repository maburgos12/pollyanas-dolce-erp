# Agente privado de horarios por Telegram

El bot reutiliza solicitudes v2 del ERP, aprobación exacta y publicación Google existente.
No tiene credenciales del bot de releases ni admite automáticamente al primer remitente.
Sólo el usuario y chat privado vinculados previamente a un usuario ERP DG/Admin activo
pueden preparar y aprobar. Revocar ese permiso también detiene ejecución/reconciliación
Telegram. No se habilitó producción durante esta implementación.

## Entrada y confirmación

- `Todas hoy cerradas`: enumera las diez sucursales Google configuradas; rechaza una configuración parcial.
- `Plaza Nio mañana de 1 pm a 9 pm`.
- `Matriz y Payan del 2026-12-01 al 2026-12-03 de 09:00 a 18:00`.
- `Tunel todos los miércoles cerrado` o `Tunel cada lunes y miércoles desde 2026-12-01 hasta 2026-12-31 cerrado`.
- `/help`, `/start`, `/status`, `/cancel`.

Gramática conservadora, sin LLM. Fechas relativas se fijan a la fecha del mensaje en
America/Mazatlan y el año omitido es el del mensaje. La vista previa siempre muestra fechas
absolutas y la vigencia; no ejecuta nada. Un botón de 20 minutos vincula bot, autor, chat,
mensaje, nonce y revisión. Correcciones requieren la instrucción completa y cancelan la
vista anterior incluso si la nueva es ambigua. `/cancel` sólo cancela vistas pendientes.
No interpreta excepciones compuestas ni múltiples horarios distintos en una instrucción.
Fechas/rangos de mayor prioridad bloquean una propuesta semanal que los contradiga; el
operador debe expresar la fecha exacta. Las excepciones manuales Google se comprueban al
publicar mediante el publicador existente.

Audio: se conserva metadata acotada y se responde que la transcripción está en preparación.
`prepare_text` es el punto de entrada para una transcripción revisable; no llama APIs pagadas.

## Runtime y activación explícita

Aplicar migración `horarios_especiales.0002` antes de activar. El worker y beat existentes
son el runtime; no se crea un contenedor/base nuevos. Compose monta sólo en web y worker
`${HOURS_TELEGRAM_CONFIG_DIR_HOST:-./.hours-agent}` como `/etc/pollyanas-hours:ro`.
En producción el operador debe fijar `HOURS_TELEGRAM_CONFIG_DIR_HOST=/etc/pollyanas-hours`
antes del despliegue oficial; este cambio no modifica el `.env` ni copia credenciales. Configuración privada en
`/etc/pollyanas-hours` (o directorio explícito):

- `owner.json`: enteros positivos `telegram_user_id`, `telegram_chat_id`, `erp_user_id`.
- `telegram-token`: token del bot dedicado; su prefijo fija el bot ID.
- Ambos archivos regulares con permisos 0600; el usuario del worker debe poder leerlos.

`manage.py configure_hours_telegram --config /etc/pollyanas-hours` registra **desactivadas**
las tareas polling cada 15 segundos y reconciliación cada 15 minutos. `--enable` es una
activación explícita posterior; no se debe usar sin autorización operativa. La ruta de
configuración (sin secretos) viaja en kwargs del polling. No ejecutar este comando durante
una revisión de sólo lectura: escribe registros django-celery-beat.

El polling usa getUpdates sin long polling, lotes de 20 y exclusión PostgreSQL por sesión.
Cada lote y su cursor se confirman atómicamente antes de reconocer el offset siguiente.
El inbox durable se retoma después de reinicio. Tras aprobación, caída de broker o respuesta
perdida no produce una segunda aprobación. Puede repetirse el enqueue al morir entre envío
y confirmación local: el publicador existente serializa por sucursal y compara readback.
Una respuesta Telegram perdida puede repetir el mensaje; sólo el ID de vista confirmado
más reciente puede aprobar. No se reinterpreta silenciosamente un borrador como aprobado.

Antes de publicar se revalida el permiso del aprobador de cada revisión ganadora que
contribuye a la proyección; una revisión revocada vigente bloquea la escritura, y una
revisión histórica ya sustituida no bloquea su reemplazo.

ERP aprobado no significa Google confirmado. `/status` y avisos muestran recibos de Google
(confirmado por lectura, pendiente, fallido, programado). Una solicitud sustituida total
o parcialmente se informa como tal, separada del recibo de la proyección Google vigente;
no se declara confirmada toda la solicitud original. Tienda y Maya siguen expresamente
**pendientes de verificación** hasta implementar/verificar sus consumidores. Los avisos de
cambios se revisan sobre las últimas 20 solicitudes encoladas; `/status` muestra la última.
No hay comprobación real del bot ni de interfaces externas en estas pruebas aisladas.

## Recursos de la tarea

Worktree `telegram-hours-agent-20261122`, Compose `erp_telegram_hours_agent_20261122`,
PostgreSQL 16 local puerto 55648: recursos heredados de la misma tarea, conservados para
la siguiente fase de audio y validación. No se crearon recursos adicionales de runtime.
