# Agente privado de horarios por Telegram

El bot reutiliza solicitudes v2 del ERP, aprobación exacta y publicación Google existente.
No tiene credenciales del bot de releases ni admite automáticamente al primer remitente.
El propietario original conserva su requisito DG/Admin activo. Las personas adicionales
requieren vinculación explícita entre ID Telegram, chat privado e identidad ERP activa,
y permiso `approve_own=true` exclusivo de este bot; sólo preparan y aprueban solicitudes propias. Revocar ese permiso también detiene ejecución/reconciliación
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


## Varias personas verificadas (2026-11-23)

`owner.json` conserva las tres claves originales y admite `users` opcional (máximo 32).
Ejemplo ficticio, sin token ni datos personales reales:

```json
{
  "telegram_user_id": 100001,
  "telegram_chat_id": 100001,
  "erp_user_id": 1,
  "users": [
    {"telegram_user_id": 100002, "telegram_chat_id": 100002, "erp_user_id": 2, "approve_own": true}
  ]
}
```

Los tres IDs son enteros positivos, únicos por campo; el chat privado coincide con el
ID Telegram. `approve_own` exige booleano: `false` deshabilita al miembro. Nunca asignar
la identidad ERP de otra persona, promover DG/Admin, aceptar el primer `/start`, ni
inferir identidad por nombre, teléfono o @usuario. Verificar con el responsable qué
usuario ERP activo corresponde a cada ID Telegram antes de editar la lista.

El operador autorizado respalda el archivo privado fuera del repo, prepara el JSON
completo en un temporal 0600 del mismo directorio y lo reemplaza atómicamente conservando
propietario/permisos. No imprimir tokens ni copiar `.env`. El montaje sigue siendo RO; también se monta en `worker_hours_publish` para que
revalide la lista vigente antes de publicar. El despliegue oficial debe recrear los
contenedores por este cambio Compose; no basta reiniciar el proceso viejo.
No cambia el bot, su cursor ni la reserva global de audio de 500 centavos. La lista se
relee al actuar; no hace falta reiniciar ni ejecutar `--enable` para añadir o retirar
una vinculación. Validación de sólo lectura, antes/después de activar usuarios:

```sh
python manage.py configure_hours_telegram --config /etc/pollyanas-hours --validate-only
```

Este comando valida todos los usuarios habilitados y cuenta identidades, sin imprimir
sus datos, enviar mensajes ni modificar Beat. Un usuario inactivo o un miembro sin
permiso no procesa trabajo; una vinculación duplicada invalida la configuración.
`/status`, `/cancel`, correcciones y avisos están aislados por identidad recibida, no
sólo por chat. Texto y audio conservan `requested_by`, `approved_by`, `executed_by` y
bitácora con la persona real. Los callbacks ajenos nunca aprueban.

Aplicar migración 0004 antes del código consumidor: vincula inbox histórico únicamente
cuando existe una solicitud con autor ERP. Entradas/audio heredados sin autor verificable
se cancelan y purgan PCM; pedir que la persona reenvíe su instrucción. Las vistas antiguas
sin snapshot requieren una nueva vista. No modifica horarios aprobados, cursor ni gasto.
El polling y el RPC purgan leases/botones de identidades retiradas. Antes de ejecutar o
reconciliar se revalida el vínculo actual de cada revisión ganadora; retirar acceso no
cancela ni borra horarios centrales ya aprobados.

La vista nueva advierte qué solicitudes aprobadas solapan sus sucursales/fechas. La
aprobación compara esas revisiones bajo el mismo lock de sucursal del publicador: si
otra persona aprobó/canceló/cambió una regla coincidente, exige reenviar la instrucción
y revisar otra vista. No se reemplazan silenciosamente horarios sobre una vista vieja.
La comprobación incluye reglas de igual prioridad, rangos y semanas sin fecha final;
fechas/sucursales sin solapamiento no bloquean.

Verificación productiva pendiente del operador: código/migración vigentes en web y
workers, validación privada sin escrituras, bienvenida al chat verificado y flujo real
por cada persona. No crear actualizaciones Telegram ficticias ni aprobar horarios reales
como prueba técnica. Las pruebas locales usan Google/Telegram/OpenAI simulados.

### Fuentes y límites de la validación local

Ficha de reutilización, 2026-11-23: PostgreSQL 16 aislado de
`telegram-hours-multiuser-20261123`, sin datos operativos.

| Concepto | Fuente oficial y clave | Decisión y consumidores |
| --- | --- | --- |
| Persona/autorización ERP | `auth.User`, PK estable y `is_active`; roles del owner | Reutilizar usuarios; no crear identidades, grupos ni permisos globales. |
| Vínculo Telegram | `owner.json` 0600, IDs numéricos explícitos | Extender con `users`; humano verifica equivalencias, no inferir por nombres. |
| Autor recibido | `TelegramHoursUpdate.erp_user_id` | Guardar FK del vínculo al recibir; texto, audio, entrega y recuperación. |
| Solicitud/aprobación/auditoría | `SolicitudHorarioEspecial`, `HorarioEspecialBitacora` | Reutilizar autor/aprobador/ejecutor y revisión; API, tienda, Maya y Google conservan la misma fuente. |
| Cursor y gasto de voz | `TelegramHoursCursor.bot_id` | Reutilizar una fila por bot; no duplicar ni reiniciar presupuesto por usuario. |

Evidencia reproducible: `inventario_fuentes_datos --term telegram --term horario`,
inspección del grafo/contratos, y `count(*)` local acotado en auth_user, inbox,
solicitudes y cursor: 0/0/0/0 antes de fixtures. Las pruebas usan identidades ficticias
777/888. La equivalencia real queda documentada fuera del repo por el coordinador;
ningún teléfono, usuario real ni token forma parte de estos ejemplos o tests.

## Correcciones a un borrador mediante IA local

Con la opción activa, una corrección breve usa únicamente el borrador vigente del
mismo remitente. El botón anterior se pausa en cuanto llega la corrección.
Qwen recibe la frase, los horarios propuestos y las excepciones vigentes por el
router privado. Puede proponer qué fechas toman la franja del borrador, mantener
un cierre semanal ya aprobado o cambiar una hora de apertura o cierre durante
el periodo. El ERP valida sucursal, fechas, horas y reglas. Si falta un dato,
conserva el borrador para otra aclaración. Sólo una vista previa validada emite
un nuevo botón de aprobación.

El contrato `hours_draft_edit` exige la identidad `telegram_hours` y sólo procesa
una corrección viva vinculada a un mensaje en espera. Los dos nodos privados
pueden tomarla, Qwen 4B en Mac-IA Debian y Qwen 8B en Mac-IA-02. Si el
primero falla, el otro puede tomar la misma solicitud mientras siga vigente. No
envía archivos, tokens ni audio al modelo de razonamiento. La corrección vence en
cuatro minutos o al concluir la vigencia del borrador, lo que ocurra primero.

Después del CI y del despliegue del ERP, publicar en ambos nodos el archivo versionado
`ops/erp-mac-ia/router_node.py` en la ruta existente del comando forzado. Verificar
hash, permisos 0600, sintaxis, probe y una inferencia sintética antes de activar
`python manage.py configure_hours_reasoning enable`. Para pausar sólo la función
de correcciones, ejecutar `python manage.py configure_hours_reasoning disable`.

Cuando la persona pide usar el horario propuesto en fechas concretas, la IA
devuelve la sucursal y una lista de fechas ISO. El ERP comprueba que cada fecha
sea una excepción abierta del borrador. El horario nuevo se toma del borrador.
Los cierres y las demás excepciones se conservan. Las fechas sustituidas adquieren
la prioridad de un horario especial de un solo día y se confirma la propuesta
completa con un botón nuevo.

Si la IA termina dentro del plazo de la corrección, el bot puede recoger el
resultado en la siguiente consulta mientras el borrador original siga vigente.
