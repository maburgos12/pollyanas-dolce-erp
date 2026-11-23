# Voz privada para horarios

Código preparado; este directorio NO instala ni activa servicios. Primero revisión,
CI, merge y despliegue oficial del ERP. Sin nuevo endpoint HTTP, servicio VPS ni
cola genérica. La Mac abre SSH al VPS con una llave exclusiva; recibe sólo PCM
mono 16 kHz, id, lease y SHA256, y devuelve texto. No recibe tokens Telegram/OpenAI.

## Contrato y presupuesto

`TelegramHoursUpdate` conserva metadata, estado, lease y PCM base64 mientras trabaja.
La nueva voz invalida de inmediato la vista anterior, aun si resulta demasiado larga.
Una corrección persistida invalida el resultado local tardío. `prepare_text` es la
única salida: texto escuchado + interpretación + botón humano existente; nunca aprobación
por transcripción. Las palabras de horas con mañana/tarde/noche se normalizan de forma
conservadora; nombres/alias y horas ambiguas se resuelven con la gramática existente.

La Mac reclama cada 5 s, espera local inicial 30 s, lease 90 s, hijo Whisper máximo
60 s y dos threads CPU. El lease es el límite de disponibilidad: sin respuesta al
vencer, el polling existente pasa a respaldo. No hace falta heartbeat ni servicio
adicional. Un resultado local posterior al cambio de estado se rechaza.

El ERP descarga únicamente desde api.telegram.org, sin redirects/proxies, máximo
8 MiB y plazo acotado. ffmpeg sin protocolos de red/archivos ni playlists decodifica
hasta 181 s; si el PCM excede 180 s, rechaza sin transcribir ni gastar. Sólo el PCM
validado viaja a Whisper o OpenAI. PCM en memoria/JSON, sin archivos de audio en disco;
se purga del trabajo al finalizar/cancelar. Se conserva hash y reserva para auditoría.

`TelegramHoursCursor.audio_reserved_cents` es acumulativo, con bloqueo PostgreSQL:
3 centavos antes de cada intento OpenAI, techo fijo 500 centavos, sin renovación ni
reembolso automático, incluso si la petición falla o su resultado queda incierto.
Por redondeo conservador caben como máximo 166 intentos (498 centavos). Un crash tras
reservar no vuelve a llamar. El texto y Whisper local siguen funcionando al agotar
respaldo. No modifica `agent_audio` ni su presupuesto. Mantener este cursor durante
rotaciones del bot; una rotación requiere transferir el saldo acumulado, nunca crear
una nueva autorización de US$5. `gpt-transcribe`, idioma es, retries=0, timeout=20 s.
La clave dedicada va en `openai-key` 0600 junto a owner.json; no usar .env compartido.

## Instalación del puente después de desplegar (root en VPS)

La migración 0003 y ffmpeg del Dockerfile deben estar en web y worker. Confirmar
que `docker compose exec -T web python manage.py check` y `migrate --check` pasan,
y que ambos contenedores tienen el mismo montaje privado HOURS_TELEGRAM_CONFIG_DIR.
Los archivos 0600 del montaje deben pertenecer al UID que ejecuta Django.
No copiar ni imprimir credenciales. No activar polling aún.

Desde `/opt/pastelerias-erp` ya desplegado:

```sh
useradd --system --create-home --shell /bin/sh hours-audio
install -o root -g root -m 0755 ops/telegram-hours/audio_rpc.py /usr/local/sbin/pollyanas-hours-audio-rpc
install -d -o root -g root -m 0755 /etc/ssh/authorized_keys
```

Crear `/etc/sudoers.d/pollyanas-hours-audio` root:root 0440 con exactamente:

```text
hours-audio ALL=(root) NOPASSWD: /usr/local/sbin/pollyanas-hours-audio-rpc ""
```

Validar `visudo -cf /etc/sudoers.d/pollyanas-hours-audio`. Crear
`/etc/ssh/sshd_config.d/82-pollyanas-hours-audio.conf` root:root 0644:

```text
Match User hours-audio
    AuthenticationMethods publickey
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    AuthorizedKeysFile /etc/ssh/authorized_keys/%u
    ForceCommand sudo -n -- /usr/local/sbin/pollyanas-hours-audio-rpc
    DisableForwarding yes
    PermitTTY no
    PermitUserRC no
    X11Forwarding no
Match all
```

La cuenta NO pertenece al grupo docker ni tiene otros sudoers. Instalar únicamente
su clave pública dedicada en `/etc/ssh/authorized_keys/hours-audio`, root:root 0644,
una línea `restrict ssh-ed25519 CLAVE_PUBLICA...`. El directorio root:root 0755 y
el archivo 0644 permiten que sshd lea la clave pública como hours-audio sin que
esa cuenta pueda modificarla. Antes del claim SSH real, comprobar:

```sh
sudo -u hours-audio test -r /etc/ssh/authorized_keys/hours-audio
sshd -t
```

Ambas comprobaciones deben terminar correctamente antes de recargar SSH; conservar
la sesión administrativa para probar. El wrapper ignora argumentos y
SSH_ORIGINAL_COMMAND; ejecuta sólo el comando Django fijo en web, stdin JSON <=8 KiB.
La cuenta no puede elegir shell, archivo, contenedor, ruta o comando. El wrapper
limpia el ambiente, limita tiempo/salida y nunca devuelve stderr de Docker/Django.

## Mac, usuario Mauricio (sin instalar paquetes ni bajar modelos)

Comprobar `/usr/local/bin/python3` con whisper/torch y `~/.cache/whisper/base.pt`.
Crear llave nueva; no sobrescribir una existente ni reutilizar agente_dg_ops:

```sh
ssh-keygen -t ed25519 -f ~/.ssh/pollyanas_hours_audio -N '' -C pollyanas-hours-audio
install -d -m 0700 ~/.local/share/pollyanas-hours
install -m 0600 ops/telegram-hours/ssh_config ~/.local/share/pollyanas-hours/ssh_config
install -m 0700 ops/telegram-hours/audio_worker.py ~/.local/share/pollyanas-hours/audio_worker.py
```

Fijar en `~/.local/share/pollyanas-hours/known_hosts` la clave de host del VPS ya
verificada por el canal administrativo, modo 0600; nunca aceptar ssh-keyscan sin
cotejar fingerprint. Transferir sólo el `.pub` al archivo root indicado arriba.
Antes de arrancar: probar JSON inválido y comando remoto arbitrario; ambos deben
devolver `{"error":"unavailable"}` o rechazo, nunca ejecutar el comando. Un claim
sin trabajo debe devolver `{}` y no contiene credenciales.

```sh
printf '%s' '{"action":"claim"}' | ssh -T -F ~/.local/share/pollyanas-hours/ssh_config hours-audio
printf '%s' '{"action":"shell"}' | ssh -T -F ~/.local/share/pollyanas-hours/ssh_config hours-audio 'id'
plutil -lint ops/telegram-hours/org.pollyanas.hours-audio.plist
install -m 0600 ops/telegram-hours/org.pollyanas.hours-audio.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.pollyanas.hours-audio.plist
```

No carga modelo si no hay audio. No logs de voz, transcripciones ni errores crudos.
FileVault/suspensión/cierre de sesión/tapa afectan disponibilidad. Para detener:
`launchctl bootout gui/$(id -u)/org.pollyanas.hours-audio`; el texto sigue operativo y
el respaldo puede consumir el presupuesto cuando haya voz pendiente. Para evitar
respaldo durante una prueba, no activar polling o mantener la prueba fuera de producción.

## Verificación antes de activar

1. Ejecutar `manage test horarios_especiales.tests_telegram_audio horarios_especiales.tests_telegram`
   en PostgreSQL aislado y `python3 ops/telegram-hours/test_audio_worker.py`.
2. Probar un audio sintético local con el worker `--transcribe` y comprobar hash,
   texto y duración. No llamar OpenAI ni enviar Telegram como parte de estos tests.
3. Verificar SSH forced command real, denegación de shell/TTY/forwarding, claim vacío
   y ausencia de tokens. Esto requiere infraestructura instalada y no lo certifican mocks.
4. Prueba privada autorizada de Mauricio: voz <=180 s, sólo vista previa; corrección
   invalida botón/lease anterior. No aprobar un cambio real durante prueba técnica.
5. Verificar Mac offline con proveedor simulado únicamente en test. La primera llamada
   real pagada requiere operación autorizada; no falsear recibos ni resetear el contador.

Los tests no acreditan precisión con voz real de Mauricio, permisos SSH instalados,
activación LaunchAgent ni entrega Telegram productiva. No hay recursos Docker nuevos:
se usa la DB aislada de la tarea telegram-hours-agent-20261122, propiedad del coordinador.

## Evidencia local de esta implementación (2026-11-22)

- PostgreSQL aislado de la tarea: suite `horarios_especiales`, 85 pruebas, 54.069 s;
  tests específicos del worker 2/2. `check`, `migrate --check` y plist válidos.
- El esquema de pruebas sin semillas emite `reportes.W001` por reglas comerciales
  ajenas al audio y aviso de staticfiles ausente; el check fuera de tests no da errores.
- Mac actual, `/usr/local/bin/python3`, modelo base ya cacheado, CPU dos threads:
  voz sintética Paulina «Todas hoy cerradas.» (1.2655 s) produjo exactamente
  «Todas hoy cerradas.» en 4.29 s, incluyendo arranque del hijo/carga de modelo.
  AIFF sintético en TemporaryDirectory propio eliminado al salir; PCM sólo en memoria.
- Sin gasto OpenAI, envíos Telegram, llave nueva, instalación de SSH/LaunchAgent,
  contenedores nuevos ni cambios de producción. Worktree y DB siguen a cargo del
  coordinador para revisión/CI/despliegue; no se retiraron recursos compartidos activos.

Revisión de aislamiento de entorno: ffmpeg y SSH reciben únicamente PATH fijo y
LANG=C; el hijo Whisper añade HOME sólo para el modelo cacheado. No se copia el
entorno del ERP/Mac. Pruebas centinela: 39 pruebas audio+Telegram y 4 worker verdes;
smoke `work()` con Whisper real y SSH simulado transcribió la misma voz sintética
en 4.34 s bajo el entorno explícito. Check sin errores; temporales eliminados.

## Claim vacío sin arranque Django

El wrapper consulta primero `audio_probe.py` dentro de web únicamente para `claim`.
El probe usa la DATABASE_URL ya presente, psycopg2 y una consulta literal de sólo
lectura sobre `horarios_especiales_telegramhoursupdate`, estado `AUDIO_WAIT` (índice
existente). No recibe SQL, ruta, credenciales ni opciones del cliente. Conexión 2 s,
statement_timeout 1 s, lock_timeout 0.5 s y límite exterior 5 s; salida exacta 0/1.
Vacío devuelve `{}` sin Django; pendiente conserva el RPC Django original con todos
sus permisos, propietario y lease. `finish` siempre pasa por Django. La consulta no
reserva trabajos ni cambia leases: `AUDIO_LOCAL` vencido y `AUDIO_PAID` siguen a cargo
del polling/fallback existente. Una voz que llegue después del probe se toma en el
siguiente ciclo de 5 s; no cambia la espera local de 30 s ni el presupuesto.

Error/timeout/salida extra produce `unavailable`, sin filtrar errores ni tomar trabajo.
El probe sólo evita trabajo en vacío; no autoriza acciones. Tras el deploy oficial,
reinstalar el wrapper root-owned con el comando de instalación anterior y verificar
claim vacío real antes de reanudar LaunchAgent. No basta actualizar el archivo Git.

Validación previa al despliegue del filtro: 80 tests `scripts/tests/test_telegram*.py`
(incluidos 7 del probe/routing) y 4 worker; Python compilado y diff válidos. Ejecución
real read-only del probe enviado por stdin a web, sin escribir archivos del VPS:
1.829 / 1.298 / 1.374 s incluyendo SSH administrativo, Docker y Python. Son tres
muestras de vacío; no prueban el wrapper SSH dedicado actualizado ni latencia con voz.
No se creó Docker/DB nuevo; check/migrate-check usan el entorno aislado de la tarea
principal con PGOPTIONS read-only. El CI completo requerido permanece sin cambios.

## Aislamiento de colas de horarios

`worker_hours_control` consume exclusivamente `hours_control` (polling y despacho
periódico). `worker_hours_publish` consume exclusivamente `hours_publish` (Google).
Ambos usan el Dockerfile ERP, PostgreSQL y Redis existentes; prefork, concurrencia
1 y prefetch 1. No crean otra base ni duplican los datos de horarios. El montaje
privado de Telegram/OpenAI es de sólo lectura en control, publicación y web;
publicación lo requiere para revalidar las vinculaciones vigentes antes de escribir. Se conserva en el worker general mientras drenan mensajes antiguos.
No purgar `celery`: contiene trabajo de otros módulos.

Tras el despliegue autorizado, ejecutar `configure_hours_telegram --enable` desde
web con el directorio privado vigente: reemplaza cualquier queue/exchange/routing_key/
priority anterior de estas dos tareas Beat. Polling cada 15 s, expiración 30 s;
reconciliación cada 900 s, expiración 840 s. Registrar sin `--enable` las desactiva,
como antes. Verificar estos valores en PeriodicTask; las rutas Celery por sí solas
no sustituyen overrides persistidos de Beat.

El despacho periódico selecciona en PostgreSQL sólo v2 aprobadas, no canceladas,
no expiradas y vencidas, antes de limitar a 100; trabaja como máximo 10 s entre
solicitudes (soft limit 20 s, hard 30 s). Consulta la profundidad Redis sumando
las prioridades y sólo llena los espacios libres hasta 100 mensajes; esto también
limita mensajes expirados aún no consumidos. El broker usa conexión/socket de 2 s
sin reintentos. Este límite afecta el despacho periódico, no las aprobaciones
interactivas del propietario. Reserva `next_reconcile_at` por 900 s bajo
bloqueo de fila y envía una ejecución por solicitud con expiración 840 s, prioridad
9 (Redis; las aprobaciones interactivas mantienen prioridad por defecto 0), sin
reintentos de envío al broker. Si falla el envío se revierte la reserva. Una caída
entre envío/commit puede duplicar una entrega: se mantienen los locks de sucursal,
readback, revisión aprobada y auditoría del publicador. La ejecución vuelve a
programar el reintento o el horizonte diario normal. Si el worker no recibe el
mensaje antes de expirar, la siguiente reconciliación recupera la intención.

Se pasa el aprobador original a la tarea; sus permisos y propiedad se revalidan al
consumir. La CLI `reconcile_special_hours [--request-id ID]` conserva su ejecución
síncrona y selección explícita. No se añade timeout que mate publicaciones: la ruta
legacy no acredita recuperación segura de toda mutación interrumpida. Una llamada
Google lenta puede retrasar otras publicaciones, pero no ocupa el worker de control.
Los límites 50/60 s y presupuesto interno de 25 s del polling no cambian.

El deploy oficial incluye ambos workers en build, recreación, restart y validación
de los diez procesos de aplicación. Para la primera actualización, el script viejo
ya cargado antes del pull no conoce estos servicios: el operador debe ejecutar el
script oficial de la revisión aprobada e inmutable, verificado contra el objeto Git,
con APP_DIR y --expected-base/--expected-sha. Ese script conserva el lock, verifica
main limpio y hace el único pull autorizado. No hacer pull manual previo ni editar
el script instalado. Las siguientes actualizaciones usan el script ya integrado.

Validación operativa después del deploy y reconfiguración: workers exclusivos
activos, diez procesos con revisión vigente, expiraciones de Beat correctas, poll
reciente mientras hay publicación pendiente; no confundir entrega al broker con
publicación Google ni respuesta Telegram. Los tests locales usan proveedores falsos
sin mensajes ni cambios Google; latencia y memoria reales se miden en producción.


## Audio con varias identidades verificadas

El formato ampliado y la activación están en
[agente Telegram](../../docs/horarios/telegram-agent.md#varias-personas-verificadas-2026-11-23).
Aplicar migración 0004 y validar el archivo privado antes de habilitar miembros.
No cambiar SSH, LaunchAgent, wrapper, queues ni presupuesto. Compose añade el mismo
directorio privado RO a publicación para revalidar revocaciones, sin otro servicio: el RPC selecciona
el audio más antiguo entre vinculaciones vigentes y devuelve el mismo contrato
`id/lease/sha256/pcm`. El resultado se vincula a su identidad ERP recibida; una corrección
de otra persona no invalida su audio. Retirar/revincular a esa persona invalida el lease,
y sus resultados tardíos no crean vistas ni consumen otra reserva. El contador único del
bot sigue siendo acumulativo, 500 centavos para todas las personas juntas.
