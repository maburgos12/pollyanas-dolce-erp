# Alertas de respaldos y disco del VPS

El timer `erp-backup-alerts.timer` revisa cada cinco minutos, directamente en el
VPS, tres preguntas: ¿se puede acceder al NAS por SSH?, ¿la recuperación sigue
verificada?, ¿el disco raíz llegó al75%? No ejecuta limpieza ni restaura datos.
La limpieza conserva sus verificaciones y siempre el respaldo local más reciente.

Canal: `PollyanaERPAlertasBot`, chat privado de Mauricio ya configurado en el VPS.
Se reutiliza `/etc/erp-mac-ia-watchdog/telegram.json` con `LoadCredential`; no
copiar tokens al repositorio ni imprimir errores HTTP completos con la URL.
El transporte existente es `/usr/local/libexec/erp-mac-ia-watchdog/vigilar_revision.py`.
El nombre del helper no implica dependencia de Mac-IA: ambos corren en el VPS.

## Qué hacer con cada aviso

- **NAS:** verificar `ssh qnap-nasdolce-archive` desde el VPS, WireGuard y lectura
  de `/share/ERP_BACKUPS`. No desactivar verificación de identidad SSH. Conservar
  todos los originales hasta restablecer acceso y verificar recuperación.
- **Recuperación:** leer `/var/lib/erp-backup-restore/health-latest.json` y
  `journalctl -u erp-backup-health.service`. Incluye informes ausentes, errores,
  antigüedad mayor a36horas y fallos/timeouts de systemd aunque no se haya podido
  escribir un informe nuevo. No inventar recibos ni ejecutar borrados manuales.
- **Disco75%:** revisar `df -h /` y el informe de recuperación. Al85% se escala
  inmediatamente aURGENTE. Usar únicamente la rotación verificada existente;
  otros candidatos requieren revisar propiedad, uso y datos únicos.

Responsable: Mauricio/Codex. Si no se resuelve con estas comprobaciones, conservar
los datos y diagnosticar la causa antes de autorizar cambios adicionales.

### NAS bloqueado y respaldo local pendiente

Un proceso en estado `D` dentro de CIFS puede conservar `.backup.flock` incluso
después de que systemd marque el servicio como fallido. Comprobar PID, espera del
kernel y propietario de ambos locks antes de reintentar. No borrar los archivos
de lock: eso crea otro inode y permite dos operaciones concurrentes.

La verificación CIFS del recuperador y del rotador se ejecuta en un proceso lector
aislado, sin heredar el candado del respaldo. Al exceder el plazo se conserva el
conjunto y el padre libera su candado; el candado separado del lector evita
acumular verificadores si el kernel aún no puede terminarlo. Un timeout no prueba
recuperación ni repara la conexión: conservar el PID y revisar CIFS/WireGuard.
El lector admite hasta una hora por punto; la comprobación real de todos los
medios puede superar 20 minutos. La rotación aplica además su presupuesto menor.

El rotador tiene un presupuesto total de 34 minutos, anterior al límite de
35 minutos del cron existente. Cada lector recibe como máximo el tiempo restante;
si se agota antes del retiro, conserva el conjunto. Una señal de terminación al
padre también cancela el grupo lector, sin esperar indefinidamente al kernel.
Esto no garantiza terminar la rotación: una comprobación completa que no quepa
en esa ventana requiere diagnóstico, nunca omitir hashes para acelerar el retiro.

Después de restablecer la conexión, comprobar el último manifiesto completo y
si el respaldo diario omitió su ejecución por el candado. Un respaldo compensatorio
debe conservar los anteriores y usar el script oficial, sin invocar manualmente
la rotación. Validar recuperación por separado antes de cualquier retiro aprobado.
Registrar y restablecer cualquier pausa temporal del timer; no silenciar los
monitores de disco ni inventarios.

La unidad programada verifica la recuperación con hashes en el NAS y SMB directo
cifrado, sin depender del montaje del kernel. Requiere el entorno instalado
`/opt/erp-backup-smb-direct/bin/python3` y la clave restringida del verificador.
Los recibos nuevos se guardan en
`/var/lib/erp-backup-restore/verification-receipts`, un directorio privado separado
de `receipts`. El verificador puede leer recibos canónicos válidos, pero no los
modifica. Solo con `--retire-verified-local` y después de verificar todos los puntos
locales completos, la unidad llama al rotador por cada punto anterior al más
reciente. El rotador vuelve a cotejar el respaldo en el NAS y protege al último
bajo candado. Solo retira archivos y enlaces de exportación en el VPS. Si falla
la verificación, no inicia ningún retiro; si falla un retiro, detiene los
siguientes y registra el punto y los retiros completados en `health-latest.json`.
No se retiran respaldos del NAS ni se hace mantenimiento Docker. El cron antiguo
mantiene su alcance y usa el mismo candado; no se le publican recibos nuevos.
Para una comprobación sin retiro, ejecutar el verificador con
`--verify-only --nas-side --smb-direct`, sin `--retire-verified-local`.
El límite total de la unidad sigue siendo ocho horas: su ejecución programada
a las 08:00 de Mazatlán deja margen antes del respaldo de las 19:00 (02:00 UTC).
Antes de un arranque manual, comprobar ese margen; la restauración remota y los
puntos pendientes también consumen tiempo mientras se conserva el candado local.

Antes de sustituir la entrada externa del rotador, el instalador conserva su
original conocido con el SHA-256 en el nombre y verifica sus bytes. Una reversión
del checkout debe restaurar también esa entrada: el wrapper nuevo no es compatible
con una revisión que aún no contenga `scripts/rotate_verified_backups.py`.
Verificar el hash del original antes de restaurarlo y conservar la evidencia;
no reconstruirlo desde logs ni reemplazar una versión desconocida. La unidad
anterior tampoco acepta `--verify-only`: mantener su timer pausado, con responsable
y fecha de revisión, hasta validar una versión compatible. La reversión no autoriza
rotación, reinicios ajenos ni retirar respaldos.

El preparador de limpieza consulta las identidades de archivos en `/proc` sin
seguir enlaces a montajes externos. Si no puede determinar el dispositivo, reserva
el inode conservadoramente y excluye el candidato. Los errores del sistema guardan
`errno`, ruta y mensaje en su estado privado para distinguir acceso, montaje y
capacidad, sin publicar esos detalles en Telegram.

## Entrega y repetición

Se avisa al aparecer o cambiar una condición y cada seis horas si persiste.
Variaciones de75% a76% no generan mensajes adicionales; el salto a85% sí.
Al desaparecer las condiciones se envía una recuperación. Solo se registra un
envío aceptado por Telegram para el chat esperado; fallos de envío se reintentan
en el siguiente ciclo. Una respuesta perdida puede duplicar un aviso, pero no
silenciarlo. La API confirma aceptación, no lectura en el teléfono.

Estado privado: `/var/lib/erp-backup-alerts/delivery.json`; trazas JSON con run_id
en `journalctl -u erp-backup-alerts.service`. Ante una caída del VPS o de su salida
a Internet no puede entregar mensajes; se necesitaría un monitor externo para
cubrir esa situación. No depende del Mac ni de Codex abierto.

## Instalación y pruebas

### Retiro manual de un respaldo verificado

Tras autorización del punto exacto, ejecutar
`/usr/local/sbin/erp-rotate-verified-backups --retire-point backup_AAAAMMDD_HHMMSS`.
Este modo vuelve a comprobar el manifiesto, cargas y medios contra el NAS real
con un límite de una hora por punto. Acepta el recibo privado de restauración
como evidencia para esa nueva comprobación; no lo publica como recibo del cron.
La lectura del NAS no toma el bloqueo diario. Antes de retirar, el comando lo
adquiere y comprueba nuevamente el punto, su recibo, los enlaces y que el
respaldo más reciente local esté completo. Si falla, conserva el punto para
revisión. No cambia el cron ni el modo `--verify-only`.

Ejecutar secuencialmente y comparar `df -B1 /` antes y después de cada punto.

### Comprobación con hashing en el NAS

El comando restringido `verify-local PUNTO SHA256 GENERACION` verifica en el NAS
el manifiesto, los cuatro archivos del punto y todos los tamaños y hashes de sus
medios, sin publicar un recibo de restauración. El VPS valida la respuesta con
la clave SSH existente, contra el punto, manifiesto, generación y conteo del
recibo privado de una restauración real. Por SMB comprueba el mismo manifiesto
antes y después y abre cada carga en el montaje real de solo lectura.

Primero actualizar el script restringido del NAS sin ampliar la clave ni cambiar
sus permisos. Después validar **sin retiro** con
`erp-rotate-verified-backups --check-proof PUNTO --nas-side` por cada punto,
o `vps_backup_health.py --verify-only --nas-side` para todo el conjunto.
Solo para un punto autorizado, usar `--retire-point PUNTO --nas-side` después
de la validación. El último punto local, los bloqueos, los recibos y los controles
de enlaces siguen siendo obligatorios. Si el NAS o SMB falla, conservar los
archivos. No existe fallback que permita retirar por falta de una prueba.

Sin `--nas-side` se conserva la comprobación histórica por SMB. Este cambio no
activa el modo nuevo en el cron ni autoriza un retiro masivo.

Si el cliente CIFS del kernel queda en reconexión, `--smb-direct` junto con
`--nas-side` usa el mismo recurso y la credencial privada de la unidad, pero
abre una sesión SMB 3.1.1 cifrada propia. No activa ni cambia el montaje.
Exige el manifiesto por SMB antes y después de comprobar los hashes en el
NAS y abre los cuatro archivos exactos sin seguir puntos de reanálisis.
Si falta el cliente o falla la lectura, no se retira nada.

Tras integrar el cambio, crear **solo para el verificador** el entorno root
`/opt/erp-backup-smb-direct` con `python3 -m venv`. Instalar las dependencias
con ese Python y `pip --isolated install --require-hashes --only-binary=:all:
-r scripts/ops/nas-smb-direct-requirements.txt`. Los hashes corresponden al VPS
Ubuntu 24.04 amd64 con Python 3.12; no instalar en el Python del sistema.
Verificar el entorno en solo lectura con `--check-proof PUNTO --nas-side
--smb-direct` antes de considerar un retiro. El modo independiente está
disponible también en `vps_backup_health.py --verify-only --nas-side --smb-direct`.
No cambiar el cron, desmontar SMB ni instalar dependencias globales para probarlo.

### Monitor de alertas

Después del merge y despliegue Git, instalar las dos unidades de `scripts/ops/`
en `/etc/systemd/system/`, validar con `systemd-analyze verify`, ejecutar
`systemctl daemon-reload` y `systemctl start erp-backup-alerts.service`.
Después comprobar sus logs y activar `systemctl enable --now erp-backup-alerts.timer`.
Las pruebas `--test nas`, `--test verification` y `--test disk` envían avisos
marcados PRUEBA mediante las mismas credenciales de systemd; simulan condiciones,
no rompen NAS/SQL ni cambian umbrales o estado real de deduplicación. Probar cada
causa y conservar los message_id aceptados antes de cerrar la instalación.
Para revertir: desactivar solo este timer; no alterar el timer de recuperación.

## Solicitudes de limpieza con botones (preparador separado)

Autorizado por Mauricio el 16/11/2026: `erp-cleanup-prepare.timer` comprueba cada
cinco minutos si el porcentaje entero de `df` es **mayor que75**. El monitor
original conserva sus avisos a75/85%, sin cambios de horario. El preparador vive
en el VPS; no necesita una Mac encendida ni consume llamadas de IA.

El preparador acepta únicamente archivos de caché APT y registros rotados de la
lista cerrada de `erp_telegram_cleanup.FILE_PATH`. Excluye symlinks, enlaces duros,
archivos abiertos/mapeados y cambios durante la comprobación. Acota cada lote a
256 archivos y1GB; Docker, respaldos, bases, medios y registros activos quedan
fuera del alcance automático. No usa `prune` ni borra candidatos.

Antes de mostrar botones debe copiar al NAS, comprobar el hash allí, leer el
archivo de regreso y recuperar cada miembro en un directorio temporal aislado,
verificando tamaño y hash contra la fuente. Comprueba espacio disponible antes
de preparar y conserva una reserva de5GiB en el VPS. Solo se retira el temporal
creado por esa comprobación; el archivo NAS y los originales se conservan.
Si falla una condición, informa un bloqueo y no presenta una autorización falsa.

El manifiesto y la evidencia quedan en estado privado. La solicitud pasa por
`erp_telegram_cleanup.request`: comprueba destinatario, mensaje y ambos botones
**Aprobar limpieza / Rechazar / revocar**. La pulsación solo registra la decisión.
No hay ejecutor automático de limpieza ni despliegue. El operador debe verificar
la aprobación vigente y las condiciones exactas antes de un retiro autorizado.
Una solicitud vencida no se renueva a ciegas; el mismo alcance no se vuelve a
solicitar automáticamente. Una entrega ambigua requiere conciliación humana.

### Instalación del preparador

Solo después de aprobar el SHA exacto, integrar y desplegar por el flujo oficial.
Desde el checkout de producción limpio, verificar primero que el módulo de
limpieza instalado corresponde al contrato de manifiestos y que existen sus
carpetas privadas. No reinstalar el receptor Telegram ni el publicador para
habilitar este preparador.

```sh
cd /opt/pastelerias-erp
systemd-analyze verify scripts/ops/erp-cleanup-prepare.service scripts/ops/erp-cleanup-prepare.timer
install -m 0644 scripts/ops/erp-cleanup-prepare.service /etc/systemd/system/erp-cleanup-prepare.service
install -m 0644 scripts/ops/erp-cleanup-prepare.timer /etc/systemd/system/erp-cleanup-prepare.timer
systemctl daemon-reload
systemctl start erp-cleanup-prepare.service
journalctl -u erp-cleanup-prepare.service --no-pager -n 30
systemctl enable --now erp-cleanup-prepare.timer
```

La unidad hace el sistema de archivos de producción de solo lectura; solo permite
escribir su estado, manifiestos, ledger y temporales privados. No cambia timers
existentes. Verificar resultado real y message_id antes de declarar instalación
completa; los tests locales no demuestran entrega real. Para desactivar, parar y
deshabilitar exclusivamente `erp-cleanup-prepare.timer`; conservar evidencia y
las alertas originales. Nunca interpretar su aprobación como permiso genérico
para borrar otros archivos.

Pruebas: `python3 -m unittest discover -s scripts/tests -p test_cleanup_prepare.py`
y `python3 -m unittest discover -s scripts/tests -p 'test_telegram*.py'`.

### Elegibilidad global antes de solicitar limpieza (19/11/2026)

Preparación APT/logs y Point, revalidación programada y emisión de botones reutilizan
`vps_backup_alerts.collect`: recuperación ausente, fallida, de más de 36 horas,
unidad fallida/no disponible o NAS inaccesible dejan el lote pendiente. Las alertas
de disco 75/85 % no bloquean esta elegibilidad. La prueba individual del lote sigue
siendo obligatoria; una pulsación no sustituye la comprobación previa al retiro.

`recovery_pending` no envía avisos adicionales, no consume un intento de recuperación
ni el día de autorización y se revisa en el siguiente ciclo normal. Si el lote ya
estaba recuperado, conserva la propuesta congelada y la cola `waiting`: no crea
otro lote. Los demás fallos conservan sus esperas y límites; no se reinician
aprobaciones, rechazos ni entregas ambiguas. Se mantiene la política existente de
revalidación de solicitudes pendientes vencidas, sin renovar decisiones. Este filtro
no cambia el cron de respaldos ni su política de retención y no reactiva decisiones.

Tras desplegar el SHA aprobado, el preparador/Point pueden devolver
`activation_pending` hasta actualizar los módulos copiados del receptor. Ejecutar
el instalador oficial únicamente cuando la publicación haya terminado:
`python3 ops/erp-telegram-release/install.py --release-nonce NONCE`;
actualiza primero `erp_telegram_release.py`, después `erp_telegram_cleanup.py` y
reinicia el receptor compartido por su procedimiento existente. `operational_module`
carga el evaluador desde `scripts/vps_backup_alerts.py` del checkout desplegado;
no se mantiene una segunda copia de sus reglas. Comprobar los imports desde el
receptor instalado y que desaparece `activation_pending`, además de instalar y
validar la unidad de recuperación con `install_backup_health.sh`. Restaurar los
estados previos de los timers si se pausaron durante la instalación; no reiniciar
servicios ERP adicionales ni borrar históricos para validar este cambio.

### Bitácora 2026-11-21: preparación Point y errores de comandos

El aviso `CalledProcessError` no identificaba etapa ni salida del comando.
Una consulta posterior exitosa no explica el fallo original. Se observó además
un cambio de inicio del worker durante el diagnóstico; no se acredita por ello
que ese cambio causara el aviso anterior. Los originales se conservaron.

El preparador registra `last_failure` y hasta tres entradas `failure_history`
en `/var/lib/erp-point-archive/status.json`: etapa, tipo, salida, motivo permitido,
fecha, cursor e intento. No publica stderr ni comandos arbitrarios: conserva sólo
un hash del stderr acotado para correlación, evitando divulgar credenciales.

Los timeouts y errores transitorios reconocidos de conexión, worker no disponible,
consulta PostgreSQL agotada o NAS ocupado se reintentan tras 60 y 300 segundos.
El tercer fallo bloquea la preparación y exige diagnóstico del operador. No se
reinician contadores por una ejecución, despliegue o cambio de día. Los contadores
anteriores se conservan. La espera no garantiza reparación y no autoriza borrar.
Errores desconocidos conservan la espera previa de seis horas; integridad y
permisos no se clasifican como transitorios. Una respuesta incierta de Telegram
no habilita reenvíos rápidos ni duplicación de solicitudes.

El comando de inventario lleva timeout dentro del contenedor además del límite
externo, para que agotar la espera del cliente Docker no deje el lector ejecutando
sin control. No se reinician servicios, modifican datos ni omiten hashes. Para
cerrar un incidente, acreditar inventario, copia y recuperación NAS, solicitud
única con botones y aprobación vigente antes del retiro del lote exacto.


### Archivado histórico Point: calidad conservada y continuidad de lotes

Las incidencias de calidad y otros intentos fallidos de la misma sucursal/fecha
se conservan en la base: no impiden por sí solos conservar una fuente íntegra en
NAS. Siguen excluidas las referencias fallidas al mismo archivo, fuentes recientes,
incompletas, con hashes/filas distintos o consumidores fuera del contrato de
categorías. Copiar no certifica las cifras del reporte ni resuelve sus incidencias.

El catálogo conserva todos los recibos históricos; el límite de cien archivos y
100 MB se aplica al lote propuesto, no al total del catálogo. Cada lote requiere
su aprobación exacta y vigente. Antes de retirar: lectura completa desde NAS,
hash/tamaño/parser equivalentes, revalidación de referencias y lease exclusivo.
No resetear contadores, cursores ni decisiones para avanzar.

Una aprobación de otro lote ya completado no bloquea nuevas preparaciones si su
recibo nativo valida nonce y digest. La misma identidad sigue bloqueada para no
repetir retiros; ejecuciones parciales, inciertas, bloqueadas o en curso conservan
el bloqueo y exigen revisión. No modificar la aprobación histórica.

Entregar cantidad/bytes realmente retirados y espacio disponible antes/después en
el mismo filesystem por separado. La variación concurrente de disco no equivale
al ahorro atribuible a Point; ni el tamaño candidato ni el deploy acreditan retiro.

El retiro Point exige respaldo y receptor de decisiones activos. El timer del
publicador automático puede estar activo o pausado, pero su estado debe conservarse
antes/después; no reactivarlo para pasar una guarda. Estados fallidos, transitorios
o cambios de servicios/respaldos bloquean el retiro bajo el mutex de despliegue.

La página nativa de 100 registros tardó 47.84 s en producción el 22 de noviembre
(3.79 s de arranque y 43.96 s de elegibilidad). Cada petición conserva un límite de
85 s dentro del contenedor y 95 s para la respuesta; cada consulta SQL conserva
su límite de 10 s.
Un timeout no autoriza reiniciar contadores ni omitir comprobaciones de integridad.

Una referencia en `VentaAutoritativaPoint` conserva su ruta lógica y todas sus
filas. El importador manual de categorías usa el mismo `open_source` y lease:
una fuente ausente solicita recuperación y produce `ArchivePending` antes de
escribir ventas; al restaurarse, lee los mismos bytes. Historial de productos y
resúmenes mensuales siguen excluidos de este piloto.

Al llenar el lote, el cursor queda antes del primer candidato aún no preparado;
no avanza al final de una página con archivos pendientes. La vista previa completa
de 100 registros produjo 89 candidatos íntegros en 55.30 s, incluido el parser.
Esto no acredita su recepción en NAS ni autoriza retirarlos.

El Broker reutiliza un proceso del mismo comando durante una operación. Django y
sus checks completos arrancan una vez; cada petición vuelve a ejecutar el handler
nativo con transacción RRO y conexión nueva. No se cachea elegibilidad y ambas
verificaciones bajo lease inmediatamente antes del retiro se conservan.
El proceso tiene límite duro de 900 s, se sustituye entre peticiones antes de
700 s o 480 respuestas, y su cierre ocurre bajo los locks del ejecutor. Peticiones
de 4 KiB, respuestas de 1 MiB y stderr acumulado de 16 KiB son máximos efectivos.
Salida inesperada, EOF o timeout producen HOLD sin reenviar una petición ambigua.
Un cierre no acreditado conserva el resultado del retiro en `retirement_state`,
marca la ejecución global `uncertain` y envía un aviso separado; no repite el lote.

Antes de proponer cien archivos, activar coherentemente productor y validador
instalado mediante el refresh oficial de la revisión publicada, conservando el
receptor y la pausa del publicador. Permanecen 100 MB por lote y 128 KiB por
propuesta, además de aprobación exacta por nonce/digest. Las aprobaciones anteriores
no amplían su alcance. Medir el recorrido real antes de atribuir una mejora de
tiempo o espacio; una base local vacía sólo comprueba la reutilización del arranque.

### Sucesora supervisada de un lote Point bloqueado sin retiros

Un `blocked/preflight_attempt_limit` con tres intentos, `removed=[]`,
`skipped=[]` y sin intención no se reinicia. Un operador puede preparar
`erp_telegram_cleanup.successor_proposal(ledger, nonce_anterior, id_nuevo)`
y guardar su resultado como propuesta privada nueva. El contrato enlaza el
nonce y los SHA de propuesta, ejecución y contador anteriores; conserva el
manifiesto exactamente y rechaza padres parciales, inciertos o ya sucesores.
El padre y sus tres intentos permanecen inmutables. No es una acción periódica.

La solicitud usa `request` normal: exige recuperación actual completa, nueva
aprobación firmada del lote exacto y prevalidación residente. La aprobación
anterior no sirve para la sucesora. Sólo una sucesora `completed`, sin intención,
sin excluidos y con todos los IDs retirados resuelve el bloqueo ancestral para
el productor; un cierre incierto mantiene HOLD. Los demás bloqueos se conservan.
No editar recibos, contadores, decisiones ni cursores para avanzar.


## Acceso NAS de servicio, sin sesión humana por ejecución

La cuenta verificadora conserva su UID/HOME. La identidad nueva es una clave
Ed25519 exclusiva: `/etc/erp-nas-access/id_ed25519`, root0600 y carpeta0700.
La configuración aislada `/etc/erp-nas-access/ssh_config` fija la host key,
identidad única, origen privado y ausencia de autenticación por contraseña,
agente, TTY y forwarding. No hereda claves de los `Host *` del administrador.

El NAS fuerza `nas_restore_daily.py --ssh-command`; sólo admite `status`,
`preflight PUNTO SHA256`, `restore PUNTO SHA256` o
`verify-local PUNTO SHA256 GENERACION`. El estado READY prueba el
acceso, no recuperación. El preflight abre todos los medios antes del SQL; el
recibo completo conserva restauración SQL, medios, hashes y cierre del scratch.
Los comandos arbitrarios y la instalación quedan fuera de esa clave.

La instalación requiere una copia cifrada recuperable y comprobada: el operador
crea la clave con OpenSSH, cifra el respaldo con OpenSSL/PBKDF2 y guarda su
contraseña de recuperación en el Llavero de macOS. El expediente guarda sólo
ubicación, huella pública y hashes. Se comprueba descifrado, hash de la clave y
clave pública antes de publicar `/etc/erp-nas-access/recovery-proof.json` privado.
No pasar secretos por argumentos ni imprimirlos, añadirlos a Git o volcarlos a
logs. La recuperación no depende de que la Mac esté encendida durante cada
verificación; la Mac custodia sólo la copia de emergencia.

Tras aprobación del SHA integrado y antes de habilitar el consumidor nuevo,
publicar el recuperador revisado con `install_backup_health.sh` (archivo NAS0600)
y ejecutar como root `python3 scripts/ops/install_nas_service_access.py SHA_RELEASE`.
El instalador comprueba Git limpio/revisión exacta, fuente NAS idéntica y copia
cifrada validada; preserva las claves existentes, respalda authorized_keys y
agrega exclusivamente su entrada restringida mediante CAS y fsync. Comprueba
acceso por la clave nueva y rechazo de shell. Si falla, HOLD sin reintento ciego.

El alias anterior `qnap-nasdolce-archive` sigue disponible para instalación y para
consumidores aún no migrados, incluidos recepción de archivos de limpieza y
alertas. Esto limita la clave nueva, no todo el acceso existente VPS→NAS. No
revocar la anterior ni cambiar su alcance hasta inventariar esos consumidores y
probar la recuperación de emergencia. No activar ni pausar timers ajenos como
atajo de aceptación.

Si los archivos vuelven a perder lectura, registrar el cambio de metadatos y
verificar el trabajo receptor exacto. No atribuir actor sólo por proximidad de
horarios. No abrir permisos globales ni repetir importaciones SQL largas sin
preflight. La pulsación exacta del lote sigue siendo obligatoria antes de retirar
originales; el acceso por clave no cambia ese límite.
