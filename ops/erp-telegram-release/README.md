# Publicación ERP autorizada desde Telegram

Único consumidor Telegram: `erp-point-decision.service`. El instalador agrega un
hook `d:` antes del parser original p/t/g, solamente si SHA256 del consumidor
coincide con el baseline inspeccionado. No instala un segundo `getUpdates`.

El timer nuevo consulta el ledger existente `/var/lib/erp-autofix/ledger.json`.
Solo propone candidatos `awaiting_approval` / `draft_complete` con PR y SHA válidos;
excluye `operator-*`, conserva decisiones anteriores y no reenvía el mismo PR/SHA.
La solicitud manual root permite revisar nuevamente un candidato expresamente.
Ninguna ruta, comando o argumento de ejecución se toma del diagnóstico generado.

## Contrato de aprobación

- Botón **Aprobar y desplegar**: repo fijo, PR, SHA, base y producción iguales,
  nonce aleatorio, expiración de una hora, chat privado autorizado y message_id.
- HMAC incluye todos esos datos de revisión. El actor Telegram debe coincidir con
  el chat privado configurado. Una pulsación repetida no ejecuta dos publicaciones.
- **Rechazar / revocar** funciona antes de que el worker reclame la aprobación.
  Después del claim no se puede prometer cancelación de una operación remota.
- Ledger root, directorio0700, archivos0600, esquema cerrado, flock y escritura
  atómica con fsync. `erp-codex` y procesos generadores no tienen permiso de escritura.
- La GitHub App registra la decisión como comentario de bot, con SHA/nonce/fecha;
  no suplanta una revisión humana GitHub ni publica identificadores privados Telegram.
- Antes de solicitar o publicar, exige protección vigente de main: strict=true,
  enforce_admins=true, check test de GitHub Actions (app15368), sin bypass. La
  credencial debe poder LEER esa política; un403 bloquea y no se interpreta como
  permiso. No modifica protecciones ni credenciales automáticamente.
- El publicador verifica CI paginado completo y `test` exitoso de GitHub Actions,
  repositorio/base/head vigentes y base ancestro del candidato. Exige merge normal
  con SHA esperado; comprueba padres `[base, head]` y árbol idéntico al candidato.
- La protección strict de GitHub rechaza la integración de una base atrasada.
  La API de merge no ofrece CAS sobre base_sha: la comprobación posterior de
  padres/árbol sigue siendo defensiva si un administrador cambia la política
  durante la operación, y bloquea desplegar una revisión distinta.
- Deploy oficial recibe `--expected-sha RELEASE --expected-base PRODUCTION`;
  no se hace git pull por adelantado. El lock oficial se comparte con deploy manual.

`published` significa que el deploy oficial terminó y HEAD de producción coincide
con el release comprobado. No certifica automáticamente la aceptación funcional
particular de cada reparación; esa evidencia sigue perteneciendo a la fábrica.

## Requisitos pendientes de habilitación

Este código no está operativo por estar escrito o probado localmente. Antes de
habilitarlo, Mauricio debe autorizar y un administrador debe configurar:

- `main`: required status checks strict=true, enforce_admins=true y check `test`
  vinculado a GitHub Actions app15368, sin bypass de revisiones.
- La GitHub App existente: permiso **Administration: Read-only** concedido en la
  instalación del repositorio ERP. No se solicita Administration:write.

El publicador importa una instancia aislada del adaptador existente y añade solo
`administration: read` a los permisos pedidos al emitir SU token. No modifica el
archivo del adaptador ni los permisos pedidos por el generador. La ruta de
protección está limitada a GET; nunca configura protecciones desde el worker.
Sin el grant, GitHub rechaza la emisión o lectura y el flujo permanece bloqueado.
Las comprobaciones realizadas con permisos existentes devolvieron REST403 y
GraphQL FORBIDDEN; no se trató esa falta de acceso como protección válida.

## Instalación y operación

1. Integrar mediante PR, checks requeridos y deploy oficial este código revisado.
2. Ejecutar desde el checkout limpio de producción:
   `python3 /opt/pastelerias-erp/ops/erp-telegram-release/install.py`.
3. El instalador respalda el consumidor original por hash, verifica bytes, instala
   módulo/unidades, reinicia el consumidor único y comprueba servicio activo.
4. Habilita `erp-telegram-release.timer` y deshabilita exclusivamente
   `erp-mobile-approval.timer` para no enviar la vieja solicitud ambigua en paralelo.
   No toca timers Point, Doctor, disco, ni archivos de datos del ERP.
5. Verificar timers, logs, ausencia de un segundo getUpdates, botones en el chat
   autorizado, comentario real GitHub y revisión exacta desplegada.

Solicitud manual sin mostrar credenciales:

```sh
CREDENTIALS_DIRECTORY=/etc/erp-mac-ia-watchdog \
  python3 -B /usr/local/libexec/erp-point-decision/erp_telegram_release.py request --pr NUMERO
```

El worker y la solicitud CLI comparten un flock exclusivo no bloqueante; se
procesan decisiones y notificaciones aunque el ledger de descubrimiento falte.
Telegram prohíbe redirecciones y sanea errores para no revelar la URL con token.

El instalador toma el mismo `.worker.lock` no bloqueante durante toda la instalación;
rechaza servicios active/activating/reloading/deactivating antes de mutar archivos.
No intenta detener el timer en su primera instalación si LoadState=not-found.
Comprueba credencial y política live antes de reemplazar archivos o desactivar
el timer anterior; un403 o strict=false conserva el flujo existente.
Valida sintaxis/unidades antes de mutar, admite repetir exactamente
el hook instalado y restaura el consumidor/archivos anteriores y estados de
timers si falla. Conserva archivos nuevos inactivos para diagnóstico; no limpia.

La credencial GitHub App se carga usando el adaptador ya instalado de autofix.
El generador conserva intacta su lista de endpoints autorizados y no recibe
permisos adicionales. El publicador root usa llamadas fijas al repositorio ERP.
No se instalan dependencias, puertos ni servicios pagados.

## Incertidumbre y desactivación

`notifying`, `executing` y `uncertain` requieren conciliación humana ante caída o
respuesta perdida. No se repiten automáticamente la solicitud inicial, el merge ni el deploy.
El outbox reintenta únicamente notificaciones informativas finales pendientes;
una respuesta Telegram perdida puede duplicar el aviso, nunca la publicación. Conservar
ledger, comparar GitHub, SHA de producción y logs del deploy antes de decidir.
No editar un estado para volver a ejecutar a ciegas. Una solicitud vencida se
invalida antes de una nueva solicitud manual. Rechazados no se reabren por timer.

Para detener publicaciones nuevas, detener y deshabilitar únicamente
`erp-telegram-release.timer`, revisar si hay servicio ejecutándose y conservar la
sesión/ledger. Para retirar el hook, comparar el consumidor instalado con el
baseline más hook; restaurar solo el respaldo verificado y reiniciar su servicio.
Volver al timer viejo requiere una decisión explícita; no cambia ni revierte
código ERP ya publicado. Nunca hay rollback de datos ni limpieza automática.

## Pruebas

`python3 -m unittest discover -s scripts/tests -p test_telegram_release.py`

Incluye suplantación, chat/grupo/mensaje/firma, expiración/replay/revocación,
head/base diferentes, CI ausente/fallido/no confiable, paginación, pérdida de
respuesta, caída tras claim, árbol/padres incorrectos, permisos/esquema/symlinks,
discovery/deduplicación y conservación de rutas p/t/g. Estas pruebas usan mocks
para servicios externos: no sustituyen un ciclo real de Telegram/GitHub/VPS.

## Autorización separada de limpieza

El módulo `erp_telegram_cleanup.py` registra decisiones; **no ejecuta limpiezas**,
no publica PR y no tiene worker. Su habilitación exige integrar, desplegar e
instalar esta revisión por el mismo procedimiento aprobado. El instalador admite
el hook anterior verificado y añade `c:` al consumidor único; conserva `d:` y p/t/g.

La solicitud root recibe un JSON local revisado de propuesta `VPS-LIM-AAAAMMDD-NN`
para una única imagen `erp-autofix-deps:<12 hex>` del VPS ERP. Valida esquema cerrado,
ID completo, referencias cero declaradas, límites de tamaño y campos; no recibe
comandos. Solo envía campos operativos seleccionados y condiciones fijas, nunca
el texto arbitrario del JSON. El operador debe revisar todas las condiciones del
JSON y explicar cualquier bloqueo de recuperación antes de solicitar aprobación.

```sh
CREDENTIALS_DIRECTORY=/etc/erp-mac-ia-watchdog \
  python3 -B /usr/local/libexec/erp-point-decision/erp_telegram_cleanup.py request \
  --proposal /RUTA/PROPUESTA.json
python3 -B /usr/local/libexec/erp-point-decision/erp_telegram_cleanup.py status \
  --proposal /RUTA/PROPUESTA.json --nonce NONCE
```

Ledger separado `/var/lib/erp-point-decision/cleanups` con permisos root0700/0600.
HMAC liga namespace, nonce, ID de propuesta, SHA256 de **bytes exactos**, imagen,
chat, mensaje y vencimiento de una hora. Se persiste el message_id antes de añadir
botones. Respuesta inicial perdida deja `notifying`, sin reintento automático; una
solicitud viva bloquea duplicados. Cualquier identidad, firma o mensaje distinto
falla cerrado. Aprobación repetida informa la decisión guardada sin escribir ni renovar su vigencia; rechazo es terminal, revocación
pasa approved a rejected; una nueva solicitud exige nonce nuevo.

`status` vuelve a calcular el hash del archivo y lee la decisión bajo lock. Sale0
con `approved=true` solo si coincide y sigue vigente; sale1 si no está aprobada o
venció. Un cambio de bytes invalida la consulta. Esta salida es una fotografía de
la decisión, **no un claim atómico ni permiso para omitir recuperación**. Antes de
cualquier retiro manual, el operador debe reconsultar justo antes de actuar,
comprobar que nadie usa la imagen, preservar todos los respaldos y probar su copia
NAS y restauración aislada al mismo ID. Si hay demora, cambio, rechazo o falta de
prueba, detenerse. No hay ejecutor automático que cierre una carrera con una
revocación simultánea; si se necesita, exige un diseño/autorización independientes.

La primera confirmación declara aprobación/rechazo registrado. Las pulsaciones
repetidas autenticadas muestran aprobación registrada, rechazo o vencimiento sin
parecer errores. Una solicitud `completed` informa limpieza terminada incluso
después de vencer; nunca vuelve a autorizar ni permite revocar una ejecución pasada. La solicitud declara recuperación pendiente, conserva el respaldo más
reciente y posteriores y excluye prune, volúmenes, datos y otros servicios.
Pruebas: `python3 -m unittest discover -s scripts/tests -p 'test_telegram*.py'`.

### Manifiesto de cachés y registros rotados

Además del formato histórico de una imagen, `request` acepta un único candidato
`cache_log_manifest`. No admite comandos, borrados automáticos ni rutas arbitrarias.
Su `id` es `sha256:` más el hash de JSON canónico (sort_keys=True, separadores
`,` y `:`) de `{files, build_cache}`. Cada archivo contiene `path`, `bytes` y
`sha256`; cada registro de construcción contiene `id` y `bytes`. Se limita a
256 archivos y 128 registros, dentro de una propuesta de 128 KiB.

Rutas permitidas: índices binarios APT, descargas `.deb`, logs rotados `.1` o
`.N.gz` en `/var/log` (hasta un subdirectorio) y journals archivados con `@`.
Se rechazan rutas con traversal, respaldos, duplicados y logs activos. Esto no
prueba ausencia de descriptores abiertos ni symlinks: el operador debe verificar
ambas condiciones, hashes y uso justo antes de actuar, sin ampliar el inventario.

`recovery` exige `archive` en
`/share/ERP_BACKUPS/vps-cleanup/ID_PROPUESTA/files-recovery.tar.gz`, su `sha256`
y `files_verified` igual al número de archivos. Es una declaración del operador,
no una comprobación remota del bot: primero completar copia, hash recibido en NAS,
lectura de regreso y extracción aislada con todos los hashes. La caché BuildKit
se declara regenerable, nunca restaurada por esa prueba; comprobar que no se usa
ni comparte y que no hay construcciones concurrentes antes de cualquier retiro.

Se conserva la forma del ledger y HMAC anteriores: el campo histórico `image_id`
contiene el digest de alcance para manifiestos. No se reinterpreta como imagen
ni se ejecuta; el hash de propuesta liga también la evidencia de recuperación.
Las solicitudes de ambos formatos pasan por el mismo método `request`: solo
termina con éxito si Telegram confirma los dos botones y el mensaje exacto.
Nunca sustituir errores por mensajes manuales sin botones ni marcar `notifying`
como aprobado. La aprobación de instalación del módulo es distinta de la limpieza.

### Registrar limpieza terminada

Después de ejecutar la limpieza autorizada y verificar candidatos retirados,
recuperación y recursos preservados, el operador conserva un JSON de evidencia:
`nonce`, `proposal_sha256`, `scope_id` (imagen o manifiesto), `verified: true` y
`result` (resumen y referencias a las comprobaciones, hasta 4000 caracteres).
Este es un registro explícito del operador: el módulo no ejecuta limpieza ni
infiere verificación a partir de aprobar o de un archivo llamado “resultado”.

```sh
python3 -B /usr/local/libexec/erp-point-decision/erp_telegram_cleanup.py complete \
  --proposal /RUTA/PROPUESTA.json --nonce NONCE --evidence /RUTA/RESULTADO-VERIFICADO.json
```

Solo root puede usar la CLI. El comando exige estado aprobado y coincidencia de
propuesta, nonce y alcance. Conserva decisión y vencimiento, registra fecha de
cierre y SHA-256 del archivo de evidencia bajo el mismo bloqueo; el operador debe
conservar ese archivo. Se admite registrar un resultado histórico tras vencer la
solicitud; eso no autoriza ejecutar nada vencido. Un rechazo impide el cierre.
`completed` es terminal: `status` devuelve `approved=false`, bloquea una nueva
solicitud con el mismo ID y cualquier pulsación autenticada indica que la limpieza
ya terminó, sin escribir decisiones. Identidad/firma incorrecta sigue fallando
cerrado antes de mostrar el estado. No se migran registros antiguos automáticamente.
Tras el cierre, retirar los botones del mensaje y enviar el resumen operativo por
el transporte existente. El registro de cierre permite responder a callbacks ya
en vuelo sin presentar un error o repetir acciones.
