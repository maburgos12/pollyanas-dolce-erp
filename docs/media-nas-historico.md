# Piloto NAS para evidencias históricas

Alcance aprobado: un mes cerrado de logística, con copia, verificación y retiro automatizados. Lote candidato observado el 6 de octubre de 2026: mayo, 96 bitácoras cerradas, 202 fotos, 511458820 bytes. No se archivan otros módulos, PostgreSQL, tickets compartidos ni datos activos. El timer preparado está limitado a ese mes; no habilita otros meses automáticamente.

## Flujo y permisos

`Pantalla / FileField → original VPS → si falta: índice privado → NAS → validar bytes y SHA → respuesta`.

El índice y los registros siguen en VPS. Los nombres y relaciones de base de datos permanecen iguales. Lecturas web requieren sesión activa y permisos actuales de bitácoras/capturas/unidades, permiso Django administrativo o pertenencia al repartidor. No se concede acceso a otros módulos. El contenido archivado usa `private, no-store`; la PWA conserva su fallback offline exclusivamente para medios públicos activos.

Con `MEDIA_ARCHIVE_ROOT` vacío, todo sigue usando el almacenamiento local. El índice privado es `storage/media_archive_index`; el NAS solo se lee. Los OCR que usan `.open()` reciben los mismos bytes. `.path` sigue refiriendo al original local; no hay consumidores `.path` en el piloto.

## Configuración autorizada, pendiente de validación

Crear cuenta técnica NAS de lectura exclusivamente sobre la carpeta de fotos archivadas; sin administración y sin acceso a SQL/backups. Preferir recurso compartido dedicado `ERP_MEDIA_ARCHIVE`, no dar al ERP lectura general de `ERP_BACKUPS`. Publicar el lote mediante copia operativa al destino elegido; no alterar el job HBS actual de backups sin revisar su contrato.

Montar ese recurso en el host VPS por WireGuard (`10.77.216.2`), CIFS con `ro,nosuid,nodev,noexec,soft` y credenciales en archivo root `0600`, fuera del repositorio. Verificar el comportamiento de timeout con el NAS desconectado; `soft` no garantiza un SLA por sí solo. Dentro del contenedor montar el mismo recurso en lectura; corroborar `/proc/self/mountinfo` desde cada consumidor (web/worker). No aceptar un directorio local como NAS.

Configuración de producción prevista, todavía no aplicada:

```text
MEDIA_ARCHIVE_ROOT=/app/storage/nas/media
MEDIA_ARCHIVE_MOUNT_SOURCE=//10.77.216.2/ERP_MEDIA_ARCHIVE
ERP_BIND_PROPAGATION=rslave
```

Esto requiere revisar el bind mount Docker, el acceso del usuario de servicio y las variables de producción. Nunca registrar contraseña ni publicarla en PR/logs. Configurar la dependencia del montaje para evitar arranque contra una carpeta local vacía. NAS apagado: originales activos siguen locales; históricos devuelven 503 recuperable. No mover base de datos al NAS.

`web` y `worker` mantienen `rprivate` por defecto para Docker Desktop. La producción puede seleccionar `rslave`: los montajes del host llegan al contenedor, sin propagar montajes del contenedor hacia el host. El host debe tener un montaje padre compartido. El recurso CIFS conserva `ro`; comprobar las opciones desde host, web y worker tras la recreación oficial. Ver [propagación de bind mounts en Docker](https://docs.docker.com/engine/storage/bind-mounts/#configure-bind-propagation).

## Automatización del piloto

El controlador `sync_bitacora_media_archive` reutiliza las operaciones anteriores. Conserva el plan y su SHA, toma un bloqueo estable, publica los originales pendientes en staging y responde `WAIT_NAS` si no existe recepción íntegra. Verifica todas las fotos mediante HTTP autenticado de Gunicorn con `?archive=1` y `private, no-store` antes de iniciar retiros. Crea una sesión temporal de un usuario existente y la elimina; no cambia permisos ni `last_login`.

Una interrupción puede dejar algunos originales retirados. El siguiente intento usa el mismo plan y publica solo los restantes; vuelve a validar todas las respuestas HTTP. El staging se limpia únicamente cuando cada original está ausente y el índice y bytes NAS coinciden. `COMPLETE` exige índices íntegros incluso después de acabar el lote.

```bash
python manage.py sync_bitacora_media_archive --month 2026-05 --plan-dir /app/storage/media_archive_jobs --stage-root /app/storage/media_archive_export --verification-user admin --verification-origin http://127.0.0.1:8000
```

El comando HTTP solo admite loopback, desactiva proxies y redirecciones y usa el Host esperado del ERP. El timer en `infra/systemd/` intenta el piloto cada hora y después de reiniciar. Su servicio exige montaje CIFS y Docker; el ERP puede arrancar aunque el NAS falle. El proceso Python tiene un límite interno de 1700 segundos y systemd de 30 minutos. Las unidades permanecen sin instalar ni activar hasta verificar cuenta, montaje, HBS, recuperación de índice/plan/auditoría y pantalla real.

El NAS recibe fotos mediante un job HBS separado y un módulo rsync privado de solo lectura; no se cambian el job ni export SQL existente. La recepción se prueba leyendo bytes NAS y comparando SHA: el éxito de HBS o el staging por sí solos no bastan. Respaldar índice, plan y auditoría en un destino recuperable antes de habilitar retiros. Una sola unidad NAS no constituye una copia independiente de las fotos.

## Operación manual controlada

En el contenedor con PostgreSQL válido, utilizar el mismo plan privado en todas las etapas. Los comandos no actualizan tablas. El mes debe tener al menos 90 días completos de antigüedad; esto es una barrera conservadora del piloto, no una política de retención aprobada.

```bash
python manage.py archive_bitacora_media --month 2026-05 --mode plan --plan /app/storage/archive_plans/2026-05.json
python manage.py archive_bitacora_media --month 2026-05 --mode stage --plan /app/storage/archive_plans/2026-05.json --stage-root /app/storage/archive_staging/2026-05 --actor operador
```

Revisar archivos, exclusiones y SHA-256 del plan. Copiar staging al NAS preservando `bitacora/...`; staging NO equivale a recepción. Conservar originales y staging durante todo el piloto.

```bash
python manage.py archive_bitacora_media --month 2026-05 --mode verify --plan /app/storage/archive_plans/2026-05.json --actor operador
```

El comando exige montaje remoto identificado, lee los bytes completos y publica índice privado. Abrir desde una sesión autorizada cada `/media/<nombre>?archive=1`: esa opción fuerza lectura NAS aunque el original exista. Corroborar byte count y SHA de la respuesta autenticada contra el plan; verificar pantalla real y denegación a usuarios ajenos. Ningún retiro si una sola evidencia falla. Registrar esta validación junto al plan antes de ejecutar:

```bash
python manage.py archive_bitacora_media --month 2026-05 --mode retire --plan /app/storage/archive_plans/2026-05.json --confirm-plan SHA256_EXACTO_DEL_PLAN --actor operador
```

Antes de cada retiro se verifican nuevamente referencias, estado cerrado, original y copia NAS. Reintentos conservan la misma identidad y no duplican registros. Operación por archivo: un error detiene el resto y puede dejar un lote parcialmente archivado, recuperable desde el índice/auditoría. Auditar antes/después espacio real VPS, fotos servidas y archivos excluidos. No borrar staging ni respaldo durante la revisión del piloto.

## Recuperación

Para un plan creado por el controlador automático, usar `--plan /app/storage/media_archive_jobs/2026-05.plan.json` en el comando de recuperación; no crear otro plan ni cambiar su SHA. El ejemplo siguiente usa la ubicación del flujo manual anterior.

```bash
python manage.py archive_bitacora_media --month 2026-05 --mode restore --plan /app/storage/archive_plans/2026-05.json --actor operador
```

Restaura originales ausentes desde bytes verificados sin sobrescribir archivos. Escribe un temporal, verifica integridad y publica el nombre final atómicamente; una escritura fallida conserva el fallback NAS. Verificar SHA y visualización local antes de desactivar archivo NAS; desactivar primero rompería los históricos retirados. Rollback de código/configuración únicamente después de restaurar todo el lote. Conservar copias independientes y proteger índice, plan y auditoría junto al respaldo; RAID no sustituye un backup.

## Validación y límites

Pruebas PostgreSQL: referencias compartidas, cambios de original/estado, confirmación, recepción, duplicados, recuperación, sesiones/roles, índice corrupto, archivos ausentes, traversal/symlinks, lectura FileField y montaje remoto. Check JS: `node scripts/test_media_archive_cache.cjs`.

Carpeta `ERP_MEDIA_ARCHIVE` y cuenta técnica creadas. El 6 de octubre de 2026 se verificó el montaje real CIFS 3.1.1 cifrado y de solo lectura por WireGuard, con UID/GID 1000; la cuenta tiene acceso denegado a `ERP_BACKUPS`. El módulo rsync `erp_media_archive` exporta únicamente staging privado, en lectura y dentro de chroot, para la misma identidad técnica existente. La conexión HBS de fotos es independiente de la conexión y del trabajo SQL existentes. No se han recibido todavía fotos reales del piloto ni habilitado el timer. Las pruebas locales simulan NAS con directorio temporal y no prueban disponibilidad de red. No se retira ningún original en producción antes de completar recepción, recuperación y validación autenticada. Mauricio autorizó la cuenta restringida, montaje privado, automatización y despliegue del piloto; esa autorización no sustituye las verificaciones.
