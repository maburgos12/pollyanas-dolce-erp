# Piloto NAS para evidencias históricas

Alcance aprobado: un mes cerrado de logística. Lote candidato observado el 6 de octubre de 2026: mayo, 96 bitácoras cerradas, 202 fotos, 511458820 bytes. No se archivan otros módulos, PostgreSQL, tickets compartidos ni datos activos. No hay scheduler ni política de borrado automático.

## Flujo y permisos

`Pantalla / FileField → original VPS → si falta: índice privado → NAS → validar bytes y SHA → respuesta`.

El índice y los registros siguen en VPS. Los nombres y relaciones de base de datos permanecen iguales. Lecturas web requieren sesión activa y permisos actuales de bitácoras/capturas/unidades, permiso Django administrativo o pertenencia al repartidor. No se concede acceso a otros módulos. El contenido archivado usa `private, no-store`; la PWA conserva su fallback offline exclusivamente para medios públicos activos.

Con `MEDIA_ARCHIVE_ROOT` vacío, todo sigue usando el almacenamiento local. El índice privado es `storage/media_archive_index`; el NAS solo se lee. Los OCR que usan `.open()` reciben los mismos bytes. `.path` sigue refiriendo al original local; no hay consumidores `.path` en el piloto.

## Configuración que requiere aprobación

Crear cuenta técnica NAS de lectura exclusivamente sobre la carpeta de fotos archivadas; sin administración y sin acceso a SQL/backups. Preferir recurso compartido dedicado `ERP_MEDIA_ARCHIVE`, no dar al ERP lectura general de `ERP_BACKUPS`. Publicar el lote mediante copia operativa al destino elegido; no alterar el job HBS actual de backups sin revisar su contrato.

Montar ese recurso en el host VPS por WireGuard (`10.77.216.2`), CIFS con `ro,nosuid,nodev,noexec,soft` y credenciales en archivo root `0600`, fuera del repositorio. Verificar el comportamiento de timeout con el NAS desconectado; `soft` no garantiza un SLA por sí solo. Dentro del contenedor montar el mismo recurso en lectura; corroborar `/proc/self/mountinfo` desde cada consumidor (web/worker). No aceptar un directorio local como NAS.

Configuración propuesta, todavía no aplicada:

```text
MEDIA_ARCHIVE_ROOT=/app/storage/nas_media
MEDIA_ARCHIVE_MOUNT_SOURCE=//10.77.216.2/ERP_MEDIA_ARCHIVE
```

Esto requiere revisar el bind mount Docker, el acceso del usuario de servicio y las variables de producción. Nunca registrar contraseña ni publicarla en PR/logs. Configurar la dependencia del montaje para evitar arranque contra una carpeta local vacía. NAS apagado: originales activos siguen locales; históricos devuelven 503 recuperable. No mover base de datos al NAS.

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

```bash
python manage.py archive_bitacora_media --month 2026-05 --mode restore --plan /app/storage/archive_plans/2026-05.json --actor operador
```

Restaura originales ausentes desde bytes verificados sin sobrescribir archivos. Escribe un temporal, verifica integridad y publica el nombre final atómicamente; una escritura fallida conserva el fallback NAS. Verificar SHA y visualización local antes de desactivar archivo NAS; desactivar primero rompería los históricos retirados. Rollback de código/configuración únicamente después de restaurar todo el lote. Conservar copias independientes y proteger índice, plan y auditoría junto al respaldo; RAID no sustituye un backup.

## Validación y límites

Pruebas PostgreSQL: referencias compartidas, cambios de original/estado, confirmación, recepción, duplicados, recuperación, sesiones/roles, índice corrupto, archivos ausentes, traversal/symlinks, lectura FileField y montaje remoto. Check JS: `node scripts/test_media_archive_cache.cjs`.

No se ha configurado todavía acceso permanente NAS ni recibido fotos reales del piloto. Las pruebas locales simulan NAS con directorio temporal y no prueban disponibilidad de red. No se retira ningún original en producción antes de completar conexión, recepción y validación autenticada. La activación requiere aprobación de permisos y configuración de producción según AGENTS.md.
