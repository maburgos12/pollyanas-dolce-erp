# Producción y diagnóstico de migraciones

Leer [AGENTS raíz](../../AGENTS.md). Este procedimiento no autoriza despliegues,
correcciones de datos, configuración, reinicios ni cambios operativos fuera del
alcance aprobado. Las tareas de documentación/instrucciones/configuración Codex
se consumen en Git/Codex y no requieren desplegar ni reiniciar la aplicación.

## Entorno de referencia

- ERP: `https://erp.pollyanasdolce.com`, VPS `68.183.165.47`, usuario `root`,
  SSH `~/.ssh/agente_dg_ops`, checkout `/opt/pastelerias-erp`, rama `main`.
- Repo: `github.com/maburgos12/pollyanas-dolce-erp` (privado).
- Compose: `docker compose -f /opt/pastelerias-erp/docker-compose.yml`.
- Otros sistemas en ese VPS: Maya `/opt/pollyana-omnichannel`, puerto 8003,
  `api.pollyanasdolce.com`; Ad agent `/opt/ad-agent`, puerto 8004,
  `ads.pollyanasdolce.com`. No intervenirlos por un cambio del ERP.
- Respaldo documentado: `/opt/pastelerias-erp/scripts/backup_db.sh`, destino
  `/opt/backups/erp/`, cron diario 2am y retención de 7 días. Verificar disponibilidad
  real antes de una recuperación; esta referencia no prueba un respaldo actual.

## Despliegue autorizado

Tras revisión, PR y CI vigente aprobado, mergear sólo con autorización aplicable.
Ejecutar en el VPS, sin `git pull` manual previo ni copias SCP:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

El script compara su propio HEAD anterior/posterior. La implementación vigente
es la fuente de cómo reconstruye imágenes por dependencias/Dockerfile, recrea por
Compose, reinicia procesos por Python y usa HUP en los demás casos. Gunicorn usa
`--preload`: HUP no sustituye un reinicio cuando cambió código Python.
No hacer pull previo ni sustituir esa selección por reinicios manuales.

Verificar la revisión realmente servida y el flujo afectado; un `git log` del VPS
sólo prueba filesystem. Si falla el deploy o persiste comportamiento anterior,
diagnosticar servicios, versión en ejecución y respuestas, y presentar recuperación
dentro de la autorización vigente. No forzar reinicios o fake por intuición.
Para interfaz/PWA verificar también estáticos, `collectstatic` y caché según
[validación UI](ui.md). No declarar listo antes de comprobar el consumidor real.

## Migraciones: evidencia antes de actuar

Un `AttributeError` no demuestra que falte una migración. Cotejar código cargado,
versión del proceso, historial de migraciones y esquema real mediante lecturas
acotadas. Una columna duplicada tampoco autoriza marcar migraciones como aplicadas.

**Prohibido usar `migrate --fake` o `--fake-initial` sin autorización explícita de
Mauricio para la migración concreta**, incluso si el esquema ya contiene el campo.
Presentar discrepancia, evidencia y efecto propuesto antes de pedir esa autorización.
No borrar migraciones existentes ni modificar las ya aplicadas. Antes de deploy
verificar `migrate --check`; para cambios de modelos inspeccionar también
`showmigrations` y compatibilidad del esquema productivo en sólo lectura.

No reset/drop de bases, cambiar `.env`, puertos Compose, datos protegidos ni
configuración operativa sin autorización explícita. La limpieza obligatoria de
Docker es local; nunca autoriza retirar servicios/volúmenes de este VPS.

## Cambios autorizados de `.env`: conservar identidad antes del reemplazo

Incidente 26-nov-2026, 21:04:38–21:05:09 UTC: una actualización de avisos de
clima creó un temporal como root, copió el modo 600 y ejecutó `os.replace`, pero
omitió UID/GID. Web y notificaciones (UID 1000) perdieron acceso hasta restaurar
el propietario por referencia al respaldo. No fue un fallo de Django ni se
corrige abriendo lectura a grupo/otros. La evidencia privada del incidente está
en la tarea `doctor-env-permission-20261127`; nunca adjuntar valores del archivo.

Para futuras actualizaciones autorizadas usar `scripts/replace_private_env.py`,
no escritores ad hoc `write_text`, `mkstemp` + `replace`, `sed -i` o copias que
pierdan metadatos. Antes, comprobar UID/GID efectivos del consumidor y modo 600,
conservar un respaldo privado verificado y preparar el contenido completo fuera
del repositorio. El helper recibe contenido por stdin y el SHA256 de la versión
que se revisó, mantiene propietario y modo ANTES del rename, rechaza enlaces y
metadatos extendidos no cubiertos, sincroniza archivo/directorio y toma el mismo
`/run/lock/erp-deploy.lock` que el despliegue. No cambia servicios ni corrige
silenciosamente un propietario o permisos que ya sean incorrectos.

Invocación desde una sesión autorizada, con rutas/identidades verificadas:

```bash
python3 scripts/replace_private_env.py --file "$ENV_FILE" \
  --expected-sha256 "$EXPECTED_ENV_SHA256" --app-uid "$APP_UID" \
  --app-gid "$APP_GID" < "$PRIVATE_REPLACEMENT"
```

No incluir valores en argumentos, logs o historial. No reutilizar un SHA de otra
versión. Todos los escritores deben respetar el bloqueo; no protege contra una
escritura externa que lo ignore. Si ocurre un fallo después del rename, comprobar
metadatos y hash antes de reintentar: una salida fallida no prueba que no cambió.
Antes de cualquier restart/deploy comprobar lectura con la identidad real del
consumidor (`test -r /app/.env`, sin imprimirlo); ante fallo detenerse antes de
checkout/reinicio y diagnosticar. Con consumidores detenidos por warm, realizar
esta comprobación en el preflight anterior al warm y conservar su recibo.
Después verificar consumidores, metadatos y ausencia de nuevos PermissionError.
No ocultar la excepción del cargador, ni aplicar chmod/chown recursivo.
