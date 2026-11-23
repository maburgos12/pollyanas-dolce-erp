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

Si la revisión puede compilar imágenes, el script verifica antes de actualizar
el checkout que haya 5 GiB libres en `/` y 12 GiB en el almacenamiento real de
Docker; en un solo sistema de archivos se aplica el mayor umbral. Si no hay
margen, conserva la revisión y los contenedores vigentes. No forzar la
compilación ni usar un prune general para pasar la guarda.

El workflow `ERP image build pilot` prueba el Dockerfile completo de
`linux/amd64` fuera del VPS en GitHub Actions. No publica imágenes ni cambia el
despliegue: el registro privado y la prueba de reversión son pasos separados.

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
