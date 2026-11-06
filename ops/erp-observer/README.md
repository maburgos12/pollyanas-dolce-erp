# ERP Doctor: observación en el VPS

Autorización: Mauricio, chat `01a1b1ef-688a-7b83-a620-20601e3a90dc`,
2026-11-06: instalar supervisor en observación y prueba aislada con suscripción
hasta PR borrador. No habilitar publicación automática ni cambiar el monitor de
cinco minutos. Esta entrega queda deliberadamente en PR borrador; no integra ni
despliega la aplicación ERP.

## Componentes y límites

- `collect.py`: lector fijo ejecutado por root cada 60 segundos mediante su propio
  timer. Consulta hasta 12 000 líneas/48 horas del worker y hasta ocho UUID
  terminados en el backend JSON existente. No ejecuta Doctor, fix, email, NAS,
  tareas Celery, consultas SQL ni cambios de horarios. El acceso privilegiado a
  Docker queda en este lector revisado, sin modelo ni comandos tomados de logs.
- `observer.py`: proceso `erp-observer`, sin red, credenciales ni acceso a Docker
  o al checkout productivo. Lee un resumen, conserva estado con escritura
  atómica/fsync, lock exclusivo y heartbeat cada 30 segundos. No contiene llamadas
  a procesos, modelos, GitHub, merge ni deploy.
- Snapshot: `/var/lib/erp-observer-source/doctor.json`, root:erp-observer 0640;
  estado: `/var/lib/erp-observer/state.json`, erp-observer 0600. No se guardan
  resúmenes, trazas, logs originales ni secretos. Máximo 256 causas/revisiones;
  al alcanzar capacidad falla y preserva evidencia, sin borrar por antigüedad.
- `revision_hint` procede del checkout, no demuestra el SHA servido. Un resultado
  anterior al inicio del worker es `predates_worker`; no vuelve a abrir una
  reparación. Retención backend de un día; falta, expiración, error del lector o
  snapshot mayor a 180 segundos nunca significan salud.
- Se deduplica por clase, checks y revisión orientativa, no por UUID. Incidentes
  conservados son observaciones, no tareas autorizadas ni reparaciones en cola.
  Una observación OK posterior no certifica el cierre de incidentes históricos.
- Observador: 128 MiB, CPU 5%, 16 tareas; lector: 192 MiB, CPU 10%, 75 segundos.
  El límite del lector no limita el proceso de consulta ejecutado dentro del
  worker existente; la consulta solo obtiene resultados y lleva alarma interna
  de 12 segundos además del timeout externo.
  No hay puertos, tablas, dependencias Python o servicios de reparación nuevos.

## Instalar una revisión revisada

Usar un repositorio de control separado, con código recibido por Git. Nunca
ejecutar el ciclo de workspaces sobre `/opt/pastelerias-erp`, que contiene archivos
operativos sin seguimiento. Desde el repositorio de control:

```bash
bash ops/erp-observer/install.sh "$PWD" "$(git rev-parse HEAD)"
systemctl status erp-observer.service erp-observer-collect.timer --no-pager
journalctl -u erp-observer.service -u erp-observer-collect.service --since '-10 min'
```

La instalación publica código operacional bajo `/opt/erp-observer/releases/SHA`,
propiedad root, y enlaza `current`. No modifica `.env`, puertos, Beat, servicios
ERP/Maya/ad-agent, ni las automatizaciones del Mac. No necesita autenticación de
ChatGPT. No puede detectar por sí mismo la caída de su propio VPS ni enviar avisos
externos: consulta de estado/journal disponible; canal externo pendiente de diseño.

## Prueba aislada con suscripción

El ensayo es manual, no una función del observador. Registrar tarea, dueño,
revisión, límites y recursos antes de crear el worktree. Ejecutar Codex CLI
oficial por usuario dedicado, sin grupos Docker/sudo ni credenciales de GitHub o
ERP, con `forced_login_method="chatgpt"`, sin API keys y sin fallback. Mantener
un único `CODEX_HOME` protegido y serializar ejecución/revisión. La sesión
temporal del 6 de noviembre pertenece a este chat; revisión 2026-11-09.

Reproducir un fallo sintético con pruebas sin credenciales; dejar que Codex
proponga la corrección, revisar diff y ejecutar pruebas bajo un usuario distinto
sin acceso a la sesión. Máximo tres fallos del ensayo; no ejecutar otra causa real.
Solo el operador autorizado registra commit/push y PR borrador. La revisión de
Codex y un exit0 no reemplazan los checks, aceptación ni revisión del operador.
No habilitar merge/deploy o un runner de GitHub en el VPS.

El CLI mantiene su propia renovación de sesión. Cuota insuficiente, sesión
revocada o timeout bloquean el ensayo; no se compran créditos ni se cambia a API.
La autenticación puntual no demuestra renovación prolongada o servicio 24/7.

## Validación y reversión

```bash
python3 -m unittest discover -s scripts/tests -p test_erp_observer.py
```

Verificar en VPS: snapshot y estado reales; ausencia de acceso a Docker/ERP/red;
duplicados y entradas inválidas en estado sintético separado; restart del nuevo
servicio conserva estado y cambia PID; unidad independiente de SSH/Mac. No
reiniciar el VPS/ERP para probar. Registrar SHA instalado y hashes de archivos.

Parada manual reversible, exclusivamente de estas unidades:

```bash
systemctl disable --now erp-observer-collect.timer erp-observer.service
systemctl stop erp-observer-collect.service
```

Conservar estado/evidencia; no borrar usuarios, sesiones o worktrees sin comprobar
propiedad y datos únicos. Entregar con responsable y fecha recursos que sostienen
el PR borrador. Al retirar ensayos del VPS, no usar la regla de limpieza Docker
Desktop para autorizar eliminación de recursos productivos.
