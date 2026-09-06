# Ejecución y recuperación de recetas

Los botones de recetas publican `pos_bridge.catalog_recipe_sync` en la cola
`recipes`. `worker_recetas` consume exclusivamente esa cola con prefork y un
proceso; el procesador general sigue en `celery`. La sesión de Point conserva
su candado compartido, por lo que el aislamiento no abre sesiones competidoras.
Los mensajes antiguos que aún estén en la cola general se reenvían a `recipes`
antes de ejecutar la lógica del trabajo.

## Contrato operativo

- Límite suave de 900 segundos y corte forzado de 960 segundos por ejecución.
- Cada producto y sus preparaciones se guardan dentro de una transacción.
  Una interrupción conserva la composición anterior; no queda un borrado parcial.
- `recetas_watchdog` revisa cada 30 segundos sin depender de Celery ni del navegador.
  Un trabajo PENDING/RUNNING sin actualización durante cinco minutos solo se
  recupera si ningún ejecutor conserva su candado PostgreSQL.
- Se vuelve a publicar el mismo trabajo, con los códigos pendientes originales.
  Máximo dos recuperaciones automáticas. Al agotarse, queda FAILED con instrucción
  visible de reintento manual; no existe un ciclo ilimitado de publicaciones.
- Una entrega duplicada no vuelve a ejecutar un trabajo terminal ni compite con
  su ejecutor activo. Si Redis rechaza una publicación, el trabajo permanece
  recuperable hasta el siguiente control, respetando el mismo límite.
- El límite de espera no equivale a éxito. Faltantes de composición/costo siguen
  siendo PARTIAL y no publican un corte semanal incompleto.

Estos tiempos requieren que el host y PostgreSQL estén disponibles. Una caída
completa del servidor no tiene tiempo de recuperación garantizado por este flujo.
Los contenedores se reinician con `unless-stopped`; los controles de salud
identifican procesador o supervisor no disponible. No hay una promesa de
disponibilidad absoluta de Point ni de importación de compras fuera del alcance
de búsqueda documentado por el trabajo.

## Comprobaciones

`docker compose ps worker_recetas recetas_watchdog` debe mostrar ambos servicios
sanos. Sus logs permiten comprobar recepción de tareas y fallas del supervisor.
`parameters.auto_recoveries`, `last_recovery_at`, `progress`, `attempt_count` y
`result_summary` conservan la trazabilidad en `PointSyncJob`.

El despliegue oficial recrea servicios cuando cambia Compose (un simple restart
no aplica colas/comandos nuevos) y reinicia ambos servicios en cambios Python.
No se requieren migraciones ni modificaciones del `.env` productivo.

## Prueba de interrupciones

Ejecutar `scripts/verify_catalog_worker_recovery.py` únicamente en Linux con
PostgreSQL y Redis aislados, `APP_ENV=development` y `CATALOG_WORKER_PROOF=1`.
El script inicia dos workers reales, sustituye solo la fuente HTTP de Point,
acelera los límites a 3/5 segundos y la antigüedad del trabajo a seis minutos.
Comprueba soft timeout, hard timeout, caída del hijo y reinicio del worker,
rollback PostgreSQL, reanudación del mismo trabajo, ausencia de duplicados y
avance de la cola general. Nunca importar el módulo de prueba en producción.

Las pruebas Django en `test_catalog_watchdog.py` y `test_catalog_recipe_recovery.py`
cubren publicación perdida, agotamiento, exclusión entre ejecutores y el caso
de un trabajo que termina justo antes de adquirir el candado de recuperación.
