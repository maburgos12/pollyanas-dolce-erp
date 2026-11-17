# Point–NAS y limpieza aprobada residente — Implementation Plan

> **For agentic workers:** Usar `executing-plans` para ejecutar por fases; si se autoriza delegación, `subagent-driven-development` con alcances separados y revisión independiente. Las casillas son gates de implementación, no resultados ya obtenidos.

**Goal:** Archivar históricos recuperables de Point y ejecutar únicamente retiros aprobados en Telegram, conservando el acceso de los consumidores, los respaldos y la operación.

**Architecture:** Extender el preparador y ledger existentes. Añadir un catálogo técnico de archivos y un lector común con recuperación acotada mediante un servicio residente en el VPS; el proceso Django no recibe credenciales SSH. Activar por fases: sólo lectura, copia/verificación, consumidores y finalmente ejecución de lotes autorizados.

**Tech Stack:** Python estándar, Django/PostgreSQL 16 existentes, systemd, SSH NAS existente, Telegram y Software Factory. Sin nuevas APIs pagadas, motor de IA periódico, montaje NAS sobre las rutas originales ni nueva tabla de ventas.

---

## 1. Alcance, evidencia y garantías verificables

Fecha local: 2026-11-16. Base inspeccionada: `3a99040a0a301f434ee367f20affb91c86eefddc`.
Tarea documental: `point-nas-lifecycle-plan-20261116`. Fuente de datos: `docs/data-reuse/point-nas-lifecycle.md`.

Hechos:
- El preparador existente corre cada cinco minutos por encima de 75%; admite APT/logs cerrados y verifica recuperación antes de pedir aprobación. No ejecuta retiros y no admite Point.
- `erp_telegram_cleanup.py` usa una propuesta de un único alcance, hash exacto, nonce, vencimiento y aprobación/rechazo. Su esquema no admite archivos Point ni el ciclo de ejecución completo.
- `PointProductHistoryImportService.parse_report` y `PointSalesCategoryReportService.parse_report` abren rutas locales directamente. El pipeline hace además `exists`, `stat` y checksum antes de parsear. Resolver sólo `read_excel` no cubre el flujo.
- Existe otra tarea activa, `telegram-cleanup-status-20261112`, para mensajes idempotentes. Revisarla y coordinar su integración; no reemplazar su worktree ni crear una segunda implementación del mismo callback.
- El inventario de fuentes PostgreSQL en sólo lectura encontró registros en tareas, staging, normalizado, hechos, importaciones históricas, ventas oficiales mensuales y `VentaAutoritativaPoint`.
- Auditoría anterior del mismo día: 24,484 XLS / 1.20 GB físicos investigados; no es una lista aprobada para borrar. El cruce detectó tareas/jobs incompletos y referencias no resueltas. Sus cifras se deben refrescar antes de formar un lote.
- La limpieza puntual anterior recuperó 1.67 GB y dejó 75%; sus pruebas no demuestran que este nuevo automatismo funcione.

No se promete ausencia absoluta de bugs. Aceptación exige pruebas adversas, invariantes de datos, instalación gradual y recuperación comprobada ante fallos. Una comprobación no ejecutada sigue pendiente.

Fuera de alcance: borrar respaldos NAS/VPS, cambiar ventas/nómina/inventario, cerrar jobs antiguos por su edad, modificar cifras o estados de negocio para volver elegibles archivos, limpiar otros servicios, cambiar horarios de monitores, podas globales Docker, autorizar automáticamente código nuevo.

## 2. Invariantes de diseño

1. Un archivo no es descartable por antigüedad ni por `SUCCESS` aislado. Elegibilidad requiere identidad verificable, ausencia de uso/trabajo pendiente y consumidores certificados para archivo remoto.
2. Conservar `source_file` y `report_path` como identidad lógica original. Separar la ruta física temporal; nunca persistir la ruta de caché/NAS como si fuera la fuente original.
3. Archivo ausente por falla NAS se representa como indisponible recuperable, nunca cero, reporte vacío, éxito o nueva descarga de Point por defecto.
4. Lectura normal local no consulta NAS. La caída NAS no debe interrumpir las pantallas que sólo consultan PostgreSQL ni archivos todavía locales.
5. No eliminar un original sin aprobación vigente del lote exacto y prueba de restauración del contenido vigente. No aplicar aprobaciones vencidas a lotes nuevos.
6. Sólo se elimina bajo exclusión mutua compartida por lectores/escritores/ejecutor. Consultar archivos abiertos es un control adicional, no una garantía de ausencia de carrera.
7. Copia publicada exige escritura temporal, fsync cuando el filesystem lo permita, publicación atómica y lectura de vuelta. Ni un listado NAS ni el éxito de SSH son evidencia de contenido.
8. Publicar el índice recuperable ANTES de retirar el original. Tras una caída debe existir al menos una copia recuperable y evidencia durable que permita reconciliar cada paso.
9. Reintentos de Telegram sólo reenvían notificaciones. Nunca repiten el borrado por no haber llegado la confirmación.
10. Una reversión de código que quite el lector NAS está prohibida mientras existan originales retirados que ese código necesite.

## 3. Fuentes y consumidores: matriz inicial obligatoria

| Fuente / lector real | Adaptación necesaria | Comprobación de regresión |
| --- | --- | --- |
| `pos_bridge/models/sales_pipeline.py`: tareas, staging, normalizado, hechos | Consultar referencias y hashes; no modificar datos operativos | Misma identidad/hash y conteos antes/después |
| `pos_bridge/models/product_history.py`: importación y filas | Mantener `report_path`, nombre lógico y deduplicación por hash | Reimportación no crea duplicados |
| `pos_bridge/models/sales.py`: oficial mensual | Mantener referencia al XLS | Parseo y totales idénticos con original sólo en NAS |
| `pos_bridge/services/product_history_import_service.py` | Resolver durante todo el parseo y hash; devolver nombre/ruta lógica originales | `test_product_history_import_service.py` local y archivado |
| `pos_bridge/services/sales_category_report_service.py` | Un único lease para Sheet1/Sheet2 y validaciones | `test_sales_category_report_service.py` equivalencia filas/Decimal |
| `pos_bridge/services/sales_pipeline/rebuild_service.py` | Cubrir exists/stat/hash/parse en el mismo lease; separar descarga nueva de recuperación | `test_sales_pipeline_rebuild_service.py` sin escrituras inesperadas |
| `pos_bridge/services/sales_report_reconciliation_service.py` | Resolver ambas hojas bajo lease | Comparación de conciliación y totales |
| `pos_bridge/services/product_month_closure_service.py` | Verificar llamada al parser y origen preservado | `test_product_month_closure_service.py` |
| `pos_bridge/services/official_sales_backfill_service.py`, comandos backfill mensuales | Verificar delegación al lector; no crear otra recuperación | Tests backfill existentes, fuente archivada |
| `ventas/services/sales_truth.py`, `reportes/sales_confidence.py` | Verificar cuáles sólo transportan referencia y cuáles acceden al archivo; no cambiar fuente canónica | Mismos resultados de ventas/confianza y misma precedencia |
| Descargas, utilidades y lectores fuera de Django | Inventariar con grafo + literales acotados y revisar invocaciones | Todo consumidor admitido tiene dueño y prueba; el no cubierto excluye su archivo |

Esta matriz NO declara cobertura exhaustiva. La fase 1 debe cerrarla por cada familia candidata. Los JSON de conteos, recetas, inventarios y datos de otros sistemas no se incorporan por compartir carpeta.

## 4. Catálogo técnico y lector recuperable

Reutilizar modelos de negocio y hashes existentes. Crear un registro técnico de ubicación, no duplicar ventas. Primera versión: documentos JSON atomizados bajo un directorio dedicado dentro de `storage/pos_bridge`, ya montado por la aplicación; comprobar el montaje real antes de instalar. No añadir una base SQLite ni guardar secretos en el registro.

Cada entrada versionada contiene: identificador de archivo, ruta lógica relativa a la raíz permitida, SHA-256, bytes, identidad de origen, referencias conocidas, clave NAS relativa, evidencia de restauración, estado físico, versión del lector compatible y última comprobación. Identificador derivado de ruta lógica + hash; igual hash en otra ruta no fusiona sus relaciones de negocio. Cambios del original invalidan la candidatura y aprobación.

Nuevo módulo propuesto: `pos_bridge/services/archive_access.py`.
Contrato de diseño:
```python
# El context manager mantiene el lease hasta que termina el consumidor.
with archive_access.open_source(logical_path) as source:
    # source.local_path es sólo para lectura física.
    # source.logical_path es lo que se conserva en la salida y en BD.
    dataframe = pandas.read_excel(source.local_path)
```
`open_source` acepta sólo fuentes Point registradas o rutas locales preexistentes permitidas por cada consumidor. No amplía el acceso a rutas arbitrarias. Para locales externos usados por comandos autorizados conserva el comportamiento anterior, sin intentar enviarlos al NAS.

Si está archivado: registrar solicitud acotada en cola técnica de recuperación; el servicio VPS descarga a staging, verifica tamaño/hash y publica en caché privada. Django espera como máximo 2 segundos; si sigue pendiente, devuelve excepción tipada de recuperación pendiente. Jobs reintentan con límite; consumidores web traducen a respuesta visible de espera/reintento, sin mantener Gunicorn esperando SSH. Definir la traducción por consumidor en fase 1, no cambiar indiscriminadamente todas las respuestas HTTP.

No entregar claves NAS al web/worker. El servicio host acepta sólo IDs ya existentes en el catálogo controlado; nunca comandos, URLs o rutas elegidas por el solicitante. Catálogo de sólo lectura para Django; cola de peticiones separada con permisos mínimos. No copiar permisos/credenciales productivos a pruebas.

Límites iniciales propuestos: una transferencia simultánea, lotes hasta 256 archivos y 1 GB, tiempo total 240 s por transferencia, reserva de disco de 5 GiB más staging requerido, caché temporal máxima 1 GiB. Archivos que exceden esos límites quedan fuera. Validar estos límites con el piloto antes de activarlos, sin cambiar silenciosamente el contrato.

Locks de archivo estables por identidad, compartidos entre host/contenedores: lease compartido de lectura y exclusivo de publicación/retirada. Una descarga concurrente del mismo ID se une a la existente. El recolector sólo retira copias temporales verificadas sin leases; nunca originales ni respaldos. Su política se documenta y autoriza con la instalación.

El NAS no se monta encima de `raw_exports`; no se utilizan symlinks como sustituto del lector. Conservar archivos recientes y requeridos para operación local. La edad mínima sirve para seleccionar candidatos, no demuestra elegibilidad.

## 5. Ciclo persistente de propuestas y ejecución

Extender `ops/erp-telegram-release/erp_cleanup_prepare.py` y `erp_telegram_cleanup.py`, reutilizando validaciones, transporte y ledger. Añadir un tipo explícito `point_archive_manifest`; no relajar la lista de rutas APT/logs para aceptar cualquier ruta. Docker permanece fuera del preparador genérico: cada estrategia nueva requiere prueba de recuperación propia.

Estado de preparación independiente del ledger de decisiones:
`detected → preparing → verified → awaiting_approval → approved → executing → completed/partial/blocked`.
Decisiones humanas siguen preservadas: pendiente, aprobada, rechazada, revocada o vencida como hecho derivado. No reescribir la decisión original como evidencia de ejecución.

- Por encima de 75%, inspección incremental y acotada. Un solo lote en preparación/ejecución; deduplicar por causa, versión de política y manifiesto. No consumir IA en el timer.
- Planificar objetivo propuesto de 73% para dar margen, pero jamás ampliar el lote aprobado para alcanzarlo. Si no hay bytes elegibles suficientes, informar cantidad restante y causa concreta.
- Preparar NAS, restauración y comprobación de consumidor ANTES de enviar botones. Hash del manifiesto incluye el conjunto y la evidencia. Botones se verifican con la respuesta real de Telegram.
- Telegram confirma autorización con ID y estado. Rechazo/vencimiento detienen ejecución. Repetir botón informa estado actual; no crea error genérico ni nuevo retiro.
- Nuevo ejecutor residente propuesto `ops/erp-telegram-release/erp_cleanup_execute.py`, mediante servicio/timer separados. Inspecciona aprobaciones cada minuto; revalida firma/vinculación, vencimiento, revocación, evidencia, identidad y uso inmediatamente antes de cada retiro.
- Reutilizar coordinación con despliegues y builds. Orden único de locks documentado y probado para no bloquear el receptor Telegram durante una copia larga. Revocación permitida hasta la reclamación atómica de ejecución; informar claramente ese límite en el mensaje.
- Reclamar operación durable con identificador único. Escribir y sincronizar intención por archivo antes del unlink, y resultado después. Si reinicia entre ambos, comprobar existencia/hash y reconciliar; no repetir operación ambigua a ciegas.
- Si vence a mitad del lote, detener los archivos restantes y reportar resultado parcial. Nunca mantener la autorización viva porque empezó antes de vencer. No borrar nuevos originales con una aprobación anterior.
- Notificaciones con outbox durable separado: reintento acotado de entrega, deduplicación por evento. No asumir entrega al usuario si Telegram no confirmó.
- Solicitud vencida sin efectos: revalidar todo y permitir una única nueva solicitud por el mismo incidente; después esperar intervención y evitar spam. Rechazo explícito bloquea esa propuesta hasta instrucción nueva. No renovar decisiones ambiguas.
- Confirmación final: archivos realmente retirados/omitidos, bytes netos `df`, porcentaje, evidencia NAS y preservación de servicios/backups. 75% no se comunica como menos de 75%.

## 6. Qué hacer cuando una comprobación no pasa

El sistema no puede corregir automáticamente cualquier problema futuro de negocio. Debe eliminar los pendientes silenciosos:
- NAS temporalmente inaccesible: reintentos 1, 5 y 15 minutos, límite tres por misma causa/lote; conserva originales. Después estado bloqueado, siguiente revisión y aviso con acción recomendada.
- Hash distinto o archivo cambiado: excluir ese archivo, conservar ambas evidencias y generar diagnóstico; no aceptarlo bajo la autorización anterior.
- Referencias incompletas/consumidor no integrado: ticket técnico con archivo/familia, lector, causa y prueba que falta. El agente continúa esa tarea existente cuando esté disponible; no crea duplicados ni convierte el fallo en éxito.
- Jobs antiguos activos: mostrar responsable y evidencia de actividad; no marcarlos SUCCESS ni cancelarlos para liberar espacio.
- Poco espacio para recuperar: reducir lote/reserva válida o usar el verificador externo registrado; nunca llenar VPS/Mac ni borrar primero para poder probar.
- Error nuevo de código: Software Factory, pruebas, revisión, PR y aprobación por SHA. El timer no se otorga autorización para publicar código.

## 7. Entregas ordenadas y archivos

### Fase 1 — contrato y cobertura (sin retiro)
- [ ] Actualizar fuentes y referencias sobre el SHA vigente; cerrar matriz con lector, escritor, jobs, utilidades y pruebas por familia.
- [ ] Definir predicado de elegibilidad con motivo de inclusión/exclusión por archivo. Conservar ambiguos. No usar cierre contable inferido como aprobación.
- [ ] Registrar fixtures sintéticos XLS, rutas lógicas y archivos malformados; sin datos personales.
- [ ] Entregar decisiones de permisos, contratos visibles y límites de configuración en el PR para aprobación del SHA. No editar `.env` como atajo.

Archivos: ficha de fuentes; `pos_bridge/tests/test_archive_access.py` (nuevo); fixtures generadas por tests; `docs/runbooks/point-archive.md` (nuevo).

### Fase 2 — lector y recuperación, modo copia
- [ ] Probar primero archivo local idéntico, archivado recuperado, NAS caído y dos lectores simultáneos.
- [ ] Implementar `archive_access.py` y `ops/erp-telegram-release/erp_point_archive.py` (nuevo servicio de catálogo/cola/NAS); reutilizar comprobaciones de archivo existentes sin duplicar lógica de aprobación.
- [ ] Adaptar los lectores de la matriz y envolver exists/stat/hash/parse en el mismo lease. Mantener salida lógica original.
- [ ] Agregar a `scripts/ops/` unidades `erp-point-archive.service/.timer`, sin activación automática al instalar archivos.
- [ ] Probar copias, restauración y comparaciones sin retirar originales. Ejecutar todos los consumidores afectados.

### Fase 3 — ejecución aprobada e idempotencia
- [ ] Coordinar tarea existente `telegram-cleanup-status-20261112`; revisar su diff y base antes de reutilizarla. Si requiere una sucesora, usar el cierre oficial y conservar evidencia original.
- [ ] Extender esquema de propuestas/estado por versión, con lectura de los registros antiguos; no invalidar aprobaciones ya emitidas ni ejecutar las antiguas automáticamente.
- [ ] Implementar `erp_cleanup_execute.py` y unidades `scripts/ops/erp-cleanup-execute.service/.timer`; pruebas en `scripts/tests/test_cleanup_execute.py` (nuevo).
- [ ] Extender `scripts/tests/test_telegram_cleanup.py` y `test_cleanup_prepare.py` para tipo Point, expiración, revocación, callbacks duplicados y fallos de entrega.
- [ ] Actualizar `ops/erp-telegram-release/install.py` preservando los hooks de Point/Mac-IA; comparar unidad y módulos instalados antes/después. Reutilizar el canal oficial de despliegue, sin copiar código al VPS como atajo.

### Fase 4 — piloto, despliegue y cierre
- [ ] Gates PostgreSQL, tests afectados, revisión del diff completo y CI del SHA exacto. Registrar en Factory.
- [ ] Desplegar lector y modo copia tras aprobación del SHA; comprobar proceso real y consumidor, no sólo HEAD en disco.
- [ ] Mínimo dos ciclos completos de preparación y restauración de una familia, sin originales retirados; medir tiempos y disco. Cualquier fallo mantiene ejecución desactivada.
- [ ] Concedida aprobación de instalación del ejecutor, activar sólo el servicio nuevo mediante procedimiento versionado. Los monitores actuales conservan horarios.
- [ ] Solicitud piloto: máximo 10 archivos y 100 MB, todos con recuperación y consumidores certificados. Aprobar por Telegram; ejecutar automáticamente y comprobar sin Mac conectada al proceso de ejecución.
- [ ] Probar recuperación desde NAS con originales locales ausentes, usando consumidor real de sólo lectura; comparar resultado con baseline. Nunca usar reimportación productiva que escriba ventas como prueba.
- [ ] Sólo ampliar familias tras su matriz y piloto. Cada nuevo lote de retiro sigue solicitando aprobación.
- [ ] Registrar antes/después de datos, respaldos, servicios y recursos; cerrar entornos propios o entregar con responsable y fecha.

## 8. Matriz de pruebas obligatorias

| Caso | Resultado exigido |
| --- | --- |
| Original local; NAS apagado | Lectura local idéntica; sin llamada NAS |
| Original ausente; NAS válido | Recuperación hash exacto; mismo nombre/ruta lógica y mismo resultado de parseo |
| NAS apagado durante recuperación | Error tipado/reintento acotado; ninguna fila cero ni retiro |
| Objeto truncado/hash incorrecto | No publicar caché, no borrar original, evidencia conservada |
| Nombre `../`, symlink, hardlink, objeto fuera de raíz | Rechazo, ninguna lectura/escritura externa al alcance |
| Archivo grande/bomba de expansión | Límites efectivos de tamaño, tiempo y reserva; sin llenar disco |
| Dos recuperaciones/lectura durante limpieza | Una transferencia; lease impide unlink durante consumo |
| Archivo modificado tras propuesta | Excluir e invalidar evidencia de ese archivo; no borrar |
| Aprobación de otro chat, nonce o SHA | Ningún efecto |
| Rechazo/revocación antes de claim | Ningún efecto |
| Vencimiento antes/durante ejecución | Nada adicional; resultado parcial exacto si corresponde |
| Reinicio antes/después de unlink | Reconciliación por archivo; jamás doble ejecución |
| Telegram caído antes/después de ejecución | Decisión/resultado durable y reenvío de aviso; sin repetir limpieza |
| Botón repetido | Estado persistente comprensible: aprobado/en curso/terminado/rechazado/vencido |
| Deploy/build concurrente | Ejecutor espera o bloquea, nunca corre sin coordinación |
| Rollback con fuentes archivadas | Lector compatible permanece o se restauran originales antes de revertir |
| Importación/ventas/cierre/datos canónicos | Misma identidad, importes Decimal, filas y precedencia de fuente; sin duplicados |
| Registro antiguo de Telegram | Visible y compatible, sin ejecución implícita |

Comandos de integración planificados en worktree de implementación con PostgreSQL 16 aislado y conectividad comprobada:
```bash
python3 manage.py check
python3 manage.py migrate --check
python3 manage.py makemigrations --check --dry-run
python3 manage.py test pos_bridge.tests.test_archive_access pos_bridge.tests.test_sales_category_report_service pos_bridge.tests.test_product_history_import_service pos_bridge.tests.test_sales_pipeline_rebuild_service pos_bridge.tests.test_product_month_closure_service pos_bridge.tests.test_official_sales_backfill_service
python3 -m unittest discover -s scripts/tests -p 'test_*cleanup*.py'
python3 -m unittest discover -s scripts/tests -p 'test_telegram_release.py'
systemd-analyze verify scripts/ops/erp-point-archive.service scripts/ops/erp-point-archive.timer scripts/ops/erp-cleanup-execute.service scripts/ops/erp-cleanup-execute.timer
```
Los archivos nuevos anteriores son entregables futuros, no comandos ejecutables hoy. Añadir al CI real todo test nuevo; comprobar descubrimiento. Unidades se validan en Linux aislado. La validación web incluye consola/Network cuando haya cambios visibles de espera o descarga.

## 9. Reversión y conservación

Parar primero sólo preparador/ejecutor nuevos para impedir retiros. Mantener catálogo, lector y NAS disponibles. Restaurar por manifiesto a ruta original bajo lease, verificar cada hash y espacio antes de continuar. Si no cabe todo, permanecer en versión compatible y restaurar por lotes; nunca revertir a código que desconoce NAS dejando archivos ausentes. Registro incompatible se migra de forma aditiva con copia recuperable, no destructivamente.

Conservar todas las copias NAS y respaldos en esta entrega. Una futura retención de NAS requiere propuesta distinta. No se promete tolerancia a pérdida simultánea NAS y caché local: antes de ampliar el piloto debe existir copia recuperable independiente del catálogo y del archivo, en el respaldo existente o en un destino aprobado; no atribuir esa cobertura al backup de BD sin probarla.

## 10. Aceptación final y evidencia

Se considera funcional sólo si: lector transparente comprobado; el ejecutor opera sin sesión Codex/Mac; rechazo y aprobación reales probados; no repite retiros tras reinicio; confirma Telegram; preserva datos/servicios; el lote piloto sigue legible con fuente local ausente y NAS recuperable; reversión ensayada; no deja recursos sin dueño. Cada evidencia registra SHA, comando, hora, resultado y límites.

Este documento entrega diseño y gates, no certifica software aún no implementado. El plan evita desplegar todo junto: un lector incompleto bloquea sólo su familia, y nunca habilita retiros porque una prueba distinta pasó.

## Decisiones de implementación tras revisión

El contrato efectivo del piloto está en `docs/runbooks/point-archive-lifecycle.md`.
Se reduce el alcance del ejecutor a Point (≤10 fuentes/100 MB). APT/logs y Docker
conservan sus procedimientos separados porque no tienen coordinación con sus
escritores. Fuentes recuperadas permanecen locales; no se introduce un recolector
nuevo. La cola restaura automáticamente archivos; no repite jobs de negocio que
fallaron esperando recuperación. Los permisos y dos ciclos NAS del SHA publicado
se verifican antes de activar timers. La validación local no cuenta como esos ciclos.
