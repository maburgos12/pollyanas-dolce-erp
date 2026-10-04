# Procedimiento por expediente

## 0. Plan de trabajo, sin reconstruir toda la investigación

Antes de revisar muchos expedientes invocar reconciliation_guard con mode=plan_month
o CLI --plan-month. La corrida del expediente ancla fija el período; sold_products
es el contrato de selección. Una barrera mensual se atiende una sola vez, no como
1650 solicitudes idénticas. Tras el bloqueo global, atender divergencias comerciales,
remanentes/desconocidos, documentos verificados/negativos heredados, historia y
documentación restante, y finalmente aprobación/conteo físico. Dentro de cada nivel
priorizar impacto registrado y atención HIGH. Un grupo genérico grande no desplaza
una diferencia comercial concreta nueva.

Ejecutar después únicamente las revisiones propuestas que aporten evidencia nueva.
Reutilizar bitácoras cuyo estado registrado no cambió, con runID trazable; no
recalcular su investigación por rutina. El plan nunca certifica fuentes vivas ni
ejecuta el lote. Una nueva respuesta humana/cambio de corrida/expediente invalida
la firma. Si cambió un documento externo sin actualizar la proyección, invocar una
revisión individual explícita: no suponer que updated_at del caso lo detecta.

Máximo diez IDs por propuesta. Guardar next_after_case_id y plan_fingerprint para
continuar; cambios de huella reinician el cursor y requieren revisar el nuevo plan.
Muestras documentales limitadas no descartan pendientes originales. Sin autoridad
mensual, seguir investigaciones independientes pero NO materializar o declarar cierre.

Evitar estos atajos: repetir todo para «estar seguros», detener todo por una fuente,
tomar firma de proyección por frescura Point, normalizar ventas o restaurar merma
sin autorización para «ganar tiempo». El progreso distingue magnitudes y evidencia,
no ofrece fecha o porcentaje de cierre sin base verificable.

## 1. Inventario mínimo de evidencia, antes de HTTP

Registrar período local, producto/receta, sucursal ERP y Point, dominio, SKU y claves externas. Verificar alias existentes; Bamoa no equivale automáticamente a Crucero histórico ni PK ERP a ID Point. Usar los modelos/servicios actuales, no nombres de tabla o firma supuestos.

Revisar fuentes existentes por claves y ámbito: apertura del día previo al mes, cierre del último día, producción real, ventas netas/cancelaciones, transferencias, carga/recepción/retorno, conversiones, mermas y ajustes. Separar transacción, evento, snapshot inmutable y proyección calculada. No enriquecer snapshots históricos con filas mutables actuales por aproximación.

Fuente → lector → materializador → auditor → pantalla es una cadena: identificar dónde se perdió identidad, cobertura o frescura. `source_trace` y `transfer_evidence` no necesariamente contienen todos los documentos; comprobar el contrato antes de interpretar una lista vacía.

Las fechas fetched_at/updated_at/snapshot_at importan. Una proyección anterior puede cuadrar aunque la fuente actual haya perdido una fila. Un raw viejo abierto no acredita que siga abierto; un snapshot anterior a recepción no contradice una recepción posterior.

## 2. Conciliación histórica sin repetir importaciones

### Contrato temporal por dominio, comprobado el 3 de octubre de 2026

La tabla y detalle actuales de Point `/Stock/tab_historial` convierten Fecha con
`moment.utc(data).toDate()` y `moment.utc(detalle.Fecha).toDate()`. Fragmento SHA256
`8755e73fc393236ec56f26354904ea5731022f6b4004c90f826e11c2f2e3e55c`.
Para imports `raw_metadata.source=POINT_STOCK_HISTORY_API`, `raw_payload.Fecha`
naive representa UTC; una fecha con Z/offset conserva su instante. Convertir ese
instante a America/Mazatlan antes del corte operacional. Raw ausente/malformado no
permite reemplazarlo silenciosamente por un timestamp derivado antiguo.

Esto NO se aplica a XLS ni a `PointNoteDetailService.Fecha_Hora`: la fecha naive
de notas es local Mazatlán. No cambiar TIME_ZONE, restar siete horas a todo dato,
ni escoger una interpretación porque hace cuadrar cantidades.

Ejemplo: Stock `2026-10-01T02:01:31.863` pertenece al 30sept 19:01 Mazatlán;
`2026-10-01T07:00:00Z` pertenece al 1oct. Una nota30sept18:59 naive permanece
30sept18:59 local, no se convierte como Stock. El mes septiembre usa el intervalo
`[2026-09-01T07:00Z,2026-10-01T07:00Z)`.

Releer raw autoritativo sin reescribir movimiento/import/metadata originales.
La ventana candidata legacy puede incluir el borde del mes siguiente; filtrar y
ordenar por instante efectivo, manteniendo consultas acotadas. Recalcular apertura
del31agosto y cierre30sept con el mismo contrato: corregir ventas pero conservar
extremos derivados antiguos crea diferencias falsas.

Un `ProductoMonthClosure` anterior LOCKED tampoco prueba el contrato temporal
correcto por sí solo. Si deriva de Stock, el carry-forward debe conservar prueba
`historical_boundary_contract=POINT_STOCK_RAW_UTC`; un ledger anterior sin esa
prueba se relee contra su fuente histórica exacta. No modificar ni desbloquear el
cierre anterior para obtener un saldo nuevo, ni emitir el marcador si falta cobertura.

La aritmética raw correcta NO renueva fetched_at. Cobertura requiere captura
posterior al fin operacional del mes. Para lotes truncados de500, usar el límite
raw del último lote efectivamente descargado, no filas más antiguas retenidas por
upsert ni una resta universal sobre earliest_movement_at. Preservar INCOMPLETE si
falta esa evidencia. Firma/refresco y protección de meses de borde deben usar el
contrato probado; publicar con pruebas antes de atribuir resultados al nuevo lector.

Un cierre Stock original anterior a los imports canónicos puede conservar prueba
explícita `no_history_current_zero`, stock0, history_rows0 y history_limit500.
Reutilizarla sólo con fuente/método/manifiesto exactos, fecha operativa correcta y
retrieved_at más created_at individual posteriores al corte correspondiente. No
tomar la fecha de una extensión como frescura de las líneas reutilizadas. Esa
prueba documental no cambia con UTC porque no tiene movimientos; no crear otro
import ni HTTP por rutina. Una canonical existente exige sus propios controles y
no puede ser ocultada por este fallback. Conservar coverageMISSING y prueba
original separada, nunca COMPLETE canónico ni conteo físico inventados. Un resumen
`latest_movement_at_or_before_close` no recibe esta excepción: sigue faltando su
historia/frontera si no están guardadas.

Reutilizar `AuditStockHistoryService.reconcile_many` para identificar casos con diferencia real, apertura y cierre comprobados, sin movimientos desconocidos y remanente histórico cero. Revisar venta comercial frente a stock antes de seleccionar. Un fetched_at anterior al fin de mes puede explicar cobertura INCOMPLETE aunque toda la suma cuadre.

### Frontera documental con snapshot original, autorización4oct2026

Usar el mismo lector `documentary_historical_boundary`, no otra captura/tabla.
Si existe canónica API `POINT_STOCK_HISTORY_API` del par, completa o incompleta,
NO ocultarla con un snapshot. XLS de costeo no se convierten en historia canónica.
Cuando no existe, consultar bulk snapshots producto desde corte hasta corte+3d.
El manifiesto VERIFIED puede proceder de captura directa o de consolidación de
intentos (`consolidated_point_stock_history_attempts`, apertura6 de agosto).
Exigir fecha, pares esperados completos y retrieved_at postcorte en ambos; el
snapshot aporta su propia prueba, no adopta stock/evidence de la consolidación.
El método consolidado NO habilita la prueba legacy de vacío/cero: esa conserva
su contrato directo estricto. No relajar canónicas ni atribuir cobertura completa.
Exigir FK branch/product y externalID exactos, row0 producto, row4 stock coherente
con persistido, row9 dominio producto explícito, job inventory SUCCESS y log
original de sucursal con branch_id/branch_external_id correctos. No identificar
por nombre/SKU ni utilizar snapshot insumo como producto.

Frontend `/Stock/tab_almacen` SHA256
`8a0516c8bd2b2d3735ec8f65d92ca5305ebab8fb902fc9b7270bd218bfb829a4`
convierte ÚltimoMovimiento/UIt_Mov con `moment.utc(data,"YYYY-MM-DD HH:mm:ss")`.
Row8 exige formato original timestamp completo naiveUTC; otro formato no se
normaliza por aproximación y queda sin prueba bajo este contrato. Captura debe
ser postcorte y lastMove estrictamente anterior al corte, no posterior a captura.
Fecha vacía/incompleta, dominio/identidad ausente, FAILED, recuento contradictorio
o stocks diferentes entre candidatos válidos dejan frontera sin prueba.

Conservar evidencia PK snapshot/job/par/stock/captura/lastMove/contrato/huella.
`snapshot_boundary_verified=True` no implica `canonical_history_verified`:
mantener cobertura MISSING y físico no acreditado. No reescribir cierre original.
Firma de refresh incluye raw/captura/status/tipo/log; verificar cambios sin signals,
consulta bulk y segunda lectura de cache sin HTTP. Registrar entrega real después
de CI/deploy/aceptación autenticada, no por actualizar este procedimiento.

Si falta frontera, documentar par/fecha exactos antes de Point. Usar historia
acotada que cruza el corte; no una fila cercana, stock actual ni resumen legado.
El cliente GetHistorial sólo tiene últimosN, no fecha/paginación acreditadas: no
inventar parámetros ni subir automáticamente a500/descargar el mes completo.
Agotar fuentes existentes y probar contrato literal de filtro antes de ampliación.
Subagentes investigan fuentes guardadas; solo principal abre una sesión protegida.

Si cobertura ya COMPLETE, no HTTP Point. Si falta exclusivamente cobertura posterior al cierre y el caso cumple las condiciones, documentar ese faltante y capturar únicamente el historial necesario: lote de hasta diez, una sola sesión protegida por `point_account_session_lock`, adquisición sin interferir con sesiones ajenas. Si ocupado, consultar titular en pg_locks/pg_stat_activity read-only cuando haga falta; no liberar candado ni reiniciar servicios. Cerrar sesión en finally.

Usar `AuditStockHistoryService.capture` **sin force**, sobre la importación canónica existente. Verificar que no crea otra importación y deduplica por FK_Movimiento. Inspeccionar firmas/callers actuales antes de preparar un comando; no proporcionar un importador genérico que eluda estos controles.

Comprobar ajustes por delta existencia_nueva−existencia_anterior y consistencia de magnitud/dirección; quantity positiva puede ser SALIDA. No invertir dos veces cantidades firmadas ni resolver contradicciones como cero. Preservar unknown cuando la evidencia contradiga el movimiento.

Aplicar proyección mediante `InventoryAuditMaterializer` solo si fuentes son autoritativas y está en alcance. Registrar si worker ya aplicó y selected=0: no adjudicarle al materializador un cambio que no hizo. Actualizar investigación por `InventoryAuditAgent` sin inventar aprobación.

## 3. Documentos de transferencia / Logística

Buscar folio/detalle y línea canónica, raw de cabecera y detalle, snapshots y carga por FK exacta. Mantener separado recibido, finalizado, cancelado, cantidades del detalle y total de cabecera; el total de todos los artículos no es cantidad del producto auditado.

Cuando SKU sea ambiguo, revisar identidad explícita del detalle, no pedir otra
captura del documento que ya existe. Para transferencia mutable, FK_articulo
positivo y dominio isInsumo=false identifica PointProduct.external_id únicamente
cuando el dominio de la fila también es producto. Clave presente inválida,
desconocida, dominio contradictorio o SKU único de otro producto conserva issue:
no usar fallback por nombre para ocultarlo. Clave ausente conserva contrato previo.
Este contrato es específico de transferencia, no de venta/merma/conversión.
Snapshot inmutable sin esa FK conserva evidencia congelada; jamás enriquecer desde
línea mutable. Leer raw_payload en consulta original evita N+1. Una identidad
resuelta recupera documentos y cantidades, no vincula un evento por milisegundos,
no elimina diferencias comerciales ni acredita custodia/conteo físico.

Si falta estado posterior, consulta read-only puntual de cabecera/detalle, con sucursal/intervalo y folio exactos documentados antes de HTTP. No extractor completo ni descarga mensual para comprobar un estado. Los filtros recibido true/false pueden excluir un folio cuyo estado cambió: inspeccionar contrato y seleccionar coincidencia exacta. No persistir automáticamente por haber encontrado una respuesta nueva; `persist_transfers` u otro sincronizador puede tener efectos operativos que exigen inspección y autorización.

Una revisión de carga cerrada sigue siendo evidencia: reutilizar FK y resolución validada con revisor/fecha/cantidades coherentes. No limitarse a discrepancias abiertas. No crear actor, explicación ni nueva discrepancia para simular aprobación.

Si enviado≠recibido, Point finalizado puede registrar retorno administrativo; verificar si se cargó y custodia física real cuando corresponda. No imputar pérdida, otro retorno o destino de rebanadas. Si falta evidencia humana, usar expediente existente, responsable autorizado y solicitud con folio, fecha, producto, sucursal, cantidad y acción comprobable. Avisar/asignar material/recurrente/alto riesgo; agrupar menores. No duplicar solicitudes ni modificar RRHH para encontrar responsable.

## 4. Fuentes comerciales y cobertura de mermas

Complementos aprobados se identifican por `RecetaAgrupacionAddon` activa/APPROVED y código no vacío, no por nombre Sabor/precio cero. Reutilizar filtro compartido de consumo y relación existente. Conservar venta original del addon, receta/base y consumo de insumos; no inventar descuento 1:1 de la base sin vínculo documental de ticket. Otros sabores requieren su propia relación confirmada.

Para diferencia comercial/stock revisar reporte original, configuración histórica, cancelaciones y horario comercial. Sin FK de ticket, una venta diaria y un movimiento de madrugada son pistas, no equivalencia. No borrar ventas ni cambiar categorías para normalizar.

Para autoridad de producción/mermas revisar manifiesto/rango/filtros/status/finished_at, writers, counters, duplicados y cobertura real. Contar hechos de resolución de SKU/nombre como tales, no registros faltantes. Un job SUCCESS y seen coincidente no prueba ausencia de omisiones en límites.

Ante fila desaparecida: recuperar evidencia read-only de raw/backup/historia/detalle. Diferenciar cancelación acreditada de omisión de listado. No restaurar backup, reinsertar merma, editar resumen del job, ejecutar persist_waste_lines/run_waste_sync ni componer autoridad por conteos para pasar el auditor. Si reparación afecta pipeline/fuentes operativas fuera del alcance, pedir autorización concreta una vez y avanzar investigaciones independientes mientras espera.

Con autorización explícita, reparar la causa y recuperar únicamente los originales
confirmados, no reimportar el backup completo. La extracción completa que omite
hashes previos debe fallar atómicamente con evidencia acotada, sin borrar fuentes.
El margen calendario y filtro operativo protegen límites sin reinterpretar fechas
ni probar exhaustividad. Validar por separado manifiestos/autoridad tras recuperar:
un contador recuperado no corrige automáticamente un job que omitió un registro.

## 5. Verificación de resultado y cierre

Antes/después: IDs canónicos, filas por importación, FK_Movimiento/row_number duplicados, saldos, source_trace/investigation, avisos y fingerprints pertinentes. Segunda ejecución debe hacer cero HTTP, no agregar filas/importaciones/duplicados ni avisos. Probar con cliente que rechaza HTTP o `requests.Session.request` prohibido en el ámbito de la prueba, sin afectar procesos ajenos.

Pantalla autenticada: ecuación, cantidades comerciales conservadas, diferencia, fuentes, Qué falta, trazabilidad y conteo separados; consola y solicitudes relevantes. No pulsar Guardar/Aprobar/Resolver para una verificación read-only. Si etiqueta contradice missing, no aceptar cierre.

Código autorizado: ficha de fuentes, worktree/branch registrados, PostgreSQL aislado, migraciones/checks, TDD de regresiones y revisión de consumidores, CI completo SHA actual, PR/merge, deploy_web_safe oficial sin pull manual previo, fresh VPS y UI autenticada, segunda ejecución idempotente y cierre exacto de la tarea. No copiar archivos al VPS. Pruebas locales no son publicación.

Cierre mensual se ejecuta por `ProductMonthClosureService` y sus guards reales:
fuentes autoritativas/frescas, cobertura, apertura/cierre e identidades resueltas,
validación lock_ready y actor autorizado. No omitir un issue para habilitarlo.
El servicio permite cierre contable/documental sin declarar conteo físico cuando
ese conteo no es requisito del flujo: registrar su ausencia y conservar expedientes
humanos/documentos/aprobaciones independientes. `closure_allowed=False` de review
solo significa que el goal no ejecuta cierre; no sustituye la evaluación del servicio.
No convertir cierre documental en aprobación de cada expediente o existencia física.
No detener automatización solo por publicar código o conciliar una suma.
