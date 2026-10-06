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
import ni HTTP por rutina. La autorización5oct2026 permite también consolidación
con procedencia original por par y canónica INCOMPLETE no contradictoria, mediante
el mismo lector compartido y los controles siguientes. Conservar coverageMISSING
o INCOMPLETE y prueba original separada, nunca COMPLETE canónico ni conteo físico. Un resumen
`latest_movement_at_or_before_close` no recibe esta excepción: sigue faltando su
historia/frontera si no están guardadas.

#### Vacío/cero independiente: contrato autorizado5oct2026

`documentary_historical_boundary` exige manifiesto STOCK/VERIFIED, fecha exacta,
pares esperados completos y retrieved_at aware postcorte. La línea debe declarar
stock0, método `no_history_current_zero`, history_rows int0 y history_limit int500.
No interpretar un listado vacío sin ese stock explícito como cero.

Para consolidado, resolver `source_closing_ids` enteros exactos únicos positivos
(no bool/string), sin autorreferencia
ni IDs ausentes. Leer en bulk las líneas originales, mismo par FK producto/sucursal,
fecha, dominio STOCK y método directo. DRAFT en la fuente original puede significar
que fallaron otros pares: aceptar sólo esta línea acreditada, no todo el intento.
Fuentes REJECTED, pares fuera de su manifiesto o cualquier línea original discrepante
vetan el cero. Exigir evidence idéntica, stock0 y created_at propio aware postcorte,
no posterior a la línea consolidada; retrieved_at original también postcorte.
No prestar fechas de extensiones ni crear historia o cambiar saldos almacenados.

Si existe canónica INCOMPLETE, comprobar import único, unknown0, conteo retenido,
tipos de metadata, IDs de lote cuando existen y todos los raws conocidos mediante
el guard compartido. IDs ausentes en un lote legado vacío con count0/retained0 no
se fabrican. Cualquier movimiento retenido anterior o igual a la consulta vacía
la contradice, incluso cancelado o qty0. Hechos posteriores no demuestran
contradicción ni prestan cobertura; deben conservar identidad, raw y fecha válidos.
Una frontera canónica COMPLETE presente mantiene su precedencia y controles.

Salida: `original_boundary_verified=True`, `original_zero_boundary_evidence`
con fuentes/líneas originales, fechas y firma; `original_zero_consistency_evidence`
con veto o validación canónica. Mantener `canonical_history_verified=False`,
coverage original y `physical_count_verified=False` cuando sólo acredita este cero.
Releer con cache transaccional; cambios de procedencia alteran la firma del balance
y cierre. Pruebas reproducibles en `test_historical_inventory_closing` y
`test_snapshot_historical_boundary`; review/plan sólo observa, no ingresa ni captura.

Reutilizar `AuditStockHistoryService.reconcile_many` para identificar casos con diferencia real, apertura y cierre comprobados, sin movimientos desconocidos y remanente histórico cero. Revisar venta comercial frente a stock antes de seleccionar. Un fetched_at anterior al fin de mes puede explicar cobertura INCOMPLETE aunque toda la suma cuadre.

#### Cero histórico con respuesta original y snapshots que rodean el corte

Si el consolidado sólo conserva `two_independent_no_history_current_zero`, **no** usar las dos existencias actuales como saldo histórico. Para un corte anterior, el lector puede componer una respuesta original íntegra y vacía de `Stock/GetHistorial` con límite500 y snapshots originales del **mismo FK producto/sucursal** inmediatamente antes y después del corte UTC. Archivar esa respuesta como `POINT_STOCK_HISTORY_BOUNDARY_ONLY`, separada del hash y la cobertura canónicos: validar petición literal reconstruida desde el script de adquisición, fecha de recibo posterior al corte, SHA de `[]`, dominio PRODUCT, external IDs y PK exactas. Segunda entrada idéntica no escribe; otra procedencia para el mismo corte requiere revisión, no reemplazo.

Exigir manifiesto STOCK/VERIFIED, fecha y membresía completas, línea original stock0 con `history_rows=0` y al menos dos intentos. Los snapshots de la ventana de tres horas a cada lado deben ser stock0 también en `raw.row[4]`, `raw.row[8]` ÚltimoMovimiento vacío y `raw.row[9]` producto, con job inventory SUCCESS y log original de sucursal válido. Ambos lados son indispensables; raw corrupto, un saldo distinto, una canónica existente, archivo o procedencia inválidos vetan la prueba. La ventana sólo delimita la evidencia consultada: no equiparar ventas y movimientos por cercanía horaria.

Salida `POINT_ORIGINAL_EMPTY_BRACKETED_SNAPSHOTS_V1`: saldo **documental Point del corte** cero, firmas de archivo y snapshots, cobertura canónica conservada MISSING, `canonical_history_verified=False`, `physical_count_verified=False`. No derivar de esto que no hubo ventas, pérdidas o errores humanos. Una venta comercial del mismo código sigue independiente y su diferencia requiere expediente; una regla de complemento aprobada por sí sola no aporta FK del ticket ni descuenta la base 1:1. Si existe nota original con ambas líneas, usar su PK/Folio exactos para explicar la selección del complemento sin alterar venta ni inventar salida de Stock. Releer dos veces sin HTTP ni cambios operativos antes de aceptar; el guard mensual completo permanece independiente.

### Frontera documental con snapshot original, autorización4oct2026

Usar el mismo lector `documentary_historical_boundary`, no otra captura/tabla.
Una frontera presente de la canónica API `POINT_STOCK_HISTORY_API` COMPLETE conserva precedencia. Cuando
no existe, consultar bulk snapshots producto desde corte hasta corte+3d. La
autorización técnica posterior del4oct permite también frontera INDEPENDIENTE
junto a canónica INCOMPLETE, sin ocultarla ni transformar su cobertura. XLS de
costeo no se convierten en historia canónica. No aceptar candidatos por conteo.
El manifiesto VERIFIED puede proceder de captura directa o de consolidación de
intentos (`consolidated_point_stock_history_attempts`, apertura6 de agosto).
Exigir fecha, pares esperados completos y retrieved_at postcorte en ambos; el
snapshot aporta su propia prueba, no adopta stock/evidence de la consolidación.
El método consolidado no prueba vacío/cero por sí solo: aplicar exclusivamente
el contrato de originales por par de arriba. No relajar canónicas ni cobertura.
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
o candidatos válidos con stock O ÚltimoMovimiento diferentes dejan frontera sin
prueba; igual stock no basta para elegir entre certificados temporales distintos.

Para frontera independiente junto a canónica, inspeccionar bulk TODOS los raws retenidos del import
exacto como vetos documentales. Exigir FK_Movimiento/row_number, fecha raw UTC,
cantidades/existencias/cancelación y dominio coherentes con lo persistido; unknown
o raw malformado impiden aceptación. Un movimiento efectivo posterior a Ult_Mov
y anterior o igual a la captura contradice el snapshot, incluso si cancelación y
reversión netean cero. En el instante Ult_Mov, un estado incompatible o ambiguo
también bloquea. No inventar vínculo de cancelación/reversión ni compensar entre
cortes. Otros cancelados conservan su tratamiento previo; la excepción histórica
MERMA/CANCELACION DE MERMA exige el contrato específico abajo, no pérdida nueva.
Comparar raw contra la representación persistida según la escala DecimalField
del modelo y el redondeo PostgreSQL; conservar raw íntegro en la huella. Las
ecuaciones, deltas y estado raw en Ult_Mov usan representación original: nunca
redondear fuentes para conseguir coincidencia. FK numérico textual sólo acredita
la misma identidad si es un entero ASCII positivo exacto, no bool, float u offset.

Ausencia legacy de metadata de lote (fetched IDs, recuento, límite o fecha de
descarga), saturación o fetched_rows distinto a retained no
prueban contradicción por sí mismos: tampoco prueban integridad de lote. Conservar
`canonical_membership_verified=False` cuando no esté acreditada. IDs presentes
inválidos, referencias ausentes o row_count distinto del total realmente retenido
son inconsistencia, no ausencia de cobertura. Si la fecha de descarga está
presente, un raw posterior a ella es contradictorio. Varios imports API para el
mismo par no se seleccionan por orden: dejar sin prueba ante canónica ambigua,
conservar las huellas y resolver su identidad por el flujo oficial.
Una canónica INCOMPLETE con huecos anteriores a Ult_Mov sigue INCOMPLETE;
no imponer continuidad ficticia ni certificar historia mediante stock. Tampoco
degradar una cobertura COMPLETE sólo porque su frontera se acredita por snapshot.

Conservar evidencia PK snapshot/job/par/stock/captura/lastMove/contrato/huella.
`snapshot_boundary_verified=True` no implica `canonical_history_verified`:
mantener cobertura MISSING sin canónica y la cobertura canónica registrada cuando
existe (INCOMPLETE o COMPLETE según su prueba), y físico no
acreditado. No reescribir cierre original. La prueba identifica snapshot y
consistencia canónica por separado: lectura independiente no verifica el mes.
Leer bajo mutex mensual compartido/atomic; cache sólo dentro de lectura protegida.
Firma de refresh incluye TODOS los raws retenidos, metadata de import, candidatos
snapshot y raw/captura/status/tipo/log; verificar cambios sin signals,
consulta bulk y segunda lectura de cache sin HTTP. Registrar entrega real después
de CI/deploy/aceptación autenticada, no por actualizar este procedimiento.

### Bamoa: COMPLETE sin frontera concreta no obliga a recapturar

Con el contrato autorizado de esta tarea, evaluar el mismo snapshot independiente
cuando la canónica sea COMPLETE, unknown0 y la apertura O el cierre solicitado
siga sin prueba canónica. Primero revisar esa frontera concreta: si la canónica
la acredita, devolverla con precedencia, sin sustituirla por stock de snapshot.
La etiqueta COMPLETE sola no acredita ambas fronteras ni membresía del último lote.

Reutilizar todos los guards anteriores: par/dominio producto, manifiesto, job/log,
captura postcorte, ÚltimoMovimientoUTC precorte, raw/stock coherentes, candidatos no
divergentes y vetos de TODOS los raws retenidos. Unknown o contradicción impiden
fallback; no elegir el snapshot que cuadra ni usar raw manipulado. Igualdad de
500 filas/500FK no prueba una única respuesta: no fabricar fetched_movement_ids,
cambiar metadata, rebajar cobertura a MISSING/INCOMPLETE o repetir HTTP COMPLETE.

Si sólo el snapshot acredita el extremo, registrar `snapshot_boundary_verified=True`
y `canonical_history_verified=False`; coverage_status permanece COMPLETE y
`canonical_membership_verified=False` si no está acreditada. Apertura y cierre
pueden tener pruebas diferentes; no transferir flags de un extremo al otro.
Mantener mutex/atomic, lectura bulk, huella de todas las fuentes y cache únicamente
en su transacción protegida. Segunda lectura exige HTTP0/escrituras0 y observación
idéntica. No cambiar roles/sold_products ni convertir insumos en producto. La
frontera no acredita conteo físico ni habilita el cierre por sí sola. No presentar este contrato como
desplegado antes de CI/deploy/aceptación reales registrados en el checkpoint.

### Representación original: dos falsos vetos, no tolerancia general

Contrato publicado por PR1473, con aceptación registrada en el checkpoint. Para
comparar movimiento raw y persistido ya interpretados como timestamps aware,
convertir ambos a UTC y comparar el instante exacto. En un fold DST, el mismo
instante puede tener representación local distinta: no veto por formato/zona.
Una diferencia de1µs conserva el veto. No cambiar parser, interpretación naive
por dominio, TIME_ZONE, fechas raw ni cortes operativos para obtener igualdad.

En ecuaciones de cantidades, probar primero igualdad Decimal exacta; conserva
precedencia mediante `Fraction(Decimal(str(value)))`, sin resta dependiente del
contexto Decimal predeterminado. Si falla, sólo es candidata la terna original de cantidad,
existencia anterior y nueva de tipo float exacto, finitos. No convertir strings,
int, Decimal, bool o mezcla de tipos a float para habilitar esta rama. Qty1.0 es
float original; qty1 entero no recibe tolerancia. La igualdad de lo persistido
con raw sigue su guard independiente: una diferencia canónica no se perdona.

Evaluar centros originales con `Fraction.from_float(x)` y radio por operando
`Fraction.from_float(math.ulp(x))/2`; sumar los tres radios y comprobar el residuo
de la ecuación binaria exacta contra esa cota. No calcular residuo/radio con
Decimal de precisión predeterminada, float aritmético ni epsilon fijo. Rechazar
la rama si el signo del delta no corresponde a la acción o la cantidad es cero;
una ecuación exactamente cero conserva la precedencia exacta anterior. Rechazar
la relajación si el radio alcanza el quantum decimal más fino de los campos del
modelo dividido entre10. Con escala3 esa barrera es0.0001; NO es una tolerancia
de aceptación. No redondear a3 decimales ni aceptar todo residuo menor0.0001.

Pruebas de aplicación obligatorias: residuo Decimal real de4e−17 con tres floats;
equivalente textual/Decimal o tipos mezclados que no obtiene tolerancia; cantidad2
con delta1, perturbación0.0004, NaN/Inf/bool, magnitud que alcanza la barrera y
raw incompatible con fila persistida conservan veto. Timestamps aware de mismo
instante en fold pasan; variación1µs falla. Mantener raw íntegro/huella y stocks
del snapshot, identidad y ÚltimoMovimiento sin tolerancia. No generalizar a
ventas comerciales, reglas, cancelaciones ni redondear una fuente para cuadrarla.

El guard sólo evita un falso veto de representación del documento existente.
No acredita membresía, renueva cobertura, aprueba expediente, cambia rol ni
prueba conteo físico. Segunda lectura HTTP0/escrituras0/observaciones iguales;
publicación exige tests, CI, deploy oficial y aceptación autenticada posteriores.

Si falta frontera, documentar par/fecha exactos antes de Point. Usar historia
acotada que cruza el corte; no una fila cercana, stock actual ni resumen legado.
El cliente GetHistorial sólo tiene últimosN, no fecha/paginación acreditadas: no
inventar parámetros ni subir automáticamente a500/descargar el mes completo.
Agotar fuentes existentes y probar contrato literal de filtro antes de ampliación.
Subagentes investigan fuentes guardadas; solo principal abre una sesión protegida.

Opciones literales del frontend:5/10/15/50/100/300/500.101 no es un límite válido:
un error/no-lista no acredita ausencia ni autoriza escalar automáticamente. Una
consulta corta que no cruza el corte no prueba la frontera; conservar raw/par/
SHA/primera-últimaFecha y secuencia para no repetirla. El cliente no acredita
paginación/filtro de fecha; PrintHistorial con mismosN no resuelve ese límite.
GetHeader/GetDetalle puede acreditar folio/componente, no stock de frontera.

Si cobertura ya COMPLETE, no HTTP Point. Si falta exclusivamente cobertura posterior al cierre y el caso cumple las condiciones, documentar ese faltante y capturar únicamente el historial necesario: lote de hasta diez, una sola sesión protegida por `point_account_session_lock`, adquisición sin interferir con sesiones ajenas. Si ocupado, consultar titular en pg_locks/pg_stat_activity read-only cuando haga falta; no liberar candado ni reiniciar servicios. Cerrar sesión en finally.

Usar `AuditStockHistoryService.capture` **sin force**, sobre la importación canónica existente. Verificar que no crea otra importación y deduplica por FK_Movimiento. Inspeccionar firmas/callers actuales antes de preparar un comando; no proporcionar un importador genérico que eluda estos controles.

### Respuesta original completa guardada: ingreso controlado, no recaptura

Con autorización de ingreso de originales, reutilizar la entrada explícita de
`AuditStockHistoryService` y su persistencia compartida con capture; no otro
importador, tabla o cliente falso. Review/plan no ejecutan este ingreso. Leer la
firma real publicada antes de invocarla, sin sustituir parámetros por suposición.

API del contrato de esta tarea: `ingest_original_response(branch, product,
month, rows, *, evidence)` devuelve `PointHistoryReconciliation`. `branch` y
`product` son las entidades Point exactas; month es date del primer día del mes.
Con esas entidades y rows/evidence verificados desde el original, la ejecución es:

```python
result = AuditStockHistoryService().ingest_original_response(
    branch, product, month, rows, evidence=evidence,
)
```

Envelope obligatorio: `source="POINT_STOCK_HISTORY_API"`, `domain="PRODUCT"`,
`response_complete=True`, `branch_id`/`product_id` PK internas enteras;
`request.path="/Stock/GetHistorial"` y params exactamente `tipo="false"`,
`almacen=branch.external_id`, `pkproducto=product.external_id`,
`movimientos=str(history_limit)`, `tipoMovimiento=""` (todos strings).
Además `retrieved_at` ISO con zona, `history_limit` literal acreditado de
5/10/15/50/100/300/500, `fetched_rows=len(rows)<=history_limit`, `original_locator`
real y `raw_sha256=hashlib.sha256(json.dumps(rows, sort_keys=True,
default=str).encode()).hexdigest()`. No añadir parámetros de fecha/paginación.
Cuando el JSON original no guardó la petición, no afirmar que la guardó: acreditar
su reconstrucción desde el script original de adquisición y contrato del cliente.
`request_provenance` es obligatorio: `kind="DERIVED_FROM_ACQUISITION_SCRIPT"`,
`source_file` real, `source_code` íntegro del script guardado,
`source_sha256` SHA256 de ese texto verificado y
`client_contract="PointHttpSessionClient.get_stock_history"`. Comprobar sus SPEC,
par y límite contra esa respuesta, no reconstruir desde un cliente modificado hoy.
El servicio valida el envelope, no lee ni autentica por sí solo un archivo del Mac
desde el VPS. No agregar claves libres para generar otro fingerprint de la misma
respuesta; conservar exactamente el esquema validado.
No usar el ejemplo hasta confirmar que el SHA servido contiene esta API.

Antes de escribir, acreditar respuesta completa de `/Stock/GetHistorial` para un
par exacto: `source=POINT_STOCK_HISTORY_API`, dominio PRODUCT, PK internas y
petición acreditada con sucursal/producto externos y tipo producto. Conservar path,
parámetros y procedencia de su reconstrucción, límite solicitado, cantidad recibida, SHA de la
respuesta y localizador original verificable (`original_locator.source_file` y
`source_line` entero positivo). No inventar ruta o línea para satisfacer el guard.
La lista íntegra conserva cada raw: no seleccionar sólo movimientos que cruzan el
corte, ensamblar dos muestras, rellenar filas ausentes ni usar count de un resumen.
SHA del archivo/manifiesto y file_hash de la canónica no sustituyen SHA de respuesta;
calcularla con la serialización exacta que valida el servicio, manteniendo el
original como evidencia. Rechazar dominio/identidad/petición/recuento/fecha/raw
inválidos antes de cualquier escritura.

`retrieved_at` es el instante original de descarga, con zona explícita, nunca now
ni mtime del archivo. `ingested_at` registra incorporación separada. Un movimiento
raw posterior a su descarga es inconsistente. Ingresar hoy una respuesta antigua
no renueva fetched_at ni cobertura; lote saturado usa membresía/límite/fecha del
lote original, no la unión de filas retenidas. Una frontera probada y una historia
COMPLETE siguen siendo pruebas distintas.

Serializar primero la canónica única del par, incluida su creación, después los
mutex mensuales afectados y escrituras. Deduplicar FK_Movimiento dentro del import,
nunca globalmente entre productos. Conservar procedencia por respuesta y fila;
un original antiguo puede aportar fila ausente sin sustituir metadata de una
respuesta posterior. Conflicto con fila más reciente o de procedencia desconocida
debe abortar atómicamente, no sobreescribirla porque el ingreso ocurre hoy.
`response_provenance` conserva cada respuesta y `movement_fetched_at` su fecha por
FK; `original_responses` archiva el raw íntegro con SHA. Conservarlos, no fabricar
provenance para resolver una colisión. Replay idéntico también valida el archivo
integrado: idempotencia no significa aceptar metadata o raw corruptos.

Antes/después verificar PK canónica, raws, metadata y procedencia; segunda entrada
idéntica con HTTP prohibido no cambia filas/imports/metadata. No añadir avisos,
propuestas ni aprobación. Reconcile decide cobertura real; materialización/cierre
requieren sus guards independientes. Ante originales parciales, conservar el
faltante exacto: no marcarlos completos para acelerar el cierre.

### Membresía original: ocurrencias distintas de identidades contables

Una respuesta íntegra puede repetir exactamente una FK. Usar la misma entrada
`ingest_original_response`, con todas las ocurrencias en su orden original y el
envelope anterior. Aceptar el duplicado sólo si todos los campos y tipos JSON son
idénticos; cualquier diferencia aborta atómicamente. No sumar dos veces cantidad,
deduplicar el archivo ni fabricar una petición menor.500 ocurrencias/499 FK conserva
`fetched_rows=500`, límite500, lista de500 IDs, SHA de500 y499 filas canónicas.

`original_responses` conserva el archivo íntegro; `latest_response_fingerprint`
identifica la respuesta promovida. Archivo, procedencia, SHA, petición, fecha y
membresía deben concordar antes de aceptar cobertura. Metadata legacy sin archivo
verificable conserva rechazo estricto de duplicados. Una captura LIVE posterior
no hereda el puntero de una respuesta original anterior.

Reconcile registra `original_batch_evidence`: firma de archivo/membresía, conteos,
posiciones duplicadas y discontinuidades de saldo. La evidencia acompaña apertura
y cierre documental para invalidar huellas si cambia su fuente. No descartar un
gap anterior al mes: conservarlo visible, sin convertirlo en pérdida ni ignorar
un gap dentro del mes o en el enlace de sus cortes. COMPLETE mensual exige descarga
posterior al fin, alcance original suficiente, identidades/raws coherentes, cadena
mensual continua y ecuación exacta; no certifica toda la historia anterior.

Segunda entrada idéntica:0HTTP/0imports/0filas/0cambios de huella/0avisos. No cambiar
los metadatos antiguos para aparentar que fueron válidos. Review/plan sólo observa;
no gana importación, HTTP, aprobación, conteo físico ni cierre mensual. Confirmar
publicación y aceptación en checkpoint antes de ejecutar el contrato nuevo.

### Corte independiente de una respuesta original parcial

La historia mensual `INCOMPLETE` puede contener una respuesta original íntegra de
5/10/15/50/100/300/500 movimientos que cruce exactamente apertura o cierre. Leer
la respuesta ya archivada con `_original_batch`: validar SHA, petición, recibo
posterior al corte, producto/sucursal de dominio PRODUCT, ocurrencias, membresía,
raw y fila canónica. Ubicar el último movimiento efectivo anterior al corte UTC
Stock y el primero posterior. Deben ser vecinos efectivos en el archivo y en
los hechos retenidos; `Existencia_nueva` del anterior debe coincidir exactamente
con `Existencia_anterior` del siguiente. Un hueco de cadena dentro del intervalo
relevante, movimiento retenido intercalado, campo canónico alterado, archivo o
procedencia inválida vetan la prueba. No derivar saldo de un solo lado del corte.

Registrar IDs de ambos vecinos, corte, stock y firma de fuentes como evidencia
`POINT_ORIGINAL_CUT_BOUNDARY_V1`. Este contrato acredita **sólo el corte**:
`coverage_status` permanece `INCOMPLETE`, `canonical_history_verified=False`,
`physical_count_verified=False`. No reingresar ni consultar de nuevo Point para
usar la evidencia; no llamar `resolve_stock_at_close` sin la prueba de membresía
original. Una respuesta sin vecinos conserva `effective_stock=None` y el bloqueo.
Si una captura LIVE posterior dejó de marcar ese original como «último», leerlo
por su fingerprint archivado sin alterar `fetched_at` ni la cobertura canónica;
varios archivos originales candidatos permanecen ambiguos y no se elige uno
por orden de entrada, fecha aproximada o conveniencia del saldo.

Registrar una evidencia reciente por par y no derivar cantidades de la posición
de una tupla: separar sales/transfer_in/transfer_out/ajuste/conversión. Un preflight
que encuentra ventas distintas aborta antes de sesión; diagnosticar contra fuente,
no corregir ERP para hacer coincidir un literal. Conteo retenido mayor500 puede ser
conservación de originales; no prueba una consulta mayor500. Después de COMPLETE,
segunda capture con HTTP prohibido y mismas filas/canónica/avisos; no recapturar.
Si el guard mensual sigue falso, conservar la proyección anterior como pendiente:
ni helper independiente de frontera ni captura individual autorizan materializar.
Un hallazgo fechado COMPLETE no sustituye reconcile actual ni transforma MISSING.

Comprobar ajustes por delta existencia_nueva−existencia_anterior y consistencia de magnitud/dirección; quantity positiva puede ser SALIDA. No invertir dos veces cantidades firmadas ni resolver contradicciones como cero. Preservar unknown cuando la evidencia contradiga el movimiento.

### Merma cancelada y reverso: efecto histórico individual, no merma vigente

Reparación `auditor-merma-reversion-historica` publicada mediante PR1476 y aceptada
con evidencia en el checkpoint. El RED documental no tenía ruta ejecutable para tipo5 cancelado
y tipo15; excluir el primero y sumar sólo el reverso inventaría un crédito. No
eliminar cancelados de la secuencia antes de comprobar esta excepción estricta.

Delimitar siempre import canónico, sucursal, producto y dominio PRODUCT. Leer cada
raw original con su FK_Movimiento y fecha StockUTC efectiva; un mismo movimiento
puede existir en otros productos/imports con distinta cantidad. IDs raw presentes
deben ser coherentes con ese ámbito; no buscar la pareja por FK global, hora,
nombre o cantidad. La clasificación no inventa vínculo entre documentos.

|Documento original especial|Efecto documental exigido|Waste firmado|
|---|---|---|
|FK_Tipo_Movimiento entero exacto5, nombre normalizado MERMA, CanceladoTrue|existencia nueva−anterior exactamente−qty; isCargoTrue si presente|+qty débito|
|FK_Tipo_Movimiento entero exacto15, nombre normalizado CANCELACION DE MERMA, CanceladoFalse|existencia nueva−anterior exactamente+qty; isCargoFalse si presente|−qty reverso|

Tipo bool/string/float no es entero exacto; no inferir tipo por similitud del
nombre ni aceptar alias sin «DE». Flags sólo bool o strings true/false acreditadas;
ausencia de isCargo no autoriza inventarlo, presencia contradictoria veta. Cantidad
debe ser positiva finita y existencias finitas, sin bool; delta debe cumplir
magnitud/dirección exactas. No epsilon, redondeo ni nueva tolerancia binaria para
rescatar un especial contradictorio. Tipo15 cancelado, raw malformado, dominio/
identidad incoherente o qty2 con delta1 quedan unknown/veto, no neto0 aproximado.
MERMA ordinaria tipo5/CanceladoFalse conserva su tratamiento previo, sin imponerle
las restricciones nuevas de estos especiales; otros cancelados no reciben crédito.

Acumular cada efecto sólo si su instante pertenece al intervalo operativo del mes;
expected_closing resta el waste firmado. Dos documentos4→0 y0→4 en septiembre
aportan+4 y−4, neto0; si el débito es31agosto y el reverso1septiembre, septiembre
contiene−4, no0. Una frontera entre ambos conserva0 como estado después del débito,
no resta el reverso futuro. La secuencia acredita efectos, no pérdida física,
merma vigente ni FK de cancelación pareada. No exigir pareja para reconocer un
evento íntegro ni compensar importaciones/productos/períodos distintos.

Reutilizar el helper puro compartido en reconciliación, resolución del cierre y
vetos snapshot; no tres clasificaciones divergentes. La excepción no omite raw
cancelado en los vetos temporales: un efecto posterior a Ult_Mov puede contradecir
snapshot aunque más tarde se revierta. Conservar raw/huella, cobertura, membresía,
unknowns/gaps y autoridad mensual independiente. No editar PointWasteLine/MermaPOS,
imports, filas, timestamps o flags para hacer cuadrar la proyección.

Pruebas exigidas antes de publicar: débito+reverso, reverso aislado documentado,
corte entre eventos y cruce agosto/septiembre; producto263 qty1 distinto de120qty4
con mismosFK; ordinaryMERMAfalse sin regresión; tipo/nombre/flags/isCargo/delta/
dominio contradictorios desconocidos; snapshot temporal vetado aunque neto0.
Segunda lectura HTTP0/escrituras0/observaciones iguales. Review/plan sólo observan;
tests/CI/deploy y aceptación nativa/UI reales preceden a declarar entregado el
lector, y no sustituyen físico, documentos humanos o guards del cierre mensual.

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

### Clasificación comercial documental, no identidad transaccional

Reparación `auditor-clasificacion-documental` publicada y aceptada por PR1477. Usar
los lectores mensuales compartidos; no modificar el matcher financiero/global,
maestros, ventas, MermaPOS ni filas de origen. Regla por nombre es clasificación,
no FK: las filas originales Coca450/Vela875 no aportan FK de producto. Nodo
corroborante no se asigna como PointProduct al movimiento ni prueba ejecución.

Autoridad se evalúa sobre TODAS las filas mensuales antes de separar dominio
fabricado. Nombre exacto usa `ProductBusinessRule.normalize_product_name`
(`strip().upper()`), no alias por tokens ni normalización sin acentos. Exigir
coherencia del raw con nombre/código/cantidad/unidad/sucursal originales, ausencia
de receta/insumo contradictorios y corroboración de catálogo de dominio producto.
Nodos candidatos inconsistentes o múltiples identidades conservan unresolved;
no elegir primero por SKU. No hardcodear PK de filas o nodos como prueba.

Coca450: regla fija existente REVENTA con nombre completo COCA-COLA 450 ML;
catálogo original código COCA450, familia Bebidas/categoría Coca-cola. No enlazar
al producto external235 ni inferir compra/precio/conteo. Vela875: documento
original código875/nombre VELA INDIVIDUAL, catálogo coherente familia Velas y
categoría Alegría bajo criterio comercial autorizado. Categoría o ProduccionFalse
solos no convierten otros artículos en accesorios. Extra10 queda pendiente.

Preservar filas y contadores de origen; exclusión es sólo del balance fabricado.
`excluded_documentary_rows` registra fuente/ID/hash, cantidad/unidad/sucursal,
criterio versionado, rawSHA, nodos/runs/raw corroborantes y regla/classification/
is_fixed/updated_at efectivamente usados. Orden determinista, sin now por lectura.
Vela AGG conserva `execution_origin_verified=False`;23AGG fabricados no reciben
origen por receta/configuración/factor ni por esta exclusión.

Firmar sólo decisiones usadas en metadata canónica waste/conversions, no un digest
global de catálogos ajenos. Un cambio relevante debe invalidar proyección antigua.
Al sellar, dentro de transacción oficial, proteger ambas tablas de catálogo con
SHARE NOWAIT en orden fijo antes de la lectura fresca y comparación de huellas.
Si ocupado, fallo cerrado/rollback: no esperar, reintentar en bucle ni rebajar guard.
SHARE permite lectura y bloquea INSERT/UPDATE/DELETE incluidos phantoms. Pausa
global de escritores mientras preview/sellado; no prometer techo no medido. En
build(lock_after_build=True) pueden existir mutex previos: NOWAIT conserva fallo
cerrado, no afirmar orden global universal ni tomar Point durante el sellado.

Publicación exige autoridad sin cambios, exclusiones exactas y segunda lectura
idéntica/HTTP0/ops0, tests de ambigüedad/mutación/firma/concurrencia y aceptación
nativa/UI. Documentación de contrato no certifica despliegue ni cierre mensual.

### Compra por descripción: no convertir FK derivada en identidad original

Revisar el raw de compra y el writer antes de afirmar identidad: un coste con
FK PointProduct puede venir de un resolver nombre/SKU/alias, no del documento.
Conservar folio/ID de compra, proveedor, fecha, descripción, unidad, cantidad y
costes como evidencia documental; no reasignar ventas ni curar una regla desde
esa FK. Dos representaciones de la misma compra no prueban dos recepciones:
identificar el folio/ID común y conservar ambas fuentes antes de evaluar un coste
ponderado. No reparar costes como parte de una conciliación sin autorización.
El checkpoint contiene el ejemplo CakeTopper PLATA/ROSA A16242. Un resultado
vacío de dos tablas para NEGRO no prueba ausencia de compras en todo el ERP.
La clasificación DG y su alcance histórico requieren decisión real, sin retrofecha.

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
