# Continuidad septiembre 2026 — corte 5 de octubre de 2026 UTC

Este archivo es un checkpoint, no una consulta viva. Revalidar hechos cambiantes con fuentes autorizadas y frescura necesaria; no volver a descargar hechos ya acreditados por rutina. Hilo/automatización: `01a0d98e-e017-7253-b4c9-9758f56c3827` / `conciliar-septiembre-hasta-cierre`. Septiembre NO cerrado. Regla humana vigente: no alterar inventario, mermas, ventas, RRHH o ajustes para forzar resultado.

## Estado verificado que sustituye bloqueos históricos posteriores

### Corte 6oct2026 posterior: cinco destinos con originales íntegros

Una consulta Point protegida, inicialmente sin persistencia, obtuvo cinco
historiales Stock exactos de producto/sucursal que no tenían importación ERP.
Cada GET tuvo
límite500, 500 FK únicos en orden descendente y llegó a fecha anterior al
1sep07:00UTC. Wire, petición, recepción y SHA íntegros están en
`consulta-cinco-destinos-conversion-resultado-20261006.jsonl` del expediente.
Entradas tipo21 no canceladas de septiembre: AGG192 Las Glorias/Fresas con
Crema Rebanada6 = FK1674315(6); AGG197 Matriz/Pay Plátano Rebanada4 =
1664857(4); AGG201 Matriz/Fresas con Crema Rebanada66 =
1661105(10),1663777(10),1666159(10),1674492(6),1674858(10),1677754(10),
1680582(10); AGG202 Matriz/3 Leches Rebanada6 = 1662640(6); AGG204
Matriz/Ciruela Rebanada10 = 1661108(10). SHA payload y wire, parámetros,
filas y orden verificados independientemente. No reconsultar estos cinco por
rutina.

El mismo original de AGG192 muestra salida tipo22 FK1674489, 6 PZA de
Fresas con Crema Rebanada Las Glorias, el 17sep23:32:19.760UTC,
`12→6`. La entrada FK1674315 de 6 ocurrió el mismo día a
21:30:41.770UTC, `6→12`. Antes de reconstruir, el caso ERP2958 tenía entrada
por conversión6, salida0 y diferencia−6. El preflight oficial acreditó
`COMPLETE`, desconocidos0, remanente0 y ventas41/41. La reconciliación acotada
por servicio oficial incorporó la salida6 y dejó diferencia0; segunda ejecución
seleccionó0 casos, HTTP/captura0. El caso conserva `NEEDS_EXPLANATION` y
`MISSING_CONVERSION_ORIGIN`: no inferir por la separación horaria que fue una
reversión ni atribuirla a una persona. La primera ejecución agrupó dos avisos
de atención alta; la segunda no emitió duplicados.

**23/23 destinos de producto** del agregado coinciden ya en suma mensual de
entrada tipo21:18 por historiales importados previos y cinco por originales
preservados que se ingresaron después como importaciones **856–860** mediante
el servicio oficial, sin HTTP. Cada una tiene 500 filas originales/canónicas,
`COMPLETE`, desconocidos0 y extremos documentales; segunda ingesta cero
cambios y 13 tablas operativas protegidas intactas. Es prueba de cantidades de destino, NO
de origen/ejecución individual, cobertura canónica COMPLETE, conteo físico
para otros productos ni cierre. El ingreso se condicionó a cadena, apertura,
cierre, remanente, desconocidos y ventas coherentes, no a la sola suma. El
ingreso por sí solo no rematerializó casos; sólo el paso acotado posterior
actualizó 2958, sin declarar resuelto su origen.
Extra10 y Vela se tratan aparte.

### Corte 6oct2026: conversiones por presentación, sin cerrar origen

Mauricio revisará con los programadores de Point el salto 23→22 piezas entre
1668666 y 1668695 del producto 0065. No repetir esa consulta, imputar una pieza
ni atribuirla a un usuario. El resto de la conciliación continúa independiente.

En CEDIS, el agregado179 Zanahoria Chico2 corresponde a una secuencia original:
1677659 entrada Rebanada16 / 1677660 salida Chico2; 1677686 entrada Chico2 /
1677687 salida Rebanada16 (reversión); 1677688 entrada Rebanada12 / 1677689
salida Chico2. Neto Chico −2, Rebanada +12. La interpretación operativa de
Mauricio es que se deshizo un corte inicial de ocho rebanadas por chico y se
repitió a seis para obtener piezas de tamaño adecuado. Es consistente con los
movimientos, pero no sustituye motivo ni actor documentales. No imputar cuatro
rebanadas faltantes. El rendimiento depende de tamaño/presentación: no aplicar
una razón universal ni suponer que sólo se rebanan pasteles medianos.

Cruce read-only de 23 destinos de producto `AGG-*` del job77629 contra entradas
Stock tipo21 guardadas, con sucursal ERP y SKU de producto únicos: 18 coinciden
en cantidad mensual; **sólo acredita suma de destino**, no origen/ejecución ni
cobertura completa. Cinco carecen de importación Stock del par exacto:
192 Las Glorias/Fresas con Crema Rebanada6; 197 Matriz/Pay de Plátano Rebanada4;
201 Matriz/Fresas con Crema Rebanada66; 202 Matriz/3 Leches Rebanada6;
204 Matriz/Ciruela Rebanada10. No afirmar que faltan en Point. Extra10 y Vela
no forman parte de esos 23 productos. Conservar el guard de orígenes sin folio
transaccional y no materializar ni cerrar por esta comparación.

### Corte5oct2026: frontera2354 y causas todavía separadas

PR1485 merge b6711b2 publicó seis cierres de originales parciales (2082,2117,2118,
2119,2120,2237) sin elevar cobertura INCOMPLETE. Aceptación dos lecturas iguales
HTTP0/ops0, native2082 dos revisiones iguales y UI Orquestación autenticada.
Preview íntegro posterior `lock_ready=False`: quedan **3 fronteras** (2131 apertura y
cierre;2354 apertura),23 orígenes AGG,2 destinos Extra10 y7 ventas CakeTopper.
No repetir preview por rutina ni tratar los95 issues proyectados/155 crudos como
pérdidas. Expediente `publicacion-corte-original-parcial-pr1485-20261005.md`.

Caso2354 Sabor Galleta Cajeta Mediano, Colosio: Point externo producto832/sucursal5,
ERP producto235/sucursal4. Línea8867 apertura31ago stock0 tenía sólo dos intentos
vacíos/stock actual0. GET original protegido100 y luego500 devolvió `[]` HTTP200;
el500 fue recibido5oct23:58:03 UTC, SHA wire
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`.
Snapshots28602633/28605628, jobs34842/35042 SUCCESS, stock0 y raw ÚltimoMovimiento
vacío rodean el corte1sep07:00UTC. La respuesta debe archivarse como evidencia de
corte independiente, **no** como importación canónica COMPLETE. No repetir Point.
El reporte comercial conserva venta923543,1PZA el1sep. Consulta Point acotada y
protegida encontró una sola nota del producto ese día: PK_Nota897829/folio65207,
Colosio, cabecera FK_Sucursal5. Su detalle reúne en **el mismo documento** `0002`
Pay de Queso Mediano1PZA/$380.01 y `SGALLETACAJETAM`1 línea/$0. La relación
de addon3 activa/APPROVED enlaza receta107 con base70; la identidad transaccional
queda acreditada por la nota, no por cercanía horaria/nombre. El complemento es
una selección de esa venta, no una segunda pieza con salida propia de Stock.
La ecuación guardada0−1=−1 contra Point0 todavía da diferencia+1 porque el caso
conserva la línea comercial original; explicarla no autoriza normalizar ventas ni
descontar la base dos veces. No atribuir error a persona. Conteo físico inexistente.

### Corte5oct2026: vacío/cero publicado y membresía original en reparación

PR1482 merge721f5c84, CI37352912991/validate37352913282 PASS. Deploy oficial agotó
readiness inicial durante arranque; comprobación posterior HEAD/check0/migrate0 y
readiness301 acreditó publicación. Dos lecturas iguales HTTP0/ops0 con huellas
intactas:11aperturas/18cierres todavía sin prueba, no551 pendientes. Native3341
runs5041/5042/context13 iguales y Orquestación autenticada/consola[]. Recursos
temporales propios retirados con respaldo verificado. No repetir entrega1482.

Seis consultas de integridad autorizadas conservaron respuestas íntegras originales:
2133/52,2135/53,2250/75,2451/144,2521/755 tienen500ocurrencias/499FK;
3877/672 tiene285/284. Duplicados exactos de contenido y tipos, no dos movimientos
contables. SHA/request/fecha/raw preservados; cadena septiembre y ambos cortes
continuos, discontinuidades anteriores permanecen visibles. Sustituye la ausencia
de respuesta íntegra del antecedente de abajo, no autoriza editar metadata vieja.
Implementación `auditor-membresia-original` usa el contrato del procedimiento;
pruebas y documentación NO acreditan publicación, ingreso ni cierre mensual.

Preview oficial post1482 READONLY/HTTP0/refreshFalse mantiene lock_readyFalse:
29fronteras,23orígenesAGG,2destinosExtra10 y7ventasCakeTopper. Son pendientes de
prueba/clasificación, no pérdidas. Investigación residual Point debe conservar
la sesión única; ocupado significa0HTTP y liberación natural, nunca desbloqueo.

### Autorización5oct2026: originales vacío/cero e integridad acotada

Mauricio entregó orden expresa de ejecutar: autoriza el contrato estricto de
vacío/cero original, frontera independiente junto a INCOMPLETE, ingreso oficial
de originales y consultas Point de integridad para defectos documentales probados
incluso con etiqueta COMPLETE. Esta orden sustituye los permisos pendientes de
esos contratos; no autoriza cambios operativos, clasificaciones por nombre,
equivalencias, explicación humana inventada ni rebajar controles del cierre.

Implementación `auditor-fronteras-cero-original`: lector compartido valida sourceIDs
y líneas originales directas de consolidado por par, fechas propias y firma;
canónica vacía precorte puede coexistir con prueba de cero postcorte, preservando
INCOMPLETE. Movimientos conocidos previos, incluso cancelados, vetan la consulta
vacía; metadata inválida/duplicada y desconocidos siguen fail-closed. Review/plan
mantiene sus límites. Publicación y aceptación deben acreditarse por ciclo oficial;
este texto y tests no declaran septiembre cerrado ni522 fronteras aceptadas.

### Corte 5oct2026UTC: PR1477 aceptado y fuentes todavía pendientes

PR1477 head8616cac6, CI37256483016 intento2 y validate37256483054 completos PASS;
merge b2b9372d41d637ae364a12f656fd36e4b97a46ee, deploy oficial exit0/web-ready,
HEAD/check0/migrate0/readiness301 frescos. Lector dos observaciones READONLY
iguales: autoridad267mermas/26conversiones, exclusiones únicamente1492 REVENTA
1PZA y193 ACCESORIO11PZA, sin FK transaccional ni origen de ejecución inventados.
HTTP0/ops0/materialización0/huellas12tablas intactas, aceptación SHAarchivo
adb6641ae3757ca733ceb2a224853aefb63bfcccc7b463ffebdbe3735047cd38.
Native3220 runs5015/5016 SUCCESS/context13/igual/HTTP0/capture0/ops0/avisos0;
Orquestación autenticada dos mensajes Exitoso/consola[]. Stdout inicial truncado:
dos result_summary_json originales recuperados sin repetir el agente, payload
íntegro SHA31cebbdccc739ab53507874f1ca4f9a6a7803798327f461a36db66d431ead64e.
Tarea cerrada y entorno5480/6480 propio retirado con respaldo verificado.
Este corte sustituye «Coca/Vela en implementación» del antecedente de abajo.

Un preview posterior íntegro READONLY/HTTP0/writes0/refreshFalse conserva
lock_readyFalse:551fronteras (450apertura/101cierre),23orígenesAGG,2destinos
Extra10 y7ventasCakeTopper. Payload comprimidoSHA
8e25d3f6415052d43e0baec7d3557ae99afbbc7a328e05a2a7d4fb311d4a35ac.
959/374 del diagnóstico post174 son antecedentes, NO pendientes actuales ni
aceptaciones por resta. No repetir preview/capturas por rutina. Fuente completa
requiere pruebas incluso de productos no elegibles para proyección: exclusión
de rol no acredita ausencia de stock ni autoriza omitir el guard global.
El corte no acredita agotadas las alternativas técnicas ni cierre mensual.

Diagnóstico posterior READONLY sobre b2b9372d, sin otro preview/HTTP/ops:
551 se separan en522ceros documentales,17residuos anteriores y12fronteras
reaparecidas por fail-closed. Los522 son439aperturas del manifiesto consolidado6
con procedencia por línea en intentos originales4/5 (413en ambos/26en uno), y
83cierres del manifiesto directo7 preemptados por canónicaINCOMPLETE. Stock0,
history_rows0/history_limit500/createdpostcut NO bastan bajo el contrato actual:
no convertir en COMPLETE. La aprobación5oct de arriba sustituye el pendiente
anterior; son bloqueos técnicos, no evidencia humana ausente. Aceptación efectiva
requiere lector publicado y comprobación nueva, no resta sobre551.

Las12 nuevas son ambos cortes de2133/2135/2250/2451/2521/3877. Imports52/53/75/
144/755 tienen500IDs/499únicos;672 tiene285/284. Hardening4c4699b9 los veta:
no permiso para deduplicar metadata ni ignorar membership. La orden5oct permite
consulta Point acotada de integridad por estos defectos exactos, conservando
capturas anteriores; no recaptura rutinaria de COMPLETE. No existe respuesta íntegra guardada que pruebe
esas seis membresías. Conservar resultados anteriores como antecedentes fechados.

Los17 residuos: ocho fronteras de2076/2084/2131/2149; siete cierres sin import
2081/2082/2117/2118/2119/2120/2237; cierre2083 sin frontera admisible; apertura2354
two_independent_no_history_current_zero. Tres respuestas íntegras existentes de
2082/2119/2120 pasan validación offline de entrada:10filas/límite10, fecha original,
SHA y script de adquisición. Ingreso autorizado no equivale a cobertura mensual
ni frontera aceptada: respuesta saturada y sin comienzo de septiembre conserva
INCOMPLETE. No repetir Point ni confundir este contrato con el de522ceros.
Ingreso oficial posterior ya completado: imports846/847/848,10filas cada uno,
unknown[], INCOMPLETE y ambas fronteras null. Fechas originales conservadas;
dos ingresos iguales/segunda0imports/0filas/0cambioshuella/HTTP0/ops0/avisos0.
Casos/ventas/waste/cierres/MermaPOS/propuestas intactos. No reingresar por rutina.
Corrección de procedencia2083: unknown1680271 corresponde al diagnóstico post174
antiguo, NO al lector vigente. PR1476 acredita débito/reverso válidos de import43,
unknown[] y merma neta0. Apertura7356 ya resuelta; permanece cierre7/línea9244,
CEDIS branchPK3/Point8 productoPK120/Point135/SKU0135, canónicaINCOMPLETE sin
snapshot de cierre calificable. No repetir reparación, HTTP ni afirmar caché fallida.

### CakeTopper: compra existente por descripción, no regla DG automática

Consulta ERP READONLY5oct03:27:36.907UTC, cuatro costes/dos históricos,
HTTP0/writes0. Raw POINT_PRODUCT_HISTORY/GetComprabyId acredita compra1668859,
folioA16242/11sept/IMPRENTA MARCO POLO/Almacen. Cost5797 ERP990/Point1052:
CAKE TOPPER FELIZ CUMPLE ESTRELLA PLATA,100PZA,3944MXN/39.44unitario;
Cost5799 ERP993/Point1055: CAKE TOPPER MAKE A WISH ROSA,100PZA,
4524MXN/45.24unitario. Original íntegro SHAarchivo
0bfb4785656eaf30c323ec92295c6140dfcb1b165bc7b82b9ea0a03354419be0,
fuente-compras-caketopper-original-20261005.json; rollout subagente
01a1079c-2766-73a0-a8f6-89e2ecd07417 línea1988.

Raw Articulo no contiene PK/FK producto. Writer
PointPurchaseResaleCostSyncService.sync_purchase_payloads→_resolve_product
resuelve nombre/SKU/alias: FK coste derivada, no identidad transaccional.
Versiones5351/5797 y5353/5799 conservan misma compra/folio/cantidad/costes
con descripciones distintas. Hist391/393 MONTHLY_WEIGHTED sample2/qty200
suman dos representaciones: NO dos compras/recepción200PZA. No reparar costes,
reasignar ventas ni curar maestros dentro de esta conciliación.
NEGRO ERP994/Point1059 sin filas en las dos tablas consultadas, no ausencia
universal de compra. Siete ventas7PZA/630MXN preservadas; permanece decisión
DG exacta de clasificación y alcance septiembre, sin retrofechar. Configuración
no reemplaza esa decisión; esta compra no explica23AGG ni acredita conteo físico.

### PR1476 merma/reverso publicado, aceptación5oct2026UTC

CI37252263255/validate37252263268 PASS head0505970f, mergecb87ef09,
deploy oficial exit0/web-ready; HEAD/check0/migrate0/readiness301 frescos.
Dos lecturas READONLY idénticas HTTP0/ops0/materialización0/originales intactos,
SHAc00a4311a5b9a7ab6d40bf78c9f99b2c6c0398dee19814af075010effafd9254.
Import43 conserva débito4/reverso4 unknown0/cobertura calculadaINCOMPLETE.
Import839/CEDIS/product263 conserva300filas y débito1/reverso1, unknown0,
ecuación documental−18+232conversión+1entrada−226salida−4merma=−15,
extremos−18/−15. No se persistieron cobertura ni saldos ni se explicó origen físico.
Review2169 corridas5010/5011 SUCCESS/context13, observaciones iguales,
SHAf4a44c50824703b5d24b31d77db4a535cfadfdf2c8ec36e33621d143d9caba38,
HTTP0/capture0/ops0/avisos0. Orquestación autenticada runkeys
c80fc3dcb1/51d9f9f0d2,4oct19:18Mazatlán/consola[]. Tarea merged y recursos
propios5478/6478 retirados con respaldo verificado. No repetir entrega/diagnóstico.
Este corte sustituye sólo estado unknown1680271/lector pendiente anterior abajo.

### Antecedente anterior a PR1477: Coca/Vela en implementación (superado)

READONLY5oct waste1492/mov1666364/Colosio/8sept/1PZA/rawnombre
COCA-COLA 450 ML/regla3 fijaREVENTA desdeabril/nodo2431 run79 Point850;
conversion193/job77629/AGG/Matriz/código875/VELA INDIVIDUAL/11PZA,
nodo2332 run72 Point1001/familiaVelas/categoríaAlegría.
Las filas no tienen FK producto: nodos corroboran clasificación comercial,
NO identidad transaccional. No inferir compra/existencia/ejecución de esos PK.
Vela tiene fecha default del reporte, no fecha individual;23AGG fabricados
y Extra10 conservan faltantes. Fuentes/raw actuales verificados en expediente,
sin HTTP ni escrituras. Ver procedimiento/ficha de fuentes; no afirmar publicada
la reparación hasta CI/deploy/aceptación. Septiembre todavía requiere guard real.

Producción VPS, verificación4oct2026: Mauricio autorizó además la sincronización
mensual protegida y la reparación lectora Stock UTC sin reescribir originales.
Job81612 fullSept1–30, sin filtro sucursal, SUCCESS: seen267/updated267/superseded0;
267 PointWasteLine y267 MermaPOS hashes1:1, autoridad mensual wasteTrue, movimiento
1683114 y parPK1676 intactos; stock/ventas originales preservados. No repetir sync
por rutina ni editar manifiestos anteriores. La recuperación1455 ya estaba completa.

PR1456 lector de identidad de transferencias publicado y aceptación autenticada
completa: LotusPayán3432 muestra32 recibidas; LotusMatriz3232 muestra129; Ciruela3963
reutiliza documento44672 con una pieza. Recupera documentos omitidos, no prueba
cierre físico ni elimina diferencia comercial. Las notas históricas de bloqueo
78023/266filas y entrega1456 pendiente describen su fecha, NO el estado de este corte.

Contrato actual Stock naiveUTC confirmado por frontend, detallado en procedimiento.
PR1457 StockUTC publicado: CI37175301193 PASS/head e9785af7, merge4900b18d,
deploy oficial/check0/migrate0/readiness301 y aceptación autenticada. Revisiones
4963/4964 HTTP0/ops0/avisos0/context13archivos/consola vacía; originales intactos,
tarea/entorno5471 cerrados con respaldo verificado. No repetir entrega.
Cierre mensual aún requiere servicio oficial y guards reales; no es un conteo.

## Autorización técnica posterior y fronteras independientes

Adjunto humano4oct autoriza expresamente: snapshot independiente junto a canónica
INCOMPLETE sin unknown/contradicción; entrada oficial de evidencia histórica
original completa conservando SHA/count/limit/retrieved original/identidad; lectores
de reglas ya aprobadas/cancelaciones; conocimiento nativo. No volver a pedir esos
permisos. No autoriza inferir compras CakeTopper ni ejecuciones AGG ni conteos.

Tarea `auditor-fronteras-independientes` entregada por PR1467 con publicación y
aceptación verificadas en el seguimiento. Implementa el primer contrato; no amplía
las capacidades del goal review/plan. La entrega del código no cierra septiembre.
Diagnóstico post174 encontró959 fronteras sin prueba;374 tienen candidato snapshot
junto a INCOMPLETE, no374 aceptaciones. Los originales retenidos se inspeccionan
como vetos, no como historia íntegra. Import454 conserva499/fetched500 sin IDs;
eso no invalida por sí solo snapshot28799456 pero mantiene membresía/cobertura
no verificadas. Unknown/raw inválido o movimiento posterior al Ult_Mov conocido
sí impiden acreditar. No repetir esas capturas ni aceptar por conteo.

174 canónicas ya capturadas COMPLETE, segunda ejecución sin HTTP/filas/imports/
duplicados/avisos; los24 primeros están abajo y los150 posteriores se conservan
en manifiesto externo de capturas aceptadas. Estado real posterior al lote:
lock_readyFalse,95 issues de proyección; ni pérdidas ni totales definitivos.
No repetir preview por rutina: hacerlo tras una integración material. PR1463
lecciones publicada/aceptada (mergefa260722, runs4987–4992/context13) no cerró el mes.

### Originales completos existentes: PR1469 entregado, no cierre mensual

Tarea `auditor-history-originales`, 4oct2026: manifiesto de17aperturas contiene
14 respuestas originales completas verificadas y3 resúmenes sin raw íntegro.
Las14 se incorporaron por entrada controlada; las3 siguen excluidas, no se
completan con primera/última fila ni count de resumen. Conservar petición/par/
dominio/SHA/límite/count/fecha original/localizador; ingreso actual no acredita
consulta actual. Procedimiento arriba distingue procedencia, locks e idempotencia.
Las peticiones de esas14 respuestas no venían en el JSON: se acreditan mediante
SPEC de4scripts de adquisición originales verificados y contrato del cliente,
con SHA/procedencia separada. No presentarlas como requestliteral guardado.
Estas fuentes no autorizan repetir HTTP de las174COMPLETE ni inventar apertura0.
PR1469 publicado/aceptado:14originales,4081 filas, imports829–842; segunda ejecución
0imports/0filas de historia, HTTP0 y cambios operativos0. Trece observaciones
unknown0;2169 conserva unknown1680271, no se convierte en historia resuelta.
Parciales originales2122/2123/2235 no ingresados: no inventar respuesta completa
para ellos. Capturas nuevas completas posteriores se registran por separado abajo.
Review nativo2075 corridas5000/5001, contexto13archivos, UI autenticada/consola[];
tarea1469 y sus recursos locales cerrados con respaldos propios verificados.
No repetir ingreso, captura o entrega por rutina.
Septiembre y los expedientes humanos siguen sujetos a sus guards y evidencias.

### Bamoa: PR1471 publicado, ocho fronteras aceptadas y cuatro vetadas

Imports721/778/785/197/816/817, casos2459/2562/2574/2580/2610/2611:
branchPK5/Point2,500filas/500FK, COMPLETE sin membresía del último lote acreditada.
Snapshots apertura28605158/28605320/28605343/28605349/28605322/28605261 y
cierre28797254/28797422/28797446/28797452/28797424/28797363 respectivamente;
stock0, apertura job35042/log260894, cierre job77519/log299172.
Estos IDs acreditan candidatos exactos; aceptación1471 dio4pares/8fronteras y
vetó2pares/4fronteras por representación, no seis aceptaciones. Imports785/816
conservaron esos vetos; no editar raws/metadata ni recapturar COMPLETE.

Tarea `auditor-bamoa-fronteras`: autorización vigente permite snapshot independiente
junto a COMPLETEunknown0 sólo si falta la frontera concreta, con vetos raw y
precedencia de frontera canónica presente. Mantener COMPLETE separado de
canonical_history_verifiedFalse cuando sólo snapshot prueba el extremo. No crear
fetchedIDs, HTTP ni prueba física; casos2574/2580 conservan sus roles excluidos.
PR1471: CI37244309158 PASS/head4c4699b9186d0a7ade4ef68b8945eb19508ad817,
merge b3317f74; deploy oficial con HEAD/check0/migrate0/readiness301.
Reviews nativas2459 corridas5004/5005 SUCCESS/context13, SHA observación
8b3a798c0dc821e6a0798e20665db7d50d2b73d11747bcf1bf7e194b1e42f7d9,
observaciones iguales/HTTP0/capture0/ops0/avisos0; UI autenticada dos mensajes,
consola[]. Entorno LOCAL5476/6476 retirado con helper/respaldo verificado
evidencia1791159769475055000 y red exacta retirada; tarea cerrada oficialmente.
No repetir entrega. Aceptación de fronteras no es materialización de proyección
ni cierre de septiembre, tampoco conteo físico.

### Tres respuestas nuevas completas: originales parciales no rehabilitados

Consulta nueva4oct2026 23:48UTC, ingreso oficial separado:100filas cadauna,
COMPLETE/unknown0. No recrear su fecha de descarga desde los resúmenes antiguos.

|Caso/import|Apertura→cierre|retrieved_at original UTC|SHA respuesta|
|---|---|---|---|
|2122/843|2→2|2026-10-04T23:48:28.772142Z|00db284c26fb1fc19a494046ca89982d5bba8d2773522d3de759c3701f7c0543|
|2123/844|1→4|2026-10-04T23:48:29.479177Z|e1a3aa8d1b71ffa005609b5342bbbc4c6295c31436b12abafbab0de3a42cc86d|
|2235/845|19→18|2026-10-04T23:48:30.115986Z|84355946f2da5db04a8a4784716350db5d590a1f7f3a0ec96971ef7f75892e7f|

Segunda ejecución0HTTP/0filas/0imports/0duplicados/0ops; materialización0.
No repetir HTTP/ingreso: la evidencia nueva no vuelve íntegros los3resúmenes
antiguos ni prueba entrega física/aprobación/mes cerrado.

### Representación original: PR1473 publicado y aceptado, no cierre mensual

Tarea `auditor-representacion-original`: import785 raw/persistido representan el
mismo instante DST pero el veto comparaba representación; import816 tiene diez
VENTA con qty1.0, anterior1.300000011920929/nueva0.30000001192092896 y residuo
Decimal4e−17. Son fuentes existentes, no diez piezas perdidas ni ajuste nuevo.
Procedimiento define comparación UTC exacta y rama binaria de tres floats finitos,
con precedencia Decimal y barrera de magnitud. No tolerancia a strings/mixtos,
cantidad2/delta1, discrepancia0.0004, raw/stock/ÚltimoMovimiento/identidad.
PR1473: CI37248346013 y validate37248346065 PASS, mergece4674d4;
deploy oficial sesión90537 exit0 y checks frescos comprobados. Aceptación41987
exit0: seis pares/doce fronteras acreditados, HTTP0/ops0 y dos observaciones
iguales, SHA85aeb92bc5686138043b0c03a4d02a405996437d6f4150121687a96fc7d4bc0a.
El output comprimido quedó truncado: se conserva resumen honesto en el artefacto,
no se afirma conservar íntegra aquella salida. Este resultado posterior sustituye
los cuatro vetos de representación de1471, sin falsear su aceptación histórica.
Reviews nativas2610 corridas5007/5008 SUCCESS/context13, SHA observación
f7aa2258794acf82170071c7f9783cc4cbaa3eed038b827007d7cf1c2f422175,
iguales/HTTP0/ops0/avisos0; UI autenticada runkeysd76c6c5de3/1604ba6f23,
consola[]. Tarea cerrada oficialmente y recursos LOCAL5477/6477 retirados por
helper con respaldo verificado. No repetir entrega/captura/diagnóstico por rutina;
doce fronteras documentales no prueban físico ni septiembre cerrado.

## Autorización y siguiente contrato snapshot, 4oct2026

Mauricio autorizó integrar snapshots originales para553pares ambos extremos y
resolver17aperturas/23cierres restantes con fuentes existentes luego Point
acotado que cruce el corte, sin descargar septiembre completo. La autorización
sustituye permiso pendiente anterior; NO vuelve a preguntarse. Implementación
tarea `auditor-snapshots-corte` ya publicada/aceptada: PR1458 merge4dae7f99 y
PR1461 mergee898f61b (manifiesto explícito Opening6 necesario), CI completo PASS
37216478756/37216478787 y deploy oficial4oct17:01UTC/check0/migrate0/readiness301.
Dos lecturas frescas iguales HTTP0/ops0:598aperturas y585cierres por snapshot,
557pares con ambos (universo actual distinto de553 iniciales); cobertura MISSING,
canonical_history_verifiedFalse y físico no acreditado. No repetir entrega.
Reviews2085 runs4983/4984/context13/HTTP0/ops0/avisos0 y dashboard autenticado/consola[];
source_authoritativeFalse/closureFalse, no cierre. Tarea/rama/worktree/entorno5472/6472
retirados exclusivamente con respaldo verificado. Ver procedimiento/ficha guards.

Frontend Ult_Mov UTC confirmado por `/Stock/tab_almacen` SHA8a0516c8bd2b2d3735ec8f65d92ca5305ebab8fb902fc9b7270bd218bfb829a4.
576pares sin canónica:559aperturas553cierres,553intersección. Snapshot28797567
job77519 SUCCESS branch4/external5 product106/external116 stock1 postcorte y
log sucursal original exacto son evidencia reutilizable, no dato para otros pares.
690snapshots extra23CEDIS no resolvieron17/23; últimos movimientos posteriores.
Producción y backups SQLOct2/Oct4 no tienen importación exacta branchPK3/productos23;
Closing6/7 sólo46resúmenes sin secuencia íntegra. No repetir estas búsquedas.
GetHistorial cliente sólo últimosN: elegir límite válido mínimo para cierre y parar al cruzar corte;
no500 automático para apertura ni parámetrosfecha inventados. ConsultaPoint principal
únicamente, subagentesHTTP0. Evidencia humana/física permanece separada.

3811 import655 ya COMPLETE313rows/unknown0/rem0, ecuación2+31-27-1=5;
segunda ejecuciónHTTP0/filas0/imports0/duplicados0. No recapturar.
Antes snapshots, preview mensual lock_readyFalse/95catalogissues y fuentes de
frontera sin prueba; cifras no representan pérdida ni totales inventariables.

## Lote rawUTC de fuentes existentes, sin materialización

Lectura PostgreSQL4oct confirmó275pares vendidos Closing7sin canonical pero con
prueba explícita original no_history_current_zero/stock0/rows0/limit500 ycreated_at
Oct1 16:36–Oct2 16:13UTC, postcorteSep07UTC. El capturador original no persistía
imports y sólo emitía método con historia[] yexistenciaactual0. Guardconservador
reutiliza evidencia original porpar/manifiesto/fechas; no la transforma en canonical
COMPLETE ni conteo físico. 576resúmenes con último movimiento no acreditan por sí
solos la ventanaUTC adicional: buscarfuenteoriginal/frontera antesHTTPacotado.

Chequeo4oct: apertura heredaba ledger de agosto8 LOCKED, construido con cierre6
Stock y sin pruebaUTC. La lectura debe revalidar el límite exacto sin alterar ese
ledger cerrado; la mera palabra LOCKED no demuestra el contrato de fechas nuevo.

Investigación de aperturas4oct: 32/50 combinaciones tienen fuentes existentes
acreditables bajo contrato conservador; faltan primeros previos de DotChocolate
Point1048/DotVainilla1047 en otras9 sucursales (18 combinaciones). Bamoa tiene
imports827/828 completos y primer1662931 desde0, sin cero universal por alta.
PanMuerto Point855 conserva1 Colosio PK4/Point5 y4 Matriz PK10/Point1; no confundir
PK4 con Point4 ni extender a ElTúnel. Los límites1058042 PanFresa y1452495 Rosca
pertenecen Matriz PK10/Point1, no CEDIS. Esto documenta fuentes, no escribe aperturas.

Conversión Snickers4oct: GetHeader/GetDetalle1679701 y1679702 consultados una sesión
protegida: detalles acreditan Rebanada10 y Mediano1, cabecera sólofecha/responsable,
sinfolio/FKorigen-destino. No repetir ni unir por mismahora/SHAcabecera/IDsadyacentes.
Frontend actual `/Stock/tab_art` SHA
bd628265f387c6dea4ea73d3e0510cf970a20ecb2b4fcf5f8fba99a69c747e55
captura origen `{PK_Producto,Cantidad,isInsumo:false}` y destinos; POST`/Stock/convertir`
retorna `True` en interfaz sinfolio de ejecución visible. No ejecutarlo para probar
un hecho histórico ni inventar un endpoint de listado: falta relación documental.

Dos lecturas READ ONLY4oct con fuentes guardadas/HTTP prohibido dieron ventas netas
rawUTC iguales a comerciales en2251=30,2451=36,3232=134,3371=169,3372=44,3432=31.
Sus aperturas/cierres raw difieren de algunos extremos legacy:2251 4→1 con ajuste−1;
2451 0→2;3232 7→1 con merma1;3371 7→8 con merma1;3372 1→1;3432 1→2.
No normalizar cantidades comerciales ni conservar extremos legacy por conveniencia.
Imports144/417 tienen cobertura posterior al mes: no recapturarlos. El estado
anterior INCOMPLETE de76/508/509/531 fue sustituido por el lote documentado abajo;
530 sigue INCOMPLETE. No tratar una frase histórica como autorización para repetir.

## Capturas completadas4oct2026: histórico, no proyección ni cierre

VPS/sesiones principales únicas protegidas/lote4 y luego dos lotes10. Antes de HTTP: canónica
exacta, diferencia real, snapshots independientes jobs35042/77519 SUCCESS con
identidad/dominio/ÚltimoMovimientoUTC/postcorte, ventas netas coherentes, unknown0,
remanente0 y continuidad activa. Faltaba exclusivamente fetched_at postcorte.
Capture sinforce, imports originales, FK_Movimiento deduplicado. Todos COMPLETE.

|Caso/import|Point sucursal/producto|Ecuación histórica|Snapshots apertura/cierre|Filas retenidas|
|---|---|---|---|---|
|2251/76|5/118|4+28−30−1=1|28605464/28797569|506|
|3371/508|1/445|7+171−169−1=8|28607214/28799418|521|
|3372/509|1/917|1+44−44=1|28607215/28799419|506|
|3432/531|4/818|1+32−31=2|28607482/28799695|505|
|2443/138|2/169|73−58=15|28605103/28797190|502|
|2835/259|3/169|46+200−131=115|28606447/28798606|510|
|3033/339|6/170|84+260−152=192|28606784/28798961|514|
|3218/402|1/169|101+300−331=70|28607119/28799314|540|
|3220/404|1/1001|16+11conversión−23=4|28607124/28799319|173|
|3413/522|1/1044|17+21entrada−65salida−29ventas+140ajuste=84|28607236/28799444|67|
|3422/524|4/169|96+100−77=119|28607455/28799668|509|
|3600/584|4/1044|3+30−10venta−10ajuste=13|28607572/28799798|21|
|3608/585|7/169|111+100−35=176|28607791/28800022|509|
|3799/649|13/170|162−28=134|28606112/28798253|192|
|2238/67|5/169|114+100−86=128|28605439/28797544|510|
|2296/92|5/403|0+10−1=9|28605558/28797677|51|
|2464/147|2/1005|18−15=3|28605166/28797262|30|
|2471/155|2/660|2+10−2=10|28605180/28797276|41|
|2478/159|2/664|2+10−1=11|28605188/28797284|30|
|2829/258|11/1044|3+12−1=14|28605892/28798028|6|
|2836/260|3/170|120+160−177=103|28606448/28798607|528|
|3271/442|1/267|100+12conversión−11=101|28607225/28799431|149|
|3278/448|1/404|88−9=79|28607234/28799441|262|
|3798/648|13/169|45−10=35|28606111/28798252|67|

Cada segunda captura con HTTP prohibido:0HTTP/0filas nuevas/0imports/0duplicados/0avisos,
misma observación. NO recapturar esos24; filas mayores500 incluyen originales
conservados, no descarga mayor500. Sold_products compartido incluye estos productos:
no excluir Pirotecnia/Tarjeta/Vela por nombre ni presentar venta como producción.
3413 tiene29ventas y65transferidas; preflight abortó una transcripción65 como venta
antes de HTTP y se corrigió sólo literal del diagnóstico, no ERP ni guard.
3220 conversión11 y3413/3600 ajustes conservan origen/aprobación independientes.

Control mensual fresco posterior al lote4 source_completeFalse por otras fronteras;
no ejecutar materializador mientras false ni sobrescribir extremos guardados.
Casos2251/3371/3372/3432 todavía reflejaban proyección legacy/diferencias−2/+1/−1/+1.
COMPLETE acredita cobertura histórica, no actualización automática de UI/mes.
Hallazgos JSON por identidad externa son evidencia fechada, no prueba viva;
si falta la canónica actual, el goal debe seguir mostrando MISSING.

## Límites históricos y documentos de conversión: no repetir fuentes agotadas

Sólo opciones frontend5/10/15/50/100/300/500;101 no es un límite válido.
Una consulta corta que no cruza el corte no prueba la frontera. 2172 CEDIS Point8
ZanahoriaR103:100filas íntegras consultadas4oct17:19:56UTC empiezan2sept20:20:46.013Z
1661288−2→−3; no prueban apertura1sept07Z. SHA
38dfbdf5d4b4df3a2c7c087d144ee9573194d6e1ad7a7fdd292f732e208a156c.
Antecedente anterior al ingreso1469 y tres capturas completas nuevas de arriba:
no repetir100 por rutina ni convertir primera existencia en apertura.23cierresCEDIS
cruzaron corte con respuestas conservadas;3aperturas2122=2/2123=1/2235=19 cruzaron:
no reutilizar FK1659466 global (existe en productos distintos).14aperturas siguen
sin prueba en ese corte anterior. No convertir esta cifra fechada en faltantes
actuales sin consultar los originales ya incorporados. Reporte1081 Aug31CEDIS ya
solicitado una vez, seguíaStatus0 a17:46UTC;
no recrearlo ni afirmar descarga mientras no esté generado.

Reporte1082 Matriz23sept tipo21 solicitado4oct16:36Z/generado16:36:37.780Z,
template0b784f9e-4089-44be-bd6f-fa3c0900d2f2. XLS SHA
f7c08d840141db5a21c0af6286c606591df99f525941e4f39fa8c6844684a833,
11filas/12columnas: SnickersR10/ZanahoriaR22/3PecadosR10,total42/costo0,
cabecera «Todos los tipos» pese filtro21. no es folio de ejecución: carece de fecha
por evento/FKorigen/vínculo origen-destino/cancelación. No regenerar/redescargar ni
usar costo o total42 para inventar pastel/rebanadas.23AGG179–204/job77629 en cuatro
sucursales siguen requiriendo documento de ejecución exacto; reglas activas son
configuración, no ejecución. Solicitud humana concreta emitida una vez, no duplicar.

## Causas lectoras pendientes, no pérdidas ni permisos implícitos

CEDIS Empanada productPK120/external135/import43:row5776/FK1680267 MERMA4→0
qty4/CanceladoTrue yrow5775/FK1680271 CANCELACION DE MERMA tipo15 0→4/qty4/
CanceladoFalse. Frontend Stock/tab_historial SHA8755e73… enumera case5/case15.
No FK directa entre ambos ni identidad con otro producto de igual movimiento.
El lector anterior rechaza tipo15 y excluye merma cancelada. Incluir sólo crédito
produciría crédito ficticio. Reparación autorizada `auditor-merma-reversion-historica`
EN IMPLEMENTACIÓN, todavía no publicada: helper compartido conserva cada efecto
documental estricto5/true débito+qty y15/false reverso−qty; expected_closing resta
waste firmado. Qty/delta/tipo/nombre/flags/dominio contradictorios quedan unknown;
ordinaryMERMAfalse sin cambio. No editar MermaPOS, cobertura ni merma/ajuste
compensatorio. Cronología4→0→4 netea0 sólo en el mismo intervalo; una frontera
entre eventos o31agosto→1septiembre conserva el efecto individual correspondiente.

El original recuperado para2169/import del rango829–842, CEDISbranchPK3/product263,
contiene los mismosFK1680267/1680271 pero qty1,10→9→10,24sept16:20:16.27UTC y
16:26:48.103UTC; no usar qty4 del producto120 ni inventar pareja por FK global.
Ese antecedente unknown1680271 sigue siendo el resultado publicado anterior:
actualizar procedimiento no prueba que ahora esté resuelto. Pruebas/CI/deploy/
aceptación del nuevo lector siguen pendientes; review/plan no ejecutan HTTP,
importación, sincronización, materialización o cierre. Físico/aprobación aparte.

Antecedente Bamoa anterior al contrato autorizado de esta tarea, descrito arriba:
Bamoa6COMPLETE imports721/778/785/197/816/817 conservan500filas/500FKúnicos y
row_count500. resolve_stock_at_close rawUTC enmemoria0 enambos extremos, últimos
movimientos1554407/1373282/1615936/1615941/1000245/1615924 respectivamente.
Helper actual rechaza500sin fetchedIDs: no fabricar fetched_movement_ids ni
recapturarCOMPLETE.197 creadoOct1/refrescadoOct2 exige probar lote vigente, no
presumir que todas las filas retenidas son última descarga. Extensión propuesta
sólo con lote íntegro/origenAPI/par/rango/IDsúnicos/postmes/huella demostrados;
unión de capturas, falta de filas o timestamp posterior a descarga no acreditan.
La autorización actual permite fallback snapshot independiente, NO convertir esa
unión en lote íntegro; no repetir la solicitud anterior ni dar cierre por este hecho.

3430 Payán/Zanahoria import530:39 ventas rawUTC guardadas +2 documentales=41
comerciales. El30sept suma1+1+2, con headers explícitos NOTA-102345 (1686015),
NOTA-102352 (1686079) yNOTA-102362 (1686312). Nuevo1686312 rawOct1T02:01:31.863UTC
es30sept19:01Maz, VENTA2/2→0/no cancelado. Header/detalle corroboran Payán/Bollo2;
no inferir PK_Nota de FK_Movimiento ni equivalencia por minutos. Fue posterior al
fetched_at01:52Z de530: aritmética correcta no acredita cobertura. No se persistió
en esa investigación; no repetir HTTP específico a esos documentos sin dato nuevo.

## Evidencia detallada conservada

Documentos versionados dentro de esta habilidad: carpeta `references/evidencias/`.

- `auditoria-merma-matriz-20261003.md`: leer ante autoridad mensual bloqueada/fila eliminada/límites de get_mermas.
- `auditoria-ciruela-identidad-20261003.md`: leer ante source_trace vacío o SKU ambiguo/identidad por dominio.
- `auditoria-finalizacion-3911-20261003.md`: leer ante etiqueta COMPLETE que ignora missing o finalización administrativa.
- `auditoria-devoluciones-3893-20261003.md`: leer ante recepciones canónicas antiguas frente a Point actual.

Esos documentos son fuentes del detalle, no copiar sus respuestas a otras entidades. Si no están accesibles, informar limitación y usar el checkpoint sin convertirlo en verificación actual. No pedir captura humana genérica para documentos que ya existen.

## Historias ya COMPLETE: no recapturar

Verificadas en este seguimiento: canónicas existentes, remanente cero, sin desconocidos, diferencia materializada cero; segundas capturas sin HTTP/filas/duplicados. Esto NO asegura conteo físico ni todos los documentos.

- CEDIS/primeros: 2108,2135,2157,2170,2171,2174,2197,2199,2231,2233,2250,2253.
- Colosio: 2297,2325,2327,2329,2351,2364,2383,2384,2385,2390.
- El Túnel: 2658,2662,2672,2760,2761,2782,2783,2784,2788,2789.
- Las Glorias: 2843,2845,2846,2852,2894,2895,2920,2921,2922,2934,2936,2953,2955,2956,2960,3008.
- Leyva/Matriz: 3039,3044,3049,3085,3113,3125,3127,3145,3148,3172,3198,3206,3228,3229,3230,3231,3232,3240,3241,3280,3282,3307,3310,3311,3312,3324,3325,3328,3334,3342,3344,3346.
- Últimos (caso/import): 2196/61,3306/467,3345/496,3397/513,3414/523,3535/571,3621/595,3812/656,3819/658,3893/680,3911/691,3963/707.

Consultar estados por servicio/base cuando haga falta, no usar la lista como permiso para cerrar. Los casos 2453,2613,2614 también quedaron stock0/BALANCED documentalmente, sin que eso autorice cierre físico.

## Bloqueo autoridad de merma: reparación autorizada, verificar resultado

Caso3237 Empanada Manzana/Matriz conserva proyección 0+518−477−41=0, pero fuente actual no autoritativa. PointWasteLine1676 y MermaPOS desaparecieron; movimiento1683114, 5PZA, fecha raw27/09/2026 19:01:00.49, motivo Merma desde la caja, hash9f27f6de945bbc8b19cd. Backup existente `/opt/backups/erp/backup_20261003_020001.sql.gz` y rawjobs78023/79066 lo conservan. Historia419 y detalle actual Point confirman MERMA43→38, Canceladofalse.

Job80579 rangoSep27–Oct3 omitió el folio y superseded1 eliminó la fila. Listado acotado inicioSep27 no lo devuelve; inicioSep26 sí. No es cancelación ni prueba de offset fijo. UI mantiene total anterior y referencia a evidencia desaparecida.

Autoridad mensual elegía78023 fullSept seen267 vs actuales266; writers234/78023,4/79066,28/80579. Errores WASTE_SYNC_COUNT_MISMATCH/WASTE_SYNC_JOB_MIXED. unchanged1650/source_incomplete13450 NO significa13450filas faltantes: incluye hechos de resolución de productos.

Mauricio respondió «si» a la autorización concreta para proteger importador y
recuperar exclusivamente el par original1683114, sin crear merma Point ni ajustar
stock. Backup confirma ambos PK1676, branch original24/Matriz ERP1, receta16,
writer79066 y timestamps originales. No reemplazar alias por otra PK. Recuperación
exacta mediante scripts/recover_point_waste_1683114.py: dry-run por defecto, apply
explícito, colisión diferente aborta, originales íntegros y segunda ejecución no-op.
La protección del importador aborta atómicamente extracciones completas que omitan
hashes existentes; margen de consulta no acredita exhaustividad. La publicación y
recuperación deben comprobarse en VPS/UI antes de considerarse realizadas. No
editar resúmenes viejos ni forzar autoridad mensual/materialización por conteos.

Actualización verificada 3oct17:16 Mazatlán: PR1455 publicado, merge75fb0de1,
CI37162776413 completo PASS y204 pruebas locales. Recuperación exacta del par
PointWasteLine1676/MermaPOS1676 ejecutada desde backup read-only: primera2filas,
segunda0, HTTP0, writer79066/alias/timestamps originales. Otros registros de
merma/waste/casos/ventas/stock/imports/historia/avisos intactos. UI autenticada3237
volvió a mostrar5PZA/folio1683114, ventas477/merma41 conservadas y consola0.
No repetir recuperación ni despliegue. El contador septiembre ahora267, pero
autoridad78023 sigue bloqueada:234 vinculadas y WASTE_SYNC_COUNT_MISMATCH/JOB_MIXED.
Una sincronización mensual nueva es alcance adicional, no consecuencia automática
de recuperar dos originales; no ejecutarla sin aprobación específica. No cerrar mes.

## Identidad Ciruela: documento existe, lector ambiguo

3963/history707: 1inicial−1transferencia=0. Línea44672 folio38598/539340 Guamúchil13→Devoluciones12,1/1 finalizada. Rawdetalle FK_articulo112/isInsumoFalse→producto541 external112, receta199. SKU0112 comparte541Ciruela,427official:0112navideño,965external948navideño; normalized_name427 conservaCiruela. No fusionar/corregir maestros.

_resolve_product devuelve AMBIGUOUS_PRODUCT; source_trace.transfers vacío. _transfer_evidence omite completos finalizados por diseño. Sept15líneasproductoFK112 y31insumoFK112: dominios distintos. Historia1662087(.747) vs transferencia(.750) solo candidata temporal, noFK ni prueba física. SnapshotMember sinrawFK: no enriquecer desde mutable; queryset.only sinraw_payload: atenderN+1 si se autoriza lector. No Point ni recaptura necesaria para identificar.

## 3911: finalizaciones faltan realmente

CrunchRGuamúchil/history691:2+22−13−8=3. Folios/detalles38657/540006,38700/540833,38927/542924,38999/543936,39541/549357, enviados/recibidos1/1,3/3,4/4,3/3,2/2. CargasFK33160,33438,34247,34805,37403 coherentes/CARGADA/paradasENTREGADA.

Point actual03/10 13:58UTC confirma los5 isRecibidoTrue/isFinalizadoFalse/CanceladoFalse. NO bandera antigua. No reconsultar sin evidencia nueva. Cantidades totales de cabecera incluyen otros artículos. _apply_transfers contabiliza recepción sin exigir finalizado; retorno/fallback separado. Resumen traceabilityCOMPLETE ignora missing aunque UI lista5pendientes: defecto lector, no permiso para omitirlos/cerrar.

## 3893: dos recepciones existen, ERP antiguo

PayQuesoGrandeGuamúchil/history680:3+44−32−10−1conversión=4. Canónicas45516/47798 guardaban1/0. Point03/10 14:56:57–58UTC acredita detalles540391/542935, productoFK1/SKU0001/isInsumoFalse,1/1, recibidas/finalizadas/no canceladas:

- 38681/540391 Guamúchil13→CEDIS8; recepciónPoint07/09 16:36:22.257 local.
- 38932/542935 Guamúchil13→Devoluciones12; recepciónPoint14/09 15:07:42.863 local.

No persist/import/sync/capture; pantalla3893 sigue estados antiguos. No pedir nuevamente documentos humanos genéricos para esas recepciones ni reHTTP sin evidencia nueva. Refresco canónico debe inspeccionar efectos/alcance autorizado; no afirmar que UI quedó reparada. No cargaFK ni conteo físico demostrado.

Otros pendientes independientes:38571/539098 enviado0/recibido1 carga32733=0;39196/545818 recibido1/1 pero carga35683null/parada859PENDIENTE. No compensarlos ni atribuir pérdida.

## Otras líneas que el agente debe conservar pendientes

- Logística responsable ya solicitada sin respuesta:2922retornos1cada39049/544276,39380/547601,39423/547939;2956retorno1/38745/541330;2960retorno3/39293/546688;3049retorno2/38705/540875;3535retorno1/39195/545797. No doble retorno/pérdida ni asignar Mauricio repartidor.3127 es distinto: carga36004ceroFALTANTE/sin_stock, discrepancia286VALIDADA_REAL acredita no carga, no viaje/retorno físico; aprobación separada pendiente.
- CEDIS2170abre−7/cierra−19,2171−2/−15,2174−12/−6: saldo cuadra, negativos heredados no explicados físicamente. ÚltimosAug31movimientos1659407,1659579,1659393. Reutilizar cadena conservada; equivalencias con transferencias por hora/cantidad son candidatas, noFK.
- 2920conversión191 y3344/3345/3346conversiones196/198/199/200 conservan origen pendiente; no inventar pastel/rebanadas.3345ajuste1677865cantidad+10pero20→10delta−10;historia ya corregida/capturada, no repetir.
- 3240GalletaChispasMatrizcerró−1;39420/547853línea52441enviado0/recibido9finalizada sin cargaFK.3414/39420/547858línea52446enviado3/recibido0finalizada sin cargaFK. No acusar pérdida/retorno físico por defecto.
- Antecedente anterior al contratoUTC/capturas4oct, NO divergencia actual de2251/3371/3372/3432:2451comercial36 vsstock35;2251(30/31),3371(169/168),3372(44/45),3430(41/39),3432(31/30). Leer loteactual arriba que sustituye estas cuatro comparaciones;3430 conserva cobertura pendiente. No normalizar ventas ni usar offset por aproximación.
- Addon2565/2746/2368mismaSKU03SPFREBPoint836: relación119→base110aprobadaRecetaAgrupacionAddon1 activa desdeabril/mayo;no clasificarpornombre.2565ventas14conservadas,stock−1SOURCE_INCOMPLETE. OtrosSabor/Litrocrema/Vasos sin relación/config propia confirmada no se reclasifican por semejanza.

## Publicaciones ya completadas

PR1438/c6ac758 cerrado: evidencia carga0 validada incluso revisión cerrada. PR1439/b071ec68 cerrado: ajustes por delta. PR1440/1181c869 cerrado: conservar ventas comerciales discrepantes. PR1441/087cbec2 cerrado: filtro shared de addons activosAPPROVED;280casos/ventas intactos,0addonsaprobados sold_products. CI/deploy/auth ya verificados en seguimiento, no repetir implementación. Ninguna de estas publicaciones acredita cierre mensual.
