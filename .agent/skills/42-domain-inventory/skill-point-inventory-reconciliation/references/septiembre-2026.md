# Continuidad septiembre 2026 — corte 3 de octubre de 2026

Este archivo es un checkpoint, no una consulta viva. Revalidar hechos cambiantes con fuentes autorizadas y frescura necesaria; no volver a descargar hechos ya acreditados por rutina. Hilo/automatización: `01a0d98e-e017-7253-b4c9-9758f56c3827` / `conciliar-septiembre-hasta-cierre`. Septiembre NO cerrado. Regla humana vigente: no alterar inventario, mermas, ventas, RRHH o ajustes para forzar resultado.

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

## Bloqueo autoridad de merma: autorización pendiente

Caso3237 Empanada Manzana/Matriz conserva proyección 0+518−477−41=0, pero fuente actual no autoritativa. PointWasteLine1676 y MermaPOS desaparecieron; movimiento1683114, 5PZA, fecha raw27/09/2026 19:01:00.49, motivo Merma desde la caja, hash9f27f6de945bbc8b19cd. Backup existente `/opt/backups/erp/backup_20261003_020001.sql.gz` y rawjobs78023/79066 lo conservan. Historia419 y detalle actual Point confirman MERMA43→38, Canceladofalse.

Job80579 rangoSep27–Oct3 omitió el folio y superseded1 eliminó la fila. Listado acotado inicioSep27 no lo devuelve; inicioSep26 sí. No es cancelación ni prueba de offset fijo. UI mantiene total anterior y referencia a evidencia desaparecida.

Autoridad mensual elegía78023 fullSept seen267 vs actuales266; writers234/78023,4/79066,28/80579. Errores WASTE_SYNC_COUNT_MISMATCH/WASTE_SYNC_JOB_MIXED. unchanged1650/source_incomplete13450 NO significa13450filas faltantes: incluye hechos de resolución de productos.

Ya se pidió autorización concreta a Mauricio para proteger importador compartido y recuperar ÚNICAMENTE registro original1683114 con IDs/dedup, sin crear merma Point ni ajustar stock. A este corte sin respuesta. No repetir pregunta ni implementar/restaurar/persist/sync. Un lector que ignore validación no resuelve omisión real. No forzar materialización mensual mientras siga bloqueada.

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
- 2451comercial36 vsstock35;2251(30/31),3371(169/168),3372(44/45),3430(41/39),3432(31/30): sin identidadticket/PointSalesNormalizedSep30–Oct1; no equivalencia/offset ni captura para normalizar.
- Addon2565/2746/2368mismaSKU03SPFREBPoint836: relación119→base110aprobadaRecetaAgrupacionAddon1 activa desdeabril/mayo;no clasificarpornombre.2565ventas14conservadas,stock−1SOURCE_INCOMPLETE. OtrosSabor/Litrocrema/Vasos sin relación/config propia confirmada no se reclasifican por semejanza.

## Publicaciones ya completadas

PR1438/c6ac758 cerrado: evidencia carga0 validada incluso revisión cerrada. PR1439/b071ec68 cerrado: ajustes por delta. PR1440/1181c869 cerrado: conservar ventas comerciales discrepantes. PR1441/087cbec2 cerrado: filtro shared de addons activosAPPROVED;280casos/ventas intactos,0addonsaprobados sold_products. CI/deploy/auth ya verificados en seguimiento, no repetir implementación. Ninguna de estas publicaciones acredita cierre mensual.
