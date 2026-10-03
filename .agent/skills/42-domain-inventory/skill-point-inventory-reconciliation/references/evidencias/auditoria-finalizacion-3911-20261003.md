# Caso 3911 — finalización documental de cinco transferencias

3 de octubre de 2026. Revisión de solo lectura, VPS main087cbec2. Historial canónico691 COMPLETE: no recapturarlo. Saldo 2+22 entradas−13 ventas−8 mermas=3; diferencia0. No conteo físico.

## Fuentes existentes y registro exacto faltante

| Transferencia / detalle | Línea Point | Día enviado Mazatlán | Enviado/recibido | Carga FK | Ruta | Última escritura canónica UTC |
| --- | --- | --- | --- | --- | --- | --- |
| 38657/540006 | 45309 | 05/09 | 1/1 | 33160=1 | RUT-202609-0009 | 07/09 10:23 |
| 38700/540833 | 45891 | 07/09 | 3/3 | 33438=3 | RUT-202609-0012 | 09/09 10:23 |
| 38927/542924 | 47784 | 12/09 | 4/4 | 34247=4 | RUT-202609-0022 | 14/09 10:24 |
| 38999/543936 | 48786 | 15/09 | 3/3 | 34805=3 | RUT-202609-0026 | 17/09 10:22 |
| 39541/549357 | 53763 | 30/09 | 2/2 | 37403=2 | RUT-202609-0053 | 02/10 10:23 |

Todas Point CEDIS→Guamúchil, artículo FK63 Crunch R, is_received true, is_finalized false, is_open false, current true. Los raw conservados en **raw_payload.transfer** confirman isFinalizado false y Usuario_Finalizo null; no error de traducción conocido. Cargas CARGADA y paradas ENTREGADA por FK exacta. No inferir pérdida ni retorno de 13 piezas recibidas.

No existen miembros de snapshots abiertos para las cuatro primeras. 39541 tiene cinco; los últimos del 30/09 18:05 UTC reflejan 2 enviadas/0 recibidas/is_open true. Son anteriores a la recepción 01/10 03:47 UTC (30/09 20:47 Mazatlán) y no contradicen la canónica posterior 2/2. No usar esos snapshots antiguos como estado actual.

**Falta concreta:** acreditar si la cabecera Point de cada folio actualmente está finalizada; los valores guardados provienen de fechas distintas. No hay prueba de un estado posterior en los snapshots revisados. Consulta autorizada acotada prevista: una sesión protegida point_account_session_lock(wait=False), solo GET /Transfer/GetTransfer, sucursal13, recibido=true, cinco intervalos de un día (05,07,12,15,30 de septiembre); conservar únicamente los cinco folios exactos y sus flags. No ejecutar extractor completo (descarga todos los detalles y exporta), sincronizador, persist, acciones Finalizar/Guardar/Resolver ni recapturar historial. Si candado ocupado, no consultar ni liberar.

## Contrato lector y límite

_transfer_evidence marca missing cuando !is_finalized. _apply_transfers sí contabiliza recepción con is_received/received_at sin exigir finalizado; finalización se usa separadamente para retorno de diferencia sent−received y fallback de fecha. Por tanto recepción y finalización no son el mismo criterio.

investigation_summary.traceability_status actual es COMPLETE porque solo comprueba movement_status BALANCED/RESOLVED y ausencia de discrepancias, **ignora missing**. Las cinco finalizaciones aparecen en missing. Esta contradicción no autoriza omitir documentos ni cambiar datos. No está probado todavía que la exigencia administrativa sea incorrecta; comprobar estado actual primero.

Fuentes: ProductInventoryAuditCase3911; PointTransferLine45309/45891/47784/48786/53763; PointOpenTransferSnapshotMember filtrado por folio/detalle; RutaCargaChecklistLinea por FK. Consumidores: BranchInventoryTraceabilityService→InventoryAuditMaterializer→InventoryAuditAgent→pantalla. Decisión: reutilizar claves y snapshots; ninguna nueva equivalencia, tabla o aviso. Importador de mermas y autoridad mensual siguen pendientes de autorización, no intervenirlos.

## Verificación puntual actual (ejecutada después de documentar el faltante)

Se obtuvo naturalmente el candado con wait=False, se creó una sola sesión y se cerró al terminar. Cinco GET de listado, cada uno limitado a una sucursal y un día, sin GetDetalle, extract/persist/sync/capture ni acciones comerciales. No descargas de mes completo. Intervalos aplicaron los métodos existentes del extractor, sin ajustar fechas ni inventar offset.

| Folio | Fecha de consulta UTC | Cabeceras del intervalo | Coincidencias exactas | Recibido | Finalizado | Cancelado |
| --- | --- | --- | --- | --- | --- | --- |
| 38657 | 03/10 13:58:06.498232 | 4 | 1 | true | false | false |
| 38700 | 03/10 13:58:06.847771 | 9 | 1 | true | false | false |
| 38927 | 03/10 13:58:07.220949 | 4 | 1 | true | false | false |
| 38999 | 03/10 13:58:07.602148 | 2 | 1 | true | false | false |
| 39541 | 03/10 13:58:07.971088 | 9 | 1 | true | false | false |

Todas las cabeceras actuales confirman origen Point8 CEDIS y destino13 Guamúchil, isEnviado true, las mismas fechas de envío/recepción guardadas. **La falta de finalización no es una bandera canónica desactualizada.** No se necesita recapturar el histórico ni volver a consultar estos cinco estados sin nueva evidencia.

Totales de cabecera 38657:71 enviados/72 recibidos; 38700:41/31; restantes31/31,55/55,37/37. Son totales de TODOS los artículos, no los de Crunch R. No atribuir su diferencia a Crunch ni a pérdidas; esta investigación no abrió ni imputó discrepancias de otros artículos.

Pantalla autenticada caso3911 revisada tras la consulta: Saldo conciliado, Movimientos/trazabilidad Conciliado, Sin conteo manual, cinco finalizaciones enumeradas en Qué falta; Agrupado para revisión/Administración/Sin persona asignada; historial sin decisiones registradas. Consola sin warnings/errores. No acciones Guardar/Aprobar/Resolver, no avisos.

Conclusión: separar saldo aritmético conciliado de **documentación administrativa aún pendiente**. No resolverlo cambiando una transferencia ni eliminando el requisito por cantidades iguales. El resumen compartido actualmente muestra COMPLETE aunque missing contiene cinco elementos: registrar defecto conservador del lector para eventual corrección autorizada por flujo oficial. Esta revisión no acredita cierre ni reemplaza aprobación humana. El expediente permanece intacto y el seguimiento activo.
