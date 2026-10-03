# Pay de Queso Grande Guamúchil — expediente 3893

3 de octubre de 2026; VPS main087cbec2; diagnóstico read-only. Historial canónico680 COMPLETE, no recapturar. Saldo 3+44 entradas−32 ventas−10 salidas−1 salida conversión=4; diferencia0. Trazabilidad PENDING/NEEDS_EXPLANATION. Ningún conteo físico ni aprobación derivados de esa ecuación.

## Fuentes existentes / registro faltante antes de consultar

| Documento | Línea Point | Origen→destino | Fecha envío Mazatlán | Cantidad guardada | Estado guardado | Evidencia Logística por FK |
| --- | --- | --- | --- | --- | --- | --- |
| 38681/540391 | 45516 | Guamúchil13→CEDIS8 | 05/09 19:17:19.007 | 1 enviada,0 recibida | sin recepción/finalización; actualizado07/09 UTC | No carga ligada |
| 38932/542935 | 47798 | Guamúchil13→Devoluciones12 | 12/09 18:34:27.317 | 1 enviada,0 recibida | sin recepción/finalización; actualizado14/09 UTC | No carga ligada |
| 38571/539098 | 44418 | CEDIS8→Guamúchil13 | 03/09 17:14:17.887 | 0 enviada,1 recibida | finalizada | Carga32733=0, CARGADA, parada697 ENTREGADA ruta RUT-202609-0005 |
| 39196/545818 | 50560 | CEDIS8→Guamúchil13 | 21/09 19:33:01.813 | 1 enviada,1 recibida | no finalizada | Carga35683 sin captura,PENDIENTE; parada859 PENDIENTE ruta RUT-202609-0036 |

Todas raw.detail.FK_articulo1, dominio producto. Raw.transfer y canonical coinciden en los estados conservados. Ninguna de estas cuatro líneas tiene miembros PointOpenTransferSnapshotMember por transferencia/detalle. No es prueba de inexistencia física ni de cancelación.

Faltante concreto prioritario: estado **actual** de recepción/finalización y cantidad del detalle 540391 del folio38681 y detalle542935 del folio38932, porque las canónicas son antiguas. Consulta prevista única sesión point_account_session_lock(wait=False): GET /Transfer/GetTransfer destino8 día05/09 y destino12 día12/09, recibido true/false para no excluir un cambio de estado; GET /Transfer/GetDetalle solo idTransfer38681/38932 y seleccionar detalle exacto. No ejecutar extractor completo, importador, persist, sync/capture ni acciones operativas. Si ocupado, no interferir. No consultar históricos COMPLETE ni mes completo.

## Fuente oficial, identidad y consumidores

PointTransferLine y su raw conservado identifican los documentos. Cargas se relacionan exclusivamente por point_transfer_line_id. El histórico acredita efectos de stock, no custodia ni identidad directa con estos documentos. InventoryAuditAgent ya mantiene ambos folios en el expediente existente; no crear solicitud genérica duplicada ni otro retorno.

Reutilizar fuentes y preservar diferencias: registro de recepción pendiente, discrepancia 0/1 de otro ingreso, y falta de captura logística de una recepción 1/1 son problemas distintos. No compensarlos entre sí ni inventar pérdida/merma. La autorización del importador de mermas y responsable Logística sigue pendiente; no repetir preguntas ni alterar asignación.

## Evidencia actual puntual que sustituye la hipótesis de recepción ausente

Se adquirió el candado wait=False, una sola sesión y cierre en finally. Cuatro GET de listado (dos filtros por intervalo) y dos GET de detalle por idTransfer, sin persist/import/sync/acciones operativas. Consultado 03/10/2026 14:56:56–58 UTC.

| Folio/detalle | Cabecera actual | Recepción Point (hora local conservada) | Detalle exacto actual | Resultado |
| --- | --- | --- | --- | --- |
| 38681/540391 | origen13,destino8,isRecibido true,isFinalizado true,Cancelado false | 07/09/2026 16:36:22.257 | artículo1,0001,Pay de Queso Grande,isInsumo false; solicitado1/enviado1/recibido1 | Recepción registrada en CEDIS; canónica45516 antigua |
| 38932/542935 | origen13,destino12,isRecibido true,isFinalizado true,Cancelado false | 14/09/2026 15:07:42.863 | artículo1,0001,Pay de Queso Grande,isInsumo false; solicitado1/enviado1/recibido1 | Recepción registrada en Devoluciones; canónica47798 antigua |

Cada cabecera y detalle tuvo una coincidencia exacta. Listas recibido=false: cero coincidencias de folio; recibido=true: una en cada caso (listados13/30 para destino8 y0/6 para destino12). No convertir esos totales de lista en cantidades de producto.

**Ya no falta evidencia Point de recepción/finalización de estas dos líneas.** No pedir constancia humana genérica para volver a probar ese hecho. La evidencia documental no es conteo físico ni prueba de custodia independiente, pero contradice claramente el estado canónico anterior. No se cambió la canónica ni se inventó un evento de aprobación. La falta de captura logística por FK sigue separada; tampoco crear otro retorno.

Próximo paso: reutilizar las respuestas exactas para un refresco canónico controlado sin impacto en inventario cuando el flujo/autorización de fuentes operativas lo permita, y actualizar investigación por flujo oficial. No ejecutar persist_transfers que pueda aplicar inventario sin inspeccionar contrato. La autoridad mensual de mermas permanece bloqueada; no saltar ese control para materializar. No repetir HTTP de estos dos folios sin evidencia nueva.

Verificación autenticada posterior: pantalla3893 conserva esas dos recepciones como ausentes, saldo4/diferencia0, trazabilidad requiere explicación, Sin conteo manual; Logística sin persona asignada por falta de jefatura activa única; historial sin decisiones. Confirmado desfase de fuente canónica/UI frente a Point, no ausencia actual de recepción. Consola sin warnings/errores. No enviar a aprobación ni crear actor/evento, no modificar formularios.
