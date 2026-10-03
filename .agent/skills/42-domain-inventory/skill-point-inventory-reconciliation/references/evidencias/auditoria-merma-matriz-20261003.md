# Evidencia puntual: merma omitida por rango de fechas

Verificación de solo lectura del 3 de octubre de 2026. Expediente existente: 3237, septiembre, Empanada de Manzana SKU 0135 / producto Point 135, Matriz ERP 1 / Point 1. Sin modificaciones de inventario, mermas, ventas o aprobaciones.

## Movimiento identificado

Folio Point 1683114; fecha original Point `2026-09-27T19:01:00.49`; cantidad 5 PZA; motivo «Merma desde la caja».

El respaldo existente `/opt/backups/erp/backup_20261003_020001.sql.gz` contiene PointWasteLine 1676, source_hash `9f27f6de945bbc8b19cd`, receta 16, sync_job 79066. La fila ya no está en PointWasteLine ni MermaPOS. Se leyó exclusivamente la tabla de mermas del respaldo, sin restaurarlo.

El historial canónico guardado 419 contiene el mismo FK_Movimiento, salida MERMA de 43 a 38, Cancelado=false. El historial actual consultado en Point confirma exactamente esos valores y la ausencia de cancelación. Los endpoints puntuales de detalle y justificación siguen devolviendo las 5 piezas y el motivo.

## Prueba acotada del listado actual

Se reutilizó `_to_epoch_ms` del extractor vigente, `/Mermas/get_mermas`, sucursal `Matriz`, fin 28/09, sin persistir ni generar exportaciones:

| Inicio solicitado | Filas devueltas | Movimiento 1683114 |
| --- | ---: | --- |
| 27/09/2026 | 4 | No aparece |
| 26/09/2026 | 9 | Aparece, cantidad de detalle 5 |

El primer rango devuelve fechas originales Point del 28/09 y madrugada del 29/09; no acredita cobertura íntegra del día inicial solicitado. Esta prueba identifica una omisión por límites de consulta, no una cancelación. No establece por sí sola una regla general de zona horaria o un desplazamiento fijo.

La exportación ya guardada `20261003_024603_point_waste_2026-09-27_2026-10-03_all.json` tiene 38 movimientos y omite este folio. El job 80579 informa superseded=1. El historial actual contradice considerar esa omisión una merma anulada.

## Continuación segura

Revisar el contrato de fechas de la interfaz Point y los llamadores del extractor/supersede antes de corregirlo; verificar extremos inicial/final, fechas ingenuas y zona horaria con pruebas. No debilitar la autoridad mensual por conteos ni ejecutar otra sincronización operativa para forzar septiembre. No reinsertar automáticamente esta merma ni alterar los resúmenes de jobs. El expediente conserva merma 41 y saldo 0 de diferencia en su proyección anterior, pero el manifiesto mensual actual no es autoritativo. Cualquier reparación de fuentes operativas requiere autorización aplicable y flujo oficial separado de la corrección lectora del auditor.

Las consultas Point fueron acotadas, protegidas por `point_account_session_lock(wait=False)`, sesiones cerradas y sin escrituras comerciales ni importaciones. No se recapturaron historiales COMPLETE.

## Verificación complementaria del siguiente seguimiento

Las exportaciones existentes `20261001_102427_point_waste_2026-09-01_2026-09-30_all.json` (267 líneas) y `20261002_024538_point_waste_2026-09-26_2026-10-02_all.json` (37 líneas) incluyen el folio 1683114, detalle 5 PZA y justificación. No fue necesario descargar otra vez esos períodos.

Se leyó la interfaz oficial Point: `/Mermas/Index` monta `tab-mermas`; `/Scripts/General.js` define la carga de `/Mermas/tab_mermas`. El formulario llama a `traerMermas` con `datepicker('getDate')`. Esa función envía `Math.floor(new Date(fechainicio))` y `Math.floor(new Date(fechafin))` como `fechaini` y `fechafin`. No aplica un desplazamiento explícito ni un ajuste de final de día. El extractor ERP usa medianoche de America/Mazatlan convertida a epoch. Esto no prueba un error general de zona horaria ni justifica cambiar la zona horaria del ERP. La discrepancia observada exige verificar la cobertura efectiva de los límites de la respuesta, no confiar exclusivamente en los nombres de las fechas solicitadas.

Código actual: `_supersede_stale_waste_rows` elimina PointWasteLine y MermaPOS ausentes del conjunto de hashes devuelto, dentro del rango local, cuando la consulta no tiene filtro de sucursal; no exige una cancelación acreditada del movimiento. Sus pruebas del extractor comprueban timestamp y reautenticación, pero no reproducen esta omisión del día inicial ni la eliminación resultante.

Pantalla autenticada del expediente 3237: inicial 0 + entradas 518 - ventas 477 - merma 41 = cierre 0; saldo y trazabilidad etiquetados Conciliado, sin conteo manual. Al desplegar Merma / Ver evidencia (5), cuatro filas muestran 6, 16, 7 y 7 piezas; la quinta muestra «Evidencia ya no disponible en la fuente. Referencia conservada: Point #1676». Por tanto el total sigue siendo una proyección anterior, no una validación de la fuente actual. También enumera cinco transferencias con finalización pendiente. No se pulsó aprobar/resolver ni se alteró el caso; consola sin errores.

Se requiere autorización concreta antes de corregir el importador compartido de mermas y reincorporar el registro original verificado, porque eso interviene fuentes operativas, no solamente la lectura del auditor. La recuperación propuesta queda limitada al folio 1683114, usando sus identificadores originales y deduplicación; no implica crear una merma comercial nueva, ajustar existencias ni modificar Point. Validar límites de extracción y sustitución con pruebas antes de elegir la implementación; no inventar un desplazamiento temporal o permitir borrar otras filas por mera ausencia del listado.
