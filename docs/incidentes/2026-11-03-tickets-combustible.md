# Tickets de combustible: saldo agotado y falso OK

## Causa comprobada

El 3 de noviembre de 2026 se encontraron 26 cargas, registradas entre el 16 de
octubre y el 3 de noviembre, cuyo detalle guardado reporta HTTP 429,
`insufficient_quota` / `credit_balance_exhausted`. Una lectura real de la carga
183 volvió a recibir el mismo rechazo. No es evidencia de fotos ilegibles.

El servicio conservaba score 0 ante un fallo de lectura, pero `_estado(0)` lo
convertía en OK. Las pruebas anteriores esperaban ese resultado. Además,
`_cotejar_ticket` agregaba `ticket_verificado` a lecturas incoherentes por el
mismo motivo: score cero. Había cuatro registros con ambos motivos.

## Invariante y corrección

Ausencia de riesgo no equivale a evidencia de verificación. Si no hay una alerta
local superior, servicio no disponible corresponde al estado existente `error`;
lectura ilegible, incompleta o incoherente corresponde a `revision`. Ninguna suma
riesgo por sí misma. Las alertas locales de duplicidad y sus puntajes se conservan.
`ticket_verificado` exige lectura coherente e importe dentro de la tolerancia.
No cambian importes, litros, fotos, tolerancias ni contratos de API/modelos.

La regresión queda cubierta en `LogisticaCombustibleAuditoriaTests`: caída del
lector, lecturas incompletas/ilegibles/incoherentes, coincidencia válida,
redondeo de litros, diferencias de importe y duplicidad incluso sin lector.
Se observaron ocho fallos antes del fix y las 16 pruebas pasaron después.

## Recuperación y límites

Restablecer saldo requiere intervención del titular de OpenAI. No confundir un
fix de clasificación con recuperación del proveedor. No recargar, cambiar claves,
aumentar presupuestos ni sustituir proveedor sin autorización específica.
Mauricio autorizó validar y reprocesar los afectados, no recargar saldo.

Cuando haya saldo, seleccionar únicamente las cargas con
`auditoria_detalle.ticket_leido.status == "no_disponible"`, respaldar sus detalles,
validar una lectura y reprocesar secuencialmente con `auditar_carga_combustible`.
Detenerse si vuelve `lectura_no_disponible`; no ejecutar el comando global
`reauditar_cargas_combustible` indiscriminadamente. Verificar persistencia y la
pantalla `/logistica/bitacoras/`; una respuesta del proveedor no basta.

La regularización histórica debe cambiar solamente estado y motivos de auditoría,
conservar todos los datos y detalle originales, respaldar y comparar antes/después.
Reversión de código por revert del commit; reversión de metadatos solo con el
respaldo exacto y comprobación de que no hubo una auditoría posterior.
