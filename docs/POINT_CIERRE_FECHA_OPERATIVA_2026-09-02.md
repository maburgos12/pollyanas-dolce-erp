# Correccion de fechas operativas del cierre Point

## Contrato

- Todos los meses usan el ultimo dia del mes anterior como inicial y el ultimo
  dia del mes seleccionado como final, en America/Mazatlan.
- El envio del correo al dia siguiente no cambia la fecha operativa. Las
  capturas en vivo tampoco se desplazan un dia automaticamente.
- El cierre previo solo aporta cantidades originales por receta y fecha exacta.
  Sus lineas agregadas a equivalentes no son inventario por producto.
- Se conservan cobertura, productos sin homologar y advertencias al trasladar
  saldos. Un problema de autoridad no oculta una cantidad Point conocida.
- No se introducen fisicos, ajustes manuales, backfill ni cambios a schedules.

## Evidencia de solo lectura

Consulta de produccion y correos Outlook realizada el 2 de septiembre de 2026:

| Cierre operativo | Correo recibido | Contenido observado |
| --- | --- | --- |
| 31/07/2026 | 01/08/2026 | Sin captura, 0 productos y todas las sucursales sin captura |
| 31/08/2026 | 01/09/2026 01:01 | 336 productos; ultima captura 31/08 a las 23:08 |
| 01/09/2026 | 02/09/2026 01:01 | 336 productos; ultima captura 01/09 a las 23:08 |

El cierre guardado de julio declara fecha efectiva 20/07, aunque su objetivo
es 31/07. El algoritmo anterior seleccionaba 03/08 para el inicial de agosto.
Ninguno acredita existencias del 31/07.

Para la receta 99, el final Point del 31/08 es 22; el reporte anterior lo
ocultaba al no acreditar conjuntamente ambos cortes. La correccion conserva
ese final disponible sin inventar inicial, diferencia ni coincidencia.

El correo vacio no demuestra ausencia de datos en Point. La consulta posterior
desde el VPS con la conexion HTTP existente acredita, para el producto 857
(P3PMINI, receta 99), 15 piezas al 31/07 y 22 al 31/08 en las diez ubicaciones
operativas. Se uso el historial `/Stock/GetHistorial`, interpretando `Fecha` en
UTC como hace la interfaz Point y cortando a medianoche de America/Mazatlan.
En julio coinciden el saldo posterior al ultimo movimiento previo y el saldo
anterior al primer movimiento posterior en cada ubicacion.

El movimiento de agosto de ese producto concilia: inicial 15 + produccion 181
- venta 168 - merma 6 + ajuste de entrada 2 - transferencias netas 2 = final 22.
La base ERP ya guarda produccion 181, venta 168, merma 6 y final Point 22.
La integracion persistente de la fuente historica sigue pendiente de autorizacion;
este cambio no importa ni modifica los cierres historicos persistidos. Los
reportes Point "Existencia a la fecha" 1040 y 1041 quedaron solicitados para
contrastar los archivos oficiales, todavia pendientes de generacion al consultar.

La auditoria de ventas diarias de agosto encontro 279 combinaciones sucursal/dia
esperadas y 279 acreditadas en los logs de 31 jobs exitosos. Las 10,476 filas
persistidas coinciden con los contadores por sucursal/dia, sin faltantes ni
diferencias. El validador actual exige un unico job mensual, mientras el escritor
oficial usa ventanas de tres dias. Produccion usa ventanas de siete dias. Este
PR no modifica esas reglas de autoridad: sus avisos no acreditan por si solos
ausencia de movimientos. Parte de las ventas sin receta corresponde a articulos
como pirotecnia y toppings; tampoco equivale por si sola a ventas faltantes.

## Validacion

Regresiones para fechas cercanas, cambio de anio, febrero bisiesto, cierre
generado al dia siguiente, cantidades originales frente a equivalentes,
cobertura JSON, productos no homologados, fingerprint y bloqueo concurrente
del mes previo. HTML y exportaciones comparten los saldos conocidos.

Pruebas locales sobre PostgreSQL 16; revision independiente del diff.
La validacion local no sustituye CI, deploy ni verificacion productiva.

La primera corrida completa de CI ejecuto 4,335 pruebas y detecto una expectativa
obsoleta de concurrencia: la prueba trataba marzo como independiente de abril.
Ahora marzo es fuente protegida del inicial de abril. Se conserva la prueba de
mes independiente usando febrero y se agrega una regresion para el escritor del
mes previo, que debe terminar sin deadlock y dejar detectable cualquier cambio
en las lineas de abril. No se relajan los locks ni las verificaciones de digests.
