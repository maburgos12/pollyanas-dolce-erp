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

La recuperacion del saldo historico real del 31/07 sigue pendiente de una
fuente acreditada. Este cambio no modifica los cierres historicos persistidos.

## Validacion

Regresiones para fechas cercanas, cambio de anio, febrero bisiesto, cierre
generado al dia siguiente, cantidades originales frente a equivalentes,
cobertura JSON, productos no homologados, fingerprint y bloqueo concurrente
del mes previo. HTML y exportaciones comparten los saldos conocidos.

Pruebas locales sobre PostgreSQL 16; revision independiente del diff.
La validacion local no sustituye CI, deploy ni verificacion productiva.
