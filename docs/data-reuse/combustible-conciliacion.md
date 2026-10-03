# Ficha de fuentes — conciliación combustible septiembre 2026

Fecha y ambiente consultado: 03/10/2026, PostgreSQL de producción, lectura.

## Necesidad y unidad de análisis

Conciliar documentos de compra anticipada, aplicación fiscal, carga física y pago. Una fila CFDI es un documento; un concepto puede identificar un ticket. Una carga es un consumo registrado por unidad. Un pago y un anticipo no son consumos adicionales.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Documento fiscal | sat_client.CfdiDescargado / sat_client_cfdidescargado | Descarga SAT | UUID, RFC, tipo y fecha | Septiembre: 300 recibidos; 13 documentos relevantes al informe (7 anticipos,5 consumos,1 impresión excluida) | Conciliación bancaria y reportes |
| Cobertura SAT | sat_client.SolicitudDescarga / sat_client_solicituddescarga | Jobs SAT | Solicitud, dirección, RFC y rango | 26 días terminados con 300 anunciados/almacenados; cuatro días con 5004 y cero | Descarga y monitoreo SAT |
| Carga física | logistica.CargaCombustibleUnidad / logistica_cargacombustibleunidad | Captura de repartidor; auditoría ticket | id, unidad, bitácora, folio y hash | 30 cargas $32,000; 28 con OCR favorable y 2 respaldos inválidos | API logística, auditoría y presupuesto real |
| Turno | logistica.BitacoraSalidaLlegada / logistica_bitacorasalidallegada | Flujo logístico | id,fecha,unidad | Fechas y campos históricos; sin importes no nulos distintos que sumar | Operación logística |
| Pago | syncfy_client.MovimientoBancario / syncfy_client_movimientobancario | Importación bancaria / conciliación | id_transaction y cfdi_relacionado | Cero movimientos de septiembre; cobertura insuficiente para confirmar pago | Conciliación |
| Gasto agregado | reportes.GastoOperativoMensual / reportes_gastooperativomensual | Captura/importación Administración | id,external_key,período/centro/categoría | Consulta acotada agosto/septiembre sin coincidencias combustible en categoría/comentario | Presupuesto y reportes |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia | Revisión |
| --- | --- | --- | --- |
| Rosa Hildeliza Famania Ortega / Servicio Carranza / iGas | Confirmada para tickets observados | Foto de tickets 2979547/2979907 muestra RFC FAOR391222TTA igual al CFDI | No extrapolar a otras estaciones |
| Carga119 ↔ FD19345 | Confirmada documental | Folio 2979547, RFC, diésel, $1,400 y foto | Pago/canje sigue no demostrado |
| Carga120 ↔ FD19363 | Confirmada documental | Folio 2979907, RFC, diésel, $1,200 y foto | Pago/canje sigue no demostrado |
| Anticipos FA ↔ vales canjeados septiembre | No resuelta | CFDI genérico no identifica canje; falta estado de cuenta | Requiere proveedor/DG |
| Facturas gasolina con centavos ↔ consumo personal | No resuelta | Importe no acredita persona/unidad | Requiere DG |
| Ribbon Premium ↔ gasolina | Distinta | Clave44103124 y descripción impresión; etiquetas clave55121600 | Excluir del análisis de combustible |

## Decisión

Reutilizar CFDI, cargas, turnos y auditoría existentes. Extender el servicio de conciliación y la tarea de correo sin crear captura maestra, alterar importes ni aplicar equivalencias en base. Conservar anticipos separados, distribuir conceptos con impuestos y descuentos, y cruzar identificadores con estación, importe y litros. El resultado agrega detalle y estado; diferencia queda null porque no representa un faltante conciliado. El único consumidor de producción localizado es la tarea mensual; se prueba el cuerpo y asunto del correo con backend de envío simulado. Falta libro/estado de cuenta de vales para saldo y aplicación; no sustituirlo por una resta mensual.

Procedimiento reproducible: conexión PostgreSQL confirmada; SELECT en transacción READ ONLY; `inventario_fuentes_datos --term combustible --term vale --term gasolina`; 517 XML I/E recibidos disponibles desde agosto al corte; cargas septiembre; movimientos bancarios y gastos acotados; comparación solicitud/conteo ingerido; revisión de ocho fotos. Evidencia y criterios en conciliacion.md, evidencia.json, clasificacion.json y cobertura-verificada.json.

Riesgos y pendientes: dos fotos inválidas, 26 tickets sin aplicación fiscal localizada, tres gasolinas sin atribución, saldo de vales inicial/final desconocido y cero cobertura bancaria septiembre. El informe automático Celery no lee un skill de Codex; la habilidad guardada no modifica el job.

## Validación del cambio

18 pruebas del servicio y consumidor mensual en PostgreSQL 16 aislado, incluyendo regresiones del ribbon, anticipos, CFDI emitidos/cancelados/otro receptor, duplicados, factura mixta, moneda extranjera y XML inválido. Snapshot real de septiembre reproducido en la base local: 7 anticipos $28,000; 5 consumos $7,710.83; 30 cargas $32,000; 2 cruces documentales $2,600. Consulta y reporte de producción se verificarán después del despliegue oficial. No cambia modelos, migraciones, API pública, entorno ni datos operativos.
