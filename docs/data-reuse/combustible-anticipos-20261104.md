# Ficha de fuentes — anticipos y aplicaciones de combustible

Fecha: 2026-11-04. PostgreSQL 16 local aislado y consultas acotadas de solo lectura en producción.

## Necesidad y unidad de análisis

Separar transferencia, anticipo facturado, aplicación fiscal y canje físico. Una pareja factura/nota de aplicación representa una sola aplicación; no son dos gastos. Mantener la fecha bancaria independiente de la emisión.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente oficial | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Anticipos y aplicaciones | sat_client.CfdiDescargado | XML recibido | UUID, emisor y receptor, relación 07 | 57 CFDI de FAOR391222TTA para GEF211230KR2 agosto-noviembre; FS2120 y NC2119 ya presentes | Reporte mensual, conciliación bancaria |
| Pago y devolución | syncfy_client.MovimientoBancario | Estado bancario e importador existente | Cuenta, referencia, tipo, fecha; vínculo CFDI explícito | BanBajío FONSMA *0201; sin movimientos septiembre/octubre antes del ajuste | Mesa bancaria y reporte |
| Trazabilidad de importación | conciliacion.ImportacionBancaria | confirmar_importacion | Hash, archivo, preview y número de filas | Selección de 19 movimientos de proveedor verificados en PDF | Mesa bancaria |
| Carga y ticket | logistica.CargaCombustibleUnidad | Captura operativa y fotografía | ID, estación y folio, hash | Fuente existente; no se sobrescriben registros | Bitácora, presupuesto y reporte |

## Alias y equivalencias

Servicio Carranza / Rosa Famania: RFC FAOR391222TTA confirmado en XML y soportes. FONSMA es cuenta BanBajío *0201; no incluir otros bancos. FA1402 se vincula al cargo de 7 octubre, no a su emisión del 8 ni al envío devuelto del 22. Vínculos ya revisados en la auditoría y ejecución autorizada por DG en este chat. No se introduce una regla automática de matching por monto/fecha.

## Decisión de diseño

Reutilizar modelos, importador y relaciones existentes, sin nuevas tablas ni migraciones. Reconstruir aplicaciones por relación 07, mismo emisor, importes consistentes y nota única. No asignar importes individuales FIFO cuando el XML agrupa anticipos. El residual corresponde solamente al circuito documental revisado, nunca al saldo físico total de vales. Relaciones incompletas, notas múltiples o importes imposibles mantienen residual pendiente.

Los movimientos se incorporan como selección parcial de proveedor, no como cobertura completa de la cuenta. Preservar página, hash y criterio del vínculo en metadatos y nota. La devolución se enlaza con su cargo y no con un anticipo. No crear gasto operativo ni modificar cargas ni importes de CFDI.

## Validación y riesgos

Inventario local: `inventario_fuentes_datos --term combustible --term anticipo --term bancario`. Consultas en producción por RFC, UUID y fechas; extractos reproducibles conservados fuera del repositorio en la carpeta de auditoría de DG. Tests: arrastre, aplicación sin doble conteo, XML incompleto, nota duplicada/otro proveedor y fecha bancaria distinta. Consumidor de código: tarea mensual `task_conciliar_combustible_mensual`; se valida su cuerpo sin enviar correo. La conciliación física continúa parcial.

La comprobación de la mesa bancaria detectó que el reverso se clasificaba como ingreso por contener SPEI. Se reutiliza el vínculo recíproco con soporte, misma cuenta/moneda/importe/rastreo para excluirlo de ventas y mostrarlo como devolución comprobada. La regla se comparte entre la mesa, el comparativo y la clasificación fiscal; conserva el abono en los totales bancarios brutos.
