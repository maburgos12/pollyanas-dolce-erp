# Ficha de fuentes — cobertura SAT

Fecha: 2026-10-14. PostgreSQL16 aislado; auditoría productiva previa de solo lectura.

## Necesidad y unidad de análisis

Conservar cada CFDI recibido/emitido por UUID, detectar ausencias frente al SAT y retomar solicitudes/paquetes interrumpidos. Una consulta terminada no prueba cobertura fiscal permanente.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creador/actualizador | Identificador | Evidencia | Consumidores |
|---|---|---|---|---|---|
| Original XML | sat_client.CfdiDescargado | importación SAT | UUID único | 9,902 XML válidos para parser; 6,593 emitidos/3,309 recibidos | conciliacion, reportes, gastos |
| Trámite y paquetes | sat_client.SolicitudDescarga | solicitud/verificación SAT | IdSolicitud único | 914 pendiente;848/850 timeout;852 cuota | job nocturno, comando mensual |
| Pago relacionado | sat_client.CfdiPagoRelacionado | XML complemento | complemento+UUID+parcialidad | 98 referencias a83 UUID ausentes | conciliación de pagos |
| Evidencia de ingesta | sat_client.LogDescargaSat | procesamiento | solicitud+fecha | conteos septiembre711 emitidos/300 recibidos | diagnóstico y pantalla conciliación |
| Inventario fiscal externo | Metadata SAT | servicio SAT | UUID+RFC emisor/receptor | no se almacenaba | nueva comparación con XML existentes |

## Alias y equivalencias

UUID del XML y Uuid de metadata representan el mismo folio fiscal, validando RFC y dirección. No se equipara factura con complemento ni fecha de pago con emisión. No se fusionan proveedores, gastos o facturas por nombre/importe.

## Decisión

Reutilizar CfdiDescargado como único archivo de originales. Extender SolicitudDescarga con folio consultado, avance por paquete e inventario metadata de esa consulta. El inventario es evidencia fechada del SAT, no otra tabla maestra de facturas. La consulta por UUID no cuenta como cobertura de un día. Metadata permite distinguir cancelaciones sin borrar XML ni convertir documentos sin XML en facturas completas. Conservar vínculos existentes y conciliado; reimportar pagos mediante actualización por su clave en una transacción.

El cambio añade columnas, no altera ni elimina migraciones previas. Los consumidores existentes mantienen el contrato del XML y sus PK/UUID. No cambia nómina, gastos, reparto o reglas contables.

## Procedimiento y límites

`manage.py inventario_fuentes_datos --term cfdi --term solicitud --limit 12` sobre PostgreSQL local configurado; resultado en task-runtime/sat-cobertura-integral/fuentes.txt. Auditoría productiva: task-runtime/sat-auditoria-originales/{audit,details}.json. Grafo incompleto para sat_client: se usó inspección directa tras resultados insuficientes.

WSDL SAT consultado en vivo: https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/SolicitaDescargaService.svc?singleWsdl ; SolicitaDescargaFolio exige RfcSolicitante y Folio; el contrato de rangos acepta TipoSolicitud=Metadata. No aplicar filtros por tipo I/E/N/P/T. XML vigente e inventario fiscal tienen alcances distintos.

## Validación y reversión

Pruebas PostgreSQL: timeout recuperable, cuota no-completa, UUID no-cobertura, metadata inválida/receptor ajeno, altas tardías, reimportación sin pérdida de relaciones ni conciliación, conteos de paquetes. Deploy oficial tras CI y revisión. Si hay regresión, revertir código por Git conservando las columnas aditivas y XML ya recuperados; no bajar la migración borrando evidencia.
