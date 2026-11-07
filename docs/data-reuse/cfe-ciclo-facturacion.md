# Ficha de fuentes — CFE bimestral y gasto facturado

Fecha y ambiente: 2026-11-06; lectura acotada de producción y PostgreSQL16 aislado para pruebas sintéticas.

## Necesidad y unidad de análisis

Separar factura reconocida, periodo de servicio y pago. El mes intermedio de un recibo CFE bimestral documentado no debe exigir una segunda factura mensual ni generar otro gasto. Se conserva el criterio autorizado por Dirección: factura completa en su mes y referencia de consumo por separado.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Importe reconocido y fechas de servicio | reportes.GastoOperativoMensual | Conciliación de recibo original y CFDI | PK, centro, categoría, mes y UUID | Gastos2292,2298,1324,1325; las fechas estaban en PDF/comentario pero no en los campos existentes | Planeación, Rentabilidad, Costeo por etapas |
| Factura fiscal | sat_client.CfdiDescargado | Descarga SAT | UUID, RFC, vigencia, tipo, total, XML | CFDI9927,9157,9013,8844; recibos bimestrales de septiembre | Validador nativo validar_cfdi_soporte |
| Destino | CentroCosto / ReglaFuenteRubro / RubroPresupuesto | Catálogos existentes | Centro, sucursal, categoría y regla | Reglas16064,16066,16063,16065 | Lector compartido leer_gastos_mensuales |
| Periodicidad contractual | GastoRecurrenteVersion | Contratos existentes | Contrato y vigencia | No se localizó calendario CFE aplicable a estos cuatro registros | Se conserva, sin crear contratos |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Servicio CFE / gasto / UUID | Confirmada para cada recibo original | PDF cotejado por UUID, RPU e importe; carpeta de almacenamiento o vencimiento no definen mes del gasto | Aplicación autorizada por Dirección |
| Cobertura contable / periodo de servicio | Distinta | Las fechas de servicio no amplían cobertura_mes_inicio/fin ni prorratean la factura | No cambiar criterio contable |
| Mes intermedio / consumo cero | Distinta | Información no sumable: monto_mensual=None; no se crea gasto de cero | Mantener explicación en pantalla y CSV |

## Decisión de diseño

Reutilizar fechas, CFDI y reglas; no crear tablas, contratos ni captura paralela. El lector explica exclusivamente el mes inmediatamente posterior a una factura CFE de Fonsma vigente, con XML e importe validados, cuyo servicio abarque dos meses de calendario y45–75días, termine en el mes facturado y no tenga cobertura contable distinta. Una factura real del mes tiene prioridad. Falta de fechas, ambigüedad, otro proveedor/receptor o CFDI inválido conservan la exigencia de fuente. El siguiente ciclo vuelve a exigir su documento.

`ciclos_facturacion` es información no sumable, propagada a Planeación HTML/JSON/CSV, Rentabilidad y Costeo por etapas. No modifica importes ni pagos. Las referencias de consumo estimado siguen separadas de los gastos facturados.

Procedimiento: `inventario_fuentes_datos --term electricidad` (solo candidatos léxicos), inspección de campos y consulta de los cuatro registros en producción; pruebas sintéticas con PostgreSQL16. La búsqueda léxica no prueba identidad.

Riesgos: este criterio explica facturación bimestral, no certifica consumo mensual ni que no exista una factura extraordinaria fuera del inventario fiscal consultado. Agua en efectivo y detalle Telcel requieren sus fuentes; no se sustituyen por ceros.
