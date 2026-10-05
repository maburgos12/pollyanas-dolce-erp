# Ficha de fuentes — soporte financiero de trabajos

Fecha y ambiente: 05/10/2026; producción consultada sólo lectura y PostgreSQL 16 local aislado para validación. Contrato aditivo Administración/Activos aprobado el 03/10/2026; preparación documental del 04/10/2026 revalidada con main.

## Necesidad y unidad de análisis

Una confirmación documental entre una OrdenMantenimiento o ReporteFalla y un documento financiero existente. Cardinalidad M:N; INSTALACION no exige Activo. Una relación no captura gasto, reconoce devengo, paga, concilia, reparte, deduce ni excluye importes. Originales financieros y costos operativos permanecen en sus fuentes.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Trabajo | activos.OrdenMantenimiento / activos_ordenmantenimiento | Mantenimiento, captura de órdenes y planes | PK y Activo.sucursal | 63 órdenes, 196 activos; lectura producción 05/10 | Bandeja, expediente QR, índice vigente |
| Incidencia | fallas.ReporteFalla / fallas_reportefalla | Fallas / Mantenimiento | PK, sucursal, tipo_objetivo | 109 reportes; INSTALACION admite Activo NULL | Bandeja, Fallas, índice vigente |
| Gasto | reportes.GastoOperativoMensual / reportes_gastooperativomensual | Captura/importación de Reportes | PK, periodo/centro/categoría; sin Area FK | 1338; lectura producción 05/10 | P&L, presupuesto/real |
| Obligación | reportes.ObligacionGasto / reportes_obligaciongasto | Servicios de gastos/compromisos | PK, área; OneToOne gasto_operativo | 172 obligaciones con espejo en preparación; conteo fresco 172 | Presupuesto/real, compromisos |
| Fiscal | sat_client.CfdiDescargado / sat_client_cfdidescargado | Descarga SAT | PK y UUID único | 9648; conteo fresco 05/10 (9644 en preparación 04/10) | SAT, conciliación fiscal |
| Banco | syncfy_client.MovimientoBancario / syncfy_client_movimientobancario | Syncfy/importaciones y conciliación | PK, transacción/cuenta; cfdi_relacionado/movimiento_relacionado existentes | 13555; preparación 67 con CFDI | Conciliación bancaria |
| Etapas | reportes.ParcialidadObligacionGasto / PagoObligacionGasto | Servicios de compromisos/pagos | PK y obligación | 0 parciales / 0 pagos en estas tablas 05/10: no acredita ausencia de pagos en otras fuentes | Compromisos, trazabilidad |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Obligación ↔ gasto espejo | Confirmada sólo por FK OneToOne existente | Mismo hecho documental; un nombre, importe, fecha o sucursal coincidente no confirma identidad | Una selección por cualquiera de sus accesos no duplica la confirmación; una obligación agregada después conserva el recibo original |
| CFDI, movimiento, obligación | Distintas etapas/soportes | Relaciones financieras propias permanecen intactas; no se suman como gastos adicionales | Ninguna equivalencia automática |
| Costos de trabajo ↔ documento | No resuelta por diseño | Vincular soporte no asigna importe ni altera costo_real/estimado, duplicado_de o índice histórico | Asignación/devengo/IVA requiere política y autorización específica |
| Crucero histórico / Bamoa | Distintos ámbitos conservados | No reasignación por coincidencias | Historia sin evidencia pendiente |

## Decisión de diseño

Reutilizar las cuatro fuentes financieras; extender Mantenimiento únicamente con DocumentoFinancieroTrabajo y AuditLog transaccional. IDs originales inmutables y FK SET_NULL conservan procedencia; unicidad global trabajo/documento y gasto espejo sin partición por actor. Reenvío exacto retorna PK/autor/evidencia originales; contenido incompatible responde conflicto. Borrado/PK reutilizado no reconstruye confirmaciones.

Lectura exige alcance actual del trabajo y fuente. Obligaciones reutilizan _areas_resumen_permitidas; su espejo exige además consulta de Reportes. Gastos independientes siguen GET global can_view_reportes y confirmación gestor Reportes: no se infiere un área por centro, categoría o nombre. Confirmación de obligación usa usuario_puede_capturar_area con alcance de lectura vigente; fiscal exige conciliacion.fiscal y banco DG/admin. Revocación se evalúa con actor recargado; relación no concede permiso ni revela importes sin can_view_costs.

Consultas reproducibles: inventario_fuentes_datos --term gasto --term obligación --term CFDI --term movimiento, PostgreSQL 16. Evidencia externa de lectura con límites/READ ONLY: entorno-documentos-financieros-20261005/integridad-produccion-inicial.json y preparación preparacion-conteos-documentos.json. Las cifras son cobertura por tabla, no relaciones inferidas. Pruebas locales comparan SHA256 de todas las fuentes antes/después; sólo vínculo y AuditLog pueden crecer.

Riesgos y pendientes: los 28 grupos del índice vigente, fuentes originales y revisión histórica conservan cálculo actual; no congelar conteos de una foto como dato vivo. Lectura de fuentes eliminadas sólo cuando la capacidad global necesaria permite el tombstone, sin reconstruir títulos/importes. Validación de navegador y producción corresponde al cierre integral de la tarea.
