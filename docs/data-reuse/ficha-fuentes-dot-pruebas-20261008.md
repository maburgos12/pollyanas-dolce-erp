# Ficha de fuentes — Dot Cake y pruebas de producción

Fecha: 8oct2026. Lectura PostgreSQL de producción; implementación PostgreSQL16 aislada.

## Necesidad y unidad

Reincorporar Dot Cake fabricado al mismo Producido vs Vendido, por producto/sucursal/mes.
No reclasificar maestros ni sumar preparaciones a productos finales. Cero venta no elimina producción o merma.

|Concepto|Modelo / tabla|Writer|Identidad y evidencia|Consumidores|
|---|---|---|---|---|
|Producto final|PointProduct / Receta|Sincronización Point / recetas existentes|Dot Chocolate SKU4358/external1048/receta507; Vainilla SKU8734/external1047/receta509; FABRICADO. Categorías originales de vasos.|read_audit_report, pantalla, JSON, CSV/XLSX/PDF|
|Producción|PointProductionLine|Sincronizador Point existente|17 detalles originales septiembre: Chocolate113PZA, Vainilla142PZA; SKU y detail.PK_Producto del dominio producto.|Materializador existente|
|Merma|PointWasteLine|Sync existente|Chocolate25PZA/Vainilla36PZA en septiembre. CEDIS4sep:2/3PZA, motivo explícito pruebas y fotos; producción22975 detalles86550/86551:2/3.|Materializador existente|
|Saldos y ecuaciones|ProductInventoryAuditCase|InventoryAuditMaterializer|Casos persistidos por par; ausencia de frontera conserva estado incompleto. No sumar ceros placeholder como prueba.|Reporte y exportaciones existentes|
|Pruebas Pan de Muerto|PointProductionLine / PointWasteLine / original Stock|Sincronizadores existentes / GET acotado principal protegido|PMH028, IsInsumo=true/FK428:74PZA preparación; merma1684716:34PZA pruebas. Original86filas/sin saturación prueba seis movimientos septiembre continuos0+74−34=40; producción40/1685163 posterior a merma y vecino octubre parte40.|Conocimiento del agente; no se traslada a producto final|

## Equivalencias y decisión

«Cake dot» del DG corresponde a los dos Dot Cake corroborados mediante códigos y recetas existentes, no por semejanza de nombres. La excepción autorizada es sólo de presentación del reporte: código exacto de receta/producto, modo FABRICADO y categorías originales de vasos compatibles. Una receta ambigua, reventa, servicio o categoría distinta no recibe excepción. Las demás exclusiones del DG permanecen.

Reutilizar read_audit_report y su validación de receta única. No nueva captura, modelo ni catálogo. La versión del reporte cambia para invalidar presentación cacheada. Se conservan saldos y estados reales; inclusión no acredita cierre.

Reutilizar también ProductDocumentaryCloseService y su nota existente «Ver motivo»: si original_batch_evidence acredita un salto cuyo movimiento pertenece al mes, mostrar una explicación breve de Point en vez de falta genérica de saldo. No aceptar continuidad ni cerrar ese par. Los saltos anteriores al mes no reciben esa nota mensual. Consumidores: evaluate/close_eligible, evento documental, detalle y popup existentes; regresión en tests_product_documentary_close.

Procedimiento: inventario_fuentes_datos --term produccion --term merma --term auditoria (candidatos léxicos, no prueba de identidad); lectura acotada septiembre de PointProductionLine/PointWasteLine y casos por producto. Originales conservados en pruebas-produccion-cake-candidatos-septiembre-20261008.json del expediente.

## Riesgos y aceptación

Pruebas: ambos Dot sin venta incluidos, reventa/categoría contradictoria excluidas, accesorios y demás categorías no reintroducidas, exportaciones usan el mismo lector. No alteración de fuentes o stock. Comprobación ejecutable comprobar-pan-preparacion-20261008.py verifica wireSHA, identidad, dominio, fechas UTC frente al corte local, cancelación, ecuación y cadena/vecino; no afirma que toda la producción acabó en merma ni infiere físico.
