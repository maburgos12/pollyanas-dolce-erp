# Ficha de fuentes — alcance real de Producido vs Vendido

Fecha y ambiente consultado: 7 octubre 2026, PostgreSQL16 local aislado y ERP producción, consultas de solo lectura.

## Necesidad y unidad de análisis

Una fila representa un producto fabricado en el mes, agregado desde los expedientes producto/sucursal. El DG excluye reventa, accesorios, bebidas, cargos adicionales, vasos preparados y Otros postres de este reporte; sus fuentes y expedientes siguen disponibles en auditoría de inventario. Devoluciones es una ubicación interna de tránsito, no una sucursal cuyo saldo deba bloquear este reporte. Las transferencias hacia/desde ella siguen contabilizadas en el expediente de cada sucursal participante.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Ecuación y cantidades | ProductInventoryAuditCase / reportes_productinventoryauditcase | InventoryAuditMaterializer | mes + PointBranch FK + PointProduct FK | 1996 casos de septiembre; cada mini tiene diez ubicaciones con saldos comprobados y Devoluciones sin extremos | auditor, reporte, CSV/XLSX/PDF |
| Clasificación original | PointProduct / pos_bridge_products | sincronización Point | external_id, dominio producto; SKU puede colisionar | Vela Individual original Alegría, receta Bollo; Tarjeta de Regalo original Otros postres, receta Media Plancha | inventario, auditor |
| Participación en fabricación | Receta / recetas_receta; RecetaCodigoPointAlias | catálogo existente | código/alias único, tipo PRODUCTO_FINAL, modo_costeo y pasa_modulo_produccion | cafés FABRICADO/True requieren exclusión por categoría original; vasos Dot Cake tienen producción guardada pero el DG excluye esta categoría del reporte | costeo, producción, reporte |
| Ubicación | PointBranch / pos_bridge_branches; Sucursal / core_sucursal | catálogo Point y vínculo ERP | FK exacta, código ERP y nombre de ubicación | PointBranch6/external12/Devoluciones sin FK ERP; Sucursal código DEVOLUCIONES | inventario, auditor, selector del reporte |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Código/alias Point de receta | confirmada sólo si única | reutilizar _confirmed_recipe_map; no unión por nombre | conservar ambigüedad |
| Categoría Point y categoría receta | distintas | Alegría→Bollo y Otros postres→Media Plancha demuestran que ambas se deben revisar para alcance | instrucción expresa del DG en este hilo |
| Devoluciones | ubicación interna | nombre Point exacto y código ERP DEVOLUCIONES; no equiparar con CEDIS | DG: no revisar sus saldos en Producido vs Vendido |

## Decisión de diseño

Extender únicamente read_audit_report, lector común de pantalla, JSON y exportaciones. Excluir las categorías expresamente señaladas en cualquiera de los dos catálogos y recetas REVENTA/SERVICIO_ACCESORIO. Conservar rebanadas y derivados FABRICADO aunque no pasen por captura de producción: sus conversiones y ventas siguen en el reporte como referencia. Productos aún sin receta conservan visibilidad si su categoría no está excluida. Rosca sin actividad mensual se omite; una rosca con movimientos permanece. No cambiar maestros, fuentes, coverage ni estado de los expedientes. Contadores y estados del reporte usan sólo sus filas elegibles.

Reutilizar las ecuaciones guardadas por ubicación; no recalcular con una segunda fuente. Los pendientes muestran el dato y la ubicación concreta dentro de Ver detalle. El cálculo y el final Point de los minis dejan de ocultarse por Devoluciones. Los otros faltantes documentales permanecen pendientes.

Procedimiento: inventario_fuentes_datos --term producido --term reventa --term sucursal, 81 candidatos léxicos; consultas acotadas por septiembre a read_audit_report, recetas confirmadas, Devoluciones y casos de 3 Pecados Mini. Antes del cambio: 194 filas y 31 categorías, cinco minis con 10/11 saldos; Devoluciones producción/ventas/merma/conversiones/ajustes=0, transferencias entrada337/salida341.3. Las cinco historias mini de Devoluciones ya consultadas se reutilizan; este cambio no realiza Point HTTP.

Recursos locales de esta tarea: propietario codex, tarea producido-vendido-alcance-real-20261007, Compose erp_producido_vendido_alcance_20261007, PostgreSQL16 puerto55492, contenedor erp_producido_vendido_alcance_20261007-db-1, volumen erp_producido_vendido_alcance_20261007_postgres_data y red homónima_default. Bases pastelerias_erp/test_pastelerias_erp sólo de pruebas de esta tarea. Retiro tras entrega con respaldo verificado.

Riesgos: categorías desconocidas conservan el contrato anterior; un nombre no decide identidad. Verificar mini por mini en la vista autenticada y exportaciones. Este alcance de reporte no cambia el guard del cierre mensual de inventario ni acredita conteo físico.

Validación local PostgreSQL16: 130 pruebas del reporte, exportaciones, shell PWA y runtime/plan nativos aprobadas; check sin errores, migrate --check sin pendientes y makemigrations --check --dry-run sin cambios. Pruebas conservan derivados/rebanadas, desconocidos visibles, Rosca con actividad, deduplicación de sucursales y falta real distinta de cero. Versión del shell y sus dos registros sincronizados; fetch permanece de red, sin caché de datos.
