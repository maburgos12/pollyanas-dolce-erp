# Ficha de fuentes — seguimiento confirmado con fotografías

Fecha: 2026-10-10. PostgreSQL 16 local aislado; caso de producción revisado previamente en modo de solo lectura.

## Necesidad y unidad de análisis

Actualizar un reporte existente con un seguimiento, fotografías y fecha real del trabajo con precisión de día. Una falla conserva su folio; cada intervención confirmada genera una entrada de bitácora, y cada archivo queda asociado a esa entrada. Cotización y costo real son conceptos distintos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Reporte y presupuesto | fallas.ReporteFalla / fallas_reportefalla | Fallas, Operación y seguimiento existente | PK, sucursal, tipo objetivo, duplicado_de | Caso 114, instalación, proveedor Pedro, base 2500; costo real nulo | Fallas, Mantenimiento, conciliación documental |
| Intervención | fallas.BitacoraFalla / fallas_bitacorafalla | Seguimiento de Fallas y Mantenimiento | PK y reporte_id | Dos entradas previas en caso 114 | Bitácora de Fallas y detalle de mantenimiento |
| Evidencias | fallas.EvidenciaSeguimientoFalla / fallas_evidenciaseguimientofalla | Seguimiento existente; default_storage protegido | PK, bitacora_id, ruta privada | Tres fotografías aportadas para el seguimiento; no estaban incorporadas al revisar | Media autenticado, Fallas y Mantenimiento |
| Propuesta y confirmación | orquestacion.ChatToolCall / orquestacion_chattoolcall | Agent Core existente | UUID, propietario, conversación, versión | Pruebas con registros sintéticos y PostgreSQL | IA privada, historial y recibos |
| Fecha real informada | Extensión nullable de ReporteFalla | Confirmación del seguimiento | Día, sin hora; independiente de fecha_resolucion | Usuario informó 2026-10-09 para caso 114 | Detalle de Fallas, detalle de Mantenimiento y recibo IA |

## Alias y equivalencias

El Túnel / El Tunel y Pedro / Pedro Navarez: identificados mediante el reporte 114 y revisión humana previa. No se agregan alias maestros ni se unen proveedores automáticamente. Instalación no exige inventar un activo. Finalizado no significa pagado, cerrado financieramente ni comprobado mediante prueba funcional.

## Decisión de diseño

Reutilizar reporte, bitácora, evidencias, almacenamiento y propuestas del agente. Extender únicamente la fecha real opcional: el endpoint existente registra el momento de resolución y no acepta la fecha real del trabajo. No crear una segunda tabla de fallas ni de archivos. Los blobs de propuesta se vinculan a la evidencia final sin copiarlos.

Procedimiento: inventario_fuentes_datos --term falla --term evidencia --term mantenimiento; revisar modelos y consumidores de fecha_resolucion; consulta acotada del reporte 114 y sus relaciones de duplicados. El inventario es léxico y no establece equivalencias. Evidencia de auditoría local en task-artifacts/ai-erp-seguimiento-fotos-20261010.

## Riesgos y pendientes

Los informes existentes por fecha de resolución conservan su semántica de registro. Este corte muestra la nueva fecha real en el detalle y no recalcula conciliaciones históricas. El costo real continúa nulo hasta un proceso específico con comprobantes. No reescribir archivos ni eliminar evidencias o reportes anteriores.
