# Ficha de fuentes — reportes de falla mediante el agente ERP

Fecha: 2026-10-09. Código main 734419ae; PostgreSQL16 local exclusivo y lectura acotada de producción.

## Necesidad y unidad de análisis

Crear un reporte de falla de EQUIPO a partir de una conversación, después de una confirmación humana. Una fila operativa representa una falla, no una conversación. El borrador técnico mantiene faltantes y versión; no duplica el catálogo de equipos ni el reporte operativo.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | activos.Activo / activos_activo | Activos | PK, sucursal, vigencia | Producción: 197 filas | Pasaporte, mantenimiento, App Operativa, READ Gateway |
| Falla | fallas.ReporteFalla / fallas_reportefalla | operacion.services_fallas.crear_reporte_falla | PK, sucursal, equipo, reportado_por | Producción: 111 filas | Fallas, mantenimiento, bitácora, notificaciones |
| Categoría de equipo | fallas.CategoriaFalla / fallas_categoriafalla | Catálogo de Fallas | PK, activo, tipo equipo | Producción: 5 categorías de equipo activas | Captura y agente |
| Bitácora | fallas.BitacoraFalla | El mismo servicio de creación | FK al reporte, usuario | Creación sintética comprobada: 1 fila por reporte | Fallas/mantenimiento |
| Borrador técnico | orquestacion.ChatToolCall + ChatToolResult | Agente, sin escritura operativa al preparar | UUID único, conversación propia, versión | PG16: continuidad, reintentos, concurrencia | Conversación y confirmación |

## Alias y equivalencias

Falla/incidente son términos de usuario para el proceso existente de ReporteFalla; el identificador seleccionado siempre es el PK del equipo autorizado. No se crea una equivalencia por parecido de nombre. Categorías de instalaciones/mobiliario/otro son distintas y quedan fuera del primer corte.

## Decisión de diseño

Reutilizar el servicio de App Operativa, sus reglas de modelo, bitácora y mecanismo de notificaciones. Reutilizar puede_reportar_activo y la autorización del submódulo fallas.reportar. Conservar el borrador en los modelos técnicos existentes; no crear tabla maestra, migración ni captura paralela.

Procedimiento: inventario_fuentes_datos --term falla --term incidente --term activo sobre PG16 propio; producción: BEGIN READ ONLY, SELECT COUNT de las tres tablas indicadas, COMMIT. Evidencia completa fuera de Git en task-artifacts/ai-erp-incidente-confirmado-20261009. Sólo conteos, sin extraer datos personales.

## Riesgos y pendientes

La lectura general de mantenimiento no concede derecho a reportar en cualquier sucursal. Este corte exige equipo vigente de la sucursal operativa asignada. Conserva la regla de fotografía o justificación; soporta texto/justificación, no archivos. Si ya existe una falla abierta, exige revisar la existente; no interpreta automáticamente que es otro problema. Las reglas empresariales actuales y su contrato se conservan.
