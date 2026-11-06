# Ficha de fuentes — protección del recálculo de costos de 2025

Fecha y ambiente consultado: 2026-11-06; código ee33c0a y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Evitar que el reconstructor general sustituya costos documentales de 2025 con la estructura vigente de recetas o elimine historias sin ventas en su selección actual. La unidad existente es mes e insumo, receta o producto Point. El bloqueo comprende todo 2025, incluso meses aún sin documentos; la ausencia de una historia no acredita que la receta actual fuera vigente entonces.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Costo de insumo | InsumoCostoHistoricoMensual | Compras históricas y reconstrucción | periodo + insumo_id | Inventario de fuentes local; pruebas con costo de seis decimales | Costeo de recetas |
| Costo fabricado | RecetaCostoHistoricoMensual | Conciliaciones documentales auditadas; reconstructor genérico | periodo + receta_id | Auditoría 43350, referencias documentales conservadas en metadata | Rentabilidad mensual |
| Costo de reventa | ProductoReventaCostoHistoricoMensual | Conciliaciones documentales auditadas; reconstructor genérico | periodo + producto_point_id | Auditoría 43344; fixtures de preservación independientes | Rentabilidad mensual |

Las auditorías citadas son evidencia del cierre de 2025 ya verificada por la tarea integral; no se importan datos productivos al entorno de pruebas. El comando local enumera candidatos y no demuestra equivalencias.

## Alias y equivalencias

No se crean alias ni homologaciones. Se conservan las claves primarias y restricciones únicas existentes. La restricción por año es un contrato del reconstructor, no una equivalencia entre fuentes.

## Decisión de diseño

Reutilizar los tres modelos y proteger el acceso compartido `MonthlyHistoricalCostingService.build_period` antes de consultar objetivos o invocar constructores. Rechazar 2025 con una excepción específica; el comando de gestión la presenta como error y Celery la propaga. No hay opción de forzar ni cambio de modelos, configuración o importadores documentales. Los demás años conservan su comportamiento, incluida la eliminación de filas obsoletas validada en las pruebas existentes de 2026.

Consultas reproducibles: `python manage.py inventario_fuentes_datos --term costo`, con PostgreSQL aislado; inspección del servicio, comando y tarea consumidores. La búsqueda del grafo no devolvió el símbolo y se continuó con lectura acotada de esos archivos.

Riesgos y pendientes: un recálculo genérico de 2025 que antes aparentaba éxito ahora falla explícitamente. La corrección histórica debe continuar mediante conciliaciones auditadas; este bloqueo no completa costos faltantes ni valida utilidad anual. Verificar ejecución real del comando y preservación de las historias tras despliegue.
