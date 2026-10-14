# Ficha de fuentes — investigación de recetas del agente

Fecha: 2026-10-13/14. Metadatos y presencia verificados en PostgreSQL de producción; pruebas con fixtures en PostgreSQL16 aislado. Inventario completo conservado en los artefactos de la tarea `ai-erp-transversal-discovery-20261013`.

## Necesidad y unidad de análisis

Buscar recetas por nombre/código, seguir sus componentes y consultar versiones guardadas de costo sin solicitar IDs al usuario ni recalcular datos operativos. Un resultado de búsqueda es una receta candidata; una línea es un componente registrado; una versión es un snapshot histórico, no el costo actual completo de operación.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
|---|---|---|---|---|---|
| Receta y rendimiento | recetas.Receta / recetas_receta | Flujos existentes de recetas | PK y hash de contenido; catálogo global protegido por can_view_recetas | Tabla y filas presentes | Recetas, costeo, nuevo lector |
| Código alternativo | recetas.RecetaCodigoPointAlias / recetas_recetacodigopointalias | Catálogo de alias existente | Código normalizado único; FK a receta; sólo alias activos | Tabla y filas presentes | Búsqueda de productos/recetas |
| Ingredientes | recetas.LineaReceta / recetas_lineareceta | Importación y revisión de vinculación | FK receta e insumo; posición; tipo de línea | Tabla y filas presentes | Recetas y costeo |
| Costo versionado | recetas.RecetaCostoVersion / recetas_recetacostoversion | recetas/utils/costeo_versionado.py | Unicidad receta/version y receta/hash | Tabla y filas presentes | Ficha de recetas, Gateway histórico, nuevo lector |
| Costos mensuales | reportes.RecetaCostoHistoricoMensual | Servicio histórico mensual | Concepto y periodo distintos | Tabla y filas presentes | No incorporado a este corte |

## Alias y equivalencias

- Alias activo con FK: se reutiliza como relación explícita; no se crea equivalencia nueva.
- Nombre parecido: candidato, no identidad; varias coincidencias exigen selección.
- Línea pendiente/rechazada: vínculo declarado sin equivalencia aprobada. Se conserva estado y texto original.
- Versiones históricas y líneas actuales: momentos diferentes; prohibido presentarlos como una fotografía conjunta.
- Subsecciones: desglose que no debe duplicar costo del componente principal.

## Decisión de diseño

Extender el puente de lecturas existente con `erp.search_recipes` y `erp.get_recipe_context`. No se añaden modelos, migraciones, importadores ni captura duplicada. Registros seleccionados por ORM y salida explícita: sin SQL del modelo, sin hashes internos ni datos del aprobador. Se conservan decimales exactos, null, fecha y truncación. Costos futuros se excluyen; ninguna versión se crea ni recalcula.

Misma autenticación, participación, gasto, auditoría y revocación del puente actual; además exige `can_view_recetas`. Esta entrega conserva la dependencia de elegibilidad de activos del piloto; no la retira de forma parcial ni incorpora nuevos participantes.

## Verificación reproducible

`inventario_fuentes_datos --all --presence --details --limit 1500` y repetición con `--term receta --term producto` dentro del conjunto de términos auditado. La presencia usa SELECT EXISTS en transacción READ ONLY; no se extraen valores personales ni se deduce cobertura temporal. Tests: `orquestacion.tests_agent_recipe_reads`, `orquestacion.tests_agent_business_reads`; presentación: `node orquestacion/tests_agent_results_ui.js`.

## Riesgos y pendientes

No ofrece precios actuales, margen, receta multinivel consolidada, costos mensuales, exportaciones ni acceso transversal completo. Las versiones guardadas pueden omitir costos operativos; la respuesta debe conservar esa limitación. No invocar servicios de generación de versiones desde el lector. La matriz transversal sigue en progreso; 382 modelos encontrados no implica 382 capacidades autorizadas.
