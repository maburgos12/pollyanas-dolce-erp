# Ficha de fuentes — rebanados y servicios en rentabilidad

Fecha y ambiente: 31 octubre 2026, PostgreSQL de producción, consultas acotadas.

## Necesidad y unidad de análisis

Costo material por rebanada y mes; distinguir un servicio sin costo asignado por decisión DG de un costo desconocido.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Ciruela R | PointProduct / pos_bridge_pointproduct | Point | 532, externo 114, SKU 0114 | Identidad coincide con receta 193 | Ventas y rentabilidad |
| Conversión | RecetaPresentacionDerivada / recetas_recetapresentacionderivada | Regla existente | 11, padre 199, derivada 193 | Activa desde 23 mayo: mediano / 10 | Costeo de recetas e histórico |
| Complementos | LineaReceta / recetas_lineareceta | Receta sincronizada | Receta 193 | Cuchara CH, etiqueta CH, envase 1414; servilleta marcada REJECTED | Costeo |
| Histórico | RecetaCostoHistoricoMensual / reportes_recetacostohistoricomensual | MonthlyHistoricalCostingService | 1066, septiembre | 87.141341 incluye 5 líneas; padre 20.130760 | Rentabilidad, confianza de ventas |
| Domicilio | ProductoReventaCosto / reportes_productoreventacosto | Confirmación DG | UNALLOCATED_SERVICE, identidad exacta | Decisión documentada en tarea anterior | Rentabilidad y confianza |

## Alias y equivalencias

Ciruela R → receta 193 → mediano 199: confirmada por códigos y relación existente. No se crea equivalencia ni se modifica BOM. Domicilio conserva la decisión ya autorizada; no se inventa costo ni margen individual.

## Decisión

Reutilizar las fuentes. El costeo histórico debe excluir líneas REJECTED, igual que el servicio canónico de presentaciones. El panel de riesgo debe excluir servicios con UNALLOCATED_SERVICE documentado; mantiene alertas de costos realmente desconocidos.

Alcance: constructor histórico (incluidos sus consumidores de preparaciones y presentaciones), panel Productos, dos regresiones. Sin modelos, migraciones, dependencias ni cambios a ventas.

Procedimiento: inventario_fuentes_datos --term rebanada --term presentacion --term costo; consulta de producto 532, recetas Ciruela, relación 11 y snapshot septiembre. Evidencias operativas, respaldo y verificación en Documents/Codex/2026-10-31/margen-rebanados.

## Riesgos

No ejecutar build_period para una corrección puntual: también elimina snapshots fuera de su selección. Conservar respaldo y corregir únicamente el snapshot afectado con costos históricos del mismo mes; verificar el reporte servido después del despliegue. Los importes administrativos IMSS/Infonavit encontrados en el soporte de septiembre no certifican por sí solos la parte exclusivamente patronal.
