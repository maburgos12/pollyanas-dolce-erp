# Ficha de fuentes — costos históricos con nombre Point

Fecha y ambiente: 2026-11-04; producción consultada en lectura y PostgreSQL local aislado para pruebas.

## Necesidad y unidad de análisis

Reconocer una línea de compra histórica mediante el nombre Point ya registrado en el insumo. Una fila de CostoInsumo conserva compra, artículo, cantidad, unidad, costo y fecha originales.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Identidad del insumo | maestros.Insumo | Catálogo existente | PK, nombre, codigo_point, nombre_point | Ocho nombres de compras sin coincidencia en nombre sí coinciden exactamente con nombre_point de un solo insumo | Importador de costos y recuperación de costos de recetas |
| Compra histórica | maestros.CostoInsumo | PointPurchaseCostImportService | source_hash de compra/artículo/cantidad/unidad/costo | 2,836 costos históricos importados, AuditLog 42423; no volver a crearlos | Costeo histórico y puente al kardex |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| nombre_point del mismo maestro | Confirmada como identidad existente | IDs 202, 174, 173, 114, 120, 161, 107 y 227; cada nombre externo ya está guardado en el registro | No se crea ni modifica equivalencia |
| Dos maestros con el mismo nombre_point | No resuelta | El importador devuelve sin resolver; no elige el primero | Resolver identidad antes de importar |

## Decisión

Reutilizar nombre_point únicamente cuando no hay coincidencia por nombre ni alias explícito y existe exactamente un candidato. Conservar la precedencia previa y source_hash; no crear maestros ni cambiar fechas, unidades o importes. Los artículos sin identidad siguen sin resolver. Sin cambios de esquema, configuración o contratos públicos.

Comprobación reproducible: inventario_fuentes_datos --term nombre_point --term CostoInsumo en PostgreSQL; consultas acotadas Insumo.nombre_point; persistencia repetida de la misma compra en pruebas. La revisión productiva de ocho identidades se conserva en preview-reventa-point2025.json del expediente privado de cierre.

Validación: pruebas del importador, extracción y recuperación de recetas; verificar después del despliegue las coincidencias y la importación acotada. Identificar una entidad no acredita por sí solo unidad, costo o cantidad: esas validaciones siguen siendo necesarias antes de cargar el histórico.
