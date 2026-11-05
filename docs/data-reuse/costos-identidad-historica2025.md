# Ficha de fuentes — identidad de productos en costos 2025

Fecha y ambiente consultado: 2026-11-05, producción en solo lectura y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Leer el costo documentado de cada producto vendido en 2025. La fila del lector agrupa ventas por producto y periodo; resolver una identidad no crea otra venta, compra o gasto.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta histórica | reportes.FactVentaDiaria | Importación histórica autoritativa | Sucursal, fecha, código y nombre originales | 97,312 filas AUTHORITATIVE de 2025; 305 grupos; point_product_id vacío | Rentabilidad, productos y cobertura |
| Producto | pos_bridge.PointProduct | Sincronización Point y espejos oficiales | PK ERP, external_id, SKU y nombre | 457 candidatos; 299 grupos con una identidad nativa única por SKU y nombre normalizado; seis sin resolver | Costos y recetas |
| Adquisición | reportes.ProductoReventaCosto, ProductoReventaCostoHistoricoMensual | Compras históricas y decisiones documentadas | Producto, vigencia, hash y soporte | Fuentes ya cargadas; el lector comprueba fecha y correspondencia del comprobante | Costos de reventa |
| Fabricación | reportes.RecetaCostoHistoricoMensual, recetas.RecetaCostoVersion | Snapshots y versiones de receta | Receta y periodo | Sin snapshots mensuales de receta en 2025 | Costos de fabricación |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| SKU y nombre original ↔ producto nativo | Regla aprobada, aplicación condicionada a unicidad | SKU exacto y nombre mediante normalizar_nombre existente; external_id numérico; nunca elegir el primero | Mauricio autorizó expresamente el 2026-11-05: «Sí, aplicar solo coincidencias exactas» |
| Dos identidades nativas para la misma pareja | No resuelta | Cuatro grupos ambiguos | Conservar pendiente |
| Solo espejo official o nombre sin coincidencia | No resuelta | Dos grupos | No aplicar la nueva regla |
| Relación point_product_id existente | Confirmada por la relación de origen | No reemplazar el enlace explícito | Conservar |

## Decisión de diseño

Extender únicamente la lectura de 2025. Reutilizar PointProduct y los lectores de costos existentes, sin crear tablas, modificar maestros ni persistir enlaces sobre ventas. Mantener las guardas de vigencia, costo de otro producto y costo futuro. Los tres consumidores comparten el resolver; el reconocimiento de identidad externa previo de cobertura se conserva donde no hay candidatos nativos por SKU. Los candidatos nativos ambiguos nunca recurren a ese reconocimiento como atajo.

Procedimiento reproducible: grafo search_graph no disponible por Transport closed; revisión acotada de services_costos, services_resultado, views_rentabilidad y sales_confidence. Inventario ejecutado en PostgreSQL 16 mediante `manage.py inventario_fuentes_datos --term producto --term costo --term reventa`; solo informa candidatos léxicos. La evidencia privada de producción incluye catalogo-costeo2025-erp.json, revision-enlace-productos2025.csv y preview-identidad-costos2025-consumidor.json.

Riesgos y pendientes: la identidad actual no acredita por sí sola costo histórico; ese costo debe tener soporte y vigencia válidos. Esta corrección recupera costos de adquisición existentes y no completa los costos de fabricación ni certifica utilidad neta. Mantener 2026 y las relaciones explícitas existentes.
