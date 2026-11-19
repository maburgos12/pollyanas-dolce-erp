# Ficha de fuentes — identidad oficial de confianza comercial

Fecha y ambiente: 2026-11-19; producción consultada en REPEATABLE READ READ ONLY, red bloqueada. Implementación aislada desde 4f6b94f2.

## Necesidad y unidad de análisis

Resolver una agrupación de ventas AUTHORITATIVE por su clave externa completa y nombre normalizado hacia el mismo producto oficial existente. El índice actual elimina `official:` aunque la clave de la venta lo conserva. No se calcula un costo nuevo.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta seleccionada | FactVentaDiaria | Importación histórica y Point v2 existentes | source_kind, producto_clave, producto_nombre, mes | Nueve grupos prefijados / 88 filas entre enero 2025 y noviembre 2026 | Confianza de ventas, Rentabilidad |
| Producto oficial | PointProduct | Catálogo/identidades existentes | external_id completo + nombre normalizado; nunca SKU nativo ni PK inferida | official:3468 corresponde a ERP983, Estuche Escolar 🐻 | Resolución de evidencia de costo |
| Tratamiento ya aprobado | ProductoReventaCosto | Registro manual autorizado existente | producto_point983, SUPPLIER_GIFT, vigente enero 2026 | Seis filas / 18 unidades en agosto recuperan evidencia de costo cero autorizado | Confianza comercial/financiera |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| official:3468 / ERP983 | Mismo identificador oficial, confirmado | Ya es external_id del producto; SUPPLIER_GIFT existente. No es Point383 ni otra variante de SKU3468 | Revisión independiente del lookup |
| Clave legacy sin prefijo | Contrato existente conservado | Se mantiene índice anterior y rechazo de candidatos ambiguos | Pruebas de preservación |
| SKU nativo, nombre solo, PK convertido a texto | No equivalencia | No se agregan como índices; identidades distintas siguen sin evidencia | Ninguna aplicación nueva |

## Decisión de diseño

Extender el índice en memoria de `_cost_evidence` con el external_id completo del mismo producto. Mantener el índice legacy, nombre normalizado y unicidad. No cambiar tablas, importadores, ventas, recetas, fuentes de costo, autorizaciones ni fórmulas.

Procedimiento: preview en memoria de la función exacta y `build_sales_confidence` completo; nueve grupos identificados. Artefacto externo `rentabilidad-74-costos-20261119/preview-official-exact-identity-full.json`. Inventario léxico local registrado en el expediente de la tarea.

Resultado esperado: octubre conserva 75 filas pendientes / $4,409.00, ventas $3,648,207.58 y resumen completo idéntico. Agosto reduce faltantes 157→151 exclusivamente por seis filas de regalo aprobado; ventas $3,409,584.02, importes, estado provisional y cobertura 99.97% permanecen iguales.

Riesgos y pendientes: verificar con PostgreSQL16, revisión independiente y CI exacto; aprobación/despliegue coordinados por el responsable raíz. Las composiciones de octubre desconocidas siguen pendientes; esto no acredita el cierre financiero. Parser de costos y nuevas aplicaciones de precios quedan fuera.
