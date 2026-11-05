# Ficha de fuentes — alias existentes en rentabilidad enero–septiembre 2026

Fecha y ambiente consultado: 2026-11-05; producción PostgreSQL, consultas de solo lectura.

## Necesidad y unidad de análisis

Algunas filas de venta carecen de receta aunque el catálogo ya conserva su alias
canónico. La unidad analizada es producto, sucursal y mes. Se busca recuperar su
costo vigente sin crear equivalencias, precios ni una segunda captura de ventas.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Alias canónico | `recetas.RecetaCodigoPointAlias` / `recetas_recetacodigopointalias` | Catálogo de recetas y sus flujos existentes | Código normalizado único, receta y estado activo | 49 alias activos candidatos para los códigos de 81 productos examinados; la coincidencia léxica no prueba identidad | Catálogo de recetas; lector compartido de costos |
| Producto y venta | `pos_bridge.PointProduct`, `PointDailySale` | Point y su sincronización existente | Producto, código, nombre, sucursal y fecha | Siete filas de cajas con receta vacía y costo recuperable: dos en mayo, dos en junio, una en julio y dos en agosto | Rentabilidad mensual, productos y confianza de ventas |
| Costo fechado | `RecetaCostoVersion`, `RecetaCostoHistoricoMensual` | Costeo e históricos ya registrados | Receta, versión y periodo | Recetas 87 y 88; se mantienen los filtros de fecha y cobertura existentes | `rentabilidad.services_costos` |
| Adquisición o decisión documentada | `ProductoReventaCosto`, `ProductoReventaCostoHistoricoMensual` | Compras Point y decisiones existentes | Producto, fecha, origen y tratamiento autorizado | Hay adquisiciones válidas que deben conservar su precedencia aunque la receta del catálogo esté clasificada como fabricada | El mismo lector compartido |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `0239` / CAJA G PARA VENTA / receta 87; `0240` / CAJA CH PARA VENTA / receta 88 | Confirmada como relación ya registrada | Alias maestros 87 y 88; código exacto y nombres normalizados coincidentes entre venta, alias y receta | No se crea ni modifica ninguna equivalencia |
| Código compartido con un nombre diferente | Distinta | Un código, sin concordancia de nombres, no habilita el costo | Conservar el faltante |
| Alias y nodos que ofrecen varias recetas | No resuelta | Se conserva el conjunto de candidatos y se exige una única receta | Conservar el faltante; no escoger el primer registro |

## Decisión de diseño

Reutilizar los alias maestros activos como respaldo del lector existente,
exclusivamente en enero–septiembre de 2026 y solo cuando falten tanto la receta
como un costo de adquisición utilizable. Los tratamientos autorizados, incluso un
costo cero explícito, y las identidades o costos ya resueltos se conservan.

El lector vuelve a aplicar sus controles de código, nombres, colisiones, fecha y
cobertura. No se actualizan ventas, recetas, alias, compras ni costos. La regla no
se extiende a 2025 ni a octubre de 2026 y no recupera precios de meses posteriores.
Los consumidores afectados son el resultado mensual, la pestaña Productos de
Rentabilidad y el cálculo de cobertura de ventas, que comparten el mismo servicio.

Consultas reproducibles: `inventario_fuentes_datos --term receta --term alias
--limit 8 --presence`, seguido de consultas acotadas a alias activos, códigos de
los productos pendientes y fuentes de costo fechadas. El inventario localiza
candidatos; las identidades se comprueban por separado. El grafo MCP no estuvo
disponible por cierre de transporte y se inspeccionaron las definiciones y sus
llamadores mediante búsquedas acotadas.

Riesgos y pendientes: recuperar una relación no acredita un precio ausente ni
certifica el cierre financiero completo. Los demás faltantes históricos permanecen
visibles. Verificar las siete filas en producción y confirmar la conservación de
los costos previamente disponibles después del despliegue.
