# Fuentes de ventas históricas para rentabilidad

Consulta: 4 de noviembre de 2026, PostgreSQL de producción. Unidad: sucursal,
producto y mes, conservando el neto fiscal y los descuentos.

| Fuente | Creador y clave | Evidencia | Consumidores |
| --- | --- | --- | --- |
| PointDailySale | Sincronización Point; sucursal Point, producto, día | Cero filas de 2025 | Resultado mensual y productos |
| FactVentaDiaria | Proyección de ventas oficiales; sucursal, día, producto y fuente | 97,312 filas seleccionadas de 2025; septiembre neto 3,263,644.18 | Confianza de ventas y BI |
| VentaAutoritativaPoint | Importador de exportaciones Point; sucursal, día y código | Archivos históricos category_report enlazados | Proyección fiscal |
| RecetaCostoHistoricoMensual y ProductoReventaCostoHistoricoMensual | Costeo por receta/producto y mes | Sin históricos de 2025 en la consulta | Costos por unidad |

Se ejecutó `inventario_fuentes_datos` con `venta` y `rentabilidad` dentro de una
transacción READ ONLY y se contrastaron conteos, periodos y archivos de origen.
El inventario es descubrimiento léxico; los conteos proceden de consultas a las
tablas y a `selected_sales_facts`.

## Identidad y decisión

La relación de sucursal y receta de cada Fact es explícita; no se crean alias por
nombre ni vínculos de productos nuevos. Un `point_product_id` ausente permanece
ausente. No se altera la venta original ni se importa otro espejo.

Cuando el mes no tiene stage Point, reutilizar `selected_sales_facts`, incluida
su precedencia por sucursal/día y exclusión de exportaciones duplicadas. Cuando
hay stage, conservar la lectura histórica existente y sus reglas Crucero/Bamoa;
no sumar ambas fuentes. Un stage parcial sigue sujeto a conciliación contra la
fuente fiscal: esta corrección no certifica su cobertura.

El resumen, recálculo, exportación y panel de productos comparten esa lectura.
El costo estimado del Fact no se reutiliza: los costos siguen exigiendo receta o
adquisición con vigencia. Ventas recuperadas no significan utilidad certificada.
El selector incluye el periodo solicitado aunque no exista snapshot guardado.

## Comprobación

Pruebas con venta oficial sin stage, espejo LEGACY descartado, coexistencia con
stage sin duplicación, costo estimado no certificado y selector histórico.
Sin modelos nuevos, migraciones, importación de nómina ni actualización de ventas.
