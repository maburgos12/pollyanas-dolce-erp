# Ficha de fuentes — agregado anual comercial

Fecha/ambiente: 20 noviembre 2026, PostgreSQL 16 aislado de tarea
`comercial-agregado-anual-20261120`, puerto 55643. Base 045b5110. No consultas
productivas nuevas. Las pruebas usan datos sintéticos y no certifican cifras ERP.

## Necesidad y unidad de análisis

El panel repetía la selección anual para obtener opciones de sucursal y sumas
mensuales. La medición productiva archivada 045b atribuyó 1.714409 s al DISTINCT y
1.542678 s al GROUP BY mensual; esos tiempos no son ahorro demostrado. Se reutiliza
un agregado por sucursal histórica/mes, sumando venta_total con IVA/descuentos ya
persistidos y conservando Max(fecha) para el corte de comparación.

## Fuentes candidatas

| Concepto | Modelo/tabla | Crea/actualiza | Identidad/ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta elegible | reportes.FactVentaDiaria / reportes_factventadiaria | reportes.analytics_service.rebuild_sales_facts | fecha,sucursal,producto_clave,source_kind; prioridad por sucursal/día | Inventario léxico local venta/sucursal; migraciones actuales; fixtures de pruebas | selected_sales_facts y panel comercial |
| Sucursal histórica | core.Sucursal | Catálogo operativo existente | PK, incluye inactivas y hechos anteriores a apertura | Fixtures activas/inactivas/nombres iguales y acentuados | Opciones globales y desglose por sucursal |
| Linaje de exportación | ventas.VentaAutoritativaPoint / ventas_autoritativas_point | Importaciones existentes | sucursal,día,archivo,hoja,código | Fixture category_report + espejo mismo archivo + archivo distinto | Exclusión de espejos del selector existente |
| Control mensual | pos_bridge.PointMonthlySalesOfficial | Importación oficial existente | mes | Prueba de control mensual existente | Control visible global, sin sobrescribir detalle |

## Alias y equivalencias

No se crean ni aplican equivalencias. Se conservan PK de sucursal, incluidos
nombres iguales; nombre no resuelve identidad. La prioridad AUTHORITATIVE/V2/LEGACY
y la exclusión de espejos se reutilizan sin cambios. Una sucursal nula aporta al
importe global y no crea opción. No se filtra por activa ni fecha_apertura.

## Decisión de diseño

Reutilizar selected_sales_facts global para el rango anual exacto, agrupando una
vez por sucursal/mes. La prioridad y los espejos dependen de sucursal/día: filtrar
la agrupación por PK conserva la misma selección de una sucursal. Derivar todas
las opciones antes de aplicar el filtro de mensualidad. ORDER BY nombre permanece
en PostgreSQL para mantener la collation; no añadir desempate no definido antes.
Mes sin filas sigue None; cero real y negativos permanecen. SUM decimal no pasa a
float ni redondea antes. Fecha máxima se toma solo de la mensualidad filtrada.

No nuevas tablas/capturas, índices, caché, parámetros públicos, permisos, HTML o SW.
No cambia summary(SUM/COUNT/MAXactualizado_en), fórmulas ni las consultas de los
periodos actual/anterior. Consumidores: commercial_panel_from_request; portada y
detalle BI. Grafo inbound y fuente 045b contrastados; la búsqueda del bloque privado no
devolvió nodo y se leyó la fuente exacta en Git.

## Consultas y verificación

`manage.py inventario_fuentes_datos --term venta --term sucursal` en PG propio.
Pruebas de reportes.tests_commercial_analytics y consumidores sales_confidence.
Comparación externa carga fuente original Git 045b y compara DTO completo y HTML,
con los mismos datos y filtros: red/sucursal/inexistente, enero/diciembre, meses,
ceros/negativos/nulos, cerradas/preapertura, prioridad y espejo de importación.
El expediente técnico fuera del repo conserva comandos, huellas, conteos y tiempos.

## Riesgos y pendientes

El orden entre nombres exactamente iguales no está especificado por el SQL
original; no es una identidad ni se añade nuevo desempate. Medición local
sintética no representa latencia productiva. Publicación/QA real quedan pendientes
del relevo autorizado por Root. No modifica fuentes financieras E10 ni datos
operativos o cierres históricos.

Verificación local: 50 pruebas comerciales y de confianza PASS en 1.143 s;
40 DTO completos y HTML idénticos a Git045b más cuatro fronteras de calendario.
Cada caso ejecuta una consulta menos. Cinco pares alternados con el mismo fixture
de 867 hechos y estadísticas locales actualizadas: mediana del componente anual
1.719→1.025 ms y del panel 9.411→8.206 ms. Son medidas sintéticas locales;
no prueban ahorro productivo. Los planes antes/después de estadísticas y el intento
original cancelado se conservan en el expediente, sin aumentar límites ni índices.
