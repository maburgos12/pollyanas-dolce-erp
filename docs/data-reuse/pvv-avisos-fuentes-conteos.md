# Ficha de fuentes — avisos de Producido vs Vendido

Fecha y ambiente consultado:19/11/2026; producción solo lectura y PG16 local exclusivo55581.

## Necesidad y unidad de análisis

Distinguir una revalidación temporalmente no disponible de un producto con diferencia. Distinguir el único conteo físico enviado de una ronda vacía creada por automatización histórica. Unidad: caso producto/sucursal y evidencia de un envío físico; no generar estados ni cantidades nuevas.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Autoridad de movimientos | PointSyncJob / pos_bridge_sync_jobs | Servicios Point existentes | job, tipo, período, sucursales, contrato | Job152101RUNNING sin resumen, transferencias01–31oct; tarea mensual programada interrumpida. Inspección read-only en expediente1753 | Lector operacional, conciliación mensual |
| Captura operativa | PointInventorySnapshot / pos_bridge_inventory_snapshots | Inventario Point existente | producto/sucursal/instante/job |44capturas03/10 conservadas por1753; no equivalen a saldo30/09 | PvsVHTML |
| Envío físico real | EventoConteoSucursal / inventario_eventoconteosucursal | Flujo humano de conteos | conteo+evento+snapshot de envío |180observaciones visibles de5folios con un solo envío y ronda2vacía; expediente conteos-fisicos-lineage-20261119 | Evidencia física PvsV, ficha conteo |
| Estado automático histórico | EventoConteoSucursal y LecturaConteoSucursal | Automatización histórica retirada en1658 | evento conciliacion_nocturna/automatizacionRECONTEO, ronda y líneas exactas |9lecturas de ronda2nulas,0envíos de segunda ronda. No basta el nombre ni un ID | PvsVHTML; estado original se conserva |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Ronda automática vacía / nuevo conteo humano | distinta | Evento automático sin actor, un envío previo válido, líneas de ronda actual exactas y totalmente vacías. Cero capturado, otro envío o inicio/reconteo manual impiden etiqueta | Revisión independiente; sin nueva equivalencia de producto |
| Captura03/10 / apertura30/09 | distinta | Diferente instante y contrato;1753mantiene frontera mensual pendiente | Ninguna promoción mensual autorizada |

## Decisión de diseño

Reutilizar fuentes y aviso None del lector. Extender únicamente enriquecimientoHTML de evidencia física con prueba de procedencia histórica y una consulta bulk; conservar importaciones, permisos, cantidades, fechas y corte común por confirmar. No crear capturas, tablas, rondas, caché ni materializaciones. API/export se resuelven antes de estos enriquecimientos.

Consultas: inventario_fuentes_datos --term conteo --term ronda conPG16; candidatos léxicos no prueban identidad. Evidencia productiva acotada ya recuperada porPoint; no repetir APIs ni lecturas masivas. Pruebas locales cubrirán fuente incompleta, cero real, envío posterior, intervención manual, payload inválido, importación y permisos.

Riesgos y pendientes: el aviso no repara autoridad de transferencias ni cierra octubre. La recuperación deljob152101 y su entrega unacked están separadas y no se ejecutan en esta tarea. La ronda histórica tampoco acredita hora de corte compartida.

## Corrección de acceso canónico y gate de entrega

Doctor detectó que el lector operativo publicado en1753 importaba y consultaba ventas diarias directamente. Se reutiliza `official_point_sales_rows_for_range` de ventas, ya utilizado por el ledger mensual. Es el mismo queryset oficial, con rango inclusivo01–03octubre, filtro por ubicaciones y exclusión de cantidades explícitas en cero. La resolución de identidad de cada fila y las autoridades/coberturas posteriores se conservan. No se cambia la allowlist ni se crea una API.

La regresión de frontera se comprueba antes y después de sustituir la consulta: excluye30septiembre,4octubre, endpointreciente y otras sucursales;1–3octubre con cantidad no cero bloquean la acreditación, también una cantidad negativa. Un cero acreditado no dispara esta barrera. La fecha diaria no permite ubicar una venta no cero antes/después de10:48del03octubre.

CI incorpora el guard stdlib existente `python scripts/check_pointdailysale_usage.py` en el job checks junto a `manage.py check`, para que una lectura directa vuelva a bloquear la entrega antes de publicar.
