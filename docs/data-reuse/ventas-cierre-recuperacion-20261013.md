# Ficha de fuentes — recuperación del cierre de ventas

Fecha y ambiente consultado: 2026-10-13, producción en consultas acotadas de solo lectura; PostgreSQL 16 local aislado para pruebas.

## Necesidad y unidad de análisis

Publicar el último día cerrado acreditado por Point y verificar su fecha visible. La cobertura de ventas por sucursal/día es distinta de la disponibilidad de tickets.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta oficial | PointDailySale / pos_bridge_daily_sales | Importador oficial Point | Sucursal, fecha y producto | Job 95690 terminó SUCCESS; importó el 12 de octubre | Lector canónico, facts, BI |
| Indicadores | PointDailyBranchIndicator | get_Ventas_ByDay de Point | Sucursal y fecha | 9 indicadores para el día 12; falta ERP 8, Point 6, Leyva | Tickets y ticket promedio |
| Recibo oficial | PointExtractionLog | OfficialSalesBackfillService | Job, sucursal externa y sale_date | 10 recibos de reportes descargados; Leyva no tiene filas ni indicador | Comparativo mensual; se extiende al corte diario |
| Capa analítica | FactVentaDiaria | refresh_incremental | Sucursal, fecha y producto | 339 filas del 12; suma 105475.01 | Dashboard, Ventas y BI |
| Verificación | PointSyncJob.result_summary; QualityFinding | Sync y guard existente | Job / sales_publication_gap:2026-10-12 | Hallazgo 5 abierto, 15 detecciones; propuesta 5 accepted | Recuperación y auditoría |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Point 6 → ERP 8, Leyva | Confirmada | FK PointBranch.erp_branch_id existente | Sin cambios de mapeo |
| Descarga oficial procesada → cobertura de ventas | Reutilizada | Contrato existente official_backfill_evidence, aprobado para comparativo desde septiembre; para cierre diario se exige job SUCCESS terminado y iniciado después del día reportado | Autorización «Procede» en este hilo |
| Recibo sin indicador → tickets cero | Distinta | No acredita tickets; quedan N/D y no se crea indicador artificial | No aplicar |

## Decisión de diseño

Reutilizar tablas y recibos existentes. No agregar maestros, capturas, migraciones ni excepciones por sucursal. El cierre diario puede acreditar ventas mediante el recibo oficial del importador; esto no crea ventas o indicadores cero por sucursal. Los tickets y el promedio de la red permanecen desconocidos si falta una sucursal. Un job fallido, reporte no descargado o captura intradía no completa el cierre.

El control de frescura compara tanto facts como corte visible, usando el día local actual para decidir qué está cerrado. Reintenta extraer solo las sucursales sin evidencia y la tarea Celery reintenta de forma acotada si la publicación sigue pendiente. Se conservan los hallazgos y el mecanismo existente que valida propuestas al resolver el rezago.

Procedimiento: inventario_fuentes_datos --term "corte oficial" --term "indicador" con PostgreSQL conectado; consultas por fecha 2026-10-12 y jobs 95690, 95784, 96905, 96936; lectura del XLS original de Leyva. El reporte global de Point devolvió HTTP 500 y no se usa como evidencia.

Riesgos y pendientes: depender de recibos generados por el importador oficial; no interpretar una consulta local vacía como cero. Validar las tres pantallas y los tickets N/D en producción tras el despliegue. No se alteran ventas, nómina, maestros ni configuración.
