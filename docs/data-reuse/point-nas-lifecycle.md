# Ficha de fuentes — archivo Point y limpieza aprobada

Fecha y ambiente consultado: 2026-11-16 Mazatlán; metadatos PostgreSQL producción en sólo lectura; código base `3a99040a0a301f434ee367f20affb91c86eefddc`.

## Necesidad y unidad de análisis

Ubicación recuperable de un archivo original Point. Una entrada representa ruta lógica + hash de contenido, con referencias a procesos existentes. No representa una venta nueva, cierre financiero ni autorización de borrado.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creador/actualizador | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Extracción | PointSalesExtractionTask / pos_bridge_sales_extraction_tasks | pipeline Point | tarea, job, source_file/source_hash | Registros presentes en inventario actual | rebuild, staging, validación |
| Evidencia cruda | PointSalesRawStaging / pos_bridge_sales_raw_staging | pipeline | extracción + fuente + fila | Registros presentes | normalizado, auditoría |
| Normalización | PointSalesNormalized / pos_bridge_sales_normalized | pipeline | tarea/fuente | Registros presentes | hechos y ventas |
| Hechos diarios | PointSalesDailyProductFact / pos_bridge_sales_daily_product_fact | rebuild | sucursal/fecha/producto/granularidad | Registros presentes | ventas/services/sales_truth.py |
| Histórico producto | PointProductHistoryImport / pos_bridge_product_history_imports | product_history_import_service | hash, report_path, source_filename | Registros presentes | reimportación y conciliación |
| Oficial mensual | PointMonthlySalesOfficial / pos_bridge_monthly_sales_official | flujo oficial | periodo/reporte | Registros presentes | backfill y cierre producto |
| Venta autoritativa | ventas.VentaAutoritativaPoint | consumidores existentes de Point | claves existentes, sin reasignación | Registros presentes | reportes y ventas |
| Decisión limpieza | Ledger JSON erp_telegram_cleanup | Telegram autenticado | propuesta/hash/nonce/expiración | Código y flujo existente | preparador; futuro ejecutor |
| Ubicación archivada | No se encontró catálogo equivalente en las fuentes inspeccionadas | Nuevo servicio técnico propuesto | ruta lógica + hash | No implementado | futuro lector común |

## Alias y equivalencias

- `source_file` y `report_path`: candidatas a la misma identidad física sólo con ruta canónica y hash concordante; no equivalencia universal.
- `/app/storage/...` y `/opt/pastelerias-erp/storage/...`: correspondencia host/contenedor debe verificarse con mounts reales; no conversión arbitraria de prefijos.
- Igual hash en dos rutas: mismo contenido, no mismo proceso ni autorización; conservar referencias separadas.
- SUCCESS extracción, SUCCESS job y cierre contable: conceptos distintos. No fusionarlos ni inferir cierre a partir del primero.
- Índice de archivo y backup: conceptos distintos. Un índice sólo permite localizar, no prueba que el objeto exista ni que sea recuperable.

## Decisión de diseño

Reutilizar modelos y hashes existentes sin recapturar ventas. Añadir exclusivamente catálogo técnico de ubicación y estado de recuperación; primera versión en documentos JSON versionados y atómicos, con un único escritor residente y catálogo de sólo lectura para consumidores. No crear tabla maestra paralela ni cambiar datos operativos.

Protocolo realizado: conexión configurada PostgreSQL comprobada con SELECT 1; `default_transaction_read_only=on`, timeout SQL 10 s y límite externo 30 s. `call_command('inventario_fuentes_datos', term=['Point','source_file','report_path'], presence=True, limit=25)` utiliza el mismo comando de inventario de manage.py. 57 candidatos léxicos; se mostraron 25. No se extrajeron valores personales ni registros de ventas. Esto no demuestra cobertura exhaustiva ni equivalencia semántica.

Evidencia local privada: `/Users/mauricioburgos/.codex/task-artifacts/point-nas-lifecycle-plan-20261116/source-inventory.json`. Auditoría anterior: `/Users/mauricioburgos/.codex/task-artifacts/vps-cleanup-20261116-followup/point-audit.md`. Sus conteos no se reutilizan como autorización ni prueba de elegibilidad actual.

## Riesgos y puertas de entrada

Cerrar inventario de lectores y escritores antes del primer retiro; comprobar montajes/permisos y compatibilidad del ledger; resolver cada familia no cubierta conservando originales. La continuidad sin NAS de un archivo ya retirado requiere caché o copia independiente. El plan exige esa prueba y limita el piloto; no afirma que exista hoy. Permisos/configuración y despliegue se presentan por revisión exacta antes de instalación.
