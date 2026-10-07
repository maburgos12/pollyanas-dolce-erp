# Ficha de fuentes — Gateway READ/SHADOW de activos y mantenimiento

Fecha: 2026-10-06 America/Mazatlan (2026-10-07 05:03 UTC). Código base local: `3e1a0d1e`. Ambiente consultado: producción PostgreSQL 16.12, transacción READ ONLY con timeout de 15 segundos; sin contenido personal, archivos ni envío a proveedor.

## Necesidad y unidad de análisis

Buscar equipos y consultar su contexto y programación preventiva mediante servicios existentes. Un activo es un equipo canónico; una orden es un trabajo; una falla es un incidente; un plan es programación. El Gateway expone lecturas y conserva referencias, sin otra captura, importación Point o tabla maestra.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | Activo / activos_activo | Catálogo y captura existentes | PK, código/QR, sucursal y vigencia | 197 registros; 6 sin sucursal | Pasaporte QR, Activos, Operación, Mantenimiento |
| Calendario preventivo | PlanMantenimiento / activos_planmantenimiento | Configuración existente | PK, activo_ref, estado y próxima fecha | 0 registros | Pasaporte, calendario y órdenes |
| Trabajo | OrdenMantenimiento / activos_ordenmantenimiento | Flujos de mantenimiento existentes | PK/folio, activo_ref | 63 registros | Pasaporte, historial y costos |
| Incidente | ReporteFalla / fallas_reportefalla | Operación/PWA y gestión existentes | PK, tipo de objetivo, sucursal y activo relacionado | 111 registros | Pasaporte, Mantenimiento y avisos |
| Alcance | auth.User, UserProfile, UserModuleAccess y Sucursal | Autenticación/configuración actuales | Usuario vigente, permisos y sucursal operativa | Servicios reales de scope; sin enumerar cuentas | Gateway, mantenimiento y operación |

Los conteos fueron refrescados con SELECT/COUNT de solo lectura. La auditoría F1 del 2026-10-05 observaba 196 equipos y 110 fallas: esos conteos anteriores no se usan como estado actual. El inventario léxico identifica candidatos; no prueba equivalencia entre registros.

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Equipo / activo | Confirmada sólo cuando se identifica la PK/código/QR | Una coincidencia textual puede representar varios equipos | Mostrar opciones autorizadas, no fusionar |
| Nombre / identidad | Distinta | La ficha F1 documentó nombres repetidos; no se reutiliza nombre como clave | Usuario resuelve ambigüedad |
| Falla / orden / plan | Distinta | Tres tablas y unidades de análisis independientes; planes actualmente vacíos | No convertir fallas u órdenes en calendario |
| Activo sin sucursal / acceso global | Distinta | 6 registros sin asignación | No inferir alcance global |
| Sucursal histórica de reporte / sucursal actual del equipo | No resuelta | F1 detectó un vínculo histórico entre ámbitos distintos | Conservar historia; no corregir ni trasladar datos |

## Decisión de diseño

Reutilizar `activos.services_pasaporte.activos_autorizados` y `construir_pasaporte`, con identidad fresca, autorización antes de materializar y DTO explícito. Separar permiso de ver equipos del permiso de costos (`mantenimiento.services_access.can_view_costs`). Consultar planes intersectando equipos autorizados; no ampliar permisos de gestión de planes para operadores.

El calendario vacío debe producir ausencia de programación registrada; no inventar fechas o afirmar ausencia de trabajos históricos. Las órdenes y fallas recientes permanecen acotadas y su límite se informa. No hay nuevas tablas, equivalencias aplicadas, captura ni migraciones.

Consultas o procedimiento reproducible: verificar `connection.vendor == 'postgresql'`; iniciar `SET TRANSACTION READ ONLY`, `SET LOCAL statement_timeout = '15s'`; obtener versión y `transaction_read_only`; COUNT de los cuatro modelos y COUNT de `Activo.sucursal IS NULL`. Ejecutar también `inventario_fuentes_datos --term activo --term equipo --term mantenimiento --presence --details --limit 12` con `PGOPTIONS` read-only. El inventario y las pruebas locales son evidencias diferentes de estos conteos de producción.

## Riesgos y pendientes

El piloto sigue deshabilitado; proveedor/modelo, presupuesto, participantes nominales y tareas persistentes necesitan sus cortes posteriores. La recuperación SQL+media, hashes de recepción NAS, cadena programada y prueba SMTP recibida se verificaron antes de este corte (PR #1500); no se probó reinicio físico ni una falla real HBS. No se retiran originales históricos adicionales ni se concede al lector de medios acceso a respaldos SQL.
