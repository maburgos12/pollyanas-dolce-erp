# Ficha de fuentes — P3, incidencias y mantenimiento

Fecha y ambiente consultado: 30/09/2026, PostgreSQL de producción, solo lectura; implementación sobre base b64233d8; baseline PostgreSQL renovado al iniciar P3A.

## Necesidad y unidad de análisis

Equipo físico, incidencia reportada, intervención y evento de bitácora son unidades diferentes. P3A necesita estado/fechas/plan/auditoría de la orden; P3B evaluará vínculo documental, sin segunda captura de equipos o gasto.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | activos.Activo / activos_activo | Activos, altas/ediciones autorizadas | PK/código/QR; sucursal | 185 | Fallas, órdenes, pasaporte |
| Incidencia | fallas.ReporteFalla / fallas_reportefalla | PWA Fallas/Operación | PK, sucursal, activo opcional, duplicado_de | 107; 33 con equipo; 2 marcados duplicados | Mantenimiento, historial, pasaporte, reportes |
| Intervención | activos.OrdenMantenimiento / activos_ordenmantenimiento | Órdenes web/API y registro rápido | PK/folio, activo_ref, plan_ref | 63; todas cerradas | Historial, pasaporte, reportes, API |
| Preventivo | activos.PlanMantenimiento / activos_planmantenimiento | Planes/ejecución y cierre de orden | PK, equipo, última/próxima ejecución | 0 | Órdenes y agenda |
| Solicitud histórica | activos.SolicitudFalla / activos_solicitudfalla | Captura histórica/registro rápido | PK/folio, equipo, orden_atencion | 0 | Registro rápido y historial; conservar compatibilidad |
| Evento de trabajo | activos.BitacoraMantenimiento / activos_bitacoramantenimiento | Acciones de la orden | PK, orden_id, autor/fecha | 63 en comprobación P2 posterior | Auditoría, no sumar como otro gasto |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Equipo / Activo | Confirmada conceptualmente | La identidad concreta se verifica por PK/código/QR | No fusionar por nombre |
| Falla / SolicitudFalla | Distinta como fuente | Modelo operativo y flujo histórico diferentes; no relación automática | Revisar consumidores antes de retirar captura |
| Reporte / orden mismo equipo-día | No resuelta | Pueden ser varios trabajos o documentos del mismo trabajo | Documento/origen y validación humana |
| Cerrada / Resuelto / Validado | Distinta como contrato | Orden y falla tienen ciclos distintos | Definir enlace, no igualar estados por texto |

## Decisión de diseño

Reutilizar OrdenMantenimiento/PlanMantenimiento/BitacoraMantenimiento para P3A. Compartir validación y unidad transaccional; no nuevo maestro. Vínculo con ReporteFalla pendiente de diseño y cardinalidad aprobados; no migración ni equivalencia aplicada en esta revisión.

Procedimiento reproducible: conectar PostgreSQL (pg_isready), connection.ensure_connection y vendor postgres; transacción SET TRANSACTION READ ONLY; conteos ORM de los modelos y filtros mencionados; call_command inventario_fuentes_datos con term=[falla,orden,mantenimiento]; rollback. El inventario encontró 34 candidatos léxicos y mostró 15. Inspección de relaciones en modelos y handlers; grafo indexado en checkout raíz y búsqueda/fragmento de actualizar_orden_estatus, complementada con lectura acotada donde faltó cobertura del grafo. No se extrajeron fotos, ubicaciones precisas, contactos ni datos personales.

Riesgos y pendientes: no hay enlace documental ReporteFalla-OrdenMantenimiento; conteos no acreditan identidad o duplicidad de gasto. P3A aprobado: cerrada/cancelada son terminales; PostgreSQL productivo tiene ATOMIC_REQUESTS=False, por lo que la transición crea una transacción explícita. Los cinco escritores operativos de estados comparten la regla y el bloqueo de orden/plan. La creación de órdenes por _registrar_plan y otras ediciones de planes quedan fuera: esta etapa no acredita concurrencia global de todos los escritores de planes. No-op de estado no escribe fechas, plan, bitácora ni auditoría. Capturas monetarias explícitas conservan sus reglas vigentes; deduplicación financiera, permisos e históricos quedan fuera de P3A.


## Baseline P3A autorizado

PostgreSQL de producción: ATOMIC_REQUESTS=False. Antes de cambio: Activo185, OrdenMantenimiento63, PlanMantenimiento0, BitacoraMantenimiento63, ReporteFalla107. Huellas de todos sus campos por PK almacenadas como digest en evidencia externa; no se exportaron valores sensibles. El inventario se repitió con términos orden/plan/mantenimiento, en transacción READ ONLY. Crear/actualizar fuente sigue siendo el handler existente; P3A reutiliza esos identificadores y reglas monetarias. Escritores adicionales de estado encontrados: seguimiento móvil, serializer de seguimiento y cierre desde update_costos; todos deben compartir transición. `_registrar_plan` en Mantenimiento crea nuevas órdenes y queda fuera de P3A.
