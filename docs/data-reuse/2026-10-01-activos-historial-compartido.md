# Ficha de fuentes — trabajos y servicios de equipos

Fecha y ambiente consultado: 01/10/2026; PostgreSQL 16 y navegador autenticado en producción, sólo lectura. Conteos históricos del diagnóstico sobre main `2bdeca59`; implementación sobre `90e27602`.

## Necesidad y unidad de análisis

Localizar desde Administración y el expediente del equipo los documentos ya disponibles en el historial de Mantenimiento. Una fila representa un documento de su fuente, no necesariamente una intervención única ni un gasto adicional.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | activos.Activo / activos_activo | Activos | PK, código, QR, sucursal | 185 | Administración, Fallas, pasaporte |
| Orden y servicio directo de equipo | activos.OrdenMantenimiento / activos_ordenmantenimiento | Activos; servicio sin orden web/móvil de Mantenimiento | PK/folio, activo_ref, plan_ref | 63 cerradas; 9 EMERGENCIA, 54 PLAN | Órdenes, historial, presupuesto |
| Falla y atención | fallas.ReporteFalla / fallas_reportefalla | Fallas/Mantenimiento | PK, activo_relacionado opcional, sucursal, duplicado_de | 107; 33 con equipo; 2 marcadas duplicadas | Historial, pasaporte, presupuesto |
| Solicitud histórica | activos.SolicitudFalla / activos_solicitudfalla | Activos | PK, orden_atencion | 0 | Clasificación de órdenes |
| Evento | activos.BitacoraMantenimiento / activos_bitacoramantenimiento | Seguimiento de orden | PK, orden_id | 63 | Trazabilidad |
| Servicio de flota | logistica.ServicioRealizadoUnidad / logistica_serviciorealizadounidad | Mantenimiento/Logística | PK, unidad | 20; 18 vigentes | Historial/flota |
| Reparación de flota | logistica.ReparacionUnidad / logistica_reparacionunidad | Mantenimiento/Logística | PK, unidad, reporte_origen | 7 | Historial/flota |
| Reporte de unidad | logistica.ReporteUnidad / logistica_reporteunidad | Logística | PK, unidad, duplicado_de | 176; 34 marcados duplicados | Historial/flota |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Servicio sin orden de equipo / OrdenMantenimiento | Confirmada a nivel de fuente | Creadores web/móvil usan la misma tabla y PK que Administración | Reutilizar consulta |
| Falla / orden del mismo trabajo | No resuelta | No existe vínculo directo entre esas fuentes; mismo equipo/fecha no demuestra identidad | Revisar cardinalidad y documentos antes de enlazar |
| Servicio de equipo / servicio de flota | Distinta | Activo y Unidad tienen identidades independientes | Conservar fuente y ámbito |
| Bitácora / orden | Documentos distintos relacionados | orden_id explícito | No sumar evento como otro gasto |

## Decisión de diseño

Reutilizar `unified_history_rows` y `/api/mantenimiento/v2/historial/`, incluyendo su filtro existente `activo`. Añadir accesos a la PWA existente desde Equipos, Órdenes y pasaporte. Abrir periodo Todo y conservar permisos de sucursal y costos. Aclarar etiquetas de tipos y corregir el contador/paginación que incluía flota al filtrar un equipo.

No crear otra captura, tabla maestra ni total financiero combinado. No modificar modelos, contratos JSON, cierres, importes o registros históricos. Consumidores afectados: enlaces HTML de Activos/Operación y presentación del historial PWA; la consulta normal conserva sus fuentes de flota.

Consultas o procedimiento reproducible: `inventario_fuentes_datos --term servicio --term mantenimiento --term trabajo --term falla` ejecutado con PostgreSQL en producción; lecturas acotadas en transacción READ ONLY de conteos, estados, relaciones y duplicado_de. Candidatos léxicos no acreditan equivalencia. Agrupación de órdenes por equipo/fecha/descripción y comparación de fallas concluidas contra órdenes por equipo/fecha efectiva sin grupos ni pares en esa lectura; no prueba ausencia global de duplicados.

Riesgos y pendientes: vinculación explícita de fallas/órdenes, reconocimiento financiero y reintentos de altas corresponden a entregas posteriores. Esta entrega no fusiona documentos. Verificar navegador local móvil/escritorio, permisos existentes, paginación y caché; después validar accesos y documentos servidos en producción.
