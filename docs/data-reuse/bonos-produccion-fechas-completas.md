# Ficha de fuentes — fecha completa en bonos de producción

Fecha y ambiente consultado: 2026-10-13; PostgreSQL 16 local aislado y producción (lecturas acotadas).

## Necesidad y unidad de análisis

Una fila representa un empleado, un corte y una fecha completa. Un número de día se repite cuando el corte cruza meses; no identifica una asistencia.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Asistencia | rrhh.AsistenciaEmpleado | Checador/RRHH | empleado + fecha | Empleado 74: existen 2026-09-28 y 29; no existen 2026-10-28 ni 29 | Contexto RRHH y bonos |
| Incidencia | rrhh.IncidenciaAsistencia | RRHH | empleado + fecha + tipo | Empleado 74: sin incidencias 2026-10-28/29 | Contexto RRHH |
| Registro de bono | bonos_produccion.RegistroDiarioProduccion | Sincronización RRHH y captura autorizada | bono + fecha; ID para edición | Bono 138: 16 filas, 12 asistencias, ninguna fila 2026-10-28/29. Total tabla 3370; 2342 sin fecha; cero fechas duplicadas por bono | Calendario, revisión, recálculo, preview, API y admin |
| Corte | bonos_produccion.ConfigBonoPeriodo | Configuración existente | periodo.id | Periodo 7: 2026-09-28 a 2026-10-29 | Sincronización y calendario |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| día 28 vs fecha completa | Distinta | 2026-09-28 y 2026-10-28 pertenecen al mismo corte | No fusionar por número |
| Registro legado sin fecha | Confirmada solo con coincidencia única dentro del corte | Si hay dos fechas posibles, conservar NULL | No inventar mes; bloquear celda ambigua |

## Decisión de diseño

Reutilizar RRHH y el campo fecha existente. Cambiar unicidad a bono + fecha, conservar unicidad bono + dia solo para legados NULL. Completar únicamente fechas inequívocas, preservando ID, marcas, capturista, notas y montos. API compatible con dia sin fecha cuando el corte permite deducirla; rechazar fechas futuras, fuera de corte e intentos de mover una fila. Calendario y observaciones usan la misma fecha completa.

Consultas: inventario_fuentes_datos --term asistencia --term registro --term bono (metadatos: 39 candidatos); lecturas BEGIN READ ONLY, filtradas por empleado 74, bono 138 y fechas indicadas; conteos agregados de fecha NULL y duplicados. Sin nuevas equivalencias de personas ni tablas maestras.

Riesgos y pendientes: validar migración, API, sincronización, interfaz y caché en producción. Una reversión al esquema anterior no es segura si ya existen dos meses con el mismo número; conservar esquema y aplicar corrección hacia adelante.
