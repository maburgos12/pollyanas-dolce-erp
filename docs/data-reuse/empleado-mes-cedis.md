# Ficha de fuentes — Empleado del mes CEDIS

## Necesidad y unidad de análisis
Una evaluación por empleado y mes; una elección y entrega por periodo CEDIS.

## Fuentes candidatas
| Concepto | Modelo / tabla | Crea y actualiza | Identificador | Evidencia | Consumidores |
|---|---|---|---|---|---|
| Colaborador y jerarquía | rrhh.Empleado / rrhh_empleado | Capital Humano | PK, codigo único, jefe_directo, usuario_erp | Lectura producción 2026-11-05:71 activos;4 reportes a DG;25 equipo producción,7 administración,35 ventas | RRHH, bonos, permisos, extras |
| Credencial | auth.User | Usuarios y accesos | FK usuario_erp | Producción: cuatro jefaturas vinculadas al usuario DG | Autorizaciones |
| Evidencia de asistencia | AsistenciaEmpleado e IncidenciaAsistencia | Checador y conciliación RRHH | empleado+fecha; incidencia id | Auditoría previa septiembre/octubre confirma registros y pendientes; no equivalen a calificación | RRHH, prenómina |
| Historial | core.AuditLog | Servicios ERP | modelo+object_id, usuario, timestamp | Modelo ya usado por operaciones | Bitácora |

## Alias y equivalencias
Colaborador referencia Empleado por PK. CEDIS no equivale al texto sucursal CEDIS: incluye equipos de Producción/Administración aprobados con sede Matriz. Jefatura confirmada por usuario_erp y jefe_directo. Puesto se presenta con helper canónico, no se fusionan campos. Ventas no participa.

## Decisión
Reutilizar identidades, vínculos y AuditLog. Crear sólo periodo/evaluación con snapshots e importes de reconocimiento; no crear nueva captura de empleados ni integrar movimientos de nómina o vacaciones. No modificar registros existentes.

## Procedimiento y límites
Producción consultada con SET TRANSACTION READ ONLY, agrupaciones y verificación autenticada en /rrhh/organizacion/. Inventario_fuentes_datos --term empleado --term evaluacion --term reconocimiento ejecutado también en PostgreSQL local aislado; allí el padrón está vacío y no prueba cobertura productiva. El grafo falló Transport closed al comenzar implementación; se usaron lecturas acotadas del código. La consulta histórica no acredita vigencia de asistencia del mes futuro.

## Actualización autorizada: captura sin fecha límite (2026-11-05)

Unidad de análisis: el mismo periodo mensual (`mes`, único), con evaluaciones por empleado. El mes evaluado no cambia al capturar después de su cierre calendario.

| Concepto | Fuente oficial | Identificador | Evidencia actual | Decisión y consumidores |
| --- | --- | --- | --- | --- |
| Periodo y plazo | `rrhh.PeriodoEmpleadoMes` / `rrhh_periodoempleadomes` | PK y mes único | Producción, lectura transaccional: 0 periodos y 0 evaluaciones | Extender `fecha_limite` para admitir NULL; conservar fechas existentes. Servicios, formularios, listado y detalle |
| Equipos evaluadores | `rrhh.Empleado`, `auth.User` | `jefe_directo`, `usuario_erp` | Una jefatura activa vinculada por departamento; Producción 25 colaboradores, Administración 7 | Reutilizar jefaturas y equipos sin cambios. Ventas fuera del alcance |
| Cierre y trazabilidad | `PeriodoEmpleadoMes.ganador`, `core.AuditLog` | periodo y usuario actor | El servicio `elegir` ya bloquea cambios posteriores y audita la decisión | Reutilizar el cierre existente; sin otro estado ni tabla |

`fecha_limite=NULL` significa captura abierta hasta que RRHH registre al ganador. La fecha, si se especifica, sigue validándose. RRHH puede retirar un límite existente con motivo y versión; no puede modificar una decisión cerrada. No se fusionan registros ni se crean equivalencias entre fuentes.

Procedimiento: grafo indexado y llamadas de `abrir_periodo` trazadas; `inventario_fuentes_datos --term empleado --term evaluacion --term reconocimiento` ejecutado en PostgreSQL 16 aislado (metadatos, no cobertura productiva). Producción consultada con `SET TRANSACTION READ ONLY`, conteos de las dos tablas y jefaturas filtradas por departamento. La autorización del chat incluye habilitar octubre de 2026 sin inventar candidatos, calificaciones o premios. La apertura excepcional debe identificar a Mauricio como actor autorizante en la bitácora; no atribuirla a Paula ni ampliar permisos web de DG.

Riesgos y comprobación: cubrir NULL en todas las comparaciones de fechas, mantener bloqueo de decisión y validar octubre visible en producción tras la apertura. Las cifras de equipos describen esta lectura y no seleccionan ganadores.
