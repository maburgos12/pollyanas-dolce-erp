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
