# Ficha de fuentes — préstamos de colaboradores directos

Fecha y ambiente consultado: 2026-11-04; código main y producción (solo lectura), PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Permitir a la jefatura capturar un préstamo por colaborador directo activo; conservar RRHH → jefe → Dirección y solicitud personal → jefe → Dirección. Una fila representa un préstamo; sus cuotas son proyecciones y cobros, no otros préstamos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Empleado y jefe | rrhh.Empleado / rrhh_empleado | Expediente RRHH | PK, usuario_erp_id, jefe_directo_id | Producción: 25 colaboradores activos de Carolina y 35 de Johana | RRHH, permisos, horas extra, préstamos |
| Identidad de usuario | auth.User / auth_user | Autenticación | PK enlazada desde Empleado | Ambas cuentas activas, sin acceso general RRHH | Autorizaciones y navegación |
| Préstamo | rrhh.Prestamo / rrhh_prestamo | Servicio y captura web existentes | PK y folio único, empleado_id | 12 activos, 4 liquidados, 2 solicitados | Web RRHH, PWA, operadoras Producción |
| Cuota | rrhh.PrestamoCuota / rrhh_prestamocuota | Aprobación Dirección y cobro manual/CONTPAQ | Préstamo + número de quincena | 65 cobradas, 3 parciales, 1 omitida, 87 pendientes | Saldo y cobros |

## Alias y equivalencias

| Términos | Estado | Evidencia | Revisión requerida |
| --- | --- | --- | --- |
| Jefatura / colaboradores directos | Confirmada | Relación existente Empleado.jefe_directo → Empleado.usuario_erp | Ninguna equivalencia nueva |
| Solicitud RRHH / móvil / operadora | Confirmada | Mismo modelo Prestamo y servicio crear_solicitud_prestamo | Unificar captura web |

## Decisión de diseño

Reutilizar empleados, identidades, préstamos, cuotas, autorizaciones y notificaciones. No crear tablas ni migraciones. Selección de equipo por relación explícita, sin inferir por nombre, departamento o rol. Conservar resolución de superior existente para solicitudes personales de jefaturas. No ampliar permisos generales de RRHH.

RRHH captura con jefe derivado del expediente; una jefatura que captura para su colaborador da visto bueno y pasa a Dirección. Las capturas propias nunca se autoautorizan. El endpoint de préstamos admite consulta, creación y acciones explícitas; bloquea modificación/borrado genéricos que permitirían cambiar una solicitud ya autorizada.

Se incluyen los cobros parciales en el cálculo de saldo y se ajusta la última cuota de planes nuevos al importe exacto. No se recalculan ni reescriben masivamente préstamos o cuotas históricos.

Procedimiento: `inventario_fuentes_datos --term prestamo --term jefe` en PostgreSQL local; consultas agregadas de solo lectura por estado en producción y conteo de empleados activos por jefe vinculado a las dos cuentas. El grafo actual devolvió rutas inconsistentes y trazas vacías; se verificaron consumidores mediante búsqueda y lectura del código fuente.

## Validación y reversión

Pruebas de solicitud propia/RRHH/jefa, equipo ajeno/inactivo, autorización ordenada, bloqueo de deuda, protección contra edición posterior, cobros parciales y CONTPAQ. Navegador local con identidades sintéticas; validación productiva de solo lectura sin generar préstamos reales. Reversión mediante revert del commit y deploy oficial, sin revertir datos ni esquema. Los saldos históricos se preservan hasta su siguiente recálculo operativo.
