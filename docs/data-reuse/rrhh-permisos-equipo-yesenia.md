# Ficha de fuentes — permisos de equipo

Fecha y ambiente consultado: 2026-10-13; producción en solo lectura y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Una solicitud de permiso/salida de un empleado. La jefatura necesita consultar, crear y resolver solicitudes de su equipo directo, sin acceso general a RRHH ni autorización de sus propias solicitudes.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Persona y jefatura | `rrhh.Empleado` / `rrhh_empleado` | RRHH | PK, `usuario_erp_id`, `jefe_directo_id`, `activo` | Usuario 7 vinculado al empleado 3; seis subordinados activos | Permisos, vacaciones, asistencia, notificaciones |
| Cuenta y acceso | `auth.User`, `core.UserModuleAccess` | Administración de usuarios | Usuario PK; acceso por módulo | Cuenta activa; sin grupos ni accesos RRHH | Navegación, vistas y API |
| Solicitud | `rrhh.PermisoSalida` / `rrhh_permisosalida` | API RRHH, Bonos y bandeja de permisos | PK y folio único; FK empleado | Cero solicitudes del equipo activo al momento de la auditoría | RRHH, Bonos, asistencia e incidencias |
| Corrección histórica | `rrhh.PermisoSalidaCambio` | Flujo auditado de Bonos | FK permiso, actor y motivo | No se modifica en esta tarea | Auditoría de correcciones |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `yesenia.soto` y empleado 3 | Confirmada | Enlace explícito `usuario_erp_id=7`; no se busca por nombre para asignar equipo | Ninguna nueva equivalencia |
| Equipo y departamento | Distinta | El equipo se define por `jefe_directo`, no por coincidencia del departamento | No reasignar personal ni ampliar a todo un departamento |
| Permiso operativo y `auth.Permission` | Distinta | El inventario devuelve candidatos léxicos; solo `PermisoSalida` representa la solicitud | Sin fusión de fuentes |

## Decisión de diseño

Extender la bandeja y API existentes. Reutilizar `PermisoSalidaSerializer`, los servicios de resolución y la notificación existente. Registrar `creado_por` sin autorizar automáticamente. Conservar el autoservicio `mis=true` limitado a solicitudes propias; la bandeja completa incluye el equipo directo. Mantener el reconocimiento legado del empleado del usuario, trasladando la función existente sin cambiar su algoritmo y conservando su importación desde `api_views`.

Consultas reproducibles: `manage.py inventario_fuentes_datos --term permiso --term 'jefe directo'` con PostgreSQL; `Empleado.objects.filter(activo=True, jefe_directo__usuario_erp__username='yesenia.soto')`; `PermisoSalida.objects.filter(empleado__in=equipo).count()`; verificar `UserModuleAccess` exclusivamente del usuario consultado.

Riesgos y pendientes: las autorizaciones reales afectan asistencia; las pruebas de creación y resolución usan datos sintéticos locales. No se alteran modelos, migraciones, roles, jefaturas ni solicitudes reales. El alcance sigue la asignación vigente de jefe directo.
