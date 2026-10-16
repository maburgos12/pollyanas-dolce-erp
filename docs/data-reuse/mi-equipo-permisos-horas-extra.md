# Ficha de fuentes — Mi equipo

Fecha y ambiente: 2026-10-16, producción, consultas acotadas de solo lectura.

## Necesidad y unidad de análisis

Capturar y corregir solicitudes de permisos y horas extra de colaboradores directos desde el ERP. Una fila representa una solicitud existente, no un bono ni un empleado nuevo.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | rrhh.Empleado / rrhh_empleado | RRHH | PK; jefe_directo.usuario_erp; activo | Yesenia: 7 colaboradores directos | RRHH y Bonos |
| Permiso | rrhh.PermisoSalida / rrhh_permisosalida | RRHH y captura de equipos | PK, folio, empleado | 3 visibles para Yesenia, incluidos propios | Asistencia, prenómina, Bonos |
| Corrección | rrhh.PermisoSalidaCambio | Flujo auditado de equipos | permiso_id, actor, fecha | Modelo existente confirmado por código e inventario | Expediente de permisos |
| Horas extra | rrhh.HoraExtra / rrhh_horaextra | RRHH, detección de asistencia y captura de equipos | PK, empleado, fecha; asistencia única cuando aplica | 14 registros de colaboradores directos | Autorización, prenómina, Bonos |

## Alias y equivalencias

“Bonos” es el nombre de una pantalla que también opera solicitudes de RRHH. Se confirmó por código que utiliza BasePermisosEquipoViewSet y BaseHorasExtraEquipoViewSet sobre los modelos anteriores. No se deducen equivalencias entre personas ni se modifica la jerarquía.

## Decisión

Extender el acceso mediante Mi equipo, reutilizando ambos flujos; filtro exclusivo de colaboradores directos activos, sin acceso general a RRHH. Solicitudes propias conservan su acceso de autoservicio. Sin tablas ni migraciones nuevas. Correcciones de permisos mantienen PermisoSalidaCambio; horas extra conservan las notas operativas y además registran antes/después en AuditLog para no perder evidencia aunque se corrija el texto de notas. No se exponen acciones de borrado.

Procedimiento: inventario_fuentes_datos --term permiso --term horaextra, PostgreSQL confirmado; conteos con permisos_visibles_usuario(yesenia) y HoraExtra filtrado por empleado__jefe_directo__usuario_erp. Los conteos son de la consulta inicial; no representan una validación de navegador.

Riesgos y pendientes: probar aislamiento, fechas, importes no finitos, estados terminales, motivo obligatorio, ausencia de autoautorización y navegador. Pruebas de escritura solo con datos sintéticos locales; producción se verifica en lectura.
