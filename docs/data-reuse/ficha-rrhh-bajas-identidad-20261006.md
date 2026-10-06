# Ficha de fuentes — bajas de RRHH

Fecha y ambiente consultado: 2026-10-06; producción PostgreSQL en solo lectura y PostgreSQL local aislado.

## Necesidad y unidad de análisis

Una persona es `rrhh.Empleado`; una baja es el evento de salida de esa persona en una fecha. El estado operativo actual es `Empleado.activo`.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Persona | `rrhh.Empleado` / `rrhh_empleado` | Ficha de RRHH | `id`, `codigo` único | Códigos 355 y 270 activos al inicio de revisión | Listado RRHH, acceso ERP, asistencia, nómina |
| Baja | `rrhh.EmpleadoBaja` / `rrhh_empleadobaja` | Formularios RRHH e indicadores; modelo | `empleado_id`, `fecha_baja` | 38 registros, 10 pares repetidos; baja 18 apuntaba a ficha distinta | Últimas bajas, rotación e indicadores |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Baja 18; código 355 | Confirmada por Mauricio | El nombre de la baja coincide con la ficha actual 355, pero el FK apuntaba a 356 por un intercambio histórico de identidades | Vincular 18 a la ficha 355; conservar historial de auditoría |
| Código 270; bajas 13 y 72 | Confirmada por Mauricio | Misma ficha, nombre y fecha de salida; motivos distintos | Conservar ambas anotaciones hasta conciliación documental; contar una salida |
| Otros pares de baja con misma ficha y fecha | Candidata a duplicidad de evento | Algunos contienen observaciones o motivos distintos | Revisión de Capital Humano antes de borrar o fusionar |

## Decisión de diseño

Reutilizar `Empleado` y `EmpleadoBaja`. Validar la correspondencia de nombre con la ficha y rechazar una segunda baja de la misma ficha y fecha. Presentar y contar un evento por persona y fecha sin borrar las anotaciones históricas.
La captura de la baja desactiva la ficha y su identidad operativa. El flujo existente de reingreso puede reactivar una ficha, por lo que Capital Humano debe verificar la fecha y el respaldo del nuevo ingreso.

Consultas o procedimiento reproducible: `inventario_fuentes_datos --term baja` y `--term empleado`; consultas SQL de solo lectura por código 355, 270 y grupos `(empleado_id, fecha_baja)`.

Riesgos y pendientes: los motivos divergentes de bajas antiguas requieren conciliación documental. `activo=True` con una baja histórica puede corresponder a un reingreso o a un error; requiere revisión individual.
