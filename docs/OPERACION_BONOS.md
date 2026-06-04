# Operación de bonos

Este runbook cubre el contrato operativo mínimo para bonos ventas y bonos
producción. No sustituye `AGENTS.md`; complementa las reglas de desarrollo con
validaciones del flujo real.

## Contrato del periodo activo

Un periodo creado con `0` empleados inicializados es un estado operativo
incompleto. No basta con que exista `ConfigBonoPeriodo` o
`ConfigBonoVentasPeriodo`.

Para considerar listo un periodo activo debe cumplirse:

- Existe el periodo correcto para el corte activo.
- Hay empleados elegibles en RRHH.
- El roster del periodo está inicializado o la app tiene un fallback explícito.
- La jefatura real puede ver empleados en permisos, captura y resumen.
- Los endpoints de resumen y permisos devuelven empleados esperados.

Si el periodo existe pero el roster está vacío, el sistema debe hacer una de
estas tres cosas de forma explícita:

- inicializar empleados;
- mostrar elegibles/autorizables sin depender solo del roster;
- bloquear con un mensaje claro y una acción para inicializar.

Nunca debe fallar silenciosamente con una lista vacía.

## Bonos ventas

Validar especialmente con Johana López.

Pantallas y endpoints mínimos:

- `/bonos-ventas/app/?captura=1`
- tabs: permisos, captura, resumen
- `/api/bonos-ventas/permisos/`
- `/api/bonos-ventas/bonos/resumen/`

Aprendizaje de junio 2026:

- El periodo de ventas podía existir con rango activo, pero con `0` registros en
  `BonoVentasEmpleado`.
- La app de ventas dependía estrictamente del roster del periodo para permisos,
  captura y resumen.
- Resultado: aunque había empleados elegibles, la jefatura veía equipo vacío.

Guardrail esperado:

- Cualquier cambio en permisos, captura, resumen o configuración de periodo en
  bonos ventas debe probar el caso "periodo existe pero roster vacío".
- Si se configura un periodo nuevo, el cierre debe incluir inicialización o
  auditoría explícita del roster.

## Bonos producción

Validar especialmente con Carolina Cayetano.

Pantallas y endpoints mínimos:

- `/bonos-produccion/app/?captura=1`
- tabs: permisos, captura, resumen
- `/api/bonos-produccion/permisos/`
- `/api/bonos-produccion/bonos/resumen/`

Aprendizaje aplicado:

- Producción no debe asumirse equivalente a ventas.
- El flujo de permisos de producción puede incluir equipo autorizable, jerarquía
  o empleados elegibles aunque falten bonos del periodo.
- Esa tolerancia evita que una jefatura quede sin empleados visibles por un
  roster incompleto, pero no elimina la obligación de auditar el periodo.

Guardrail esperado:

- Cualquier cambio en permisos, captura, resumen o reglas de producción debe
  probar roster del periodo y equipo autorizable por separado.
- Verificar que `recalcular()` y `recalcular_todos()` no pisan `bono_extra`,
  `ajuste_positivo` ni `ajuste_negativo`.

## Validación de cierre

Los siguientes checks no prueban por sí solos que bonos quedó operativo:

- `python manage.py check`
- `python manage.py migrate --check`
- tests unitarios sin datos vivos del periodo
- deploy correcto al VPS

Antes de cerrar una tarea de bonos, validar en producción o ambiente objetivo:

- conteo de empleados elegibles;
- conteo de roster del periodo;
- respuesta de endpoints de permisos y resumen;
- app visible con empleados para la jefatura real;
- consola y Network/XHR sin errores.
