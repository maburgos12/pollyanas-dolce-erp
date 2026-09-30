# Horas extra por corte quincenal — diseño

Fecha: 2026-09-30

Aprobación operativa: el alcance retroactivo solicitado corresponde a Beatriz y Clarisela (solicitudes 281–284). El resto del rezago de la semana conserva su estado `PENDIENTE`; este despliegue no autoriza, rechaza, paga ni modifica tiempos.

## Resultado esperado

Una hora extra pendiente al momento de ejecutar el corte no entra a esa nómina. Cuando se autorice después, queda disponible para el siguiente corte. Cada solicitud puede pertenecer a un solo corte y la pantalla debe mostrar claramente su situación de pago sin cambiar la fecha real trabajada.

## Diagnóstico

`_horas_extra_por_empleado` filtra hoy por fecha trabajada dentro del rango del corte. Esa regla excluye del corte siguiente una solicitud trabajada en la quincena anterior aunque se autorice después. Ampliar el rango sin más sería inseguro: producción tiene 204 solicitudes autorizadas, pero solo un movimiento de prenómina de hora extra, por lo que el sistema no puede inferir cuáles históricos ya fueron pagados fuera del ERP.

## Alternativas consideradas

1. **Recomendada: reutilizar `PrenominaMovimiento` como asignación y marcar qué solicitudes participan en el nuevo control.** Es el menor cambio que conserva trazabilidad, evita duplicados y no reactiva históricos.
2. Agregar un `ForeignKey` directo de `HoraExtra` a `PrenominaCorte`. Repite una relación que ya expresa `PrenominaMovimiento` y obliga a sincronizar dos fuentes.
3. Calcular el arrastre solo por fechas. No requiere migración, pero no distingue horas ya pagadas fuera del ERP y puede duplicar pagos.

## Regla funcional aprobada

- `HoraExtra.fecha` siempre conserva el día trabajado.
- El instante real de corte es `PrenominaCorte.creado_en`; `fecha_corte` conserva la fecha operativa mostrada.
- Una solicitud puede incorporarse cuando está `AUTORIZADO`, participa en el control nuevo, su fecha trabajada no es posterior al fin del corte y `fecha_autorizacion_jefe <= corte.creado_en`.
- Si al crear el corte sigue pendiente, no se incorpora. Al autorizarse después queda sin movimiento y será candidata del siguiente corte.
- La primera incorporación crea un `PrenominaMovimiento` `HORA_EXTRA`. Una restricción de base de datos impedirá que la misma `HoraExtra` se ligue a dos cortes.
- Recalcular el mismo corte conserva su movimiento y es idempotente.
- Rechazadas, canceladas y pagadas no son candidatas.

## Protección del histórico

Se agregará a `HoraExtra` una bandera `requiere_aplicacion_prenomina`:

- solicitudes nuevas: `True` por defecto;
- migración: `True` únicamente para las que estén `PENDIENTE` al desplegar;
- autorizadas, rechazadas, canceladas y pagadas existentes: `False`.

Esto incluye las solicitudes 281–284 cuando sean autorizadas, pero no interpreta las 204 autorizadas históricas como deudas. Una regularización histórica queda fuera de alcance y requeriría conciliación explícita.

## Cambios técnicos

- `rrhh/models.py`: bandera de seguimiento y unicidad global condicional de movimientos cuya fuente sea `rrhh.HoraExtra`.
- migración nueva de `rrhh`: alta de bandera, inicialización segura por estado y restricción.
- `rrhh/services_prenomina.py`: seleccionar horas ya asignadas al corte y horas autorizadas elegibles todavía sin asignación; dejar de depender exclusivamente del rango trabajado.
- `rrhh/views.py`: consultar el movimiento de prenómina asociado sin consultas por fila y preparar una etiqueta de situación.
- `rrhh/templates/rrhh/horas_extra_list.html`: mostrar `Pendiente de autorizar`, `Pendiente del próximo corte` o el folio/rango del corte asignado.
- `rrhh/tests_prenomina.py` y pruebas de la bandeja: cubrir corte inmediato, arrastre, no duplicidad, protección del histórico y texto visible.

No se modificará `NominaPeriodo`: es un flujo heredado sin registros de producción ni llamadores comprobados. Tampoco se cambiarán montos, estados de pago ni solicitudes existentes.

## Estados observables

| Situación | Texto esperado |
| --- | --- |
| Pendiente y bajo seguimiento | Pendiente de autorizar · irá al siguiente corte disponible |
| Autorizada después del último corte, aún sin asignar | Pendiente del próximo corte |
| Asignada | Pago: `<folio>` · `<fecha_inicio>–<fecha_fin>` |
| Histórica fuera del control nuevo | Sin etiqueta de corte; no se reactiva automáticamente |

## Criterios de aceptación

1. Una solicitud autorizada antes de crear el corte aparece una sola vez en ese corte.
2. Una solicitud pendiente al crear el corte no aparece aunque se autorice y se recalcule después.
3. Esa misma solicitud aparece en el siguiente corte.
4. Recalcular o crear cortes posteriores no duplica la solicitud ya asignada.
5. Las autorizadas históricas migradas con seguimiento desactivado no se incorporan.
6. Las 281–284 conservan fecha y horas; mientras estén pendientes muestran que requieren autorización y, una vez autorizadas, siguen la regla del siguiente corte.
7. La pantalla móvil conserva su diseño compacto y muestra horas/minutos sin fracciones.

## Fuera de alcance

- Marcar automáticamente `PAGADO` al exportar o cerrar una nómina.
- Conciliar masivamente las 204 solicitudes autorizadas históricas.
- Unificar `NominaPeriodo` con `PrenominaCorte`.
- Cambiar las horas o autorizaciones de las solicitudes 281–284.

## Riesgos y reversibilidad

La migración es aditiva. La bandera evita incorporar históricos ambiguos y la restricción detiene una doble asignación incluso bajo concurrencia. El despliegue no modifica montos ni estados; revertir el código deja intactos los movimientos ya creados y la migración puede revertirse mientras no existan dependencias posteriores.
