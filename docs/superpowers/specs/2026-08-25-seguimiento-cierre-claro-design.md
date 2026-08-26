# Diseño: cierre claro de acuerdos con checklist completo

## Problema

Una minuta puede tener todos sus puntos terminados y conservar el estado técnico
`PENDIENTE` mientras la fuente Agente DG siga `OPEN`. La pantalla muestra a la vez
`1 de 1 · 100%`, el punto `Hecho` y la insignia `Pendiente`, sin explicar que aún
falta confirmar el cierre general. El caso productivo que motivó el cambio es la
minuta 576, «Actualización Receta Galletas».

## Resultado esperado

Cuando un acuerdo abierto tenga checklist y todos sus puntos estén completos:

- su estado operativo visible será `Listo para cerrar`;
- el colaborador verá como acción principal `Cerrar acuerdo`;
- Dirección verá que el colaborador terminó los puntos y que falta la confirmación
  final del responsable;
- el estado técnico ERP y el estado de la fuente seguirán disponibles en
  `Detalles técnicos`;
- el acuerdo solo se cerrará mediante la acción explícita existente, con su
  write-back y auditoría actuales.

## Alcance

El cambio se limita al módulo `seguimiento`:

- clasificación visual del estado de un `SeguimientoItem`;
- detalle compartido de colaborador y Dirección;
- listados del Panel de acuerdos que hoy muestran directamente el estado técnico;
- pruebas de regresión del flujo.

No se modificarán modelos, migraciones, API, datos productivos, permisos ni la
lógica de sincronización con Agente DG.

## Diseño funcional

La clasificación visual reconocerá cuatro condiciones en este orden:

1. Cerrado o cancelado: conservar el estado final existente.
2. En revisión: conservar `En revisión`.
3. Abierto con checklist completo: mostrar `Listo para cerrar`.
4. Cualquier otro acuerdo abierto: conservar su estado técnico actual.

`Listo para cerrar` es un estado de presentación, no un nuevo valor persistido.
Esto evita migraciones y mantiene a Agente DG como fuente de verdad del cierre.

En el detalle del colaborador, el panel derecho conservará la llamada principal
al cierre existente y explicará que todos los puntos están listos. En la vista de
Dirección, el mismo panel reemplazará el mensaje genérico de consulta por:
`El colaborador terminó todos los puntos. Falta que confirme el cierre del acuerdo.`
Dirección continuará en solo lectura.

Los listados usarán la etiqueta y el tono operativos para no volver a presentar
un acuerdo 100% como simplemente `Pendiente`. `Detalles técnicos` continuará
mostrando `OPEN · Pendiente` cuando esa sea la realidad sincronizada.

## Manejo de errores y estados límite

- Un acuerdo sin checklist no se considerará listo para cerrar.
- Un checklist parcialmente completo conservará el estado técnico.
- Si el write-back de cierre falla, el acuerdo permanecerá `Listo para cerrar` y
  mostrará el error existente; no se fingirá un cierre local.
- Un acuerdo ya cerrado siempre prevalecerá sobre el porcentaje del checklist.
- La vista de Dirección no incorporará acciones de escritura.

## Pruebas

Se agregarán pruebas que demuestren primero el fallo actual y luego verifiquen:

- checklist 100% + fuente `OPEN` produce `Listo para cerrar`;
- la vista de Dirección explica que falta el cierre del colaborador;
- la vista del colaborador mantiene visible la acción `Cerrar acuerdo`;
- checklist incompleto conserva `Pendiente` o `En proceso`;
- acuerdo cerrado conserva `Completado`;
- el estado técnico continúa visible en la sección técnica.

La validación final incluirá pruebas enfocadas de `seguimiento`, `manage.py check`,
`migrate --check`, revisión en navegador del detalle de colaborador y Dirección,
y comprobación del resultado visible después del despliegue.

## Riesgos y mitigaciones

El principal riesgo es confundir una etiqueta visual con un estado persistido.
Se mitiga nombrándola como estado operativo, manteniendo los estados técnicos en
su sección y cubriendo ambos con pruebas. No se automatiza el cierre para preservar
la confirmación, el write-back y la trazabilidad.
