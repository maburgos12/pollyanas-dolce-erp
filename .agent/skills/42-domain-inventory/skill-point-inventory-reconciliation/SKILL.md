---
name: point-inventory-reconciliation
description: Habilidad versionada del agente ERP de conciliación para revisar expedientes mensuales con evidencia existente, separar saldo, trazabilidad y conteo físico, y seleccionar el siguiente paso seguro sin sincronizaciones ni cambios operativos.
version: "1"
---

# Conciliación verificable Point / ERP

Esta habilidad vive en el ERP y se carga obligatoriamente en el objetivo
`reconciliation_guard` de `agente_conciliacion`, incluso si su definición antigua
solo menciona el contexto fundacional. El binding ejecutable es
`orquestacion.services.inventory_reconciliation.observe_review`; no es un documento
que se presume aprendido ni un sincronizador general.

## Contrato de ejecución

Entrada: expediente exacto `ProductInventoryAuditCase`, agente activo y acción
`review`. Invocación mediante `run_agent_goal(Goal(goal_type="reconciliation_guard",
objective="Revisar expediente", entity_type="ProductInventoryAuditCase",
entity_id=ID), base_dir=settings.BASE_DIR)`.

CLI oficial: `python manage.py run_agent_goal --goal reconciliation_guard
--event-id ID --entity-type ProductInventoryAuditCase --requested-action review`.
No ejecutar sin PostgreSQL configurado. `--event-id` es el argumento histórico
del runtime; en este goal significa el ID del expediente, no un evento de ventas.

Lee la investigación existente con `InventoryAuditAgent.investigate_case` y
reconcilia únicamente el historial canónico guardado. Conserva cantidades comerciales,
clasifica fuente mensual no autoritativa, movimientos desconocidos, pendientes
documentales y negativos heredados. No llama Point, no captura, no materializa,
no cambia maestro/stock/ventas/mermas/RRHH ni aprueba expedientes.

Registra contexto cargado, observación, decisión y verificación en los modelos
existentes de orquestación. El éxito significa **revisión terminada**, nunca cierre
del expediente o del mes. Cada invocación explícita conserva su propia bitácora;
repetirla no agrega importaciones, filas operativas, notificaciones ni aprobaciones.
Las capacidades externas declaradas en el catálogo no son ejecutadas por este goal.

## Conocimiento obligatorio

Leer [procedimiento](references/procedimiento.md) y
[continuidad septiembre 2026](references/septiembre-2026.md). El runtime carga ambos
archivos y [hallazgos fechados](references/hallazgos.json), además de este archivo,
antes de observar. Falta o formato inválido impide crear una ejecución.

Los hallazgos fechados son evidencia histórica, no actualización de fuentes canónicas.
Se seleccionan por período y claves externas de producto/sucursal, nunca solo por
nombre, SKU ambiguo o PK del expediente. No reemplazan FK documental ni prueban conteo
físico. No consultar otra vez un folio verificado sin evidencia nueva.

## Límites y siguiente paso

### Plan mensual antes de revisar uno por uno

Usar el mismo goal con `metadata={"mode":"plan_month","batch_size":10}` y un
expediente ancla del mes. CLI: `run_agent_goal --goal reconciliation_guard
--event-id ID --entity-type ProductInventoryAuditCase --plan-month`.
El plan reutiliza expedientes y bitácoras existentes, sin reinvestigar el mes ni
ejecutar el lote. Muestra un bloqueo mensual compartido una vez, grupos por
comprobación pendiente, progreso registrado y hasta diez IDs de próxima revisión.
Atender primero el bloqueo global según autorización y avanzar investigaciones
independientes; no detener todo ni repetir la misma solicitud por cada expediente.

Revisiones con firma idéntica de caso/corrida/eventos/hallazgos se reutilizan SOLO
para planificación. Cambios registrados regresan el caso a la cola. Una firma de
proyección NO certifica fuentes externas actuales: no llamar «vigente en Point» a
una bitácora guardada. Si hay evidencia externa nueva, revisar ese expediente
explícitamente con el contrato individual; no esperar que un caché la adivine.

Para continuar el lote usar `--after-case-id` y `--expected-plan-fingerprint`
devueltos por el plan. Si cambia la evidencia, el cursor se reinicia y lo informa.
Lotes no autorizan HTTP, captura o notificaciones. No fabricar porcentaje de cierre
a partir de BALANCED; estado documental, aprobación y conteo físico siguen aparte.
Los expedientes excluidos por sold_products se conservan, no se consideran cerrados.

La prioridad es autoridad mensual, divergencia venta comercial/stock, evidencia
guardada, desconocidos y cobertura. Solo recomienda verificar cobertura cuando
existe diferencia real, apertura/cierre referenciados, remanente cero y ventas
coherentes. La recomendación no concede HTTP: otra ejecución autorizada debe usar
sesión protegida, lote máximo diez, capture sin force y canónica única.

Un missing documentado siempre conserva trazabilidad pendiente aunque una etiqueta
anterior diga COMPLETE. Una merma ausente del listado no es cancelación ni pérdida.
No restaurar 1683114 ni modificar importador: su autorización independiente sigue
pendiente. No repetir solicitudes humanas ya enviadas. America/Mazatlan permanece.

## Verificación

Pruebas de runtime con HTTP prohibido, capture prohibido, fuentes no autoritativas,
ventas originales, finalización pendiente, identidad de hallazgos y segunda revisión.
Publicación exige CI, deploy oficial y revisión real visible en Orquestación.
Sin scheduler nuevo: el operador/heartbeat invoca una revisión acotada por expediente.
