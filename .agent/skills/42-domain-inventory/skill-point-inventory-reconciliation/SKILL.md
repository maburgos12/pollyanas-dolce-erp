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

El lector compartido puede acreditar una frontera con snapshot original Point
únicamente bajo el contrato del procedimiento: identidad exacta, procedencia
documental, captura posterior y ÚltimoMovimiento UTC anterior al corte. Esto no
certifica historia COMPLETE ni conteo físico, ni da nuevas capacidades al goal.
Si hay canónica INCOMPLETE, la autorización técnica posterior del 4 de octubre permite evaluar
la frontera independiente sin ocultar cobertura; inspeccionar raws conocidos y
contradicciones según procedimiento. El contrato Bamoa permite también evaluar
snapshot si la canónica es COMPLETE, unknown0 y falta la frontera concreta: una
frontera canónica presente manda. No alterar coverageCOMPLETE ni fabricar membresía
de500 filas; frontera exclusivamente snapshot conserva canonical_history_verifiedFalse.
Ver procedimiento y checkpoint para distinguir contrato autorizado de publicación.
Para vacío/cero original, la autorización5oct permite prueba independiente junto
a INCOMPLETE y consolidación con fuentes directas por par: consultar el contrato
específico del procedimiento. No extrapolar ÚltimoMovimiento a listado vacío,
inventar membresía o promover coverage. Los flags originales y físicos permanecen
separados; review/plan no adquiere HTTP, captura ni permiso de cierre.
No recapturar para reemplazar evidencia ya
acreditada ni exigir historia completa como condición de un snapshot independiente.
La autorización humana del 4 de octubre para este contrato está registrada en el checkpoint;
no repetir su solicitud ni presumir publicada una implementación todavía pendiente.

Si existe una respuesta histórica original íntegra ya guardada, usar la entrada
controlada del mismo servicio descrita en el procedimiento, dentro de la ejecución
autorizada: no simular un cliente HTTP para hacerla pasar por captura actual. El
goal review/plan sigue sin importar. Fecha de ingreso no es fecha de consulta;
conservar petición, dominio, SHA, membresía y procedencia por fila. Un resumen o
una muestra no se convierte en respuesta completa ni en cobertura COMPLETE.

Los hallazgos fechados son evidencia histórica, no actualización de fuentes canónicas.
Se seleccionan por período y claves externas de producto/sucursal, nunca solo por
nombre, SKU ambiguo o PK del expediente. No reemplazan FK documental ni prueban conteo
físico. No consultar otra vez un folio verificado sin evidencia nueva.

El procedimiento contiene el contrato temporal comprobado por dominio: Stock
`raw_payload.Fecha` naive es UTC; notas `Fecha_Hora` naive es hora local. Aplicarlo
también a apertura, cierre y cobertura, no solo a ventas. La actualización de este
conocimiento no certifica que el lector haya sido publicado ni autoriza reescribir
originales. El checkpoint fechado distingue evidencia, reparación y entrega.

Para vetos por representación original, leer el contrato específico del
procedimiento: comparar timestamps aware en UTC exacto y distinguir ecuación
Decimal exacta de representación binaria IEEE de tres floats originales finitos.
No aplicar epsilon general, redondeo a3 decimales ni tolerancia a stock/cortes/
identidades. Un residuo de representación no es pérdida ni permiso de cierre;
la actualización documental no significa que el nuevo lector esté desplegado.

Ante MERMA cancelada o CANCELACION DE MERMA en historia Stock, leer la excepción
documental estricta del procedimiento: cada original puede conservar débito o
reverso firmado por su propia ecuación, fecha y ámbito. No excluir el débito por
Cancelado ni acreditar crédito sólo por etiqueta, inventar FK de pareja o modificar
MermaPOS. PR1476 fue publicado y aceptado; consultar su evidencia en el checkpoint;
review/plan no ganan HTTP, importación, materialización ni permiso de cierre.

Para Coca450 y Vela875, consultar clasificación comercial documental en el
procedimiento: regla curada/catálogo corroborante no son FK de la transacción.
Preservar fuente, cantidad y unidad fuera del balance fabricado cuando el lector
estricto lo acredita; no transformar AGG en ejecución ni ampliar tokens globales.
PR1477 fue publicado y aceptado; el checkpoint registra evidencia de lectura,
runtime y pantalla. Consultar también los límites de compras CakeTopper: una FK
derivada por nombre no es identidad transaccional ni decisión comercial curada.

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
El 3 de octubre Mauricio autorizó proteger el importador y recuperar exclusivamente
el par original1683114/PK1676/hash9f27f6de945bbc8b19cd, con pruebas, publicación y
validación independiente. Esa autorización no es una capacidad del goal: review y
plan siguen sin executor ni escrituras operativas. Ausencias de listado abortan la
sustitución; recuperación por backup conserva identificadores/timestamps/escritor,
colisiones abortan y segunda ejecución no agrega filas. No reconstruir inventario,
crear merma Point ni dar por reparada autoridad mensual solo por266→267. No repetir
solicitudes humanas ya enviadas. America/Mazatlan permanece.
Esa autorización inicial fue seguida de una autorización mensual específica;
el checkpoint4oct registra job81612 y autoridad comprobada. No tratar el bloqueo
histórico anterior como vigente sin consultar ese corte y las fuentes actuales.

## Verificación

Pruebas de runtime con HTTP prohibido, capture prohibido, fuentes no autoritativas,
ventas originales, finalización pendiente, identidad de hallazgos y segunda revisión.
Publicación exige CI, deploy oficial y revisión real visible en Orquestación.
Sin scheduler nuevo: el operador/heartbeat invoca una revisión acotada por expediente.
