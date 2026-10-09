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

Regla DG comprobada el 8oct2026: **Recibida es el estado terminal de una
transferencia normal Point**. Exigir `is_received` y `received_at`, no
`is_finalized` como otro paso. Conservar esa bandera original, sin generar
«falta finalización». Enviado=recibido no deja pendiente por esa bandera;
enviado>recibido conserva la diferencia y el retorno administrativo al origen
en la fecha de recepción, sin duplicarlo ni inventar devolución física. Una
transferencia sin recepción sigue pendiente. El ejemplo39577 pertenece a
octubre, nunca a septiembre: Snickers Mini2 enviadas/0 recibidas, salida1686766
y retorno1687006 comprobados en CEDIS. Ver procedimiento y checkpoint.

Septiembre es cierre documental contra Point, no aprobación de custodia física.
Un retorno administrativo recibido, fechado en el mes y con FK original exacta
puede permitir el cierre individual cuando ambos cortes, cadena y TODOS los
rubros coinciden con el historial canónico. Conservar diferencia de recepción,
advertencia y custodia pendiente en la evidencia, sin quitarla del expediente.
Recibido mayor que enviado, identidad secundaria o retorno no registrado en las
entradas del origen no pasa esta excepción. No ampliar el guard mensual.

Una merma asignada secundariamente puede corroborarse sin modificar aliases:
exigir archivo original íntegro de ese producto/sucursal, membresía y receipt
verificados, historial COMPLETE, FK de movimiento en waste y único detalle
exacto con cantidad/unidad coincidentes. Guardar `corroborated_waste` y conservar
la advertencia. Todos los cortes/rubros y la cadena siguen siendo obligatorios.
No extenderlo a producción/SKU ni convertir merma en prueba sin motivo explícito.

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
Ante una respuesta íntegra con FK repetida, consultar el contrato de membresía del
procedimiento: conservar todas las ocurrencias y su límite original; sólo raws con
contenido y tipos idénticos comparten una fila canónica. Nunca reducir500 a499 para
aparentar lote no saturado. Discontinuidades fuera del mes permanecen en evidencia;
cadena mensual y ambos cortes requieren prueba propia y firma del archivo usado.

Los hallazgos fechados son evidencia histórica, no actualización de fuentes canónicas.
Se seleccionan por período y claves externas de producto/sucursal, nunca solo por
nombre, SKU ambiguo o PK del expediente. No reemplazan FK documental ni prueban conteo
físico. No consultar otra vez un folio verificado sin evidencia nueva.

Para producción Point, cuando el SKU colisiona, revisar
`PointProductionLine.raw_payload.detail.PK_Producto`: es la identidad original
del producto, distinta de un nombre, receta o SKU. El procedimiento exige
`IsInsumo=false`, FK positiva exacta y `PointProduct.external_id` único;
contradicciones fallan cerrado. Resolver esa identidad no acredita por sí solo
la ecuación, los cortes ni el cierre mensual.

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

Decisión DG de 6 octubre: `Extra 10` es cargo adicional, y CakeTopper es una
línea de accesorios comprados, no producción. Excluir únicamente del balance
producido-vendido cuando el raw exacto corrobore clasificación; conservar ventas,
stock Point, costos y demás controles. Para conversiones, los Stock tipo 21 y 22
son movimientos independientes: no exigir FK entrada↔salida inexistente ni
derivar salidas con un factor fijo del agregado `AGG`. Auditar salida tipo 22 por
producto/sucursal/FK original y preservar cobertura incompleta como incidencia.
El reporte `AGG` y Stock pueden tener distintos PK de `PointBranch` para la misma
sucursal: cotejar FK ERP y nombre originales de ambos dominios; nunca sus PK
Point entre sí ni una pareja de movimientos que no existe.

Corrección DG8oct: Dot Cake Chocolate4358 y Vainilla8734 sí son productos
fabricados, aunque estén clasificados en vasos. Incluirlos por receta única,
código original coincidente y modo FABRICADO; no reincorporar todos los vasos.
Cero ventas no elimina producción de pruebas. Comparar producción, merma y
saldo del mismo dominio/período, y exigir motivo documental antes de afirmar
«pruebas». Pan de Muerto PMH028 es preparación IsInsumo=true: no sumarlo como
producto final ni llamar pérdida a producción menos merma sin revisar consumos.
Un producto nuevo puede no tener línea en el cierre anterior. Reutilizar el
historial original íntegro con cortes acreditados: contrato
POINT_ORIGINAL_HISTORY_BOUNDARY_V1, sin inventar referencia de manifiesto ni
alterar su cobertura. La prueba se comparte entre cálculo, pantalla y cierre.
La traza también reutiliza la exclusión comercial exacta aprobada: Extra10 y
accesorios acreditados no bloquean el cierre de pasteles de toda una sucursal.

Alcance DG de 7 octubre para `Producido vs Vendido`: reutilizar
`read_audit_report`; sólo producción en sucursales de venta y CEDIS. Devoluciones
conserva su expediente y movimientos, pero sus saldos no son requisito de esta
vista. Excluir reventa, accesorios, cargos, cafés/té/Coca-Cola, Alegría, Cake
Topper, D-rigaldi, Granmark, Industrias Lec, Plásticos, Regalos, Velas, vasos y
Otros postres. Revisar categoría original Point además de categoría de receta:
una vela en Bollo o tarjeta en Media Plancha no se convierte en producto fabricado.
Recetas de modo REVENTA/SERVICIO_ACCESORIO quedan fuera; conservar rebanadas y
derivados fabricados aunque no pasen por captura de producción. Rosca sin
actividad mensual también queda fuera. La exclusión sólo afecta pantalla,
JSON y exportaciones de este reporte, no maestros, fuentes ni guard de cierre
mensual. Mostrar saldos y ecuaciones guardados de las ubicaciones elegibles;
si falta un dato real, explicarlo por sucursal en Ver detalle, sin claves técnicas.
Pruebas y conocimiento actualizado no acreditan despliegue ni cierre por sí solos.

Confirmación DG del 6 de octubre: auditar cada producto y sucursal en el
Historial de Inventario de Point por fecha, tipo, cantidad y continuidad de
existencia anterior/posterior. Un folio o referencia común no es requisito
universal, especialmente para conversiones. Una vista de los últimos 500
movimientos no prueba por sí sola cobertura completa de septiembre; conservar
el límite, el original y los extremos acreditados antes de cerrar el par.

Ante recetas estacionales sin cierre calculado, separar preparación (`IsInsumo=True`)
de producto final antes de interpretar producción, venta o merma. La presencia en
catálogo o en un snapshot con saldo cero no prueba actividad mensual ni historia
completa. La comprobación acotada de Pan de Muerto y Rosca de septiembre está en
el checkpoint; no trasladar cantidades de preparación a producto terminado ni
atribuir diferencias a personas por una ausencia en tablas filtradas.

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

La pantalla `Cierre producto` lee `ProductoMonthClosure` guardado, no la
observación `review` ni un `preview` reciente. Tras una corrección de fuente,
contrastar el corte y la firma visibles con el preview sin HTTP. Si el cierre
no está bloqueado y el rebuild está autorizado, actualizarlo por
`ProductMonthClosureService.build(rebuild=True, lock_after_build=False)` y
verificar la vista autenticada y las exportaciones. Una frontera incompleta de
una receta no oculta los saldos documentados de otras; la autoridad mensual y
el permiso de lock permanecen falsos hasta resolver todas las guardas. No
presentar un saldo parcial de la receta afectada como dato final ni inferir
conteo físico.

Pruebas de runtime con HTTP prohibido, capture prohibido, fuentes no autoritativas,
ventas originales, recepción pendiente, identidad de hallazgos y segunda revisión.
Publicación exige CI, deploy oficial y revisión real visible en Orquestación.
Sin scheduler nuevo: el operador/heartbeat invoca una revisión acotada por expediente.
