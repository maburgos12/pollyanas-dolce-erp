# Compras departamentales: cancelación, reembolso y cierre de entrega

**Fecha:** 2026-09-28

**Estado:** Diseño aprobado en conversación; pendiente de revisión del documento

**Módulo:** `compras` — Gestión de Compras Departamentales

## Problema

El flujo actual solo acepta cantidades recibidas mayores que cero. Si un proveedor
cancela o no entrega después de que la compra fue registrada, Compras no puede
representar ese resultado: intentar capturar `0` se rechaza y el artículo queda
indefinidamente como pendiente de entrega.

Existe además una confusión de estado. Cuando Compras registra la cantidad total,
el artículo pasa a confirmación del área solicitante, pero la interfaz sigue
presentándolo como pendiente sin explicar con suficiente claridad que la entrega
ya fue registrada y quién debe confirmarla. La solicitud solo queda completada
cuando todos sus artículos están recibidos conforme, rechazados o cancelados.

La edición existente cubre dos casos distintos y no resuelve este problema:

- permite editar una cotización antes de que exista compra o recepción;
- permite corregir importe, fecha, referencia y comprobante de una compra pagada;
- no permite cancelar un intento de compra, solicitar un reembolso ni volver a
  comprar el mismo artículo con otro proveedor conservando el antecedente.

## Objetivo

Permitir que Compras registre de forma auditable que un proveedor canceló o no
entregó, distinga si hubo pago, dé seguimiento al reembolso cuando corresponda y
vuelva a cotizar o comprar el artículo con otro proveedor sin borrar la historia.

Al mismo tiempo, hacer explícita la diferencia entre:

1. entrega registrada por Compras;
2. entrega pendiente de confirmación del área;
3. entrega recibida conforme y completada;
4. intento cancelado sin pago;
5. pago con reembolso solicitado;
6. reembolso confirmado.

## Alcance

### Incluido

- Acción separada `Proveedor canceló / no entregó`; una cancelación nunca se
  representará como recepción con cantidad cero.
- Cancelación antes o después de registrar la compra, siempre que no exista una
  recepción positiva del intento que se pretende cancelar.
- Registro de pago existente, solicitud de reembolso y confirmación posterior del
  reembolso.
- Nueva cotización y nuevo intento de compra para el mismo artículo.
- Conservación de órdenes, cotizaciones, comprobantes, avisos e historial previos.
- Estados y responsables claros en detalle, bandeja y resumen monetario.
- Confirmación final del área solicitante antes de marcar una entrega como
  recibida conforme.
- Permisos, concurrencia, validación, acciones asíncronas y pruebas del módulo.

### Fuera de alcance

- Cancelación parcial del saldo no entregado después de una recepción parcial.
- Conciliación bancaria automática del reembolso.
- Modificación o eliminación de movimientos financieros externos.
- Envío de mensajes retroactivos para compras existentes.
- Cambio de permisos generales o navegación del módulo.

Una recepción parcial seguirá el flujo actual de diferencias y requerirá una
decisión posterior específica; no debe tratarse como cancelación total.

## Decisión de arquitectura

### Alternativas consideradas

1. **Cambiar solamente el estado del artículo.** Es simple, pero mezcla el ciclo
   del artículo con el del proveedor, pierde intentos anteriores y no permite una
   segunda compra porque la compra actual es uno-a-uno con el artículo.
2. **Crear un artículo duplicado como reemplazo.** Conserva la compra anterior,
   pero infla cantidades, artículos y totales de la solicitud.
3. **Permitir varios intentos de compra por artículo.** Conserva cada operación,
   mantiene un solo artículo solicitado y permite sustituir al proveedor sin
   borrar evidencia.

Se adopta la tercera alternativa.

El intento comienza al generar una línea de orden para una cotización seleccionada
y puede incorporar después una compra pagada. Por ello, el contrato no puede
limitarse a cambiar `CompraRealizadaDepartamental`: tanto la línea de orden como
la compra deben quedar asociadas a un intento identificable y permitir varios
antecedentes para el mismo artículo.

El artículo podrá tener varios intentos históricos, pero como máximo uno vigente.
Los consumidores que hoy leen `item.linea_orden` o `item.compra_realizada` deberán
migrarse a una interfaz explícita como `item.intento_vigente` e
`item.intentos_compra`, evitando que una relación inversa ambigua elija un registro
arbitrario.

Cada intento conservará su cotización, línea de orden, proveedor y actor. Si se
registró el pago, conservará además importe, fecha, comprobante y avisos. La
cancelación y el reembolso serán datos propios del intento; no se sobrescribirá la
cotización ni se eliminará la orden o compra anterior.

## Estados

### Estado operativo del artículo

El artículo conserva los estados que describen el trabajo pendiente. Se agregan
o ajustan las transiciones necesarias para que pueda volver a cotización después
de un intento fallido.

| Situación | Estado visible del artículo | Responsable siguiente |
| --- | --- | --- |
| Compra vigente esperando entrega | Comprado, pendiente de entrega | Compras |
| Cantidad total registrada | Entregado por Compras, pendiente de confirmación | Área solicitante |
| Área confirma conformidad | Recibido conforme | Nadie |
| Proveedor cancela sin pago | Por cotizar nuevamente | Compras |
| Compra pagada y cancelada | Por cotizar nuevamente · reembolso pendiente | Compras |
| Reembolso confirmado, sin reemplazo comprado | Por cotizar nuevamente | Compras |
| Intento cancelado y reemplazo comprado | Comprado con proveedor sustituto | Compras |

El estado global de la solicitud seguirá derivándose de sus artículos. Solo se
considerarán terminales `Recibido conforme`, `Rechazado` y `Cancelado` cuando el
artículo ya no se necesite. Cancelar un proveedor no cancela automáticamente el
artículo solicitado.

### Estado del intento de compra

Cada intento tendrá un estado explícito:

- `VIGENTE`: compra activa pendiente de entrega;
- `CANCELADO_SIN_PAGO`: proveedor canceló o no entregó y no salió dinero;
- `REEMBOLSO_SOLICITADO`: hubo pago y el dinero aún no ha regresado;
- `REEMBOLSADO`: el reembolso fue confirmado;
- `ENTREGADO`: el intento originó la entrega completa del artículo.

No se podrá volver de un estado terminal a `VIGENTE`. Una corrección de datos no
cambiará el estado financiero ni operativo del intento.

## Flujos

### 1. Registrar entrega

1. Compras registra una cantidad mayor que cero.
2. Las recepciones se acumulan contra la cantidad ordenada.
3. Si el acumulado es menor, el artículo queda `Recibido parcialmente` y continúa
   con Compras.
4. Si el acumulado cubre la cantidad, el intento queda `ENTREGADO` y el artículo
   muestra `Entregado por Compras, pendiente de confirmación del área`.
5. La pantalla identifica al área responsable y ofrece la acción `Recibido
   conforme` únicamente a una persona autorizada para esa área.
6. Al confirmar, el artículo queda `Recibido conforme`. La solicitud pasa a
   `Completada` solo cuando todos sus artículos están en estado terminal.

Registrar la cantidad completa no cerrará automáticamente el artículo. Esta
doble validación fue aprobada para evitar que Compras confirme por el usuario que
realmente debía recibir.

### 2. Proveedor canceló o no entregó sin pago

1. Compras abre `Proveedor canceló / no entregó` desde el intento vigente.
2. Selecciona `Proveedor canceló`, `Proveedor no entregó` u `Otro`, y escribe un
   motivo obligatorio.
3. Declara que no hubo pago.
4. El sistema marca el intento `CANCELADO_SIN_PAGO`, desactiva su compromiso y
   registra la fecha de liberación.
5. La orden y la compra permanecen visibles como antecedente cancelado.
6. El artículo vuelve a cotización y permite seleccionar otro proveedor.

### 3. Proveedor canceló o no entregó después del pago

1. Compras realiza la misma acción y declara que sí hubo pago.
2. Registra fecha de solicitud, importe solicitado y evidencia opcional. El
   importe debe ser mayor que cero y no superar el importe pagado menos los
   reembolsos ya confirmados.
3. El intento pasa a `REEMBOLSO_SOLICITADO`.
4. El pago permanece visible y no se libera como si el dinero ya hubiera vuelto.
5. El artículo vuelve a cotización. Una compra sustituta se evaluará contra la
   disponibilidad real, incluyendo el importe todavía pendiente de reembolso.
6. La tarjeta del artículo muestra ambos hechos: reemplazo operativo y reembolso
   financiero pendiente.

### 4. Confirmar reembolso

1. Compras abre `Confirmar reembolso recibido`.
2. Captura fecha, importe recibido, referencia y comprobante opcional.
3. El sistema valida que el importe acumulado no exceda lo solicitado.
4. Si se recibió el importe total solicitado, el intento pasa a `REEMBOLSADO` y
   su compromiso deja de contar como vigente.
5. Si el reembolso es parcial, el intento permanece `REEMBOLSO_SOLICITADO` y
   muestra saldo pendiente.
6. La confirmación no borra ni reduce silenciosamente el importe original de la
   compra; el reembolso se presenta como movimiento separado.

### 5. Cancelar definitivamente el artículo

Si el área ya no necesita el artículo, Compras podrá marcarlo `Cancelado` solo
cuando no haya una entrega positiva y no exista un saldo de reembolso pendiente.
La acción exige motivo y confirmación. Este cierre es distinto de cancelar a un
proveedor para buscar reemplazo.

## Modelo de datos propuesto

La implementación podrá ajustar nombres concretos a las convenciones del módulo,
pero debe preservar estos contratos:

- Entidad o contrato equivalente de intento vinculado al artículo y a la
  cotización seleccionada.
- `LineaOrdenCompraDepartamental` y `CompraRealizadaDepartamental`: vínculos
  protegidos al intento; la compra pagada es opcional hasta que se registre.
- Accesos explícitos al intento vigente y al historial completo; no conservar una
  relación inversa singular que oculte múltiples intentos.
- Estado, versión y fechas de cancelación del intento.
- Motivo normalizado de cancelación y comentario obligatorio.
- Registro auditable de solicitudes y recepciones de reembolso; los reembolsos
  parciales no deben sobrescribirse entre sí.
- Restricción de un solo intento `VIGENTE` por artículo.
- Recepciones vinculadas inequívocamente al intento u orden que las originó.
- Eventos de dominio para cancelación, solicitud de reembolso, reembolso recibido
  y sustitución de proveedor.

La migración de datos clasificará las compras existentes como `VIGENTE`, salvo
aquellas cuyo artículo ya esté `RECIBIDO_CONFORME`, que quedarán `ENTREGADO`.
No enviará avisos, no alterará importes y no liberará compromisos históricos.

## Presupuesto y totales

Los importes se mostrarán por naturaleza y no se sumarán entre sí:

- **Comprometido vigente:** intentos vigentes formalizados.
- **Pagado pendiente de reembolso:** compras canceladas cuyo reembolso no se ha
  confirmado por completo.
- **Reembolsado:** importe confirmado de regreso, presentado como información
  histórica y no como una compra negativa.
- **Gastado:** seguirá perteneciendo a la fuente financiera existente; esta
  funcionalidad no inventará un gasto ni lo pondrá en cero por una cancelación.

El resumen y la exportación deberán excluir intentos cancelados sin pago de los
compromisos vigentes y mantener separados los reembolsos pendientes.

## Interfaz

En la tarjeta de cada artículo se mostrará una línea de tiempo de intentos con:

- proveedor;
- importe y fecha;
- orden o referencia;
- estado del intento;
- entrega registrada;
- reembolso solicitado, recibido y saldo;
- actor, fecha y motivo de cada cambio.

Las acciones estarán agrupadas por intención:

- `Registrar entrega` para cantidades positivas;
- `Proveedor canceló / no entregó` para el intento fallido;
- `Confirmar reembolso recibido` cuando exista saldo pendiente;
- `Recibido conforme` para el área solicitante;
- `Cancelar definitivamente el artículo` solo cuando aplique.

Las acciones financieras o destructivas usarán el modal accesible compartido.
Todas usarán `data-async-action`, bloquearán únicamente el botón presionado,
mostrarán el toast global y conservarán el fragmento estable `#item-<id>`.
Ante error, los campos y el contexto se conservarán para reintentar.

## Permisos y validaciones

- Solo usuarios con gestión de Compras podrán cancelar un intento, solicitar o
  confirmar reembolsos y cancelar definitivamente un artículo.
- El área solicitante únicamente podrá confirmar conformidad o registrar una
  diferencia en la entrega.
- No se podrá cancelar un intento con recepción positiva; los casos parciales
  quedan fuera de este alcance.
- No se podrá registrar una recepción sobre un intento cancelado.
- No se podrá confirmar un reembolso mayor que el saldo solicitado.
- Toda acción exigirá la versión vigente del registro para evitar que dos usuarios
  pisen cambios concurrentes.
- La operación completa será transaccional e idempotente ante doble envío.

## Avisos

Los avisos automáticos existentes de compra realizada no se eliminarán. Para las
nuevas transiciones, el primer alcance garantiza avisos dentro del ERP mediante
estado, responsable y evento auditable. Correo o WhatsApp adicionales requieren
una decisión separada sobre destinatarios y contenido; no se enviarán mensajes
reales ni retroactivos como efecto secundario de la migración.

## Pruebas y aceptación

La implementación se considerará correcta cuando demuestre, como mínimo:

1. La cantidad de recepción `0` sigue siendo inválida como entrega y la interfaz
   ofrece en su lugar la acción de cancelación.
2. Una cancelación sin pago conserva la orden, libera el compromiso y permite un
   nuevo proveedor.
3. Una cancelación con pago conserva el importe comprometido y muestra el saldo
   de reembolso pendiente.
4. Un reembolso parcial no cierra el seguimiento; el total sí lo marca
   reembolsado y actualiza los totales correspondientes.
5. Un segundo intento puede registrarse sin alterar el primero.
6. No pueden coexistir dos intentos vigentes para el mismo artículo.
7. Un intento cancelado no admite recepciones.
8. La entrega total queda pendiente del área y señala claramente quién debe
   confirmar.
9. La confirmación del área cambia a `Recibido conforme` y completa la solicitud
   cuando todos los artículos son terminales.
10. Los permisos impiden que el solicitante ejecute acciones exclusivas de
    Compras y que Compras confirme por el área.
11. Doble envío, versión antigua y errores conservan datos y no duplican eventos,
    reembolsos ni compromisos.
12. El resumen HTML y XLSX separan comprometido, reembolso pendiente y reembolsado.
13. La migración clasifica datos existentes sin alterar importes ni generar avisos.
14. La vista funciona con teclado, muestra foco correcto, toast accesible y vuelve
    al mismo artículo.

Antes del cierre deberán ejecutarse las pruebas del módulo `compras`, los checks de
Django y de migraciones, la revisión de consumidores de la relación de compra, y
una validación real en navegador. Al tratarse de cambios de modelo, estados y
presupuesto, el despliegue requerirá migración verificada y comprobación en
producción de un caso controlado, sin alterar compras reales fuera del alcance
autorizado.

## Riesgos y mitigaciones

- **Doble conteo de dinero:** separar compra, compromiso y reembolso; no convertir
  un reembolso en compra negativa.
- **Pérdida de historia:** relaciones protegidas, eventos y registros append-only
  para reembolsos.
- **Consumidores rotos por la relación uno-a-uno:** inventariar y migrar todos los
  accesos a `compra_realizada` antes de cambiar el modelo.
- **Dos compras activas para el mismo artículo:** restricción de base y bloqueo
  transaccional.
- **Cierre falso de entrega:** conservar confirmación independiente del área.
- **Liberación anticipada de presupuesto:** no liberar importes pagados hasta
  confirmar el reembolso completo.
- **Estados históricos incorrectos:** migración conservadora, sin inferir
  cancelaciones ni reembolsos que no estén documentados.
