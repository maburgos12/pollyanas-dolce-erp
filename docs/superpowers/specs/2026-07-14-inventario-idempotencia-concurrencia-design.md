# Idempotencia concurrente de stock en compras e inventario

## Objetivo

Evitar que una recepción de compra o un ajuste de inventario modifique dos veces la existencia cuando la misma acción se reintenta, se envía con doble clic o llega simultáneamente desde dos solicitudes HTTP.

El usuario debe recibir respuesta visible durante el procesamiento y una explicación clara cuando la operación ya había sido aplicada.

## Alcance

Incluye exclusivamente:

- cierre de recepciones de compras que generan una entrada de inventario;
- aprobación o aplicación de ajustes de inventario;
- movimientos y existencias de insumos afectados por esos flujos;
- botones que disparan Cerrar, Aprobar o Aplicar;
- pruebas secuenciales, concurrentes y de interfaz.

No incluye solicitudes de compra, proveedores, costos, órdenes no relacionadas con el cierre, otros movimientos de almacén ni correcciones de datos productivos.

## Diseño de backend

Cada aplicación de stock se ejecutará dentro de `transaction.atomic()`.

### Recepciones de compras

1. Volver a leer la recepción con `select_for_update()`.
2. Comprobar bajo el bloqueo si ya existe el movimiento identificado por `recepcion:<id>:entrada`.
3. Obtener la existencia canónica del insumo y bloquearla con `select_for_update()`.
4. Actualizar la existencia y crear el movimiento dentro de la misma transacción.
5. Si el movimiento ya existe, regresar un resultado idempotente sin modificar stock.

El `source_hash` único seguirá siendo la identidad durable del movimiento. La transacción garantiza que una colisión al crear el movimiento no deje previamente confirmado un incremento de stock.

### Ajustes de inventario

1. Volver a leer el ajuste con `select_for_update()`.
2. Si su estado ya es `APLICADO`, regresar un resultado idempotente sin modificar stock.
3. Obtener y bloquear la existencia canónica.
4. Actualizar existencia, trazabilidad, movimiento y estado del ajuste dentro de la misma transacción.

Los bloqueos se adquirirán siempre en el orden entidad operativa y después existencia para reducir riesgo de deadlocks.

## Respuestas y experiencia de usuario

Los formularios afectados usarán el contrato compartido `data-async-action` cuando el endpoint admita la respuesta progresiva JSON/HTML sin duplicar lógica de negocio.

Al enviar una acción:

- se deshabilita únicamente el botón presionado;
- su texto cambia inmediatamente a `Procesando…`;
- se ignoran envíos repetidos del mismo formulario mientras la solicitud está activa;
- el éxito, error o resultado idempotente se muestra mediante el toast global;
- ante error, se conservan los datos, se restaura el texto original y se habilita el botón para reintentar;
- el flujo tradicional conserva la consulta o fragmento estable para regresar al mismo contexto.

Cuando una segunda solicitud encuentre la operación terminada, el mensaje será informativo: la recepción o ajuste ya estaba aplicado y el stock no cambió nuevamente.

## Manejo de errores

- Los errores de validación ocurren antes de modificar stock.
- Cualquier excepción posterior revierte existencia, movimiento y estado como una sola unidad.
- Una solicitud que espera el bloqueo vuelve a evaluar el estado real al adquirirlo.
- No se capturarán silenciosamente `IntegrityError` o errores de bloqueo; se transformarán únicamente cuando exista un resultado idempotente comprobable.
- Los errores inesperados conservarán trazabilidad en logs y devolverán una respuesta recuperable para la interfaz.

## Estrategia de pruebas

### Pruebas de dominio y vistas

- aplicar dos veces secuencialmente la misma recepción;
- aplicar dos veces secuencialmente el mismo ajuste;
- verificar un solo movimiento y una sola modificación de existencia;
- verificar el resultado y mensaje de “ya aplicado”.

### Pruebas PostgreSQL de concurrencia

Se usarán `TransactionTestCase`, dos conexiones independientes y sincronización con barrera para iniciar dos transacciones sobre el mismo registro.

Para recepción y ajuste se comprobará:

- ambas solicitudes terminan sin excepción no controlada;
- no hay deadlock;
- una sola transacción aplica el stock;
- la otra obtiene resultado idempotente;
- existe un solo movimiento;
- la existencia final refleja una sola aplicación;
- el estado final de la recepción, orden o ajuste es coherente.

### Pruebas de interfaz

- el formulario expone el contrato de acción asíncrona;
- el botón cambia a `Procesando…` y queda deshabilitado;
- un segundo clic no genera otro envío;
- un error restaura el botón;
- el toast distingue éxito, información y error.

## Validación y despliegue

Antes del PR se ejecutarán:

- pruebas enfocadas de compras e inventario;
- pruebas concurrentes en PostgreSQL;
- suites completas de ambos módulos;
- `python manage.py migrate --check`;
- `python manage.py check`;
- validación en navegador de los botones y mensajes.

Después del merge se desplegará mediante `scripts/deploy_web_safe.sh`. La validación productiva usará registros controlados o una operación segura acordada; no se duplicarán ni alterarán movimientos reales únicamente para probar concurrencia.

## Criterios de aceptación

- Ningún reintento secuencial o simultáneo duplica stock.
- Ningún reintento crea movimientos duplicados.
- Una segunda solicitud recibe un resultado informativo y controlado.
- El botón muestra `Procesando…` y evita doble envío en navegador.
- Los usuarios pueden reintentar después de un error.
- Compras e inventario conservan sus permisos, validaciones y trazabilidad actuales.
