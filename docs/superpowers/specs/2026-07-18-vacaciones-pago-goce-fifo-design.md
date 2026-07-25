# Vacaciones: pago, goce y aplicación FIFO por periodo

**Fecha:** 2026-07-18  
**Estado:** Diseño aprobado por Mauricio; pendiente de revisión escrita y plan de implementación  
**Módulo:** `rrhh`  
**Tipo de cambio:** Contrato de datos compartido, migraciones nuevas, servicios, nómina, asistencia y UI

## 1. Problema

El ERP mezcla parcialmente tres hechos distintos:

1. El derecho vacacional generado por antigüedad.
2. El pago de vacaciones y prima vacacional reflejado en nómina.
3. El descanso efectivamente disfrutado mediante una solicitud aprobada.

`MovimientoVacaciones` registra actualmente movimientos por `periodo_anio`, pero una solicitud siempre reserva y consume contra el año de su fecha de inicio. La pantalla agrega por separado ajustes históricos cuya descripción contiene `pendiente de goce`, pero el servicio que valida y consume saldo no utiliza esos días anteriores.

En el caso observado, una empleada tenía siete días pendientes de goce de 2025 y una solicitud aprobada de cinco días en 2026. El ERP consumió los cinco días en 2026 y dejó intactos los siete de 2025. El total visible sugería que ambos saldos existían, pero no había una aplicación contable entre ellos.

La Ley Federal del Trabajo distingue el descanso del pago: las vacaciones no se compensan con remuneración y deben concederse dentro de los seis meses siguientes al aniversario. El ERP debe conservar esa separación y servir como control administrativo, sin presentar el pago como prueba de goce.

## 2. Objetivos

- Llevar bolsas vacacionales por aniversario o periodo de derecho.
- Separar explícitamente días generados, pagados, reservados y gozados.
- Aplicar solicitudes primero al saldo de goce más antiguo disponible.
- Permitir que una solicitud consuma varias bolsas y conservar el desglose.
- Tomar la nómina importada como evidencia económica sin duplicar capturas en Capital Humano.
- Distinguir pago ordinario, prima vacacional y pagos por finiquito o liquidación.
- Preservar folios, solicitudes y movimientos históricos.
- Evitar doble consumo, doble conciliación y exposición indebida de importes de nómina.
- Mantener la integración existente con asistencia para justificar ausencias aprobadas.

## 3. No objetivos

- Sustituir CONTPAQi ni convertir el módulo de vacaciones en un sistema de nómina.
- Inferir días únicamente a partir de importes monetarios.
- Borrar o reescribir movimientos históricos.
- Marcar vacaciones como gozadas por el solo hecho de haber sido pagadas.
- Marcar vacaciones como pagadas por aprobar una solicitud de goce.
- Reutilizar `PermisoSalida` para representar vacaciones.
- Automatizar desde la primera entrega todos los conceptos extraordinarios de nómina.

## 4. Decisiones aprobadas

### 4.1 Dos controles independientes

Cada bolsa tendrá dos dimensiones:

- **Económica:** días pagados, pago de prima y saldos pendientes de conciliación.
- **Descanso:** días gozados, reservados y disponibles para goce.

Una dimensión no cerrará automáticamente la otra.

### 4.2 Aplicación FIFO obligatoria

Las solicitudes reservarán primero la bolsa pendiente de goce más antigua. Si una bolsa no alcanza, la solicitud continuará con la siguiente.

Ejemplo: quedan siete días de 2025 y hay dieciocho de 2026. Una solicitud de diez días reservará siete de 2025 y tres de 2026.

Capital Humano podrá cambiar la distribución únicamente antes de la aprobación final, con motivo obligatorio y registro de auditoría. El empleado y la jefatura podrán consultar el desglose, pero no modificarlo.

### 4.3 La nómina es la evidencia económica

Los conceptos importados en `NominaConceptoLinea` serán la fuente del pago. Capital Humano no volverá a capturar importes. Cuando el periodo de derecho no pueda determinarse con seguridad, el concepto quedará pendiente de conciliación.

### 4.4 Historia inmutable

Las correcciones se representarán mediante aplicaciones y movimientos compensatorios. No se editarán ni eliminarán hechos históricos para ajustar un saldo.

## 5. Modelo conceptual

Los nombres finales podrán ajustarse a las convenciones del módulo durante el plan, pero las responsabilidades no deberán mezclarse.

### 5.1 `PeriodoVacacional`

Representa una bolsa de derecho por aniversario.

Campos conceptuales:

- empleado;
- aniversario que origina el derecho;
- fecha inicial y fecha límite de goce;
- antigüedad aplicable;
- días generados;
- origen: cálculo LFT, saldo inicial o ajuste autorizado;
- estado operativo: en plazo, por vencer, vencido o cerrado;
- metadatos de creación y auditoría.

Los saldos derivados no serán valores manuales independientes. Se calcularán desde aplicaciones vigentes y movimientos auditables.

### 5.2 `AplicacionGoceVacaciones`

Relaciona una solicitud con una bolsa y registra cuántos días se reservaron o consumieron.

Debe permitir que una solicitud tenga varias aplicaciones. Su identidad y restricciones impedirán duplicar una misma aplicación lógica.

Estados conceptuales:

- reservada;
- consumida;
- liberada;
- revertida.

Conservará actor, fechas y motivo de cualquier excepción a FIFO.

### 5.3 `PagoVacaciones`

Representa la evidencia económica detectada en nómina.

Debe vincularse con `NominaConceptoLinea` y conservar:

- empleado;
- periodo de nómina;
- tipo: vacaciones, prima vacacional o terminación;
- cantidad o valor informado;
- importe;
- estado de conciliación;
- referencia estable a la importación y concepto de origen.

No se calcularán días desde el importe cuando el concepto no proporcione una cantidad confiable.

### 5.4 `AplicacionPagoVacaciones`

Distribuye un pago entre una o varias bolsas. Un pago de doce días puede aplicarse a siete días de 2025 y cinco de 2026.

La prima vacacional se vinculará con el mismo evento económico cuando sea posible, pero conservará estado independiente para detectar pago parcial o ausencia de prima.

### 5.5 `MovimientoVacaciones`

Se conserva como libro de auditoría y compatibilidad. La implementación definirá movimientos explícitos o descripciones estructuradas para:

- generación;
- reserva;
- liberación;
- goce;
- pago;
- prima;
- reversión;
- ajuste histórico.

Las migraciones existentes no se modificarán.

## 6. Reglas de saldo

Por bolsa:

- `pendiente_goce = generado + ajustes_goce - gozado - reservado_neto`
- `pendiente_pago = generado + ajustes_pago - pagado`

La prima tendrá su propio estado económico y no alterará el número de días.

El saldo consolidado disponible para una solicitud será la suma de `pendiente_goce` positivo de todas las bolsas elegibles, ordenadas de la más antigua a la más reciente.

Una bolsa puede quedar económicamente pagada y aún pendiente de goce. También puede existir goce aprobado pendiente de aparecer en nómina. Ambas situaciones deben ser visibles y no tratarse como errores destructivos.

## 7. Flujo de solicitud

### 7.1 Creación

1. Validar empleado activo, fechas laborables, traslapes e incapacidades con las reglas actuales.
2. Abrir una transacción y bloquear las bolsas elegibles del empleado.
3. Recalcular el saldo consolidado dentro del bloqueo.
4. Asignar los días por FIFO.
5. Crear la solicitud, sus aplicaciones y los movimientos de reserva.
6. Mostrar el desglose antes y después del envío.

Mensaje de ejemplo:

> Se solicitan 10 días: 7 del periodo 2025 y 3 del periodo 2026.

### 7.2 Preautorización

La jefatura conserva el flujo actual. Puede consultar el desglose, pero no cambiar la imputación ni ver importes de nómina.

### 7.3 Aprobación de Capital Humano

1. Bloquear la solicitud y sus aplicaciones.
2. Confirmar que la solicitud siga en un estado resoluble.
3. Confirmar la vigencia de las reservas.
4. Cambiar cada aplicación de reservada a consumida.
5. Generar los movimientos de goce ligados a la bolsa y a la solicitud.
6. Mantener la integración de asistencia basada en las fechas aprobadas.

### 7.4 Rechazo y cancelación

- Un rechazo libera exactamente las aplicaciones reservadas.
- Una cancelación previa a las fechas revierte mediante movimientos compensatorios.
- Si las fechas ya ocurrieron, Capital Humano deberá confirmar si el descanso sucedió realmente.
- No se eliminará directamente una solicitud aprobada que ya afectó saldos o asistencia.

## 8. Conciliación con nómina

### 8.1 Catálogo de equivalencias

Los códigos y nombres de conceptos podrán variar. Se requiere una configuración explícita que clasifique cada concepto como:

- pago de vacaciones;
- prima vacacional;
- pago por terminación;
- no aplicable.

La equivalencia también indicará si `valor` representa días de forma confiable. Un concepto sin equivalencia quedará pendiente de configuración o conciliación.

### 8.2 Aplicación ordinaria

Cuando un concepto proporcione días confiables:

1. Crear o recuperar idempotentemente `PagoVacaciones`.
2. Buscar bolsas con saldo económico pendiente, de la más antigua a la más reciente.
3. Proponer la distribución FIFO.
4. Aplicar automáticamente solo cuando las reglas sean inequívocas.
5. Enviar casos ambiguos a la bandeja de conciliación.

La aplicación económica no modificará el goce.

### 8.3 Prima vacacional

La UI distinguirá:

- vacaciones pagadas;
- prima pagada;
- pago parcial;
- pendiente de conciliación.

La ausencia de un concepto no será inventada ni cubierta con un ajuste automático.

### 8.4 Finiquito y liquidación

Un pago clasificado como terminación:

- podrá liquidar económicamente bolsas aplicables;
- no creará días disponibles para descanso futuro;
- no marcará días como gozados;
- conservará nómina, fecha, concepto, valor e importe.

### 8.5 Idempotencia y reemplazo

Una clave estable basada en importación, empleado y concepto impedirá duplicados. Si una lista de raya importada se reemplaza, sus conciliaciones deberán revertirse mediante eventos compensatorios o quedar invalidadas de manera auditable antes de aplicar la nueva versión.

## 9. Pantallas

### 9.1 Resumen por empleado

La ficha mostrará:

- derecho total generado;
- pagado y pendiente de pago;
- gozado y pendiente de goce;
- reservado en solicitudes abiertas;
- periodo más antiguo pendiente;
- próxima fecha límite;
- alertas de inconsistencia.

Ejemplo:

| Periodo | Estado económico | Estado de descanso |
| --- | --- | --- |
| 2025 | Pagado 12 de 12 | Gozado 5; pendiente 7 |
| 2026 | Pagado 12 de 18 | Gozado 5; pendiente 13 |

### 9.2 Detalle por bolsa

Cada periodo desplegará:

- aniversario y fecha límite;
- días generados;
- pagos de nómina y prima relacionados;
- solicitudes y aplicaciones de goce;
- ajustes y conciliaciones;
- actor, fecha y motivo de cada movimiento.

### 9.3 Solicitud

Antes de enviar se mostrará la propuesta FIFO. Después de crearla, el desglose quedará visible en el detalle y en las etapas de autorización.

### 9.4 Bandeja económica

Capital Humano tendrá una bandeja de pagos por conciliar con filtros por empleado, quincena, concepto y causa de bloqueo. La acción utilizará el contrato compartido `data-async-action`, toast global, bloqueo exclusivo del botón, conservación de contexto y reintento sin perder inputs.

## 10. Permisos y privacidad

- **Empleado:** consulta sus días, estados económicos sin detalle salarial y solicitudes; crea solicitudes propias.
- **Jefe directo:** gestiona solicitudes de su equipo y consulta el desglose de días; no ve importes.
- **Capital Humano:** aprueba, concilia pagos, modifica una distribución antes de aprobar y registra ajustes justificados.
- **Superusuario/Dirección General:** conserva acceso total conforme a las reglas actuales.
- **Otros usuarios:** sin acceso.

La autorización para consultar días y la autorización para consultar importes serán controles separados.

## 11. Migración histórica

La transición será conservadora y por etapas.

1. Construir bolsas históricas en modo sombra.
2. Convertir movimientos identificados como `pendiente de goce` en saldos iniciales de la bolsa correspondiente.
3. Conservar todos los movimientos, folios y solicitudes originales.
4. Proponer aplicaciones FIFO para solicitudes antiguas cuando sean inequívocas.
5. Enviar casos ambiguos a una bandeja de conciliación.
6. Generar un reporte por empleado con saldo anterior, saldo propuesto, diferencias y motivos.
7. Detener la activación si cambia el total consolidado sin una explicación explícita.

Caso de aceptación de Carolina:

- bolsa 2025 con siete días iniciales pendientes de goce;
- solicitud `VAC-2607-AJUT` por cinco días aplicada a 2025;
- saldo de goce 2025 igual a dos;
- bolsa 2026 sin consumo por esa solicitud;
- historial original visible.

## 12. Despliegue gradual

### Entrega 1: goce y FIFO

- bolsas por periodo;
- aplicaciones de goce;
- asignación FIFO transaccional;
- migración histórica en modo sombra;
- reporte comparativo;
- resumen y detalle de saldos;
- compatibilidad con solicitudes, autorizaciones y asistencia.

El cálculo nuevo se comparará contra el existente antes de convertirse en fuente oficial.

### Entrega 2: conciliación económica

- clasificación de conceptos de nómina;
- pagos y prima vacacional;
- aplicaciones FIFO económicas;
- bandeja de conciliación;
- tratamiento de finiquito y liquidación;
- idempotencia de reimportaciones y reemplazos.

No se acoplará la entrega urgente de goce a todas las variaciones de nómina desde el primer despliegue.

## 13. Errores y concurrencia

- Las bolsas se bloquearán al reservar y aprobar para evitar doble consumo.
- Una solicitud con saldo insuficiente no se creará parcialmente.
- Si una reserva perdió vigencia, la aprobación fallará sin dejar movimientos incompletos.
- Una conciliación ambigua no cerrará bolsas automáticamente.
- Los errores conservarán datos, contexto y posibilidad de reintento.
- Las acciones serán atómicas y auditables.

## 14. Pruebas de aceptación

### Saldos y FIFO

- Cinco días consumen cinco del periodo más antiguo.
- Diez días consumen siete de 2025 y tres de 2026.
- Dos solicitudes concurrentes no consumen los mismos días.
- Un rechazo libera exactamente lo reservado.
- La aprobación conserva el desglose creado.
- Una excepción FIFO exige permiso y motivo.

### Independencia económica

- Un pago no marca goce.
- Un goce no marca pago.
- La prima no altera el número de días.
- Una reimportación no duplica pagos.
- Un finiquito no genera descanso futuro.
- Un reemplazo de nómina no deja aplicaciones huérfanas o duplicadas.

### Permisos

- El empleado solo consulta sus datos.
- La jefatura ve días, no importes.
- Capital Humano puede conciliar y justificar excepciones.
- El superusuario conserva el comportamiento autorizado actual.

### Integraciones

- Las vacaciones aprobadas justifican asistencia en las fechas correctas.
- Las API existentes conservan inicialmente sus campos.
- Los campos nuevos no rompen consumidores actuales.
- Si el flujo está bajo un service worker, el cambio visible incluye el bump de caché correspondiente.

### Migración

- El total consolidado por empleado se conserva o presenta una diferencia explicada.
- Los casos ambiguos no se asignan automáticamente.
- El caso de Carolina termina con dos días pendientes de 2025.

## 15. Riesgos y mitigaciones

- **Conceptos variables de CONTPAQi:** catálogo explícito y conciliación manual segura.
- **Historia ambigua:** no inferir; mantenerla pendiente de revisión.
- **Doble consumo:** transacciones y bloqueo de bolsas.
- **Confusión pago/goce:** modelos, estados y UI independientes.
- **Exposición salarial:** separar permisos de días e importes.
- **Regresión de asistencia:** pruebas de integración con solicitudes aprobadas.
- **Cambio amplio de contrato:** mantener compatibilidad, modo sombra y despliegue gradual.
- **Caché PWA:** verificar el service worker y actualizar `CACHE_NAME` si corresponde.

## 16. Validación antes de producción

La implementación futura deberá realizarse en un worktree limpio e incluir:

- `python manage.py check` sin errores;
- `python manage.py migrate --check` sin pendientes no explicados;
- estado de migraciones de `rrhh` verificado;
- pruebas unitarias y de integración de vacaciones, prenómina y asistencia;
- reporte comparativo histórico revisado por Capital Humano;
- validación en navegador real, incluyendo consola y solicitudes de red;
- deploy manual mediante `scripts/deploy_web_safe.sh`, sin `git pull` previo;
- comprobación visible y con datos reales en producción antes de cerrar.

## 17. Criterio de terminación

La funcionalidad estará terminada cuando:

- una solicitud use automáticamente el saldo de goce más antiguo;
- el desglose por bolsa sea visible y auditable;
- pago y goce sean independientes;
- el historial existente permanezca íntegro;
- el caso de Carolina muestre dos días pendientes de 2025;
- asistencia, permisos y nómina mantengan sus contratos;
- la migración y el flujo real estén validados en producción.
