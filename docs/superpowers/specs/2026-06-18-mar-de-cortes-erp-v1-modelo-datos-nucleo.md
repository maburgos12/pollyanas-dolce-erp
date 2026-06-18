# Mar de Cortés ERP V1 — Modelo de Datos Núcleo

**Estado:** Spec de modelo (previo al plan de implementación)
**Base:** `2026-06-18-mar-de-cortes-erp-v1-design.md`
**Propósito:** Fijar las entidades centrales y sus reglas duras *antes* de construir fases, para que crédito, conciliación CONTPAQi, comodato y producción se monten encima sin retrabajo ni migraciones de rescate.

> Regla de oro de este documento: si una decisión no está aquí, no se improvisa en la implementación. Cada entidad mapea a un modelo de Django. Esto NO es código de migración; es el contrato que las migraciones deben respetar.

> Referencia bidireccional: este modelo es el contrato de datos del diseño V1
> `2026-06-18-mar-de-cortes-erp-v1-design.md`. Las referencias `§` son secciones
> internas de este modelo; su correspondencia con el diseño V1 es por nombre:
> `Catalogo` -> `Alcance V1` / `Clientes, Precios y Credito`;
> `Pedido`, `Ruta y Carga`, `Entrega`, `Nota de Venta`, `Venta en Planta`,
> `Venta Movil en Ruta`, `Cuentas por Cobrar y Pagos`,
> `Facturacion CONTPAQi Conciliada`, `Inventario`, `Cierre de Ruta`, `Mermas`,
> `Reposiciones`, `Mobiliario Prestado`, `Produccion Simple`,
> `Migracion Desde el ERP Dolce` y `Validacion de Implementacion`.

---

## 0. Convenciones generales

Aplican a todas las tablas salvo que se indique lo contrario.

- **PK:** `id` autoincremental (`BigAutoField`).
- **Auditoría base:** todas las tablas operativas llevan `creado_en`, `actualizado_en`, `creado_por` (FK a usuario, nullable para procesos del sistema).
- **Borrado:** no se borra físicamente nada operativo. Se usa `activo` (bool) o un campo `estado` con valor de baja. El borrado físico queda prohibido en V1 (coherente con el principio "si algo no existe físicamente, no debe existir como inventario falso" — y su inverso: lo que existió no se desaparece).
- **Dinero:** `DecimalField(max_digits=14, decimal_places=2)` para totales/importes. Moneda única MXN en V1.
- **Precio unitario:** `DecimalField(max_digits=14, decimal_places=4)`. Cuatro decimales porque un costal o garrafón puede tener precio unitario con fracción y no quiero que el redondeo nazca en el catálogo.
- **Cantidad:** `DecimalField(max_digits=14, decimal_places=3)` para permitir kg/fracciones en producción; las presentaciones de venta normalmente serán enteras pero el tipo no lo limita.
- **Tasa IVA:** se guarda como campo en catálogo (ver §1). Nunca se asume.

---

## 1. Catálogo: Producto, Presentación y Precios

### 1.1 `Producto`
Concepto base (Hielo, Agua).

| Campo | Tipo | Notas |
|---|---|---|
| nombre | Char | "Hielo", "Agua purificada" |
| tipo | Choice | `hielo` / `agua` (alimenta producción) |
| activo | Bool | |

### 1.2 `Presentacion`
**Esto es lo que se compra, se produce, se mueve en inventario y se vende.** El inventario y las notas SIEMPRE referencian `Presentacion`, nunca `Producto` suelto.

| Campo | Tipo | Notas |
|---|---|---|
| producto | FK → Producto | |
| nombre | Char | "Bolsa 5 kg", "Costal 15 kg", "Garrafón 19 L" |
| unidad_base | Char | kg / L / pza |
| contenido | Decimal | 5, 15, 19… (para reportes y futuro costeo) |
| **tasa_iva** | Decimal | **0.16 / 0.00 — por presentación.** Confirmar con contador cuáles van a tasa 0%. Vive aquí, no se asume. |
| sku | Char (unique) | |
| activo | Bool | |

> **Arreglo #1 (parte A):** el IVA vive en el catálogo, por presentación. Esto es lo que hace que la conciliación de §10 pueda cuadrar subtotal + IVA en vez de pelearse con totales.

### 1.3 `ListaPrecios` y `PrecioListaItem`
Precio "general" y listas por tipo de cliente.

`ListaPrecios`: nombre, es_general (bool), activo.
`PrecioListaItem`: lista FK, presentacion FK, precio_unitario, vigente_desde.

### 1.4 `PrecioCliente`
Precio fijo presentación–cliente (override más específico).

cliente FK, presentacion FK, precio_unitario, vigente_desde, activo.

### 1.5 Precedencia de precios (regla dura)
Resolución de precio para una línea, **de más específico a más general**:

```
1. PrecioCliente (cliente + presentación)   → si existe y vigente
2. Lista de precios asignada al cliente
3. Lista general
```

> **Arreglo #3 (parte A):** la precedencia se escribe aquí y se implementa como UNA sola función `resolver_precio(cliente, presentacion, fecha)`. El vendedor nunca edita; cualquier precio especial pasa por permiso + bitácora (§12).

---

## 2. Clientes y Crédito

### 2.1 `Cliente`

| Campo | Tipo | Notas |
|---|---|---|
| nombre / razon_social | Char | |
| rfc | Char | nullable (público general puede no tener) |
| tipo | Choice | `contado` / `credito` / `ambos` |
| lista_precios | FK → ListaPrecios | nullable; si null usa general |
| dias_credito | Int | |
| limite_credito | Decimal | nullable (opcional según spec) |
| estado | Choice | `activo` / `bloqueado` / `revision` |
| reglas_facturacion | texto/JSON | datos mínimos para facturar |
| activo | Bool | |

La **validación de crédito en vivo** (cliente activo, crédito habilitado, saldo vencido, límite disponible, bloqueo, notas pendientes) es lógica de servicio que LEE este modelo + el saldo de §9. No es una tabla; es una función `validar_credito(cliente)` que se invoca solo con conexión. Se documenta como contrato, no como entidad.

---

## 3. Proveedores y Compras

### 3.1 `Proveedor`
nombre, rfc, contacto, activo.

### 3.2 `Compra` / `CompraDetalle`
`Compra`: proveedor FK, fecha, ubicacion_destino FK (§4), referencia, total, estado.
`CompraDetalle`: compra FK, presentacion FK, cantidad, costo_unitario.

**La compra confirmada genera movimientos de entrada de inventario (§4.2).** Una compra no toca existencias directamente; las toca a través del ledger.

---

## 4. Inventario (núcleo del sistema)

Patrón: **ledger inmutable + saldo materializado.** El movimiento es la verdad; la existencia es un cache para lectura rápida. Esto hace que el cuadre de ruta (§13) sea auditable.

### 4.1 `Ubicacion`
Una sola tabla para todas las ubicaciones de stock.

| Campo | Tipo | Notas |
|---|---|---|
| nombre | Char | "Planta", "Unidad GS-1" |
| tipo | Choice | `planta` / `unidad` |
| unidad_placa / codigo | Char | nullable; para tipo unidad |
| activo | Bool | |

> Cliente/equipo NO es ubicación de stock (coherente con el spec: solo contexto de merma/comodato). No se inventa stock en cliente.

### 4.2 `MovimientoInventario` (ledger — inmutable)

| Campo | Tipo | Notas |
|---|---|---|
| presentacion | FK | |
| ubicacion | FK | |
| cantidad | Decimal **con signo** | **positivo = entra, negativo = sale.** Saldo = suma de cantidades. |
| tipo | Choice | `compra` / `produccion` / `traspaso` / `venta` / `reposicion` / `merma` / `devolucion` / `ajuste` |
| costo_unitario | Decimal | nullable; para valorar merma/reposición |
| documento_tipo | Char | "nota", "merma", "reposicion", "compra", "produccion", "cierre_ruta"… |
| documento_id | BigInt | liga al documento origen |
| creado_en / creado_por | | |

Reglas:
- Un **traspaso** (planta → unidad) genera **DOS** movimientos: `-cantidad` en planta, `+cantidad` en unidad. Misma `documento_id`.
- Ningún movimiento se edita ni borra. Una corrección es un `ajuste` nuevo con autorización.
- El movimiento se crea SIEMPRE desde un servicio, nunca a mano desde admin.

### 4.3 `ExistenciaUbicacion` (saldo materializado)
presentacion FK, ubicacion FK, cantidad (saldo actual). **Unique(presentacion, ubicacion).**
Se actualiza dentro de la misma transacción que el movimiento. Debe poder reconstruirse 100% sumando el ledger (test de integridad).

---

## 5. Pedido

El pedido es intención, **no descuenta inventario ni genera cartera**.

### 5.1 `Pedido` / `PedidoDetalle`

`Pedido`: cliente FK, fecha_entrega, canal (`planta`/`ruta`/`administracion`), ruta_sugerida FK (nullable), observaciones, estado.
Estados: `pendiente` / `programado` / `entregado_parcial` / `entregado` / `reprogramado` / `cancelado`.

`PedidoDetalle`: pedido FK, presentacion FK, cantidad_solicitada, cantidad_entregada (default 0).

> **Aclaración menor (entrega parcial):** `cantidad_entregada < cantidad_solicitada` cierra el renglón en V1; el faltante NO se reprograma automático. Si el cliente lo quiere de nuevo, se crea pedido nuevo. (Decisión explícita para no construir un motor de backorders en V1; cámbiala aquí si el negocio lo pide.)

---

## 6. Ruta y Carga

### 6.1 `Ruta`
vendedor FK, ubicacion_unidad FK (§4, tipo unidad), fecha, estado.
Estados de ruta = estados de cierre (§13): `programada` / `cargada` / `en_ruta` / `regresada` / `cerrada` / `cerrada_con_diferencia`.

### 6.2 `RutaPedido`
Liga pedidos previos a la ruta (M2M con orden de visita). ruta FK, pedido FK, orden.

### 6.3 La carga = traspaso
La carga NO es una tabla nueva de stock; **es un conjunto de movimientos `traspaso`** (planta → unidad de la ruta). La "cantidad por cliente" es guía operativa (vive en el pedido); la **carga total es la que limita** lo vendible y queda como saldo en la unidad.

---

## 7. Nota de Venta (el hecho económico)

La nota es la evidencia de lo realmente entregado. Descuenta inventario y, si es crédito, genera cartera.

### 7.1 `NotaVenta`

| Campo | Tipo | Notas |
|---|---|---|
| cliente | FK | (público general = cliente genérico) |
| ruta | FK | nullable (venta en planta no trae ruta) |
| ubicacion_origen | FK | planta o unidad — de dónde baja el inventario |
| origen | Choice | `planta` / `ruta_online` / `ruta_offline` |
| condicion | Choice | `contado` / `credito` |
| subtotal | Decimal | |
| iva | Decimal | |
| total | Decimal | |
| estado_pago | Choice | `pagada` / `pendiente` |
| estado_factura | Choice | `pendiente` / `facturada` / `no_facturable` / `cancelada` |
| **folio_offline** | UUID | **nullable, UNIQUE.** Generado por el dispositivo en venta offline. |
| creado_en / creado_por | | |

### 7.2 `NotaDetalle`

| Campo | Tipo | Notas |
|---|---|---|
| nota | FK | |
| presentacion | FK | |
| cantidad | Decimal | |
| **precio_unitario** | Decimal | **CONGELADO al momento de la nota.** No referencia el catálogo. |
| tasa_iva | Decimal | copiada de la presentación al momento de la nota |
| importe | Decimal | cantidad × precio_unitario |

> **Arreglo #3 (parte B):** `precio_unitario` y `tasa_iva` se *copian* a la línea. Cuando subas precios el mes que viene, las notas viejas conservan lo que se cobró. Mismo criterio que costos congelados en Dolce.

### 7.3 Idempotencia offline (regla dura)

> **Arreglo #2:** una venta de contado offline **es un hecho, no una solicitud.** El producto y el dinero ya cambiaron de manos en campo.

- El dispositivo genera `folio_offline` (UUID) al crear la venta.
- El servidor tiene **constraint UNIQUE** sobre `folio_offline`.
- En el sync: si el UUID ya existe → devolver la nota existente (200), **no** crear otra. Si no existe → crear.
- El servidor **NO rechaza** una venta offline de contado por reglas de negocio (precio cambió, cliente, etc.). El precio ya se aplicó en campo. Solo deduplica y registra.
- **Crédito offline = bloqueado** a nivel de cliente PWA; nunca llega al servidor como pendiente.
- La nota crea sus `MovimientoInventario` tipo `venta` (negativos) sobre `ubicacion_origen` dentro de la misma transacción del sync. La unicidad del UUID es lo que garantiza que el inventario no se descuente dos veces.

---

## 8. Venta en planta y en ruta

No hay dos motores. Ambas crean `NotaVenta`; cambia `origen` y `ubicacion_origen`:
- **Planta:** `origen=planta`, `ubicacion_origen=planta`, online siempre (crédito puede validarse en vivo).
- **Ruta online:** `origen=ruta_online`, `ubicacion_origen=unidad`.
- **Ruta offline:** `origen=ruta_offline`, solo `condicion=contado`, con `folio_offline`.

> **Riesgo del spec evitado:** no se duplican planta y ruta como sistemas separados.

---

## 9. Cuentas por Cobrar y Pagos

### 9.1 Saldo
El saldo del cliente NO es una columna que se edita; se deriva de notas a crédito menos aplicaciones de pago. (Puede materializarse igual que la existencia, pero la verdad es el ledger de pagos.)

### 9.2 `Pago`
cliente FK, fecha, forma (`efectivo`/`transferencia`/`cheque`), monto, referencia, evidencia (file, nullable), creado_por.

**Transferencia** añade: banco, fecha_transferencia, referencia.
**Cheque** → tabla `Cheque`: banco, numero, fecha, estado (`recibido`/`depositado`/`cobrado`/`rebotado`), monto. (Cheque puede rebotar y reabrir saldo; por eso es entidad propia con estado.)

### 9.3 `AplicacionPago`
Un pago puede abonar a una o varias notas (abonos parciales).
pago FK, nota FK, monto_aplicado. Suma de aplicaciones ≤ monto del pago.

---

## 10. Conciliación CONTPAQi

> **Arreglo #1 (parte B):** el ERP **no timbra** en V1. CONTPAQi factura; el ERP liga y cuadra. La relación nota–factura es **muchos-a-muchos con monto asignado**, no "una factura = total de notas".

### 10.1 `FacturaContpaq`
serie_folio, uuid (CFDI), rfc, fecha, subtotal, iva, total, xml (file nullable), pdf (file nullable), cliente FK, estado (`conciliada`/`con_diferencia`).

### 10.2 `FacturaNota` (M2M con monto)
factura FK, nota FK, monto_asignado.

### 10.3 Validación
```
suma(monto_asignado de la factura)  ==  total factura   ± tolerancia_redondeo
```
- `tolerancia_redondeo` configurable (p. ej. $0.50) para absorber centavos de IVA/redondeo.
- Si no cuadra dentro de la tolerancia → factura queda `con_diferencia` para resolver (no se fuerza).
- Soporta factura parcial de una nota y factura global de varias notas, porque el monto asignado es por par nota–factura, no el total de la nota.

> Confirmar con contador / configuración CONTPAQi cómo exporta/importa (serie, UUID, XML). Esto es dato a parametrizar, no decisión de modelo. *(No es asesoría fiscal; el modelo solo refleja lo que el contador confirme.)*

---

## 11. Mermas y Reposiciones

### 11.1 `Merma` (una entidad, con origen)
| Campo | Tipo | Notas |
|---|---|---|
| presentacion | FK | |
| cantidad | Decimal | |
| origen | Choice | `planta` / `ruta` / `cliente_equipo` |
| ubicacion | FK | de dónde baja (planta o unidad) |
| motivo | Choice | roto / descongelado / caducado / falla_ruta / falla_conservador / bolsa_rota / garrafon_roto / ajuste_conteo / diferencia_no_explicada |
| costo_estimado | Decimal | |
| activo_comodato | FK → ActivoComodato | nullable (liga falla de equipo) |
| autorizado_por | FK | nullable (según umbral) |
| ruta | FK | nullable (merma de ruta) |

La merma genera `MovimientoInventario` tipo `merma` (negativo) sobre `ubicacion`.

### 11.2 `Reposicion`
> **Aclaración menor (costo):** reposición es costo, pero **se categoriza aparte de merma** para no distorsionar márgenes. Son dos costos por motivos distintos.

| Campo | Tipo | Notas |
|---|---|---|
| merma | FK | nullable; reposición ligada a merma o motivo autorizado |
| presentacion | FK | |
| cantidad | Decimal | |
| ubicacion_origen | FK | si es en ruta, baja del almacén móvil |
| autorizado_por | FK | |
| ruta | FK | nullable; aparece en el cierre como reposición |

Genera `MovimientoInventario` tipo `reposicion` (negativo). **Nunca** se registra como venta en cero pesos.

---

## 12. Autorizaciones y Bitácora

Permisos base (grupos/permios Django, reutiliza el patrón de Dolce):
`autorizar_credito_bloqueado`, `editar_limite_credito`, `bloquear_cliente_credito`, `autorizar_descuento`, más umbrales de merma/diferencia.

### 12.1 `Autorizacion` (bitácora genérica)
documento_tipo, documento_id, accion (`credito_bloqueado`/`descuento`/`merma`/`diferencia_cierre`/`precio_especial`), usuario FK, fecha, motivo, monto.

> Toda excepción a una regla dura (precio especial, crédito bloqueado, diferencia de cierre, merma sobre umbral) deja registro aquí. La autorización guarda usuario + fecha + motivo + monto, como pide el spec.

### 12.2 Estados de venta a crédito bloqueada
`borrador` / `pendiente_autorizacion` / `autorizada` / `rechazada` / `vendida` / `cancelada`. (Campo de estado en `NotaVenta` cuando `condicion=credito` y la validación en vivo falló.)

---

## 13. Cierre de Ruta (inventario **y** efectivo)

> **Arreglo #5:** el arqueo de efectivo tiene el mismo rango que el cuadre físico.

### 13.1 `CierreRuta`
ruta FK (1:1), estado, autorizado_por (nullable).

**Cuadre de inventario** (derivado del ledger de la unidad):
```
carga_inicial − ventas − reposiciones − mermas − devoluciones = diferencia_inventario
```

**Arqueo de efectivo** (campos del cierre):
| Campo | Tipo |
|---|---|
| efectivo_esperado | Decimal (suma de notas contado efectivo de la ruta) |
| efectivo_entregado | Decimal (lo que el vendedor entrega) |
| diferencia_efectivo | Decimal (entregado − esperado) |

- Si `diferencia_inventario ≠ 0` o `diferencia_efectivo ≠ 0` → estado `cerrada_con_diferencia`, requiere motivo + autorización (§12) según umbral.
- **La ruta no cierra limpia si hay ventas offline pendientes de subir** (chequeo de sync antes de permitir cierre).
- Las devoluciones de ruta generan movimiento `devolucion` (unidad → planta).

---

## 14. Mobiliario en Comodato

### 14.1 `ActivoComodato`
codigo_interno/serie (unique), tipo (`conservador`/`enfriador`/`estante`/`otro`), estado, cliente FK (nullable), ubicacion_texto, fecha_entrega, responsable FK, fotos/docs (nullable).

### 14.2 `MovimientoComodato`
activo FK, tipo (`entrega`/`retiro`/`cambio_cliente`/`falla`/`mantenimiento`), fecha, cliente FK, responsable FK, nota.

La merma por falla de equipo se liga vía `Merma.activo_comodato` (§11.1).

---

## 15. Producción Simple

### 15.1 `ProduccionCorrida`
| Campo | Tipo | Notas |
|---|---|---|
| producto_tipo | Choice | `hielo` / `agua` |
| fecha_hora | DateTime | |
| responsable | FK | |
| ubicacion_destino | FK | planta |
| **cantidad_entrada** | Decimal | **kg de hielo cargado / litros de agua producidos.** Ver arreglo #4. |
| estado_maquina | Choice | normal / saturada / mantenimiento / falla / limpieza / paro |
| incidencia | texto | nullable |

### 15.2 `ProduccionDetalle` (salida empacada)
corrida FK, presentacion FK, piezas_empacadas, (entrada a inventario via movimiento `produccion`).

### 15.3 Merma de producción derivada
> **Arreglo #4:** con `cantidad_entrada` la merma de producción **se deriva**, no se declara a ciegas:
```
merma_produccion = cantidad_entrada − Σ(piezas_empacadas × contenido_presentacion)
```
- Si el negocio **puede** medir kg/litros de entrada (contador de máquina, tiempo, báscula) → la merma sale sola y el motor de aprendizaje (promedio móvil + rango + alerta) tiene señal real.
- Si **no puede** medir entrada en V1 → se acepta explícitamente que V1 mide *empaque*, no *rendimiento*, y `cantidad_entrada` queda nullable. **Decisión a confirmar con el negocio**, no se promete analítica que no se sostiene.
- **No se crea inventario de hielo a granel** (coherente con el spec): el inventario nace solo con `ProduccionDetalle` (empacado).

### 15.4 Motor de aprendizaje
No es tabla nueva en V1. Es lectura sobre `ProduccionCorrida` para calcular promedio por máquina, rango normal, variación por turno y alerta simple cuando una corrida sale del rango. Se construye al final, sobre datos ya capturados.

---

## 16. Mapa de bugs/faltantes → solución en el modelo

| # | Hueco detectado en el spec V1 | Resuelto en |
|---|---|---|
| 1 | Conciliación asume `total factura = total notas`; IVA implícito | §1.2 (IVA por presentación) + §10 (M2M con monto + tolerancia + subtotal/IVA/total) |
| 2 | "Folio único" sin contrato de idempotencia | §7.1/§7.3 (UUID unique + dedup en sync + venta offline = hecho) |
| 3 | "Vendedor no edita precios" sin precedencia ni snapshot | §1.5 (precedencia) + §7.2 (precio congelado en línea) |
| 4 | Producción sin cantidad de entrada → merma no derivable | §15.1/§15.3 (`cantidad_entrada` + merma derivada, con decisión explícita) |
| 5 | Cierre cuadra producto pero no efectivo | §13 (arqueo de efectivo de primera clase) |
| A | Entrega parcial sin regla de saldo | §5.1 (cierra renglón, sin backorder en V1) |
| B | Costo de reposición mezclado con merma | §11.2 (categoría aparte) |
| C | Separación de infraestructura Dolce | §17 |

---

## 17. Infraestructura (recordatorio de aislamiento)

- Base PostgreSQL **propia** de Mar de Cortés, dominio propio. **No tocar** la base ni configuración de Pollyana's Dolce.
- Se reutilizan **patrones** (auth/permisos, ledger de inventario, PWA, cierre de ruta), **no datos**.
- Dev: Codex debe levantar un **dev DB dedicado** para correr `manage.py check` y `migrate --check`. Nunca SQLite improvisado ni la base de Dolce. (Que no corrieran en el worktree limpio por falta de `DATABASE_URL` fue lo correcto.)

---

## 18. Criterio de "modelo listo"

El modelo núcleo se considera fijo cuando, sobre el dev DB dedicado:
1. Migran las tablas de §1–§15 con sus constraints (UNIQUE de `folio_offline`, UNIQUE `(presentacion, ubicacion)` en existencia).
2. `ExistenciaUbicacion` se reconstruye exactamente sumando `MovimientoInventario` (test de integridad del ledger).
3. Una nota offline reenviada dos veces produce **una** nota y **un** juego de movimientos.
4. `resolver_precio()` respeta la precedencia de §1.5.
5. La conciliación cuadra con factura que incluye IVA y con factura parcial.

Con esto cerrado, el siguiente documento es el **plan por fases** (Fase 1: lazo comercial básico) montado encima de este modelo.
