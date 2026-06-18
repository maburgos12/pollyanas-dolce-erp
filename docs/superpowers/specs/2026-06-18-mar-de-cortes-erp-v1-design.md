# ERP Mar de Cortes V1 - Diseno Operativo

## Objetivo

Crear una base ERP autonoma para Mar de Cortes Agua y Hielo, derivada del
aprendizaje operativo del ERP de Pollyana's Dolce pero sin depender de Point ni
de un POS externo.

La V1 debe permitir operar pedidos, ventas, rutas, planta, inventario,
produccion simple, mermas, reposiciones, cobranza y conciliacion fiscal con
CONTPAQi. Pollyana's Dolce no se modifica ni pierde funcionalidad; este diseno
es para una base separada.

## Decision Principal

El ERP sera la fuente de verdad operativa.

No habra integracion obligatoria con Point. Las ventas nacen en el ERP, el
inventario se mueve en el ERP, la cobranza se controla en el ERP y las facturas
emitidas en CONTPAQi se concilian contra notas del ERP.

## Alcance V1

Incluye:

- Catalogo de productos, presentaciones, tasa de IVA, precios y clientes.
- Proveedores, compras y entradas de inventario.
- Pedidos previos por cliente.
- Venta en planta.
- Venta movil en ruta por PWA.
- Inventario por almacen/planta y por unidad/ruta.
- Carga, salida, regreso y cierre de rutas.
- Venta contado y credito.
- Autorizacion por permiso para credito bloqueado.
- Cuentas por cobrar, pagos, abonos, transferencias y cheques.
- Conciliacion de facturas CONTPAQi contra notas de venta.
- Mermas en planta, ruta y cliente/equipo.
- Reposiciones por merma sin contaminar ventas.
- Mobiliario prestado a clientes.
- Produccion simple de hielo y agua.
- Reportes operativos basicos.

Queda fuera de V1:

- SaaS multiempresa.
- Timbrado CFDI directo desde el ERP.
- Conciliacion bancaria automatica.
- App nativa iOS/Android.
- Optimizacion de rutas por GPS.
- Forecast avanzado o IA.
- Nomina completa.
- Costeo industrial detallado por energia, filtros o mantenimiento.

## Principios de Diseno

- Un solo flujo de pedidos y ventas.
- Pedido no es venta.
- Venta nace cuando se entrega producto.
- Reposicion no es venta en cero pesos.
- Merma registra perdida; reposicion registra entrega sin cobro.
- Credito requiere conexion en vivo.
- Contado puede operar con cola offline.
- El vendedor no edita precios.
- La nota congela precio unitario e IVA por linea.
- Las reglas especiales se autorizan con permisos y bitacora.
- Si algo no existe fisicamente, no debe existir como inventario falso.

## Flujo Comercial Unico

```text
Pedido -> Ruta/Carga -> Entrega -> Nota de venta -> Factura/Conciliacion -> Cobranza
```

### Pedido

El pedido representa lo que el cliente solicito antes de la entrega.

Campos minimos:

- Cliente.
- Fecha de entrega.
- Productos y cantidades.
- Observaciones.
- Ruta sugerida.
- Canal: planta, ruta o administracion.
- Estado.

Estados:

- Pendiente.
- Programado en ruta.
- Entregado parcial.
- Entregado.
- Reprogramado.
- Cancelado.

El pedido no descuenta inventario y no genera cartera.

En V1, una entrega parcial cierra el renglon entregado con la cantidad real. El
faltante no se reprograma automaticamente ni queda como backorder. Si el cliente
lo quiere de nuevo, administracion captura un pedido nuevo. Esta decision es
reversible: si el negocio decide manejar backorders, se cambia esta regla.

### Ruta y Carga

Administracion crea la ruta con:

- Vendedor.
- Unidad.
- Fecha.
- Clientes a visitar.
- Pedidos previos.
- Producto extra para venta libre.
- Carga total de la unidad.

La carga genera un traspaso:

```text
Almacen planta -> Almacen movil de la unidad/ruta
```

La cantidad por cliente es guia operativa, no obligacion cerrada. La carga total
si es obligatoria y limita lo que se puede vender.

### Entrega

En celular, el vendedor ve:

- Pedidos programados.
- Clientes de la ruta.
- Producto disponible en la unidad.
- Precio bloqueado por cliente o por publico general.
- Entrega completa, parcial o no entregada.
- Venta extra si hay producto disponible.
- Reposicion autorizada.
- Merma reportada.

La entrega confirmada genera la nota de venta.

Si la entrega es parcial, la nota solo incluye la cantidad realmente entregada.
El faltante no crea venta, deuda, reserva de inventario ni reprogramacion
automatica.

### Nota de Venta

La nota de venta es la evidencia operativa de lo realmente entregado/vendido.

La nota:

- Descuenta inventario de planta o de unidad/ruta.
- Genera cuenta por cobrar si es credito.
- Queda pagada si es contado y el pago se registra en el cierre.
- Queda pendiente de facturar, facturada, no facturable o cancelada.

## Venta en Planta

La planta usa el mismo motor comercial.

Flujo:

```text
Cliente/publico -> productos -> pago contado o credito -> nota de venta -> inventario baja
```

Si es credito, requiere internet y validacion actual del cliente. Si es publico
general, usa lista de precio general.

## Venta Movil en Ruta

La venta movil sera una PWA.

La PWA debe guardar localmente:

- Usuario.
- Ruta asignada.
- Clientes de la ruta.
- Productos cargados.
- Precios aplicables.
- Ventas contado pendientes de sincronizar.

Con internet, la venta se envia al ERP al momento.

Sin internet:

- Solo se permiten ventas de contado.
- Las ventas quedan en cola local.
- Cada venta offline lleva un UUID generado por el dispositivo.
- El servidor guarda ese UUID con constraint unico.
- Si el UUID ya existe, el servidor devuelve la nota existente y no crea otra.
- El servidor no rechaza una venta offline de contado por reglas de negocio.
- La pantalla muestra sincronizada, pendiente o error.
- La ruta no cierra limpia si hay ventas pendientes de subir.

Principio: una venta de contado offline es un hecho, no una solicitud.

Credito offline queda bloqueado.

## Clientes, Precios y Credito

Cada cliente puede tener:

- Tipo: contado, credito o ambos.
- Lista de precios.
- Precio fijo por producto.
- Dias de credito.
- Limite de credito opcional.
- Estado: activo, bloqueado o revision.
- Reglas de facturacion.

El vendedor no puede editar precios. Si se requiere precio especial manual,
debe ser con permiso y bitacora.

Precedencia de precio:

```text
precio cliente+producto -> lista del cliente -> lista general
```

La linea de nota congela precio unitario, tasa de IVA y totales calculados al
momento de vender. La nota no depende de referencias vivas al catalogo para
mantener su valor historico.

### Validacion de Credito

Credito requiere conexion en vivo.

El ERP valida:

- Cliente activo.
- Credito habilitado.
- Saldo vencido.
- Limite disponible si existe.
- Bloqueo manual.
- Notas/facturas pendientes.

Si falla la validacion, la venta queda pendiente de autorizacion.

### Autorizacion de Credito Bloqueado

No se amarra a un puesto fijo. Se controla con permisos.

Permisos base:

- autorizar_credito_bloqueado.
- editar_limite_credito.
- bloquear_cliente_credito.
- autorizar_descuento.

Estados de venta bloqueada:

- Borrador.
- Pendiente de autorizacion.
- Autorizada.
- Rechazada.
- Vendida.
- Cancelada.

La autorizacion guarda usuario, fecha, motivo y monto.

## Cuentas por Cobrar y Pagos

V1 debe soportar:

- Ventas de contado.
- Ventas a credito.
- Abonos parciales.
- Transferencia bancaria.
- Cheque.
- Efectivo.

Transferencia:

- Banco.
- Fecha.
- Referencia.
- Monto.
- Evidencia opcional.

Cheque:

- Banco.
- Numero.
- Fecha.
- Estado: recibido, depositado, cobrado, rebotado.
- Monto.

Los saldos se reportan por cliente, vendedor, ruta, factura y antiguedad.

## Facturacion CONTPAQi Conciliada

En V1, CONTPAQi sigue siendo el facturador.

El ERP no timbra CFDI en V1.

Flujo:

```text
Notas de venta -> paquete de facturacion -> factura en CONTPAQi -> liga en ERP -> cobranza
```

Administracion selecciona notas de un cliente y genera un paquete de
facturacion. La factura se emite en CONTPAQi. Luego se captura o importa en el
ERP:

- Serie/folio.
- UUID.
- RFC.
- Fecha.
- Total.
- XML/PDF opcional.
- Asignaciones nota-factura con monto aplicado.

Validacion minima:

```text
suma(monto asignado) = total factura +/- tolerancia de redondeo
```

La relacion nota-factura es muchos-a-muchos. Una factura puede cubrir varias
notas y una nota puede quedar cubierta por varias facturas si hay facturacion
parcial. Tambien debe soportar factura global.

Cada asignacion guarda:

- Nota.
- Factura.
- Monto asignado.
- Base gravada.
- IVA asignado.
- Tasa de IVA usada.

Cada presentacion/producto tiene su tasa de IVA como campo propio. No se asume
una tasa global.

Si la suma asignada no cuadra dentro de la tolerancia, queda como diferencia de
conciliacion para resolver.

## Inventario

Inventario se maneja por ubicacion:

- Planta/almacen.
- Unidad/camioneta.
- Ruta activa.
- Cliente/equipo solo como contexto de merma o mobiliario, no como stock normal.

Movimientos base:

- Compra/entrada.
- Produccion/entrada.
- Traspaso a unidad.
- Venta.
- Reposicion.
- Merma.
- Devolucion de ruta.
- Ajuste autorizado.

## Cierre de Ruta

El cierre debe cuadrar:

```text
carga inicial - ventas - reposiciones - mermas - devoluciones = diferencia
```

Tambien debe hacer arqueo de efectivo:

```text
efectivo esperado - efectivo entregado = diferencia de efectivo
```

Estados:

- Programada.
- Cargada.
- En ruta.
- Regresada.
- Cerrada.
- Cerrada con diferencia.

Si hay diferencia de inventario o efectivo, se requiere motivo y autorizacion
segun umbral.

## Mermas

Una sola entidad de merma con origen.

Origenes:

- Planta.
- Ruta.
- Cliente/equipo.

Motivos:

- Producto roto.
- Descongelado.
- Caducado/no vendible.
- Falla de ruta.
- Falla de conservador/enfriador.
- Bolsa rota.
- Garrafon roto.
- Ajuste por conteo.
- Diferencia no explicada.

La merma descuenta inventario de la ubicacion correcta y guarda costo estimado.
Puede requerir autorizacion por monto o cantidad.

## Reposiciones

La reposicion es un movimiento de inventario sin cobro, ligado a una merma o
motivo autorizado.

No se registra como venta en cero pesos.

El costo de reposicion se categoriza aparte del costo de merma. Ambos son
costos operativos, pero por motivos distintos: mezclarlos distorsiona margenes.

Flujo:

```text
Merma cliente/equipo -> reposicion autorizada -> salida de inventario -> entrega al cliente
```

Si la reposicion se entrega en ruta, descuenta del almacen movil y aparece en el
cierre como reposicion.

## Mobiliario Prestado

Se controla como activos en comodato.

Tipos:

- Conservador/congelador de hielo.
- Enfriador de agua.
- Estante para garrafones.
- Otro mobiliario.

Datos minimos:

- Codigo interno o serie.
- Tipo.
- Estado.
- Cliente asignado.
- Ubicacion.
- Fecha de entrega.
- Responsable.
- Fotos/documentos opcionales.
- Historial de movimientos.
- Fallas y mantenimientos basicos.

Las mermas por falla pueden ligarse al activo.

## Produccion Simple

La V1 registra produccion real para alimentar inventario y aprender del
historial.

### Hielo

La maquina produce hielo en cubo a granel, pero no se guarda a granel. Todo debe
empacarse inmediatamente o se pierde.

Flujo:

```text
Carga de hielo -> empaque -> producto terminado -> inventario
```

Productos de salida:

- Bolsa 5 kg.
- Costal 15 kg.
- Otras presentaciones futuras.

La carga se cierra con:

- Cantidad de entrada de la corrida en kg, si planta puede medirla.
- Piezas empacadas por presentacion.
- Kg empacados calculados desde piezas por presentacion.
- Merma derivada: entrada kg - kg empacados.
- Responsable.
- Hora.
- Estado de maquina: normal, saturada, mantenimiento, falla, limpieza, paro.

No se crea inventario de hielo a granel.

### Agua

La produccion de agua se captura por contenedor/tanque o corrida simple.

Campos:

- Fecha/hora.
- Responsable.
- Contenedor/tanque si aplica.
- Producto resultante.
- Cantidad de entrada de la corrida en litros, si planta puede medirla.
- Cantidad real.
- Merma derivada si hay entrada medible.
- Incidencia.
- Entrada a inventario.

El costeo fino y consumos detallados quedan para una fase posterior.

Pendiente operativo: confirmar con planta si la entrada por corrida se puede
medir de forma consistente en kg/litros. Si no se puede medir al inicio, V1
captura salida real y merma declarada, dejando el campo de entrada opcional.

### Motor de Aprendizaje Operativo

No se implementa IA en V1. Se guarda historial suficiente para calcular:

- Promedio por maquina.
- Rango normal.
- Variacion por turno.
- Alertas cuando una carga cae fuera del rango.
- Relacion entre demanda, rutas y produccion.

Primera version:

```text
promedio movil + rango normal + alerta simple
```

## Reportes V1

Reportes minimos:

- Ventas por dia, ruta, vendedor, cliente y producto.
- Pedidos pendientes y cumplimiento.
- Cierre de ruta.
- Cartera por cliente y antiguedad.
- Pagos y abonos.
- Notas pendientes de facturar.
- Facturas CONTPAQi conciliadas y con diferencias.
- Inventario por ubicacion.
- Mermas por origen, motivo, producto y costo.
- Reposiciones por cliente y motivo.
- Produccion por maquina/turno/producto.
- Mobiliario prestado por cliente y estado.

## Migracion Desde el ERP Dolce

Se reutilizan patrones, no datos reales.

Mar de Cortes usa base PostgreSQL propia y dominio propio. Nunca se toca la base
de datos, dominio, variables de entorno ni configuracion productiva de
Pollyana's Dolce. Para validar se usa una base de desarrollo dedicada; no se usa
SQLite improvisado como sustituto del flujo real.

Reutilizable:

- Autenticacion y permisos.
- Catalogos base.
- Inventario y movimientos como concepto.
- Compras/proveedores.
- RRHH como fuente de personas.
- Reportes/exportables.
- PWA y cierre de ruta como patron.

No se arrastra:

- Point como fuente obligatoria.
- Datos reales de Pollyana's Dolce.
- Reglas fijas de sucursales Dolce.
- Bonos, forecast y recetas especificas Dolce.
- Branding Dolce.
- Credenciales, dominios o configuraciones productivas.

## Validacion de Implementacion

Antes de considerar lista la V1:

- El ERP debe poder crear cliente, producto, proveedor y empleado.
- Compras deben subir inventario.
- Produccion debe subir inventario.
- Ruta debe cargar producto desde planta.
- Venta de contado en planta debe bajar inventario y cerrar pago.
- Venta de contado en ruta debe bajar inventario movil.
- Venta offline contado debe sincronizar sin duplicarse por UUID.
- Reintento de UUID existente debe devolver la nota ya creada.
- Venta a credito sin internet debe bloquearse.
- Venta a credito bloqueada debe pedir autorizacion.
- Pedido previo debe convertirse en nota solo al entregar.
- Factura CONTPAQi debe ligarse a notas por monto asignado y cuadrar con
  tolerancia.
- Pago parcial debe bajar saldo.
- Merma de planta debe bajar inventario de planta.
- Merma de ruta debe bajar inventario movil.
- Reposicion debe bajar inventario sin aparecer como venta.
- Cierre de ruta debe mostrar diferencias de inventario y efectivo.
- Mobiliario debe poder asignarse a cliente y ligarse a falla/merma.

## Riesgos

- Intentar hacer SaaS multiempresa antes de estabilizar Mar de Cortes.
- Timbrar CFDI directo antes de controlar notas y cobranza.
- Duplicar ventas planta y ruta como sistemas separados.
- Permitir credito offline con datos viejos.
- Registrar reposiciones como ventas de cero pesos.
- Crear inventario a granel que no existe fisicamente.
- Migrar datos de Dolce al template por accidente.

## Datos Operativos Por Levantar Antes De Implementar

Estos datos no cambian el flujo V1; sirven para parametrizar la primera carga.

- Listado inicial de productos y presentaciones.
- Reglas exactas de precios por cliente.
- Tasas de IVA por producto/presentacion.
- Estados fiscales que CONTPAQi exporta o permite importar.
- Formato real de comprobantes de pago.
- Umbrales de autorizacion para mermas y diferencias.
- Si planta puede medir entrada por corrida en kg/litros.
- Datos minimos para solicitud de credito.
- Campos requeridos para mobiliario en comodato.
