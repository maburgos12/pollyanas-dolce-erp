# Analisis de Politicas e Indicadores para Orquestacion

## 1. Objetivo

Traducir las politicas operativas y los indicadores compartidos por area a reglas, disparadores, KPI y responsabilidades útiles para el orquestador nativo del ERP.

Este analisis se enfoca en responder:

- que reglas operativas ya existen por area
- que KPI y umbrales ya se estan midiendo
- que señales se pueden convertir en triggers para agentes
- que aprobaciones humanas siguen siendo obligatorias

## 2. Fuentes revisadas

Politicas:

- `/Users/mauricioburgos/Downloads/Politica de Compras.docx`
- `/Users/mauricioburgos/Downloads/POLLYANA’S DOLCE/Manual Organizacional/Administración/Politicas/Politica de Control de Inventarios.docx`
- `/Users/mauricioburgos/Downloads/POLLYANA’S DOLCE/Manual Organizacional/Logística/Politicas/Politica de Logistica de entregas a sucursales.docx`
- `/Users/mauricioburgos/Downloads/POLLYANA’S DOLCE/Manual Organizacional/Compras/Politicas/Politica de Mantenimiento.docx`
- `/Users/mauricioburgos/Downloads/POLLYANA’S DOLCE/Manual Organizacional/Producción/Politicas/Politica de Producción.docx`
- `/Users/mauricioburgos/Downloads/POLLYANA’S DOLCE/Manual Organizacional/Administración/Politicas/Politica de Salidas de Almacén.docx`

Indicadores:

- `/Users/mauricioburgos/Documents/Organización Pollyana's Dolce/Indicadores/...`

## 3. Hallazgos ejecutivos

Si hay informacion util y accionable para el orquestador.

Lo mas valioso encontrado:

- horarios de corte reales para solicitudes y surtido
- responsables y aprobadores por area
- reglas de excepcion para compras, mantenimiento e inventario
- KPI y formulas reutilizables para exactitud, cumplimiento, costo, ticket, rotacion y tiempos
- señales claras para ventas, produccion, compras, logistica y conciliacion

## 4. Compras

### 4.1 Politicas utiles

Reglas encontradas:

- la solicitud de compra debe enviarse por correo y formato digital con copia a Direccion General
- Produccion entrega solicitud semanal los lunes a las `12:00 pm` con `15 dias de antelacion`
- Almacen solicita insumos para mantener niveles de stock
- productos o servicios especiales solo pueden ser solicitados por Ventas, Produccion y Administracion
- compras mayores a presupuesto, fuera de catalogo o mayores a `$5,000` requieren autorizacion de Direccion General
- para compras superiores a un monto determinado se deben pedir al menos `3 cotizaciones`
- no se debe comprar con proveedores no evaluados o no dados de alta
- alta de proveedor requiere autorizacion de Direccion General
- Compras debe documentar cotizaciones, ordenes, facturas y comprobantes
- Compras debe inspeccionar calidad y seguridad alimentaria de lo recibido

### 4.2 Indicadores utiles

Archivos relevantes:

- `Indicadores compras/INDICADOR CUMPLIMIENTO COMPRAS.xlsx`
- `Indicadores admon/Ind. cumplimiento compras.xlsx`

Indicadores detectados:

- seguimiento de solicitudes y compras
- status por requisicion
- area solicitante
- entregada a

### 4.3 Reglas para agentes

Aplicacion recomendada:

- `agente_compras` debe vigilar solicitudes semanales de Produccion con 15 dias de horizonte
- crear trigger si el lunes despues de `12:00 pm` no existe solicitud de compra esperada para Produccion
- crear trigger si una compra supera `$5,000`, esta fuera de catalogo o arriba de presupuesto
- crear trigger si falta evaluacion o alta formal del proveedor
- crear sugerencia si faltan `3 cotizaciones` donde la politica lo exige

### 4.4 Aprobaciones obligatorias

- alta de proveedor
- compra fuera de catalogo
- compra arriba de presupuesto
- compra o servicio mayor a `$5,000`

## 5. Control de inventarios y salidas de almacen

### 5.1 Politicas utiles

Reglas de inventario:

- Almacen debe mantener orden, limpieza y acomodo estandar
- debe mantener funcionando el sistema de stock
- debe detectar baja rotacion y proximidad a caducidad e informar a Administracion
- producto no apto debe separarse, etiquetarse y registrarse
- debe haber inventarios fisicos periodicos
- Administracion revisa comprobantes de entradas y salidas diariamente
- Administracion debe muestrear `minimo 7 productos por semana`
- ajustes de inventario requieren autorizacion de Jefe de Administracion
- diferencias deben justificarse con evidencia

Reglas de salidas:

- requisiciones diarias antes de las `8:20 am`
- solo ciertos puestos pueden autorizar requisiciones
- prohibido surtir sin firma/autorizacion
- si no hay existencia total, se registra cantidad parcial entregada
- todo surtido debe registrarse en sistema y control interno
- se entrega reporte de salidas y vales a Administracion antes de las `9:30 am`
- Administracion revisa consistencia documental y ordena correcciones

### 5.2 Indicadores utiles

Archivos relevantes:

- `Indicadores admon/Ind. abastecimiento almacén.xlsx`
- `Indicadores admon/Ind. auditoria de inventarios.xlsx`

Indicadores detectados:

- `Exactitud de inventarios`
- formula: `cantidad de articulos sin diferencia / cantidad de articulos inventariados`
- captura de diferencia por producto: `existencia fisica - existencia sistema`
- abastecimiento almacen por dia y por area

Notas:

- aparecen siglas `C.P` y `P.A.C` que no quedaron definidas en los archivos revisados

### 5.3 Reglas para agentes

Aplicacion recomendada:

- `agente_compras` y `agente_conciliacion` deben vigilar quiebres y diferencias
- trigger si no existen requisiciones esperadas antes de `8:20 am`
- trigger si la documentacion de salida no fue entregada antes de `9:30 am`
- trigger si un producto entra en `baja rotacion` o `proximo a caducar`
- trigger si la exactitud semanal cae por debajo del objetivo definido por Administracion
- trigger si hay diferencias recurrentes sin evidencia justificativa

### 5.4 Aprobaciones obligatorias

- ajuste de inventario en sistema
- salida por incidente o merma extraordinaria

## 6. Logistica a sucursales

### 6.1 Politicas utiles

Reglas encontradas:

- solicitudes de producto final antes de las `8:30 am`
- Logistica debe tener existencias actualizadas antes de las `8:30 am`
- Logistica debe comparar solicitud vs inventario vs pronostico
- si no hay existencia, debe consultar viabilidad con Produccion e informar a Ventas
- sucursal con capacidad menor al `30%` debe priorizarse
- ruta se planifica por prioridad, tiempo, costo, distancia, volumen y unidad disponible
- transporte debe operar entre `3°C y 5°C`
- monitoreo de velocidad: ciudad max `30 km/h`, carretera max `110 km/h`
- se requieren firmas de recibido y hojas de recepcion del sistema
- Logistica lleva bitacora de mantenimiento de unidades

### 6.2 Indicadores utiles

Archivos relevantes:

- `Indicadores Logistica/Ind abastecimiento cedis.xlsx`
- `Indicadores Logistica/Ind. cumplimiento costos.xlsx`
- `Indicadores Logistica/Ind. incidencias.xlsx`
- `Indicadores ventas/Cronograma visitas a Sucursales.xlsx`

Indicadores detectados:

- efectividad de CEDIS por dia y sucursal
- costo logistico por producto
- meta de costo por producto: `2`
- porcentaje de efectividad con formula `MIN(1, meta / costo_real)`
- kilometros recorridos
- costo por km
- incidencias logistico-operativas
- seguimiento de areas de oportunidad en sucursales

### 6.3 Reglas para agentes

Aplicacion recomendada:

- `agente_demanda_ventas` y `agente_logistica` futuro pueden usar el corte de `8:30 am`
- trigger si una sucursal cae por debajo de `30%` de capacidad
- trigger si costo por producto supera la meta
- trigger si ruta real no cumple tiempo estimado o condicion de transporte
- trigger si se detectan incidencias recurrentes en una sucursal
- trigger si la cadena fria sale del rango `3°C a 5°C`

### 6.4 Oportunidad de agente nuevo

Si se mantiene este alcance, si se justifica un agente futuro:

- `agente_logistica_sucursales`

Motivo:

- ya hay politicas, criterios operativos y KPI propios del area
- hoy el dominio logistica tiene suficientes reglas para vivir separado de Compras y Produccion

## 7. Produccion

### 7.1 Politicas utiles

Reglas encontradas:

- Produccion debe garantizar confidencialidad de recetas y procesos
- al iniciar la jornada debe tener existencia fisica de producto final
- Jefe de Produccion puede modificar el plan diario segun necesidad
- el plan de produccion debe ingresarse diariamente al sistema
- Produccion pide insumos a Almacen por requisicion
- Produccion solicita panes e insumos preelaborados a Logistica via plan
- preparaciones deben etiquetarse con:
  - nombre
  - fecha
  - cantidad
  - tiempo de vida
- dreamwhip debe elaborarse `al dia`
- calidad del producto final debe registrarse
- incidencias deben ir a bitacora
- al final del dia se debe ingresar al sistema la produccion realizada
- incumplimiento del plan debe escalarse para no afectar Ventas
- Logistica resguarda producto final con etiqueta que incluya caducidad, lote e ingredientes
- cuarto frio opera con `PEPS`

### 7.2 Indicadores utiles

Archivos relevantes:

- `Indicadores producción/CHECKLIST.xlsx`
- `Indicadores producción/Costo de mano de obra.xlsx`
- `Indicadores producción/Ind. incidencias.xlsx`
- `Indicadores producción/P. Armado-emberunado.xlsx`
- `Indicadores producción/p. hornos.xlsx`
- `Indicadores ventas/Indicador venta vs producción.xlsx`

Indicadores detectados:

- checklist de Produccion con objetivo minimo `95%`
- costo de mano de obra con objetivo maximo `5%` sobre presupuesto mensual
- cumplimiento de plan de produccion por horno
- cumplimiento de plan de produccion por armado/terminado
- incidencias de Produccion
- comparativo venta vs produccion

### 7.3 Reglas para agentes

Aplicacion recomendada:

- `agente_produccion` debe verificar que el plan diario quede cargado en sistema
- trigger si no se carga plan de produccion diario
- trigger si el checklist cae por debajo de `95%`
- trigger si mano de obra supera `5%` sobre presupuesto
- trigger si venta vs produccion muestra desviacion recurrente

## 8. Inventario y control operativo

### 8.1 Reglas priorizadas para seed inicial

Estas reglas si se pueden bajar ya al catalogo de `orquestacion` porque tienen:

- politica formal o KPI claro
- ownership razonablemente definido
- impacto directo en control operativo
- posibilidad de observacion, recomendacion o aprobacion sin tocar el core transaccional

Reglas priorizadas:

- `production_weekly_purchase_request_deadline`
  - revisa si Produccion entrego su solicitud semanal de compra el lunes despues de `12:00 pm`
  - horizon requerido: `15 dias`
  - agente primario: `agente_compras`
  - agente secundario: `director_operativo`
- `purchase_exception_requires_dg_approval`
  - detecta compras fuera de catalogo, arriba de presupuesto o mayores a `$5,000`
  - agente primario: `agente_compras`
  - agente secundario: `director_operativo`
- `inventory_adjustment_authorization_guard`
  - obliga validacion para ajustes de inventario
  - agente primario: `agente_conciliacion`
  - agente secundario: `director_operativo`
- `near_expiry_or_low_rotation_review`
  - detecta producto con baja rotacion o cercania a caducidad
  - agente primario: `agente_conciliacion`
  - agente secundario: `agente_compras`
- `branch_capacity_below_threshold`
  - alerta si una sucursal baja de `30%` de capacidad operativa o stock de servicio
  - agente primario: `agente_demanda_ventas`
  - agente secundario: `agente_compras`
- `cold_chain_temperature_breach`
  - alerta si transporte sale del rango `3°C a 5°C`
  - agente primario: `agente_conciliacion`
  - agente secundario: `director_operativo`
- `daily_production_plan_missing`
  - alerta si no se carga el plan de produccion diario en sistema
  - agente primario: `agente_produccion`
  - agente secundario: `director_operativo`
- `production_checklist_below_target`
  - alerta si checklist de Produccion baja de `95%`
  - agente primario: `agente_produccion`
  - agente secundario: `director_operativo`

### 8.2 Reglas no sembradas aun

Estas se documentan, pero todavia no conviene sembrarlas como regla activa por falta de ownership o fuente de datos confirmada:

- mantenimiento preventivo y correctivo
- velocidad de unidades en ruta
- costo logistico por producto
- mano de obra vs presupuesto mensual

Datos faltantes para activarlas con seguridad:

- fuente de datos oficial dentro del ERP o integracion estable
- periodicidad y responsable operativo
- criterio de aprobacion y escalamiento
- trigger si una preparacion no trae fecha, lote o vida util
- trigger si dreamwhip no fue producido el mismo dia cuando aplica

### 7.4 Aprobaciones obligatorias

- cambios extraordinarios de plan con impacto comercial
- cambios de receta o estandar
- decisiones de producto especial no autorizado

## 8. Mantenimiento

### 8.1 Politicas utiles

Reglas encontradas:

- Jefes de Area deben recorrer instalaciones `2 veces por semana`
- deben registrar hallazgos en checklist de mantenimiento
- cada area lleva bitacora preventiva
- Compras recibe bitacora preventiva semanalmente
- solicitudes de mantenimiento deben incluir:
  - preventivo o correctivo
  - equipo
  - area
  - fecha y hora de deteccion
  - prioridad
- Compras debe obtener `al menos 2 cotizaciones`
- Direccion General autoriza mantenimiento con modificacion de instalacion
- Compras agenda proveedor y documenta evidencia, firma y observaciones

### 8.2 Indicadores utiles

No se detecto workbook de mantenimiento puro en la carpeta de indicadores, pero el cronograma de visitas a sucursales y los hallazgos operativos sí funcionan como fuente de backlog de mantenimiento.

### 8.3 Reglas para agentes

Aplicacion recomendada:

- `agente_compras` y `agente_activos_mantenimiento` futuro pueden usar estas reglas
- trigger si un area no reporta bitacora semanal
- trigger si un mantenimiento preventivo esta proximo y no tiene programacion
- trigger si faltan 2 cotizaciones
- trigger si la solicitud implica modificacion de instalacion sin autorizacion DG

## 9. RH y soporte corporativo

Aunque no eran el foco principal para ventas-produccion-compras, la carpeta de indicadores ya trae señales útiles:

- rotacion de personal
- permanencia
- vacantes
- horas extra
- cumplimiento de pago de gastos fijos

Esto sirve para una fase posterior del orquestador, porque:

- Produccion con alta rotacion y horas extra puede degradar cumplimiento del plan
- Logistica con vacantes puede afectar tiempo de entrega
- gastos fijos fuera de tiempo pueden usarse en tablero de Direccion

## 10. Mapa de KPI utiles para el orquestador

### 10.1 Direccion / Director Operativo

- cumplimiento de compras
- exactitud de inventarios
- cumplimiento plan de produccion
- venta vs produccion
- costo logistico por producto
- ticket promedio
- productos no vendidos o negados
- incidencias por area

### 10.2 Agente de Demanda y Ventas

- ventas por producto y sucursal
- ticket promedio
- pedidos a domicilio
- productos no vendidos/negados
- venta vs produccion
- cronograma y hallazgos de visitas a sucursales

### 10.3 Agente de Produccion

- cumplimiento plan hornos
- cumplimiento plan armado/terminado
- checklist Produccion
- costo de mano de obra
- incidencias
- venta vs produccion

### 10.4 Agente de Compras

- cumplimiento compras
- abastecimiento almacen
- abastecimiento CEDIS
- faltantes vs plan
- proveedor sin alta o sin cotizaciones

### 10.5 Agente de Conciliacion

- exactitud inventarios
- diferencias fisico vs sistema
- incidencias por area
- venta vs produccion
- jobs fallidos o sincronizaciones pendientes

## 11. Triggers recomendados inmediatos

Los siguientes triggers ya se pueden modelar sin esperar mas documentos:

- solicitud de producto final no capturada antes de `8:30 am`
- requisicion de almacen no capturada antes de `8:20 am`
- documentacion de salidas no entregada antes de `9:30 am`
- sucursal con capacidad menor a `30%`
- checklist de Produccion menor a `95%`
- costo de mano de obra mayor a `5%` sobre presupuesto
- compra fuera de catalogo
- compra mayor a `$5,000`
- compra arriba de presupuesto
- proveedor nuevo sin autorizacion
- mantenimiento sin 2 cotizaciones
- inventario con diferencias semanales recurrentes
- producto de baja rotacion o proximo a caducar

## 12. Huecos y datos faltantes

No conviene asumir aun:

- definicion exacta de las siglas `C.P` y `P.A.C` en abastecimiento
- si las metas en archivos corresponden al modelo vigente o a una version anterior
- si el calendario comercial formal existe por escrito
- si las hojas de indicadores se capturan en tiempo real o solo como control manual
- si los cortes de `8:20` y `8:30` ya estan respaldados por timestamps en sistema
- si los indicadores de visita a sucursales hoy viven en ERP o solo en Excel

## 13. Recomendacion

Si conviene usar estas politicas e indicadores como insumo directo del orquestador.

Orden sugerido de implementacion:

1. convertir reglas horarias y de aprobacion en triggers del sistema
2. normalizar KPI por area en un diccionario formal
3. ligar los KPI al agente responsable
4. modelar aprobaciones obligatorias de DG y Administracion
5. crear el futuro `agente_logistica_sucursales` cuando se cierre Fase 1

## 14. Impacto sobre el diseño actual

Este material fortalece directamente:

- [docs/ARQUITECTURA_ORQUESTADOR_NATIVO_ERP.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/ARQUITECTURA_ORQUESTADOR_NATIVO_ERP.md)
- [docs/MAPA_CAPACIDADES_ERP_A_AGENTES.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/MAPA_CAPACIDADES_ERP_A_AGENTES.md)

La principal conclusion es que ya no estamos diseñando agentes "en abstracto". Con estas politicas e indicadores, ya existen reglas y KPI suficientemente concretos para empezar a gobernar el orquestador por area.
