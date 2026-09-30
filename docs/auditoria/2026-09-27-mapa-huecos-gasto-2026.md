# Mapa de huecos del gasto 2026

**Auditoría de solo lectura · 27-sep-2026 · sobre copia local fiel de producción**

La copia se verificó tabla por tabla contra producción antes de analizar:
9,046 líneas de presupuesto, 1,138 capturas de gasto, 805 rubros, 472 reglas,
50 contratos del maestro, 9,310 CFDIs. Idénticos.

---

## Lo que hay que saber primero

**El problema no es que falten datos. Es que 271 rubros activos no tienen
ninguna fuente configurada.**

| | Rubros | Líneas vacías | Presupuesto ene–sep |
|---|---|---|---|
| Administración | 56 | 357 | **$1,216,108** |
| Producción | 19 | 155 | $428,291 |
| Gastos de venta | 174 | 1,418 | $426,393 |
| Logística | 22 | 195 | $120,755 |
| **Total** | **271** | **2,125** | **$2,191,547** |

Ningún dato va a llenarlos, por más que se capture: no hay `ReglaFuenteRubro`
que los lea. Capturar sin conectarlos primero es trabajo perdido.

---

## Cómo se llena hoy el presupuesto

| Fuente | ene–abr | may–ago | sep | oct–dic |
|---|---|---|---|---|
| **Excel legado** | 771 | 191 | 8 | 198 |
| Ventas POS | 320 | 305 | 73 | 0 |
| Consumo MP | 89 | 338 | 0 | 0 |
| SIPARE | 92 | 96 | **0** | 0 |
| Gasto operativo | 113 | 47 | 21 | 0 |
| Nómina | 46 | 48 | **0** | 0 |
| Resto (bonos, mermas, mantenimiento) | 43 | 84 | 21 | 0 |

Dos cosas saltan:

**Septiembre no tiene nómina ni SIPARE porque el mes no ha cerrado.** Verificado
en producción el 27-sep: nómina completa del 1-ene al 31-ago (16 quincenas,
todas PAGADA o CERRADA) y 12 expedientes de cédula IMSS enero–agosto, todos
`APLICADO`. La quincena 16-30 de septiembre aún no se paga y la cédula de
septiembre se emite en octubre. **No hay nada que correr.**

**El Excel legado tiene 198 líneas en octubre–diciembre**, meses que aún no
ocurren. Son proyecciones cargadas como real.

---

## Los cuatro orígenes posibles

### 1. Fuente viva — ya llega solo

Nómina, SIPARE, ventas POS, consumo de materia prima, bonos, mermas,
mantenimiento y combustible. **No requieren captura y no tienen hueco:** nómina
e IMSS están completos hasta agosto, el último mes cerrado.

### 2. Contrato en el maestro — cargado pero dormido

**50 contratos, 0 obligaciones generadas.** $99,195/mes y $89,878/año esperando.

| Categoría | Contratos | Importe | Vigencia desde | Ciclo |
|---|---|---|---|---|
| Renta | 9 | $71,884 | sep | mensual |
| Seguros | 8 | $65,137 | feb | anual |
| Sistemas corporativos | 2 | $17,702 | mar | anual |
| Point | 9 | $12,528 | sep | mensual |
| Teléfono | 10 | $7,958 | sep | mensual |
| COEPRIS | 1 | $7,040 | mar | anual |
| Alarmas | 9 | $4,988 | sep | mensual |
| Fumigación producción | 1 | $950 | abr | mensual |
| Monitoreo flota | 1 | $887 | sep | mensual |

**Dos obstáculos:**

Las vigencias mensuales arrancan en **septiembre** porque fue cuando se cargó el
maestro, no cuando empezaron los contratos. Generar mayo–agosto falla con *"No
existe una versión vigente"*. Retrocederlas es seguro hoy porque hay 0
obligaciones; después ya no.

**No existe generación masiva.** La única vía es un botón por contrato y por mes
en `presupuesto_real_captura.html:198`. Para 50 contratos × 8 meses son **400
clics**. No hay management command entre los ~55 existentes, ni tarea de Celery.

### 3. CFDI ya descargado — el comprobante está, nadie lo registró

9,310 CFDIs de 2026 en el ERP, ~800 por mes. Los emisores de gasto más grandes:

| Emisor | CFDIs | Importe |
|---|---|---|
| Sigma Alimentos | 108 | $2,504,198 |
| Foodservices AM | 52 | $1,973,536 |
| Nueva Wal Mart | 86 | $1,661,559 |
| Abarrotera del Duero | 44 | $1,569,844 |
| IMSS | 9 | $1,413,246 |
| Dawn-Mixco | 30 | $1,015,336 |
| CFE | 43 | $612,124 |

La mayoría son insumos, que deberían entrar por consumo de materia prima, no por
captura de gasto.

### 4. Sin fuente — hace falta un documento

**La luz por sucursal.** Los nueve medidores ya están registrados, pero el CFDI
de CFE no dice a qué sucursal pertenece. Cada recibo trae ~11 bimestres de
histórico, así que **un recibo por sucursal llena el año completo**. Sin ellos no
avanza.

---

## Rubros duplicados: cuatro de cinco no lo eran

La primera lectura dio «14 rubros duplicados». Al aplicar la prueba de agregado
—el presupuesto del rubro de cadena debe igualar la suma de sus rubros por
sucursal— sólo uno la pasó. Los otros resultaron ser **los únicos que tienen
presupuesto**: sus contrapartes del maestro están en cero.

| Rubro | Concepto | Ppto/mes | Qué es en realidad |
|---|---|---|---|
| 776 | Fumigación | $5,150 | agregado: 9×$450 + $1,100 de producción, **exacto** |
| 766 | Alarmas | $3,990 | único con presupuesto; los 9 `ALARMAS_SUC` en $0 |
| 769 | Point | $10,800 | único con presupuesto; los 9 `SISTEMAS_SUC` en $0 |
| 767 | Licencias de sistemas | $10,800 | su mensual es el mismo Point del 769 |
| 805 | Renta y agua Leyva | $3,610 | duplicado: Leyva ya tiene renta en el rubro 1039 |

**El doble conteo de fumigación ya estaba vivo.** Enero suma $3,760 en el
agregado más $2,857.50 en las sucursales, por el mismo gasto. Igual en febrero,
abril y mayo.

### El problema estructural

**29 de los 50 contratos apuntan a rubros con presupuesto $0**: las 9 alarmas,
los 9 Point, los 8 seguros, los 2 corporativos y el monitoreo de flota. El
maestro se cargó creando rubros nuevos en vez de usar los que traían el
presupuesto del Excel. Sin corregirlo, cada obligación produce un renglón con
real y sin presupuesto contra el cual medirse.

Dos fallas puntuales del mismo recorrido: el rubro 1189 (fumigación de planta)
tiene **las dos reglas** —`GASTO_OPERATIVO` y `OBLIGACION_GASTO`— y duplicará en
cuanto se genere su obligación; y la póliza compartida Matriz-CEDIS apunta a un
rubro **sin ninguna regla**, así que su obligación no se leería.

### Qué se hizo

`realinear_agregados_cadena` baja el presupuesto al detalle y desactiva el
agregado. Verificado sobre copia local: la suma del detalle iguala al agregado
al centavo, y el presupuesto total baja $234,720 — exactamente lo retirado por
duplicado.

`cargar_fumigacion_sucursales` registra los 7 contratos con evidencia de
captura. Quedan fuera dos sucursales a propósito: **Bamoa** ya recibe su parte
por la regla de distribución al 35% sobre el centro compartido de Crucero
($450 × 0.35 = $157.50, y el 65% restante = $292.50 va a producción), y
**Plaza Las Glorias** no tiene una sola captura de fumigación en 2026 — su
presupuesto dice $450 pero nada lo respalda.

## Qué se recomienda, en orden

**1. Resolver los 14 rubros duplicados** antes de generar nada del maestro. Si
no, la primera obligación duplica contra el rubro del Excel.

**2. Construir el comando de generación masiva.** Sin él, activar el maestro son
400 clics manuales — y eso garantiza que no se haga.

**3. Decidir qué hacer con los 271 rubros sin regla.** No es un trabajo de
captura: es de configuración. Cada uno necesita una fuente o una baja.

**4. Pedir los ocho recibos de CFE faltantes.** Cada uno llena el histórico
completo de su medidor.

Las fuentes vivas **no entran en esta lista**: no tienen hueco que cerrar.

---

## Lo que este mapa no cubre

`fecha_apertura` de las 12 sucursales · la póliza de Guamúchil · el renombre de
`PRODUCCION`→`EMBETUNADO` en bonos · el kardex sin entradas de compra.
