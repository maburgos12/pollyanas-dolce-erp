# Spec: Mano de obra de producción diaria por área/proceso

## Origen y relación con el resto del proyecto

Refinamiento de `reportes/services_nomina_produccion.py` (ya deployado,
PR #846): hoy calcula UNA tarifa mensual promedio de mano de obra para
TODOS los productos (Σ nómina depto PRODUCCION del mes ÷ Σ unidades
vendidas del mes). Esta rebanada añade una vista más precisa y diaria,
separada por área/proceso (Hornos, Armado, Embetunado), **sin reemplazar**
el cálculo mensual — ambos conviven, el mensual sigue alimentando
`GastoOperativoMensual`/rentabilidad por producto; esta pieza es un
reporte/análisis adicional.

## Modelo (propuesto por Mauricio, no diseño propio)

> "Ligar lo que gana cada empleado al mes con su área o puesto de trabajo...
> de ahí repartirlo a los productos, a lo que se produce... en el área de
> hornos saca el pan, ahí estaría la mano de obra de lo que sale de puro
> pan... estarías dividiendo lo que producen por día entre costo diario...
> esto también revela si hay más capacidad para producir y no se está
> aprovechando en esas 8 horas de trabajo."

Sin tiempos estándar ni minutos por receta. Es: **nómina del área ÷ unidades
que salieron de esa área ese día**, sumando el costo de cada área por la
que pasa una receta.

## Decisiones ya tomadas (no reabrir)

- **Clasificación empleado→área:** reutilizar tal cual `rrhh.Empleado.puesto_operativo`
  (HORNOS/ARMADO/EMBETUNADO/CRUCERO/ENVIO_SUCURSAL) y el mismo criterio de
  `bonos_produccion.area_bono_produccion_empleado()` (`bonos_produccion/models.py`
  líneas 46-54) — no inventar una clasificación nueva.
  **Dato real verificado en producción:** 29 empleados en depto PRODUCCION,
  de los cuales HORNOS=5, ARMADO=5, EMBETUNADO=9, CRUCERO=2,
  ENVIO_SUCURSAL=3, PRODUCCION(sin más detalle)=1. Los 4 sin
  `puesto_operativo` claro caen al criterio genérico ya existente en
  `bonos_produccion`.
- **Clasificación receta→área: por `Receta.familia`, con excepción por
  receta puntual.** Nueva pantalla en el ERP para que Carolina/producción
  capture y ajuste qué área(s) requiere cada familia (Pastel, Pay, Bollo,
  PAN, Galletas, Cheesecakes, Flan, etc. — ~13 familias reales hoy) y,
  cuando una receta específica no siga el patrón de su familia, una
  excepción a nivel receta que sobreescribe la familia. **`LineaReceta.etapa`
  NO se usa para esto** (verificado: sus valores reales son nombres de
  sub-preparaciones como "Dream Whip", no estaciones de producción).
- **Prorrateo diario de nómina: por rango de fechas del período, NO por
  `NominaLinea.dias_trabajados`.** Verificado en producción: ese campo
  solo está poblado en 66 de 646 líneas de nómina (10%) — no es
  confiable. En su lugar: días laborables del período ≈
  `round((fecha_fin - fecha_inicio + 1) × 6/7)`, aplicando el supuesto que
  confirmó Mauricio ("uno de los 7 se descansa por ley"). Esto es una
  aproximación explícita y documentada (no cuenta faltas/incapacidades
  individuales ni si un feriado específico se trabajó o no) — el ceiling
  conocido de esta simplificación.
- **Producción diaria por receta:** `pos_bridge.PointProductionLine`
  (`produced_quantity`, `production_date`, `receta` FK) — ya existe, ya
  confiable, no tiene campo de área (por diseño de Point).
- **Día sin producción en un área** (fin de semana, día de descanso,
  feriado no trabajado): no se fuerza un costo por unidad ese día — se
  declara "sin producción". El costo de nómina de ese día NO se traslada
  ni se acumula al siguiente día — simplemente ese día no aporta un "costo
  por unidad" a ninguna receta. Si ocurre en un día que debería haber sido
  laborable (no descanso), es la señal de capacidad ociosa que busca
  Mauricio; si es el día de descanso esperado, es simplemente el
  comportamiento normal ya contemplado en el prorrateo de 6/7.
- **Convive con el cálculo mensual**, no lo reemplaza.

## Requisitos exactos

### Modelo nuevo (`reportes/models.py` o app nueva, decidir al implementar)
- `RecetaAreaProduccion`: `familia` (CharField, nullable — fila por
  familia) O `receta` (FK nullable — fila de excepción puntual, tiene
  prioridad sobre la fila de familia), `area` (choices reutilizando las
  mismas constantes de `bonos_produccion` — HORNOS/ARMADO/EMBETUNADO/etc.).
  Una familia o receta puede tener varias filas (pasa por varias áreas).
- `CostoManoObraDiarioArea` (snapshot, para histórico/gráficas):
  `fecha`, `area`, `nomina_dia_area` (Decimal), `unidades_producidas`
  (Decimal), `costo_unidad` (Decimal, null si `unidades_producidas == 0`),
  `es_dia_laborable_esperado` (Boolean, para distinguir descanso esperado
  de capacidad ociosa real).

### Servicios (`reportes/services_mano_obra_diaria_area.py`, nuevo)
- `calcular_costo_diario_area(fecha: date, area: str) -> dict`:
  1. Ubicar `NominaPeriodo`(s) cuyo rango cubre `fecha`.
  2. `nomina_area_periodo` = Σ `NominaLinea.total_percepciones` de
     empleados con área == `area` (mismo criterio que
     `bonos_produccion.area_bono_produccion_empleado()`), en esos períodos.
  3. `dias_laborables_periodo` = `round((fecha_fin - fecha_inicio + 1) * 6/7)`
     por período (sumar si `fecha` cae en el traslape de más de uno, caso
     raro).
  4. `costo_diario_area` = `nomina_area_periodo / dias_laborables_periodo`.
  5. `unidades_producidas` = Σ `PointProductionLine.produced_quantity` de
     recetas cuya área (vía `RecetaAreaProduccion`, excepción de receta
     tiene prioridad sobre familia) incluya `area`, con
     `production_date == fecha`.
  6. `costo_unidad` = `costo_diario_area / unidades_producidas` si
     `unidades_producidas > 0`, si no `None` (no forzar).
- `costo_mano_obra_diario_receta(fecha: date, receta) -> Decimal | None`:
  suma `costo_unidad` de cada área que le corresponde a esa receta ese
  día (vía `RecetaAreaProduccion`); si alguna área relevante no tiene
  `costo_unidad` calculable ese día, el resultado declara qué área falta
  en vez de omitir el hueco en silencio.

### Pantalla de clasificación (`reportes/` o `recetas/`, vista nueva)
- Lista de familias con checkboxes de área (HORNOS/ARMADO/EMBETUNADO/etc.),
  editable por quien tenga permiso de producción/dirección.
- Buscador de receta individual para agregar una excepción puntual que
  sobreescriba la familia.
- RBAC: reutilizar un permiso ya existente equivalente a "gestionar
  producción" (confirmar el más adecuado al implementar, ej.
  `can_manage_produccion` si existe, o el que ya usa `bonos_produccion`
  para configurar áreas).

### Reporte/vista de consumo (para ver capacidad ociosa)
- Por definir el detalle exacto en fase de plan: como mínimo, una tabla
  diaria por área mostrando `nomina_dia_area`, `unidades_producidas`,
  `costo_unidad`, y una marca visual cuando el costo por unidad se dispara
  muy por encima de su promedio reciente (señal de capacidad ociosa).

## Edge cases

- Receta cuya familia no tiene ninguna área clasificada todavía: declarar
  "sin clasificar", no asumir ninguna área por default.
- Receta con excepción puntual que contradice su familia: la excepción de
  receta manda, ignorar la de familia para esa receta específica.
- Período de nómina que cruza fin de mes o se traslapa con otro (raro):
  sumar la nómina de todos los períodos vigentes ese día antes de dividir.
- Empleado que cambia de área a mitad de un período de nómina: fuera de
  alcance de este MVP — se usa el `puesto_operativo` vigente al momento del
  cálculo (snapshot actual del empleado), no un histórico de cambios de
  puesto.
- Sucursal/CEDIS: `PointProductionLine` ya distingue sucursal — decidir en
  implementación si el costo por área se calcula agregado a nivel empresa
  o por sucursal/CEDIS de producción (probablemente empresa, ya que la
  nómina de producción es centralizada, no por sucursal — confirmar al
  implementar revisando si hay empleados de producción asignados a más de
  una sucursal).

## Fuera de alcance (explícito)

- Tiempos estándar o minutos por receta por estación — no se pidió, no se
  construye.
- Asistencia/incidencias día por día para afinar el prorrateo — se usa la
  aproximación 6/7 sobre el rango del período, no un calendario real de
  quién trabajó qué día.
- Reemplazar el cálculo mensual de `services_nomina_produccion.py` — sigue
  intacto, alimentando `GastoOperativoMensual` como hoy.
- Historial de cambios de puesto operativo por empleado.

## Definición de "hecho"

- [ ] Modelo `RecetaAreaProduccion` (familia + excepción por receta) con
      migración.
- [ ] Modelo `CostoManoObraDiarioArea` con migración.
- [ ] Pantalla de clasificación familia→área (+ excepción por receta),
      con RBAC adecuado.
- [ ] `calcular_costo_diario_area()` y `costo_mano_obra_diario_receta()`
      implementados, usando el prorrateo 6/7 documentado explícitamente en
      el código (no `dias_trabajados`, confirmado no confiable).
- [ ] Reporte/vista mínima mostrando costo diario por área y señal de
      capacidad ociosa.
- [ ] Tests: familia sin clasificar, excepción de receta que sobreescribe
      familia, día sin producción (costo_unidad = None, no forzado), día
      con producción normal, período que cruza fin de mes.
- [ ] `python manage.py check` y `migrate --check` en 0.
- [ ] Validado contra un día real de producción: comparar manualmente el
      costo calculado contra una cuenta hecha aparte (nómina real ÷ unidades
      reales de Point ese día) para al menos un área.
