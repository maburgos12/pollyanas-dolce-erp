# Spec: Minutos estándar por receta/área (refinamiento de mano de obra diaria)

## Origen y relación con el resto del proyecto

Segunda vuelta sobre `specs/mano-obra-diaria-por-area.md` (deployado, PR
#853/#866/#867). Ese diseño explícitamente dejó fuera de alcance "tiempos
estándar o minutos por receta por estación — no se pidió, no se construye".
Mauricio, viendo la pantalla de clasificación en vivo, encontró dos
problemas que justifican reabrir esa decisión:

1. **Duplicados de familia sin cubrir.** El fix anterior (PR #867) solo
   fusionó 2 casos confirmados (variantes de tamaño de Pastel, "Betún y
   Rellenos"). Al revisar la base real se encontró al menos un caso más sin
   cubrir: `RELLENOS Y CREMAS` (15 insumos) es probablemente el mismo grupo
   que `Betún, Cremas, Rellenos (INSUMO PRODUCIDO)`. Cada caso nuevo
   requería un cambio de código + PR + deploy — no escala.
2. **El modelo de "nómina del área ÷ piezas del área, parejo" distorsiona
   el costo por producto.** Confirmado por Mauricio con el flujo real de
   producción: Hornos hornea el "pan"/base (vainilla, chocolate), la
   tarta/base de pays, y re-hornea con la batida de queso (Pay Horneado);
   también bollos y a veces galletas. Armado toma esas bases ya horneadas +
   mezclas/rellenos (a veces preparados por gente de Embetunado) y arma el
   producto (pan, tres leches, relleno, pan, tres leches), lo manda a
   Embetunado, que decora y empaca. Es una línea de proceso con áreas
   distintas de personal, no departamentos paralelos e intercambiables —
   y un Pastel Grande no consume el mismo tiempo de Hornos que un Pay
   Horneado o un Bollo. Dividir la nómina del área entre TODAS las piezas
   del área por igual, sin importar cuál receta es, ignora esa diferencia.

## Decisiones tomadas en esta vuelta (no reabrir)

- **La fusión de familias/categorías duplicadas de Point deja de ser
  código, se vuelve dato editable por Carolina.** Confirmado explícitamente
  por Mauricio: "Carolina lo controla desde la pantalla", no un mapeo fijo
  en `reportes/mano_obra_grupos_familia.py` que requiera un PR cada vez que
  Point trae una familia nueva parecida.
- **"Minutos estándar" = minutos-persona de trabajo ACTIVO, no tiempo de
  horno/reposo pasivo.** Confirmado explícitamente por Mauricio. Mientras
  un producto se hornea sin que nadie le esté poniendo mano, ese tiempo NO
  cuenta como mano de obra de ESE producto — la persona normalmente ya está
  atendiendo el siguiente lote, y ese trabajo se cuenta aparte, en el
  minuto estándar de lo que sea que esté haciendo en ese momento. Contar
  también el tiempo de horno duplicaría el costo de esos minutos.
- **Se captura por lote típico, no por pieza directamente.** Carolina no
  hace la división mental — captura "cuántas personas, cuántos minutos
  activos, cuántas piezas salen de un lote típico" y el sistema calcula
  minutos-persona por pieza. Si el lote involucra a 2 personas 20 minutos
  para 30 piezas, son 40 minutos-persona ÷ 30 piezas.
- **Sin calibrar ≠ costo cero ni costo inventado.** Igual que "sin
  clasificar" hoy, si una receta/grupo no tiene minutos capturados en un
  área, esa área no aporta costo a esa receta ese día y tampoco cuenta en
  el denominador del área (para no distorsionar el minuto de las demás
  recetas que sí están calibradas). Se muestra explícitamente como "sin
  calibrar" en el reporte.
- **Se incluye capacidad ociosa en minutos en esta misma entrega.**
  Confirmado por Mauricio (opción recomendada). Con minutos reales se puede
  comparar `minutos_demandados_área_día` contra
  `minutos_disponibles_área_día` (personas del área ese día × minutos de
  turno estándar) y mostrar % de aprovechamiento — el dato que motivó el
  proyecto desde el inicio ("revelar si hay más capacidad para producir y
  no se está aprovechando").
- **Convive con lo ya deployado.** No se reemplaza `RecetaAreaProduccion`
  ni `CostoManoObraDiarioArea`, se extienden. Tampoco se toca el cálculo
  mensual de `services_nomina_produccion.py` (PR #846).

## Requisitos exactos

### `reportes/models.py` (editar modelos existentes + 1 modelo nuevo)

- **`RecetaAreaProduccion`** — agregar 3 campos nullable:
  `lote_personas` (PositiveSmallIntegerField), `lote_minutos`
  (DecimalField), `lote_piezas` (PositiveSmallIntegerField). Property
  `minutos_estandar_pieza` calculada como
  `(lote_personas * lote_minutos) / lote_piezas` cuando los 3 están
  presentes, si no `None` ("sin calibrar"). No se guarda como campo
  aparte — se deriva siempre de los 3 valores capturados, para que no haya
  inconsistencia entre "lo que Carolina capturó" y "lo que el sistema usa".
- **`FamiliaGrupoManoObra`** (nuevo, reemplaza
  `reportes/mano_obra_grupos_familia.py` como fuente de verdad):
  `familia_real` (CharField, unique — el texto exacto que viene de
  `Receta.familia`/`Insumo.categoria`), `grupo` (CharField — el grupo
  canónico que agrupa varias familias reales; por default igual a
  `familia_real`). Migración de datos: seed automático de toda familia
  real distinta ya vista en `Receta`/`Insumo`, con `grupo=familia_real`
  excepto las 2 fusiones ya confirmadas (Pastel, Betún/Rellenos), que se
  siembran ya fusionadas para no perder esa decisión de negocio.
- **`CostoManoObraDiarioArea`** — agregar `minutos_demandados` (Decimal,
  null), `minutos_disponibles` (Decimal, null), `costo_minuto` (Decimal,
  null — reemplaza conceptualmente a `costo_unidad`, que ya no aplica
  porque el costo real varía por receta según sus minutos, no es una tasa
  única por área). `unidades_producidas` se conserva como dato informativo
  de contexto en pantalla, ya no como divisor del costo.

### `reportes/mano_obra_grupos_familia.py` (reescribir)

`grupo_de_familia()`/`familias_del_grupo()` dejan de leer el diccionario
`GRUPOS_FAMILIA_MANO_OBRA` fijo y consultan `FamiliaGrupoManoObra` en su
lugar. Firma de las funciones no cambia — los call sites en
`services_mano_obra_diaria_area.py` no se tocan por este cambio.

### `reportes/services_mano_obra_diaria_area.py` (editar)

- `unidades_area_dia()` se reemplaza por `minutos_area_dia(fecha, area)`:
  para cada receta/insumo producido ese día cuyo grupo esté clasificado a
  esa área Y tenga `minutos_estandar_pieza` calculado, suma
  `produced_quantity * minutos_estandar_pieza`. Recetas/insumos sin
  calibrar no participan en la suma (ver "sin calibrar" arriba).
- `calcular_costo_diario_area()`: `costo_minuto = nomina_diaria_area /
  minutos_area_dia` si `minutos_area_dia > 0`, si no `None` (no forzado).
  Además calcula `minutos_disponibles` = número de empleados clasificados
  en esa área (mismo criterio que `area_produccion_empleado`, vigentes en
  el período) × minutos de turno estándar (constante, 480 = 8h — mismo
  nivel de aproximación ya aceptado para el prorrateo 6/7, documentado
  explícitamente como tal).
- `costo_mano_obra_diario_receta(fecha, receta)`: por cada área que le
  corresponde a la receta (vía grupo), si tiene `minutos_estandar_pieza`
  para esa receta en esa área Y la área tiene `costo_minuto` calculable
  ese día, suma `minutos_estandar_pieza * costo_minuto_area`. Si a la
  receta le falta calibración en alguna de sus áreas, se declara
  `sin_calibrar` para esa área (no se omite en silencio, ni se sustituye
  con 0).

### Pantalla de clasificación (`reportes/templates/reportes/mano_obra_area_clasificacion.html` + vista)

- Cada tarjeta de grupo (ya existente) gana, por cada área marcada, 3
  campos: personas del lote, minutos activos del lote, piezas que salen —
  con el `minutos_estandar_pieza` calculado mostrado como referencia
  ("≈ 1.33 min/pieza"), no editable directamente.
- Acción nueva en la tarjeta: "Fusionar con otro grupo" — Carolina escribe
  o selecciona el grupo destino, y el sistema actualiza `grupo` en
  `FamiliaGrupoManoObra` para esa familia real (y re-agrupa la vista).
- Nota de transparencia ya existente ("Incluye de Point: ...") se conserva
  tal cual.

### Reporte diario (`reportes/templates/reportes/mano_obra_area_reporte.html` + vista)

- Se agrega, por área y por día, % de aprovechamiento
  (`minutos_demandados / minutos_disponibles`) y minutos ociosos
  (`minutos_disponibles - minutos_demandados`, mínimo 0). Si
  `minutos_disponibles` no se puede calcular (sin empleados clasificados
  ese período), se omite el % en vez de mostrar un número falso.

## Edge cases

- Receta/grupo clasificado en un área pero sin minutos capturados: "sin
  calibrar" para esa área — no aporta costo, no participa en el
  denominador de esa área ese día.
- Familia real fusionada por Carolina después de que ya había minutos
  capturados en la familia real original (no en el grupo): al fusionar,
  los minutos capturados quedan asociados a la fila `RecetaAreaProduccion`
  del grupo anterior; si el grupo destino ya tenía sus propios minutos
  capturados, prevalecen los del grupo destino (evita promediar
  automáticamente 2 calibraciones distintas sin que Carolina lo revise).
- Grupo con más de una familia real fusionada que tienen procesos
  ligeramente distintos en la práctica (ej. "Betún y Rellenos" vs "Betún,
  Cremas, Rellenos"): fuera de alcance distinguir minutos por familia real
  dentro de un mismo grupo — un grupo fusionado comparte un solo minuto
  estándar. Si en el futuro se nota que dos familias fusionadas en realidad
  toman tiempos muy distintos, la solución es separarlas de grupo (deshacer
  la fusión), no capturar minutos por familia real dentro del grupo.
- `minutos_disponibles` asume que todos los empleados clasificados del área
  trabajan el turno completo ese día — no descuenta incapacidades/permisos
  individuales (mismo nivel de aproximación que el prorrateo 6/7 ya
  documentado y aceptado). Ceiling conocido: si hay ausentismo real no
  capturado, el % de aprovechamiento se ve artificialmente bajo.

## Fuera de alcance (explícito)

- Captura de tiempo real diario por empleado o por lote real (opción C
  descartada explícitamente por Mauricio: "no sé cómo lo podamos hacer de
  manera que se sienta simple para el personal y no tan confuso"). Los
  minutos son un estándar de referencia calibrado por Carolina, no un
  cronómetro de cada lote real.
- Niveles de complejidad (Simple/Medio/Complejo) como alternativa a minutos
  exactos — se descartó a favor de minutos-persona por lote.
- Trazabilidad de lotes/BOM entre áreas (qué lote específico de Hornos se
  usó en qué lote específico de Armado) — el costeo sigue siendo agregado
  por día y área, no por lote físico individual.
- Asistencia real día por día para `minutos_disponibles` — usa la misma
  aproximación de headcount clasificado × turno estándar, no un calendario
  real de quién se presentó ese día.
- Historial de cambios en la fusión de grupos (quién fusionó qué y cuándo)
  — la tabla `FamiliaGrupoManoObra` no lleva auditoría de cambios en esta
  entrega.

## Definición de "hecho"

- [ ] Migración: 3 campos nuevos en `RecetaAreaProduccion`, modelo nuevo
      `FamiliaGrupoManoObra` con seed de datos (incluyendo las 2 fusiones
      ya confirmadas), 3 campos nuevos en `CostoManoObraDiarioArea`.
- [ ] `mano_obra_grupos_familia.py` reescrito para leer de
      `FamiliaGrupoManoObra` en vez del diccionario fijo.
- [ ] `minutos_area_dia()`, `calcular_costo_diario_area()` con
      `costo_minuto`/`minutos_disponibles`, `costo_mano_obra_diario_receta()`
      actualizados.
- [ ] Pantalla de clasificación: captura de lote (personas/minutos/piezas)
      por área, acción de fusionar grupo.
- [ ] Reporte diario: % de aprovechamiento y minutos ociosos por área.
- [ ] Tests: cálculo de `minutos_estandar_pieza` desde lote; receta sin
      calibrar no aporta costo ni distorsiona el denominador; fusión de
      grupo vía UI persiste y re-agrupa la clasificación; `minutos_area_dia`
      combina receta+insumo igual que antes; % de aprovechamiento con y sin
      `minutos_disponibles` calculable.
- [ ] `python manage.py check` y `migrate --check` en 0.
- [ ] Regresión: `git stash -u` baseline diff contra suite `reportes`
      completa, cero fallas nuevas.
- [ ] Validado contra un día real: comparar manualmente el costo por
      receta calculado (nómina real ÷ minutos reales de Point ese día)
      contra una cuenta hecha aparte, para al menos una receta con 2+
      áreas.
