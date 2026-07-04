# Spec: Calibración de mano de obra por preparación de insumo (unidad real)

## Origen y relación con el resto del proyecto

Tercera vuelta sobre la mano de obra diaria por área. `specs/mano-obra-minutos-estandar.md`
(deployado, PR #870) introdujo minutos-persona activos por lote, capturados
una vez por **grupo de familia** (ej. "Pastel", "Betún, Cremas, Rellenos").
Mauricio, al revisar el resultado, preguntó cómo se iba a diferenciar
piezas de kg/lt en la captura — pregunta que reveló un problema real no
contemplado en esa vuelta.

## Problema encontrado

`PointProductionLine.unit` (`pos_bridge/models/movements.py`) es texto
libre poblado directo de Point (`"PZA"`, `"U"`, `"KG"`, `"Litro"`).
`produced_quantity` es un número plano sin esa unidad — y
`minutos_area_dia()` (PR #870) suma `produced_quantity` de TODAS las
recetas/insumos clasificados a un área, sin distinguir unidad. Verificado
con datos reales de producción:

- Distribución global: `PZA` 3,913 líneas, `U` 3,424 (mismo concepto que
  PZA, solo otra etiqueta de Point), `KG` 866, `Litro` 356.
- El problema está concentrado: 84% de las líneas en KG/Litro caen en el
  grupo `"Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"`.
- **Confirmado por Mauricio y verificado en datos**: dentro de ese mismo
  grupo, la unidad es **consistente por preparación específica, no
  aleatoria**. Ej.: "Betún Dream Whip Pastel" siempre `KG` (151 líneas),
  "Mezcla 3 Leches" siempre `Litro` (151 líneas), "Batida pay de queso"
  siempre `Litro` (144 líneas). Son procesos distintos (unos se pesan,
  otros se miden por volumen) que hoy comparten el mismo grupo de
  calibración de mano de obra — y por lo tanto el mismo (e incorrecto)
  minuto-por-unidad.

Total: 58 preparaciones de insumo distintas con producción real (de 221
`Insumo` con `tipo_item=INSUMO_INTERNO` registrados), un número manejable
para calibrar una por una.

## Decisiones tomadas en esta vuelta (no reabrir)

- **Las recetas (productos terminados: Pastel, Pay, Bollo, etc.) NO
  cambian.** Confirmado que sus variantes de tamaño comparten proceso —
  siguen calibrándose por grupo de familia, como en PR #870.
- **Los insumos (preparaciones internas) se calibran por preparación
  específica, no por categoría.** Cada preparación (`Insumo` individual,
  ej. "Betún Dream Whip Pastel") tiene su propio lote/minutos capturado,
  no comparte número con otras preparaciones de la misma `categoria`.
- **Se reutiliza el mecanismo de fusión ya construido (PR #870)** para que
  Carolina pueda seguir agrupando variantes de tamaño de una misma
  preparación (ej. "Pan Vainilla Dawn Chico/Grande/Mediano/Mini", que
  comparten proceso salvo por tamaño) — mismo patrón, namespace separado
  del de familias de receta para evitar colisiones de texto.
- **La unidad se detecta automáticamente de la producción real ya
  registrada, nunca se le pregunta a Carolina.** Como la unidad es
  consistente por preparación (verificado arriba), se lee de
  `PointProductionLine.unit` para ese `Insumo` y se muestra como etiqueta
  en la captura de lote ("kg", "litros"). Es puramente informativo para
  la pantalla — el cálculo simplemente multiplica lo producido × minutos
  del lote, confiando en que la agrupación ya está hecha al nivel correcto
  (una preparación o sus variantes de tamaño, nunca mezclando procesos con
  unidades distintas).
- **Preparación sin producción histórica no se puede calibrar todavía** —
  no hay de dónde inferir la unidad, así que no aparece como tarjeta
  calibrable hasta que tenga al menos un registro de producción real.

## Requisitos exactos

### `maestros/models.py` (editar — más simple que extender `FamiliaGrupoManoObra`)

- **`Insumo`** — agregar `grupo_mano_obra` (CharField, blank, default="").
  Nombre del grupo canónico de calibración de mano de obra para este
  insumo — en blanco significa "su propio grupo es su propio `nombre`"
  (autocontenido, igual patrón de fallback que `grupo_de_familia()`).
  Se prefiere esto sobre extender `FamiliaGrupoManoObra` (que obligaría a
  un discriminador `tipo` + una constraint de unicidad más compleja para
  evitar que un `Insumo.nombre` choque por texto con una
  `Receta.familia`) porque `Insumo` ya tiene identidad estable por FK —
  no hace falta resolver por texto en absoluto ni arriesgar colisiones de
  namespace.

### `reportes/models.py` (editar)

- **`RecetaAreaProduccion`** — agregar `es_grupo_insumo` (BooleanField,
  default `False`). Una fila con `familia` poblado y
  `es_grupo_insumo=True` representa un grupo de preparación de insumo
  (el texto en `familia` se resuelve contra `Insumo.grupo_mano_obra`/`nombre`,
  no contra `Receta.familia`), en vez de un grupo de familia de receta
  (comportamiento existente, `es_grupo_insumo=False`). La
  `UniqueConstraint(["familia", "area"])` existente se amplía a
  `["familia", "area", "es_grupo_insumo"]`. No se toca la fila de
  excepción por receta (`receta` poblado) — sigue igual.

### `reportes/mano_obra_grupos_familia.py` (sin cambios)

Sigue resolviendo únicamente `Receta.familia`/`Insumo.categoria` →
grupo de familia, exactamente como en PR #870 — la agrupación de
preparaciones de insumo vive en `Insumo.grupo_mano_obra` directamente, un
mecanismo separado y más simple (ver arriba), no una extensión de este
módulo.

### `reportes/services_mano_obra_diaria_area.py` (editar)

- Nueva función `_grupos_insumo_por_area(area) -> dict[int, Decimal]`:
  para cada fila `RecetaAreaProduccion.objects.filter(area=area,
  es_grupo_insumo=True, familia__gt="")`, resuelve qué `Insumo` pertenecen
  a ese grupo (`Insumo.objects.filter(Q(grupo_mano_obra=fila.familia) |
  Q(grupo_mano_obra="", nombre=fila.familia))` — el fallback captura los
  insumos autocontenidos que no han sido fusionados) y mapea
  `insumo_id -> minutos_estandar_pieza` de esa fila.
- `_insumos_minutos_por_area()` (la de PR #870, basada en `categoria`) se
  **reemplaza** por la nueva función basada en preparación — ya no se usa
  `Insumo.categoria` para calibración de minutos (sigue existiendo el
  campo, pero deja de ser la fuente de agrupación de mano de obra para
  insumos). `_insumo_ids_por_area()` (la de membresía simple, para el dato
  informativo `unidades_producidas`) tampoco cambia — sigue siendo por
  categoría para ese propósito informativo únicamente.
- `minutos_area_dia()` usa `_grupos_insumo_por_area()` en vez de
  `_insumos_minutos_por_area()` para el componente de insumos.

### Pantalla de clasificación

- Nueva sección "Preparaciones de insumo" (paralela a "Familias de
  receta"), construida desde `Insumo.objects.filter(tipo_item=TIPO_INTERNO)`
  con producción real, agrupadas por `grupo_mano_obra` (o su propio
  `nombre` si está en blanco). Cada tarjeta: unidad real detectada (ej.
  "kg", leída de `PointProductionLine.unit` para ese insumo), toggle de
  área, captura de lote (mismo patrón que PR #870, ahora con la unidad
  real en el placeholder/label en vez de asumir "piezas"), botón de
  fusionar.
- La acción POST `capturar_lote` gana un parámetro `es_grupo_insumo` para
  marcar la fila `RecetaAreaProduccion` correspondiente.
- Nueva acción POST `fusionar_insumo` (paralela a `fusionar_grupo`, que
  sigue intacta para familias de receta): recibe `insumo_id` y
  `grupo_destino`, hace `Insumo.objects.filter(id=insumo_id).update(
  grupo_mano_obra=grupo_destino)` — no toca `FamiliaGrupoManoObra` en
  absoluto.

## Edge cases

- Preparación con múltiples `unit` distintos en su propia historia (no
  detectado en los datos reales revisados, pero si ocurriera): se toma la
  unidad más frecuente para la etiqueta de pantalla; el cálculo no se ve
  afectado porque igual sigue multiplicando produced_quantity × minutos
  del lote sin conversión — si de verdad una preparación cambia de unidad
  con el tiempo, es una señal de que Point la está registrando mal, no
  algo que este sistema deba adivinar a resolver.
- Preparación fusionada a un grupo cuyas piezas base tienen unidades
  distintas entre sí (ej. fusionar por error "Betún Dream Whip" (KG) con
  "Mezcla 3 Leches" (Litro)): no se valida automáticamente en este MVP —
  es responsabilidad de quien fusiona (Carolina) no mezclar preparaciones
  con procesos/unidades distintas. Se podría agregar una advertencia
  visual en una vuelta futura si se vuelve un problema recurrente.
- `Insumo` sin ninguna línea de producción histórica: no aparece como
  tarjeta calibrable (no hay de dónde inferir la unidad) — no es un bug,
  es la misma honestidad de "sin datos, no se inventa nada".

## Fuera de alcance (explícito)

- Conversión automática entre unidades (ej. litros↔kg por densidad) — no
  se necesita, porque la agrupación ya está pensada para nunca mezclar
  preparaciones de unidades distintas bajo un mismo grupo.
- Validación automática de que una fusión no mezcle unidades distintas —
  queda como responsabilidad humana en esta entrega.
- Tocar el modelo de clasificación de recetas — sigue exactamente igual
  que en PR #870.
- Recalcular retroactivamente reportes históricos con la nueva
  granularidad — el cambio aplica hacia adelante en cuanto se calibren las
  preparaciones de insumo.

## Definición de "hecho"

- [ ] Migración: campo `grupo_mano_obra` en `Insumo`; campo
      `es_grupo_insumo` en `RecetaAreaProduccion` + constraint de
      unicidad ampliada (`["familia", "area", "es_grupo_insumo"]`).
- [ ] `_grupos_insumo_por_area()` nueva, reemplaza a
      `_insumos_minutos_por_area()` en `minutos_area_dia()`.
- [ ] Pantalla: sección "Preparaciones de insumo" con captura de lote
      (unidad real detectada) y acción `fusionar_insumo`.
- [ ] Tests: unidad detectada correctamente por preparación; grupo de
      insumo fusionado (vía `grupo_mano_obra`) agrega correctamente sus
      minutos; una familia de receta y una preparación de insumo con el
      mismo texto en `familia` no colisionan gracias a `es_grupo_insumo`;
      preparación sin producción histórica no aparece calibrable.
- [ ] `python manage.py check` y `migrate --check` en 0.
- [ ] Regresión: `git stash -u` baseline diff contra suite `reportes`
      completa, cero fallas nuevas.
- [ ] Validado contra datos reales: comparar el costo de "Betún, Cremas,
      Rellenos" antes (un solo grupo) vs después (por preparación) para un
      día real, confirmando que ya no mezcla KG con Litro en el mismo
      cálculo.
