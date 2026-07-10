# Spec: Calibrar Productos (Receta) por sabor/receta específica

## Origen y relación con el resto del proyecto

Cuarta vuelta sobre la mano de obra diaria por área. Mauricio preguntó cómo
se relaciona el costo con "un solo sabor de pan o pay" — al explicarle el
cálculo con un ejemplo numérico, quedó claro que **hoy Productos (Receta)
se calibra por familia completa** (ej. "PAN"), no por sabor específico: dos
sabores distintos de pan, si comparten familia, comparten el mismo minuto
estándar salvo que se agregue una excepción manual por ID de receta. Es
exactamente el mismo problema que ya se corrigió para Catálogos
(`specs/mano-obra-unidad-preparacion-insumo.md`, PR #871) — ahí cada
preparación de insumo se calibra por separado. Confirmado por Mauricio:
extender el mismo patrón a Productos.

## Decisión de alcance (confirmada explícitamente)

Se **reemplaza por completo** el mecanismo de familia de receta — no
convive con el nuevo. Esto incluye:
- El modelo `FamiliaGrupoManoObra` y el módulo
  `reportes/mano_obra_grupos_familia.py` (`grupo_de_familia`/
  `familias_del_grupo`) dejan de usarse y se eliminan.
- La acción `fusionar_grupo` y su datalist en la pantalla de clasificación
  se eliminan (reemplazadas por `fusionar_producto`, mismo patrón que
  `fusionar_insumo`).
- El mecanismo de excepción por receta (`RecetaAreaProduccion.receta` FK,
  acciones `agregar_excepcion`/`quitar_excepcion`, tabla "Excepciones por
  receta") se elimina — queda redundante porque ahora **toda** receta se
  clasifica individualmente por default (autocontenida, fusionable), no
  solo las que necesitaban una excepción.
- El dato informativo "unidades producidas" del reporte diario también
  migra al nuevo mecanismo (por receta/insumo específico), no al de
  familia — unifica el modelo mental en un solo lugar.

Verificado antes de proceder: **0 filas de excepción por receta y 0
fusiones de familia con impacto operativo real** en producción (solo
Pastel/Betún, que se documentan aquí como referencia y se siembran
directo como `grupo_mano_obra` en la migración de datos). No hay riesgo
de pérdida de datos de negocio real.

## Decisiones tomadas en esta vuelta (no reabrir)

- **`Receta` gana `grupo_mano_obra`** (CharField, blank, default=""),
  exactamente como `Insumo.grupo_mano_obra` (PR #871). En blanco = "su
  grupo es su propio `nombre`".
- **`RecetaAreaProduccion.receta` (FK) se elimina** — junto con su
  `CheckConstraint` (`rap_familia_xor_receta`) y su `UniqueConstraint`
  (`rap_receta_area_unico`). Toda fila de esta tabla, para Productos o
  Catálogos, ahora tiene la misma forma: `familia` (texto del grupo) +
  `area` + `es_grupo_insumo` (distingue si el grupo se resuelve contra
  `Receta.grupo_mano_obra`/`nombre` o `Insumo.grupo_mano_obra`/`nombre`).
- **Se extrae un helper genérico** para resolver minutos por grupo,
  compartido entre Productos y Catálogos — la lógica de
  `_grupos_insumo_por_area()` (PR #871) es estructuralmente idéntica a lo
  que necesita Productos, solo cambia el modelo (`Receta` vs `Insumo`).
- **El dato informativo `unidades_area_dia`** también se recalcula vía el
  nuevo mecanismo (mismo grupo que ya resuelve los minutos), no hace falta
  mantener una resolución de membresía separada.
- **La pantalla de clasificación pierde la sección "Excepciones por
  receta"** — ya no aplica, cada receta se maneja igual que una
  preparación de Catálogo.
- **Migración de datos**: siembra `Receta.grupo_mano_obra` para las 2
  fusiones ya confirmadas anteriormente (Pastel Chico/Grande/Mediano/Mini
  → `grupo_mano_obra="Pastel"`; recetas con familia "Betún y Rellenos" →
  `grupo_mano_obra="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"`) para no
  perder esas 2 decisiones de negocio ya tomadas. El resto de recetas
  queda con `grupo_mano_obra=""` (autocontenida, cada una su propio
  grupo) — Carolina decide después si quiere fusionar más.

## Requisitos exactos

### `recetas/models.py` (editar)

`Receta` — agregar campo:
```python
grupo_mano_obra = models.CharField(max_length=250, blank=True, default="")
```

### `reportes/models.py` (editar)

`RecetaAreaProduccion`:
- Eliminar el campo `receta` (FK) y sus constraints
  (`rap_familia_xor_receta`, `rap_receta_area_unico`).
- `familia` deja de ser `blank=True` en la práctica (siempre poblado para
  cualquier fila real), pero se mantiene `blank=True, default=""` a nivel
  de esquema por simplicidad (sin fila alguna debería quedar así).
- La `UniqueConstraint(["familia", "area", "es_grupo_insumo"])` (PR #871)
  se mantiene igual.

`FamiliaGrupoManoObra` — se elimina el modelo (migración con
`RemoveModel` en la misma migración que los cambios de arriba).

### `reportes/mano_obra_grupos_familia.py` — se elimina el archivo

Ningún call site debe seguir importando `grupo_de_familia`/
`familias_del_grupo` al terminar esta vuelta.

### `reportes/services_mano_obra_diaria_area.py` (editar)

- Nuevo helper genérico, reemplaza a `_recetas_minutos_por_area()` y
  `_grupos_insumo_por_area()` (PR #871) con una sola función parametrizada:
  ```python
  def _minutos_por_grupo(area: str, es_grupo_insumo: bool, modelo) -> dict[int, Decimal]:
      resultado: dict[int, Decimal] = {}
      for fila in RecetaAreaProduccion.objects.filter(
          area=area, familia__gt="", es_grupo_insumo=es_grupo_insumo
      ):
          minutos = fila.minutos_estandar_pieza
          if minutos is None:
              continue
          ids = modelo.objects.filter(
              Q(grupo_mano_obra=fila.familia) | Q(grupo_mano_obra="", nombre=fila.familia)
          ).values_list("id", flat=True)
          for obj_id in ids:
              resultado[obj_id] = minutos
      return resultado
  ```
  `minutos_area_dia()` la llama dos veces:
  `_minutos_por_grupo(area, False, Receta)` y
  `_minutos_por_grupo(area, True, Insumo)`.
- `_familias_reales_clasificadas()`, `_recetas_ids_por_area()`,
  `_insumo_ids_por_area()` — se **eliminan** (dependían de
  `FamiliaGrupoManoObra`/`Receta.familia`/`Insumo.categoria`).
- `unidades_area_dia()` — se reescribe para sumar `produced_quantity` de
  `PointProductionLine` filtrando por los mismos ids que ya resuelve
  `_minutos_por_grupo` (receta_ids = claves del dict de recetas,
  insumo_ids = claves del dict de insumos), sin distinguir si están
  calibrados o no — para eso, resolver membresía por separado de minutos
  usando el mismo helper pero sin filtrar por `minutos_estandar_pieza`
  (variante `_ids_por_grupo(area, es_grupo_insumo, modelo)` que retorna
  `set[int]` de todos los ids clasificados, calibrados o no).
- `costo_mano_obra_diario_receta(fecha, receta)`: se simplifica —
  ya no hay `excepcion_filas`, se resuelve directo:
  ```python
  grupo = receta.grupo_mano_obra or receta.nombre
  filas_por_area = {
      fila.area: fila
      for fila in RecetaAreaProduccion.objects.filter(familia=grupo, es_grupo_insumo=False)
  }
  ```

### `reportes/views_mano_obra_area.py` (editar)

- `toggle_familia`/`capturar_lote`: eliminar la rama que llama
  `grupo_de_familia()` — con `es_grupo_insumo=False` o `True`, el texto
  posteado en `familia` ya ES el grupo canónico (resuelto en el template
  igual que Catálogos hoy). Ambas ramas quedan idénticas salvo el
  booleano — se puede colapsar a una sola rama sin `if/else` por tipo.
- Eliminar acciones `agregar_excepcion`/`quitar_excepcion`.
- Nueva acción `fusionar_producto` (idéntica a `fusionar_insumo`, sobre
  `Receta` en vez de `Insumo`):
  ```python
  elif accion == "fusionar_producto":
      grupo_actual = request.POST.get("grupo_actual", "").strip()
      grupo_destino = request.POST.get("grupo_destino", "").strip()
      if grupo_actual and grupo_destino:
          Receta.objects.filter(
              Q(grupo_mano_obra=grupo_actual) | Q(grupo_mano_obra="", nombre=grupo_actual)
          ).update(grupo_mano_obra=grupo_destino)
  ```
- Eliminar acción `fusionar_grupo`.
- GET: la sección "Productos" se reconstruye igual que "Catálogos" (PR
  #871), pero desde `Receta.objects.exclude(familia="")` (se mantiene el
  filtro de que sea una receta real de venta, no una preparación interna
  — revisar si `Receta.tipo == TIPO_PRODUCTO_FINAL` debe ser parte del
  filtro; hoy la pantalla ya mostraba recetas con `familia` sin filtrar
  por tipo, mantener ese comportamiento para no reducir cobertura sin
  pedirlo). Agrupar por `receta.grupo_mano_obra or receta.nombre`.
- Eliminar el bloque de "Excepciones por receta" del contexto.

### `reportes/templates/reportes/mano_obra_area_clasificacion.html` (editar)

- La sección "Productos" se reescribe con la misma estructura que
  "Catálogos" (tarjeta por grupo, toggle de área, captura de lote, botón
  "Fusionar con:" apuntando a `fusionar_producto`, datalist propio
  `grupos-producto-existentes`). Se conserva el desplegable "Ver los N
  productos" (PR #875, si ya está mergeado para entonces) mostrando los
  nombres reales agrupados.
- Se elimina la sección completa "Excepciones por receta" (tabla +
  formulario "Agregar excepción").

## Edge cases

- Receta sin `familia` (no aplica a este flujo — la pantalla ya excluía
  estas): sin cambios, se mantiene fuera del alcance de clasificación.
- Receta fusionada a un grupo cuyos miembros tienen procesos distintos:
  responsabilidad de quien fusiona, igual que en Catálogos — sin
  validación automática en esta entrega.
- Migración: recetas con `familia` en {"Pastel Chico", "Pastel Grande",
  "Pastel Mediano", "Pastel Mini"} → `grupo_mano_obra="Pastel"`; recetas
  con `familia="Betún y Rellenos"` → `grupo_mano_obra="Betún, Cremas,
  Rellenos (INSUMO PRODUCIDO)"`. El resto queda con `grupo_mano_obra=""`.

## Fuera de alcance (explícito)

- Historial/auditoría de fusiones (quién fusionó qué y cuándo) — ni
  Catálogos ni Productos lo llevan en esta entrega.
- Validación de que una fusión no mezcle procesos distintos.
- Cambiar el criterio de qué recetas aparecen en la pantalla (se
  mantiene el filtro actual: cualquier `Receta` con `familia` no vacía).
- Tocar `unidad_detectada` para Productos — ese concepto (detectar kg/lt/
  pza automático) solo aplica a Catálogos, donde se confirmó que la
  unidad es consistente por preparación; los Productos ya se miden en
  piezas de venta, sin ambigüedad de unidad.

## Definición de "hecho"

- [ ] Migración: `Receta.grupo_mano_obra`; eliminar `RecetaAreaProduccion.receta`
      + sus 2 constraints; eliminar modelo `FamiliaGrupoManoObra`; seed de
      datos para las 2 fusiones ya confirmadas (Pastel, Betún).
- [ ] Eliminar `reportes/mano_obra_grupos_familia.py` y su archivo de tests
      (`tests_mano_obra_grupos_familia.py`).
- [ ] `_minutos_por_grupo()` genérico (reemplaza `_recetas_minutos_por_area`
      y `_grupos_insumo_por_area`); `_ids_por_grupo()` genérico para
      `unidades_area_dia()`; `costo_mano_obra_diario_receta()` simplificado.
- [ ] Vistas: `fusionar_producto`, eliminar `agregar_excepcion`/
      `quitar_excepcion`/`fusionar_grupo`, sección "Productos" reconstruida
      como "Catálogos".
- [ ] Template: sección "Productos" con el patrón de Catálogos, sin
      "Excepciones por receta".
- [ ] Tests: reescribir/actualizar todos los tests que dependían de
      `FamiliaGrupoManoObra`/excepciones por receta (múltiples archivos
      existentes); nuevos tests de fusión de recetas específicas y de que
      dos sabores distintos de la misma familia ya NO comparten minuto por
      default.
- [ ] `python manage.py check` y `migrate --check` en 0.
- [ ] Regresión: `git stash -u` baseline diff contra suite `reportes`
      completa, cero fallas nuevas.
- [ ] Validado localmente (RequestFactory o servidor local, ver memoria
      `verificacion-local-limites`): pantalla de clasificación renderiza
      "Productos" y "Catálogos" con el mismo patrón, capturar un lote por
      receta específica y ver el cálculo reflejado en el reporte diario.
