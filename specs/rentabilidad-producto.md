# Spec: Rentabilidad por Producto (rebanada 1 de "Consejo Estratégico de IA")

## Origen y alcance

Este es el primer recorte de un proyecto más grande ("Consejo Estratégico de
IA": dashboard ejecutivo, rentabilidad por producto, rentabilidad por
sucursal, simulador financiero y comité de IA con 9 roles). Decisiones ya
tomadas con Mauricio, no reabrir:

- **Stack:** Django/DRF/Postgres del ERP actual. Cero Node/FastAPI/React/Next
  nuevos — el documento original los proponía para un proyecto greenfield,
  pero aquí ya existe el monolito con los datos reales.
- **Esta rebanada = solo "Rentabilidad por producto".** Dashboard general,
  rentabilidad por sucursal, simulador y el comité de IA quedan para specs
  futuros independientes.
- **Ubicación:** nueva pestaña dentro de `monitor_margenes_precio_sugerido`
  (`recetas/views/recetas.py:6706`), no una pantalla nueva. Mismo RBAC
  (`can_view_recetas`), mismo catálogo fuente de verdad (Point activo/precio
  vigente), misma lógica de etiquetado de fuente de costo.
- **Mano de obra de producción automática es un spec y una rama aparte:**
  [mano-obra-produccion-automatica.md](mano-obra-produccion-automatica.md).
  Esta vista de rentabilidad no depende de esa automatización para
  funcionar — muestra lo que haya en cada momento (`MP_FALLBACK` o
  `FAB_COMPLETO`) etiquetado con honestidad. Cuando la otra rama se
  implemente y se corra `snapshot_operating_finance`, más productos migran
  solos a `FAB_COMPLETO` sin tocar esta vista.

## Objetivo

Dar a Dirección un ranking real de qué producto deja más utilidad, usando
únicamente datos que el ERP ya calcula o ya tiene almacenados en alguna
tabla — nunca un número inventado o estimado sin fuente.

## Qué YA existe y se reutiliza (verificado en código, no en memoria vieja)

- `reportes.ProductoCostoOperativoMensual`: costo_mp_unit, mano_obra_prod_unit,
  indirecto_prod_unit, empaque_prod_unit, costo_fabricacion_unit, asp,
  unidades_base — por receta y periodo.
- `reportes.ProductoSucursalContribucionMensual`: costo_producto_unit,
  gasto_comercial_unit/total, contribucion_unit/total,
  margen_contribucion_pct — por receta, sucursal y periodo.
  **Confirmado en `reportes/services_operating_finance.py`:** el
  `gasto_comercial_unit` YA incluye, prorrateados, los buckets
  `COMERCIAL_SUCURSAL` (que agrupa "Plataformas y comisiones" — o sea la
  comisión bancaria) y `LOGISTICA`. **No hay que agregar campos nuevos para
  comisión bancaria ni costo logístico: ya están adentro de
  `gasto_comercial_unit`.** Lo único que hace falta es desglosarlos en la
  vista (ver "Requisitos").
- `recetas.RecetaCostoHistoricoMensual` / `RecetaCostoVersion`: fallback de
  costo materia-prima-solo cuando no hay fabricación completa (ya usado por
  `monitor_margenes_precio_sugerido`).
- `control.MermaMensualSucursal.costo_merma`: costo de merma real, por
  `receta_id` + `sucursal` + `periodo`. **Confirmado: hoy NO se une a
  `ProductoSucursalContribucionMensual`** (se usa en `control`, `proyecciones`
  y en el dashboard de charts, pero nunca en el cálculo de contribución por
  producto). Es la única pieza real que falta conectar — no hay que inventar
  el dato, ya existe, solo falta el join.
- Etiquetado de fuente (`FAB_COMPLETO` / `MP_FALLBACK` / `REVENTA_HISTORICO`
  / `SIN_COSTO`) y márgenes meta (65% MP_FALLBACK, 55% FAB_COMPLETO/reventa):
  reutilizar tal cual, no reinventar.

## Requisitos exactos

1. **Nueva pestaña "Rentabilidad" en `monitor_margenes_precio_sugerido`**
   (mismo template/vista, mismo query param de familia/meses ya soportado).
2. Por cada producto activo de Point (mismo universo que la vista actual),
   calcular y mostrar:
   - Precio de venta vigente (ya existe: `precio_por_sku`).
   - Costo de fabricación por unidad, con su fuente etiquetada (ya existe).
   - Desglose de `gasto_comercial_unit`: mostrar el total y, si el dato lo
     permite, el desglose por bucket (comisión/plataformas vs logística vs
     otro comercial) usando `CategoriaGasto.BUCKET_*`. Si el detalle por
     bucket no está disponible a nivel producto (solo a nivel empresa/mes),
     mostrar el total y declarar explícitamente "desglose no disponible a
     nivel producto" — no prorratear a mano un número que no existe.
   - **Merma unitaria:** unir `control.MermaMensualSucursal` por
     `receta_id` + periodo (sumando sucursales si la vista es agregada;
     dividir entre `unidades_base` para obtener costo de merma por unidad).
     Si no hay filas de merma para ese producto/periodo, mostrar 0 y no
     "SIN_DATO" (merma en 0 es un valor válido, a diferencia de costo en 0
     que sí sería sospechoso).
   - Margen bruto = precio − costo_fabricacion_unit.
   - Margen de contribución = `contribucion_unit` / `margen_contribucion_pct`
     ya calculados, **restando también la merma unitaria** (que hoy no está
     restada ahí) para llegar a una utilidad estimada por unidad más
     honesta que la actual.
   - Utilidad estimada por unidad = margen de contribución − merma unitaria.
3. **Ranking:** ordenar por `contribucion_total` del periodo (utilidad total
   aportada, no solo % margen) descendente por default — responde
   directamente "qué producto deja más utilidad real". Debe poder
   reordenarse por `margen_contribucion_pct` como columna secundaria (mismo
   patrón de sort_by/sort_dir que ya usan los listados del ERP).
4. **Exportable:** reutilizar el mecanismo `export=csv|xlsx` que ya tiene
   `monitor_margenes_precio_sugerido`.
5. **Regla de no inventar datos:** cualquier campo sin fuente real
   (mano de obra/indirecto en 0, desglose de gasto comercial no disponible a
   nivel producto, etc.) se muestra etiquetado como tal, nunca como un
   número simulado.

## Edge cases

- Producto con `costo_fabricacion_unit` = 0 en todas las fuentes → fuente
  `SIN_COSTO`, excluir del ranking de "más rentables" pero listar aparte
  como "sin costo calculado" (mismo patrón que la vista actual probablemente
  ya maneja para SIN_COSTO — confirmar en implementación).
- Producto con merma pero sin ventas en el periodo (`unidades_base` = 0) →
  no dividir por cero; mostrar merma total pero costo de merma unitario como
  "no aplica".
- Multi-sucursal: `ProductoSucursalContribucionMensual` es por sucursal; la
  vista de ranking es a nivel empresa, así que hay que sumar/ponderar entre
  sucursales igual que ya hace el resto de `monitor_margenes_precio_sugerido`
  para venta y precio (revisar cómo agrega hoy antes de duplicar lógica).
- Addons (sabores): igual que hoy, se combinan en su base, no aparecen como
  fila propia en el ranking.
- Producto reventa (`REVENTA_HISTORICO`): no tiene `gasto_comercial_unit` de
  producción propia necesariamente — verificar que el join de merma y
  contribución también aplique o declarar "no aplica" si el modelo de
  reventa no tiene esas filas.

## Fuera de alcance (explícito)

- Poblar mano de obra / indirectos de producción — ver
  [mano-obra-produccion-automatica.md](mano-obra-produccion-automatica.md).
- Rentabilidad por sucursal, dashboard ejecutivo, simulador financiero,
  comité de IA de 9 roles (specs futuros independientes).
- Agregar campos nuevos de comisión bancaria o costo logístico: ya existen
  dentro de `gasto_comercial_unit`, no se duplican.
- Nueva infraestructura, nuevo stack, nuevas tablas de proyecto estratégico
  genéricas (`strategic_projects`, `scenario_simulations`, etc. del doc
  original) — no aplican a esta rebanada.

## Definición de "hecho"

- [ ] Pestaña "Rentabilidad" visible en `monitor_margenes_precio_sugerido`
      para usuarios con `can_view_recetas`.
- [ ] Ranking ordenado por `contribucion_total` desc, con utilidad estimada
      por unidad incluyendo merma real (no solo margen de contribución
      actual).
- [ ] Cada fila muestra la fuente de costo (`FAB_COMPLETO`/`MP_FALLBACK`/
      `REVENTA_HISTORICO`/`SIN_COSTO`) y, si aplica, qué componente falta.
- [ ] Export CSV/XLSX funcional igual que el resto de la vista.
- [ ] `python manage.py check` y `migrate --check` en 0.
- [ ] Tests: al menos un test de la vista/ranking cubriendo producto con
      fabricación completa, producto MP_FALLBACK, producto con merma real,
      y producto SIN_COSTO.
- [ ] Validado en navegador real (sesión con `can_view_recetas`): la pestaña
      carga, el ranking ordena correcto, el export descarga.
