# Plan: poblar costo de fabricación completo para Precio Sugerido

## Contexto y hallazgo

El módulo **Precio sugerido** (dentro de Monitor de Márgenes) debe calcular el precio
con **costo de fabricación completo** = materia prima + mano de obra de producción +
indirectos de producción + empaque, leído de
`reportes.ProductoCostoOperativoMensual.costo_fabricacion_unit`.

**Auditoría en producción (junio 2026) — el dato hoy NO existe:**

| Fuente de costo | Cobertura | Contenido real |
| --- | --- | --- |
| `ProductoCostoOperativoMensual.costo_fabricacion_unit` | 18 productos (abr), 14 (may), 0 (jun) | **Materia prima sola**: `mano_obra_prod_unit`, `indirecto_prod_unit`, `empaque_prod_unit` = 0 en todos |
| `RecetaCostoVersion.costo_total` | 201 recetas (134 producto final) | **Materia prima sola**: `costo_mo` y `costo_indirecto` = 0 (no hay `CostoDriver` cargado) |
| `RecetaCostoHistoricoMensual.costo_total` | ~53–74 productos/mes | Materia prima |
| `ProductoReventaCostoHistoricoMensual.costo_promedio` | 48 productos | Costo de adquisición de reventa (OK) |

Además, **las 17 bases con sabores (addons) tienen `costo_fabricacion_unit = 0`**
(el costo quedó volcado en el addon), y junio no tiene snapshot generado.

**Conclusión:** mano de obra, indirectos y empaque de producción **no están cargados**.
Mientras eso siga así, el panel opera en modo `MP_FALLBACK` (margen sobre materia prima),
etiquetado con honestidad, y NO finge fabricación completa.

## Objetivo del plan

Poblar correctamente, por producto y mes, en `ProductoCostoOperativoMensual`:
- `costo_mp_unit` (ya parcial)
- `mano_obra_prod_unit`
- `indirecto_prod_unit`
- `empaque_prod_unit`
- `costo_fabricacion_unit` = suma de los anteriores

…para que el panel pase automáticamente de `MP_FALLBACK` a `FAB_COMPLETO`.

## Flujo oficial existente (no inventar capas nuevas)

1. **Gasto operativo de sucursal (OPEX)** → `GastoOperativoMensual`
   - Comando: `python manage.py import_branch_real_operating_expenses --file <xlsx> --year 2026`
   - Guía: `docs/GUIA_CARGA_MENSUAL_GASTO_OPERATIVO_2026.md`
   - Alimenta gasto comercial/corporativo del P&L (`EmpresaResultadoMensual`).

2. **Costos de producción (MO / indirectos / empaque)** → componentes por producto
   - Comando: `python manage.py import_production_operating_expenses` (revisar firma real y plantilla).
   - Define las tarifas/montos de mano de obra, indirectos y empaque de producción que
     luego se distribuyen por producto.

3. **Snapshot mensual que distribuye a producto** → `ProductoCostoOperativoMensual`
   - Comando: `python manage.py snapshot_operating_finance --period YYYY-MM`
   - Toma materia prima (receta) + costos de producción y los reparte por producto/volumen,
     produciendo `costo_fabricacion_unit` con sus componentes.

## Tareas pendientes (a detallar con finanzas/producción)

### 1. Asignación de MO / indirectos / empaque por producto
- Definir base de prorrateo: por producto, por familia Point, por volumen (unidades) o por
  tiempo de producción. Documentar la regla elegida.
- Cargar las tarifas/montos vía `import_production_operating_expenses` para los meses de la
  ventana de análisis (al menos los últimos 12 meses que el panel puede pedir).

### 2. Corregir bases con addons en $0
- Hoy el snapshot deja la base (p. ej. `Pay de Queso Grande`) en `costo_fabricacion_unit = 0`
  y pone el costo en el sabor (addon). Revisar la lógica de
  `reportes/services_operating_finance.py` para que la base reciba su costo de fabricación
  y el sabor sume sólo su incremento (ver `RecetaAgrupacionAddon`).
- Validar que la suma base + sabores no duplique ni deje huecos.

### 3. Regenerar snapshots
- Correr `snapshot_operating_finance --period YYYY-MM` para cada mes de la ventana
  (3/6/12 meses) una vez cargados MO/indirectos/empaque.
- Generar el mes corriente cuando cierre.

### 4. Validación contra producción real
- Verificar que `costo_fabricacion_unit` > materia prima para productos con mano de obra real.
- Confirmar que los componentes (`mano_obra_prod_unit`, `indirecto_prod_unit`,
  `empaque_prod_unit`) son > 0 donde corresponde.
- Cruzar el total de costo de fabricación contra `EmpresaResultadoMensual.costo_fabricacion_total`.
- Verificar en el panel que esos productos cambian de etiqueta `MP_FALLBACK` → `FAB_COMPLETO`.

## Criterio de aceptación

- El panel muestra `FAB_COMPLETO` para los productos con costo de fabricación real cargado.
- El margen y el precio sugerido de esos productos usan `costo_fabricacion_unit` (no MP).
- Ningún producto activo con receta/costo queda como `SIN_COSTO` por falta de snapshot.
- Las bases con sabores ya no aparecen en $0.

## Estado actual (interino)

Hasta completar este plan, el panel calcula **precio sugerido sobre costo disponible**:
`FAB_COMPLETO` si el operativo trae componentes reales; si no, `MP_FALLBACK` (materia prima,
rotulado); `REVENTA_HISTORICO` para reventa; `SIN_COSTO` si no hay ninguna fuente.
