# Spec: Mano de obra de producción automática (fuente real de nómina)

## Origen y relación con otras piezas

Este spec nace de revisar a fondo el hueco descrito en
`docs/PLAN_COSTO_FABRICACION_COMPLETO_PRECIO_SUGERIDO.md` mientras se
preparaba [rentabilidad-producto.md](rentabilidad-producto.md) (rebanada 1 de
"Consejo Estratégico de IA"). Es una **rama y PR separados**: toca datos
financieros de producción, no la vista de rentabilidad. La vista de
rentabilidad simplemente se beneficia de que, al implementarse esto, más
productos pasen de `MP_FALLBACK` a `FAB_COMPLETO`.

## Hallazgo (verificado en código, no en memoria vieja)

La lógica de prorrateo YA funciona (`reportes/services_operating_finance.py:697-779`):
lee `GastoOperativoMensual` por categoría/centro de costo, reparte por unidades
producidas (`ReglaAsignacionGasto.BASE_UNIDADES`) y llena
`ProductoCostoOperativoMensual.mano_obra_prod_unit` /
`indirecto_prod_unit`. **El motor no está roto.**

El hueco es el **insumo**: `GastoOperativoMensual` (categoría `MANO_OBRA_PROD`,
centro de costo `PROD`) hoy solo se llena por dos vías, ambas manuales:

1. `import_production_operating_expenses --file "PRESUPUESTO NOMINA/PRODUCCIÓN
   2026 AUTORIZADO.xlsx"` (`reportes/services_production_expense_import.py`):
   parsea un workbook de finanzas concepto por concepto (SUELDO, IMSS,
   AGUINALDO, etc. → `MANO_OBRA_PROD`; UNIFORMES/GORRA/MANDIL →
   `INDIRECTO_PROD`), año fiscal 2026 hardcodeado.
2. `importar_nomina_real --archivo <csv>` (`reportes/management/commands/importar_nomina_real.py`):
   CSV manual `area,periodo,concepto,monto`, todo a `MANO_OBRA_PROD`.

Ninguna de las dos lee `rrhh.NominaLinea`, que ya tiene el dato real cargado
por Capital Humano cada periodo de nómina. Por eso el costo de mano de obra
depende de que alguien vuelva a capturar a mano lo que el ERP ya sabe.

## Decisión (confirmada con Mauricio)

Construir una tercera fuente, **automática**, que reemplaza a las dos
manuales para `MANO_OBRA_PROD`/`PROD` hacia adelante:

- `mano_obra_prod_total(periodo)` = Σ `rrhh.NominaLinea.total_percepciones`
  de líneas cuyo `empleado.departamento == Empleado.DEP_PRODUCCION`.
- **Nivel de detalle aceptado:** todo `total_percepciones` cuenta como mano
  de obra (no se separan uniformes/gorra/mandil como indirecto — el monto es
  marginal frente al total y no cambia el costo de fabricación total, solo
  la etiqueta del componente).
- Los **indirectos de producción** (agua, luz, gas, mantenimiento, etc.)
  **no cambian**: siguen viniendo del import de OPEX general
  (`import_branch_real_operating_expenses`), porque no existen en nómina.

## Requisitos exactos

1. Nuevo servicio (`reportes/services_nomina_produccion.py` o función dentro
   de `services_operating_finance.py`, decidir en fase de implementación
   cuál ensucia menos el archivo) que, dado un periodo calendario
   (año-mes):
   - Selecciona `rrhh.NominaPeriodo` cuyo **`fecha_fin` cae dentro del mes**
     objetivo (regla de mapeo periodo→mes: nómina se atribuye al mes en que
     cierra/paga, no en que empieza).
   - Filtra `estatus` en `{CERRADA, PAGADA}` — excluye `BORRADOR` (no
     finalizada, no confiable para reportes).
   - Suma `total_percepciones` de `NominaLinea` cuyo `empleado.departamento
     == DEP_PRODUCCION`.
   - Hace `GastoOperativoMensual.objects.update_or_create` con
     `external_key` en un namespace propio y distinguible, p. ej.
     `f"NOMINA_PROD_AUTO|{periodo:%Y-%m}"`, `categoria_gasto=MANO_OBRA_PROD`,
     `centro_costo=PROD`, `tipo_dato=REAL`, `fuente=IMPORTADA`.
2. **Evitar doble conteo (crítico, es dinero real):** antes de escribir la
   fila automática de un periodo, borrar cualquier fila previa de
   `GastoOperativoMensual` con `categoria_gasto=MANO_OBRA_PROD`,
   `centro_costo=PROD`, `periodo=ese mes`, que **no** sea del namespace
   `NOMINA_PROD_AUTO|...` (es decir, limpiar residuos de los dos pipelines
   manuales anteriores para ese mes antes de insertar el valor automático).
   Esto puede borrar filas reales cargadas en abril/mayo 2026 por el import
   manual — **confirmar con Mauricio antes de correrlo contra producción**,
   aunque el dato que reemplaza es equivalente (mismo total de nómina real,
   solo que ahora se calcula en vez de teclearse).
3. Nuevo management command, ej. `sync_mano_obra_produccion --periodo
   YYYY-MM` (o `--desde`/`--hasta` para un rango), que llama al servicio.
   Debe poder correr en modo `--dry-run` (mostrar el monto calculado sin
   escribir) antes de aplicarlo en producción.
4. Después de correr este comando, sigue siendo necesario correr
   `snapshot_operating_finance --period YYYY-MM` (sin cambios ahí) para que
   el monto llegue a `ProductoCostoOperativoMensual`.
5. Los dos comandos manuales (`import_production_operating_expenses` con el
   Excel de nómina, `importar_nomina_real`) **dejan de usarse para mano de
   obra de producción** hacia adelante. No se borra el código (puede seguir
   usándose para indirectos vía el Excel de producción), pero se documenta
   que no hay que volver a correr la hoja de nómina de ese Excel.

## Edge cases

- Periodo de nómina que cruza fin de mes (ej. quincena 26-may al 10-jun):
  se atribuye completo al mes de `fecha_fin` (junio), no se prorratea entre
  dos meses. Simplifica mucho y es consistente con "cuando se paga".
- Mes sin ninguna `NominaPeriodo` CERRADA/PAGADA todavía (mes en curso,
  nómina aún en borrador): el comando debe reportarlo explícitamente
  ("0 periodos cerrados para este mes") y no escribir un monto en 0 que se
  confunda con "no hay mano de obra" — mejor no tocar `GastoOperativoMensual`
  para ese mes hasta que haya al menos un periodo cerrado.
- Empleados de Producción dados de baja a mitad de periodo: ya están
  incluidos o excluidos por el propio dato de `NominaLinea` (no hay lógica
  extra que inventar aquí, se usa lo que ya calculó nómina).
- Re-ejecución del comando para un mes ya procesado: debe ser idempotente
  (mismo `external_key` → `update_or_create`, no duplica).

## Fuera de alcance

- Separar uniformes/gorra/mandil como indirecto (decisión explícita: no
  vale la pena el detalle).
- Tocar `bono_extra`, `ajuste_positivo`, `ajuste_negativo` u otros campos de
  captura de nómina real — este spec solo LEE `NominaLinea.total_percepciones`,
  nunca escribe en `rrhh`.
- Automatizar indirectos de producción (agua/luz/gas) — siguen viniendo del
  import de OPEX general, sin cambios.
- La vista de rentabilidad por producto (spec separado:
  [rentabilidad-producto.md](rentabilidad-producto.md)).

## Definición de "hecho"

- [ ] Servicio + comando `sync_mano_obra_produccion` implementados, con
      `--dry-run`.
- [ ] Limpieza de filas manuales previas del mismo mes antes de insertar la
      automática, sin afectar otros meses/categorías.
- [ ] Test que cubre: mes con periodos CERRADA/PAGADA reales → monto
      correcto; mes solo con BORRADOR → no escribe nada; re-ejecución →
      no duplica (idempotente); mes con residuo de import manual previo →
      se reemplaza, no se suma.
- [ ] `python manage.py check` y `migrate --check` en 0 (no debería requerir
      migración nueva, reutiliza `GastoOperativoMensual`/`CategoriaGasto`
      existentes — confirmar en implementación).
- [ ] Corrida real en un mes de prueba (ej. el mes más reciente cerrado) y
      verificación manual: el monto calculado por el comando coincide con
      Σ `total_percepciones` de Producción de ese mes consultado aparte.
- [ ] Después de correr `snapshot_operating_finance` para ese mes, el panel
      `monitor_margenes_precio_sugerido` muestra esos productos como
      `FAB_COMPLETO` en vez de `MP_FALLBACK`.
