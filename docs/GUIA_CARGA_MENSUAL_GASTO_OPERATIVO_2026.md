# Guía Operativa: Carga Mensual de Gasto Operativo 2026

## Objetivo

Dejar un flujo mensual estable para que finanzas y operación carguen `GastoOperativoMensual` real por sucursal, refresquen los proyectos de expansión y mantengan confiables los snapshots, el health score y el simulador.

## Canal oficial

- Ruta web oficial: `/reportes/gastos-operativos/importar/`
- Template oficial: [output/spreadsheet/gastos_operativos_mensuales/plantilla_carga_gasto_operativo_mensual_2026.xlsx](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/output/spreadsheet/gastos_operativos_mensuales/plantilla_carga_gasto_operativo_mensual_2026.xlsx)
- Comando alterno para operación interna o carpeta monitoreada:
  - `python manage.py import_branch_real_operating_expenses --file /ruta/archivo.xlsx --year 2026`
  - `python manage.py process_operating_expense_inbox --dir /ruta/inbox --year 2026`

## Formato oficial del archivo

### Archivo esperado

- Tipo: `XLSX`
- Hoja obligatoria: `GastosSucursal`
- Año operativo permitido actual: `2026`

### Columnas obligatorias

| Columna | Tipo esperado | Regla |
| --- | --- | --- |
| `sucursal` | texto | Debe coincidir con un código o nombre real de sucursal del ERP |
| `periodo` | fecha ISO | Formato `YYYY-MM-01`; el día siempre debe ser `01` |
| `monto` | decimal | Sin símbolo `$`, sin comas de texto, sin celdas vacías |

### Columnas opcionales

| Columna | Tipo esperado | Regla |
| --- | --- | --- |
| `tipo_dato` | texto | Recomendado: `REAL`. Si llega `PRESUPUESTO`, se ignora o se rechaza según política |
| `categoria_gasto` | texto | Código válido de categoría sucursal. Si se omite, se usa `OPEX_TOTAL_SUC` |
| `comentario` | texto | Nota libre para trazabilidad |
| `external_key` | texto | Llave única opcional. Si se omite, el sistema genera una determinística |

### Ejemplos válidos de fila

Estos ejemplos son sólo de formato, no datos reales de operación:

| sucursal | periodo | monto | tipo_dato | categoria_gasto | comentario |
| --- | --- | ---: | --- | --- | --- |
| `MATRIZ` | `2026-04-01` | `12345.67` | `REAL` | `OPEX_TOTAL_SUC` | `Cierre validado por finanzas` |
| `COLOSIO` | `2026-05-01` | `9800` | `REAL` | `` | `Importe total de sucursal` |

## Reglas de validación reales del importador

Basadas en [reportes/services_branch_real_operating_expense_import.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/services_branch_real_operating_expense_import.py):

1. `sucursal` debe existir en catálogo ERP y tener centro de costo configurado.
2. `periodo` debe venir en `YYYY-MM-01`.
3. El año debe coincidir con el objetivo de carga actual, hoy `2026`.
4. `monto` debe ser numérico válido.
5. `tipo_dato` debe resolverse como `REAL`.
6. `categoria_gasto`, si viene, debe existir en catálogo de categorías sucursal.
7. No se permiten duplicados dentro del mismo archivo para la combinación:
   - sucursal
   - periodo
   - categoría
   - tipo_dato
8. No se permite `external_key` duplicada.
9. No se puede mezclar `OPEX_TOTAL_SUC` con detalle categorizado para la misma sucursal y periodo.
10. Si el archivo es válido, el proceso refresca automáticamente los proyectos de expansión afectados.

## Diagnóstico de archivos reales revisados

### 1. `PRESUPUESTO DE GASTOS VENTAS diciembre 2025.xlsx`

- Clasificación: `fuente útil de transformación histórica`, no carga directa
- Motivo:
  - estructura matricial por sucursal
  - mezcla `PRESUPUESTADO` y `REAL`
  - corresponde a `2025`
  - no trae columnas `sucursal`, `periodo`, `monto`

### 2. `PRESUPUESTO_2026_LINKED_AREAS_Y_CONSOLIDADO/AREA_VENTAS_GASTOS_2026.xlsx`

- Clasificación: `presupuesto / planeación`
- Motivo:
  - hoja matricial
  - orientado a consolidado presupuestal
  - no sirve para carga directa de gasto real mensual

### 3. `PRESUPUESTO_2026_OPCION_A_AREAS_Y_CONSOLIDADO/01_Gastos_Ventas_2026.xlsx`

- Clasificación: `fuente potencial de captura`, no carga directa
- Motivo:
  - mantiene estructura por sucursal
  - muestra columnas de presupuesto y espacios de real
  - sigue sin el layout tabular requerido por el importador

## Qué sí sirve hoy

- Un archivo tabular nuevo, mensual, preparado con la plantilla oficial.
- Un archivo transformado previamente a ese layout a partir de una fuente real validada.

## Qué no debe subirse

- Presupuestos
- Consolidados anuales
- Hojas matriciales por sucursal
- Archivos mezclando `REAL` y `PRESUPUESTO`
- Archivos de `2025` intentando alimentar operación `2026`

## Proceso operativo mensual recomendado

### Paso 1. Fuente de origen

- Responsable sugerido: Finanzas / administración de sucursales.
- Fuente de verdad: cierre mensual real de gasto por sucursal.
- El dato debe estar ya conciliado como `REAL`.

### Paso 2. Preparación del archivo

1. Abrir la plantilla oficial.
2. Capturar un renglón por sucursal y periodo.
3. Usar `periodo` con día `01`.
4. Marcar `tipo_dato=REAL`.
5. Si no se tiene desglose por categoría, dejar vacío `categoria_gasto` para usar `OPEX_TOTAL_SUC`.

### Paso 3. Validación previa

Checklist manual antes de subir:

1. Confirmar que todas las sucursales existen en ERP.
2. Confirmar que todos los periodos son del mes correcto y del año `2026`.
3. Confirmar que no hay presupuesto mezclado.
4. Confirmar que no se repite una misma sucursal-periodo-categoría.
5. Confirmar que el archivo tiene hoja `GastosSucursal`.

### Paso 4. Carga al ERP

- Canal oficial para el equipo: `/reportes/gastos-operativos/importar/`
- Subir sólo un archivo ya validado.

### Paso 5. Revisión del resultado

Después de cargar, revisar en el historial:

1. `status = SUCCESS`
2. `loaded_rows`
3. `affected_branches`
4. `covered_periods`
5. `project_refresh_count`

### Paso 6. Confirmación de impacto

Verificar después en el módulo de expansión:

1. snapshots actualizados del periodo cargado
2. `gastos_operativos` del mes ya presentes
3. mejora de `expense_coverage_status`
4. recalculo de flujo libre, health score y clasificación

## Impacto directo en el módulo de expansión

Cuando la carga mensual real opera correctamente:

- los snapshots dejan de depender de huecos mensuales
- mejora la calidad del cálculo de `flujo_libre`
- mejora el `health_score`
- la clasificación `EXPANDIR / VIGILAR / RIESGO` deja de castigar por faltantes
- el simulador usa bases históricas más confiables
- la calibración futura se vuelve defendible frente a dirección

## Riesgos operativos a evitar

1. Cargar presupuesto como si fuera real.
2. Cargar un periodo de año incorrecto.
3. Duplicar el mismo archivo o la misma combinación sucursal-periodo-categoría.
4. Subir una hoja matricial sin transformar.
5. Intentar “rellenar” meses faltantes con datos de otro año.
6. Mantener marzo 2026 sin gasto real, porque sigue degradando la preparación para calibración.

## Estado actual y conclusión

### Qué ya existe

- importador robusto
- historial de cargas
- detección de duplicados
- refresh automático de proyectos
- trazabilidad en audit log
- template oficial generado

### Qué sigue bloqueando hoy la operación mensual formal

- el equipo aún no produce el archivo mensual en layout tabular compatible
- falta gasto real de marzo 2026
- siguen existiendo fuentes matriciales/presupuestales que no pueden subirse directamente

### Conclusión operativa

Sí puede arrancar la operación mensual formal si finanzas adopta desde ya la plantilla oficial y deja de usar archivos matriciales como carga directa.

Mientras no exista ese archivo mensual tabular real:

- el importador seguirá funcionando técnicamente,
- pero el proceso mensual seguirá incompleto desde negocio.
