# Reporte de asistencia: rendimiento Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminar el producto cruzado de asistencias e incidencias y permitir que la pantalla y los archivos del reporte terminen en pocos segundos.

**Architecture:** La selección de empleados usará `Exists` correlacionados en lugar de joins reversos con `DISTINCT`. La respuesta HTML evaluará una vez el catálogo y lo reutilizará; las exportaciones conservarán filtrado directo en PostgreSQL.

**Tech Stack:** Django 5, PostgreSQL 16, Django TestCase, openpyxl.

---

### Task 1: Proteger la forma eficiente del queryset

**Files:**
- Modify: `rrhh/tests_reporte_asistencia.py`
- Modify: `rrhh/views_asistencia.py:13-67`

- [ ] **Step 1: Escribir la prueba fallida del SQL**

Agregar una prueba que convierta `_empleados_reporte_asistencia(fecha, fecha).query`
a texto y compruebe que contiene `EXISTS`, pero que la consulta exterior no contiene
`LEFT OUTER JOIN` hacia `rrhh_asistenciaempleado` ni `rrhh_incidenciaasistencia`.

- [ ] **Step 2: Ejecutar la prueba y confirmar RED**

Run: `python manage.py test rrhh.tests_reporte_asistencia.ReporteAsistenciaTests.test_query_empleados_evitar_join_cruzado --noinput`

Expected: `FAIL` porque el SQL actual contiene los joins reversos y no usa `EXISTS`.

- [ ] **Step 3: Implementar el cambio mínimo**

Importar `Exists`; crear subconsultas de `AsistenciaEmpleado` e
`IncidenciaAsistencia` correlacionadas por `empleado_id=OuterRef("pk")` y rango de
fechas; anotar ambos booleanos y filtrar por `activo`, `tiene_asistencia` o
`tiene_incidencia`, sin `distinct()`.

- [ ] **Step 4: Ejecutar la prueba y confirmar GREEN**

Run: `python manage.py test rrhh.tests_reporte_asistencia.ReporteAsistenciaTests.test_query_empleados_evitar_join_cruzado --noinput`

Expected: `OK`.

### Task 2: Reutilizar el catálogo y preservar filtros

**Files:**
- Modify: `rrhh/tests_reporte_asistencia.py`
- Modify: `rrhh/views_asistencia.py:176-526`

- [ ] **Step 1: Escribir la prueba fallida de una sola evaluación**

Parchear `_empleados_reporte_asistencia` con `wraps`, solicitar la vista HTML y
comprobar que la función se invoca una sola vez. Verificar además que el contexto
mantiene el empleado esperado.

- [ ] **Step 2: Ejecutar la prueba y confirmar RED**

Run: `python manage.py test rrhh.tests_reporte_asistencia.ReporteAsistenciaTests.test_vista_reutiliza_catalogo_empleados --noinput`

Expected: `FAIL` con dos invocaciones.

- [ ] **Step 3: Implementar la reutilización mínima**

Determinar `export` antes de construir el reporte. Para HTML, evaluar el queryset
con `select_related("sucursal_ref", "jefe_directo__usuario_erp")`, pasar esa lista
a `_build_reporte_asistencia` y usarla en el contexto. Para CSV/XLSX, conservar el
queryset filtrado dentro del constructor. Aplicar sobre la lista los filtros por
ID y sucursal cuando corresponda.

- [ ] **Step 4: Ejecutar la prueba y confirmar GREEN**

Run: `python manage.py test rrhh.tests_reporte_asistencia.ReporteAsistenciaTests.test_vista_reutiliza_catalogo_empleados --noinput`

Expected: `OK`.

### Task 3: Verificación integral y entrega

**Files:**
- Test: `rrhh/tests_reporte_asistencia.py`
- Verify: `rrhh/views_asistencia.py`

- [ ] **Step 1: Ejecutar pruebas enfocadas**

Run: `python manage.py test rrhh.tests_reporte_asistencia --noinput`

Expected: 12 pruebas, `OK`.

- [ ] **Step 2: Ejecutar verificaciones Django**

Run: `python manage.py migrate --check && python manage.py check`

Expected: ambos comandos con código 0 y sin errores.

- [ ] **Step 3: Medir el queryset local**

Crear datos representativos en la base de pruebas o usar `QuerySet.explain()` para
confirmar que el plan contiene semi-joins/subplanes correlacionados y no el producto
cruzado exterior.

- [ ] **Step 4: Revisar, commitear y abrir PR**

Revisar `git diff --check`, el diff completo y el estado; commitear solo los cuatro
archivos de alcance, solicitar revisión de código, subir la rama y abrir un PR.

- [ ] **Step 5: CI, merge, deploy y validación real**

Esperar CI verde, mergear a `main`, ejecutar `scripts/deploy_web_safe.sh` en el VPS
sin `git pull` previo, medir el rango predeterminado y descargar CSV/XLSX con una
cuenta autorizada. Confirmar tipo de contenido, tamaño no vacío y tiempo total.
