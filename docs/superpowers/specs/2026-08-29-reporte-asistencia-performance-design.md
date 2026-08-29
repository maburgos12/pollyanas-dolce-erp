# Diseño: rendimiento del reporte de asistencia

## Problema

El reporte de asistencia localiza empleados activos o con actividad mediante dos
relaciones reversas unidas con `OR` y remueve duplicados con `DISTINCT`. PostgreSQL
materializa combinaciones de asistencias e incidencias antes de deduplicar. En
producción, el plan estimó 344,976 filas intermedias para 69 empleados y la
construcción completa superó 90 segundos.

## Contrato que se conserva

- Los usuarios y permisos existentes no cambian.
- Se incluyen empleados activos y empleados dados de baja con asistencia o
  incidencia dentro del rango.
- Se conserva la fecha de la última baja mostrada en el filtro.
- Los filtros por empleado, sucursal y fechas conservan su comportamiento.
- La pantalla y los archivos CSV/XLSX conservan columnas, contenido y nombres.
- No se modifican modelos, migraciones ni datos operativos.

## Solución

`_empleados_reporte_asistencia` anotará dos expresiones correlacionadas `EXISTS`:
una para asistencias y otra para incidencias dentro del rango. El filtro usará
`activo OR tiene_asistencia OR tiene_incidencia`; así cada empleado permanece una
sola vez y deja de ser necesario `DISTINCT`.

El queryset cargará también `sucursal_ref` y la relación del jefe que ya consume
el reporte. Para la respuesta HTML, la lista de empleados candidatos se evaluará
una vez y se reutilizará tanto en el selector como al construir el reporte. Las
exportaciones seguirán construyendo solo el conjunto filtrado y no renderizarán
HTML.

## Manejo de filtros

La función que construye el reporte aceptará opcionalmente una lista de empleados
ya evaluada. Cuando exista esa lista, aplicará en memoria los mismos filtros de
identificador y texto legacy de sucursal. Cuando no exista (CSV/XLSX), conservará
el filtrado en PostgreSQL para no cargar empleados ajenos al archivo solicitado.

## Pruebas y aceptación

- Una prueba de regresión inspeccionará el SQL del queryset: debe contener
  subconsultas `EXISTS` y no debe unir directamente las tablas de asistencias e
  incidencias en la consulta exterior.
- Las pruebas existentes deben conservar activos, bajas con actividad, exclusión
  de bajas sin actividad, permisos y CSV.
- Una prueba verificará que la vista HTML evalúa una sola vez el catálogo de
  empleados.
- `manage.py check` y `migrate --check` deben terminar sin errores.
- En producción se medirá el mismo rango de 14 días y se descargará un XLSX real.

## Riesgos y mitigaciones

El riesgo principal es omitir una baja histórica o alterar un filtro. Se mitiga
con las pruebas funcionales existentes y una prueba específica del SQL. No se
aumentará el timeout: el objetivo es eliminar el trabajo innecesario en la base.
