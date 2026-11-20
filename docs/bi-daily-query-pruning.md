# Poda de la consulta mensual en BI diario

El constructor diario usa `get_dashboard_sales_dataset(daily_only=True)` y
`months=0`. Su SQL conserva el corte de `latest_closed_sales_date` y el máximo
histórico anterior; no debe agregar trabajo mensual para producir una lista vacía.

La EXPLAIN productiva de 20/11/2026 sobre `89d7efb8`, corte 19/11 y `months=0`,
estima 1,000 filas para `generate_series` aunque sus límites den una serie vacía.
El subplan mensual concentra el coste estimado y el plan incluye 146 funciones
JIT. No se ejecutó ANALYZE: esto no mide tiempo de compilación ni acredita lecturas
mensuales realizadas. Evidencia archivada: `reader.jsonl`, SHA256
`00cd34676971a85ad6e0932f132e3ab26d91162aa8d1d62aeeca495903a84b19`.

Se añade una sola condición `months > 0` a `monthly_rows`. La condición en el
bloque mensual completo elimina su unión y su fuente del plan diario.

Se reutilizan las tablas de ventas diarias, indicadores, corte y mensual oficial;
no hay captura, equivalencia, columna, índice ni contrato nuevo. Dashboard, BI,
monitores y guardas de publicación conservan el mismo DTO. No cambian el corte,
el historial, las sedes, los nulos, los ceros, la fuente canónica ni la caché.

La regresión compara planes reales de PostgreSQL: la fuente mensual se excluye
para cero meses y permanece para un mes. La paridad local contra `52eaf161`
compara 44 resultados completos y planes mensuales positivos iguales. Las pruebas
incluyen corte nulo, huecos históricos, sede inactiva/nula y ausencia de ventas.
El PostgreSQL nativo usado no dispone de JIT; sus tiempos submilisegundo no
demuestran una mejora representativa. Antes de aceptar la entrega, falta medir
la carga real con caché vacía y validar cifras, filtros y fecha en el ERP.
