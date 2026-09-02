# API de cierre mensual de producto Point

Los endpoints `GET /api/pos-bridge/product-closures/` y
`GET /api/pos-bridge/product-closures/{id}/` conservan los campos históricos y
añaden la proyección canónica `POINT_PRODUCT_BALANCE_V1`.

## Semántica de cantidades

- Los decimales disponibles se serializan con seis posiciones.
- `null` significa que la fuente no estaba disponible o no tenía autoridad; no
  equivale a cero.
- `0.000000` significa cero medido por una fuente disponible.
- Una fuente `source_present=true` pero `authoritative=false` produce `null` en
  todos sus campos dependientes; sólo una fuente autoritativa puede publicar cero.
- En metadata canónica antigua, un flag de autoridad ausente se interpreta como
  no autoritativo y el estado se publica como `REVISAR_FUENTE`.
- Un total mensual es `null` cuando al menos una línea tiene ese dato faltante.

## Campos canónicos por línea

`opening_point`, `opening_source`, `production`, `sales_direct`, `sales_derived`,
`sales_total`, `waste_total`, `point_conversion_in`, `point_conversion_out`,
`calculated_closing`, `closing_point_cedis`, `closing_point_sucursales`,
`closing_point`, `point_difference`, `point_status`, `point_status_label`,
`conversion_origin`, `conversion_origins`, `projection_sources`,
`source_authority`, `source_issues` e `is_historical_inventory`.

`source_authority` sólo expone la etiqueta de fuente, presencia, autoridad,
estado/incidencias, identificadores de jobs y conteos/fechas de cobertura. No
proyecta URLs de solicitud, rutas de reportes ni muestras crudas persistidas.

Para importaciones históricas, `closing_point*` y `point_difference` son `null`
porque el conteo no proviene de Point. `historical_count` y
`historical_difference` separan explícitamente el conteo físico del cálculo. El
conteo importado puede existir aunque `historical_difference` sea `null`: la
diferencia solo se publica cuando ventas, producción, merma y conversiones tienen
autoridad mensual comprobada. Los decimales almacenados por compatibilidad cuando
falta una fuente son placeholders internos y no se exponen como cero. Sus totales
son `total_historical_count` y `total_historical_difference`.

`historical_excel_import` identifica archivo/hoja únicamente como procedencia del
conteo físico. Las etiquetas de movimientos nombran por separado las tablas
operativas consultadas y las marcan como observadas no validadas; no atribuyen al
Excel ventas, producción o merma que el archivo no importó.

`point_difference` siempre usa el signo `Point final - final calculado`. Los
cierres importados de Excel conservan su semántica histórica y la identifican
con `is_historical_inventory=true`.

## Campos de resumen y procedencia

Además de los totales existentes, el resumen expone `total_direct_sales`,
`total_derived_sales`, `total_conversion_in`, `total_conversion_out`,
`total_closing_point`, `total_point_difference`, `source_authority` y
`source_issues`. `source_authority` contiene la evidencia compactada de opening,
ventas, producción, merma, conversiones y closing; `source_issues` reúne las
guardas de autoridad sin duplicados.
