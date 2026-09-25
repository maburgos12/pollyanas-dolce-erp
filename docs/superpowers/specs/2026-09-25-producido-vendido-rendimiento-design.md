# Producido vs Vendido: carga mensual bajo demanda

## Objetivo

La pantalla debe abrir el mes solicitado sin recorrer todos los snapshots históricos de Point ni calcular costos que no se usan. El periodo actual seguirá mostrando movimientos cerrados hasta el día anterior; al seleccionar otro mes, el balance consultará únicamente ese intervalo.

## Diseño aprobado

1. `ProducidoVsVendidoMermaView._available_periods` dejará de ejecutar `DATE_TRUNC DISTINCT` sobre `PointInventorySnapshot`. El selector se formará con evidencia mensual compacta y con el periodo seleccionado. Los snapshots crudos continuarán consultándose dentro del servicio canónico solamente para las fechas de apertura y cierre del mes solicitado.
2. La vista pedirá costos solo para recetas con merma distinta de cero. Para las demás filas el costo de merma es exactamente cero, por lo que calcular toda la receta no aporta información.
3. El calculador compartido de costos evitará resolver dos veces la identidad de una preparación y reutilizará el costo ya resuelto cuando varios insumos apuntan a la misma receta interna. No cambiará la prioridad vigente: BOM primero, costo `POINT_PRODUCTION_REPORT` como respaldo y costo canónico de compra para insumos sin preparación.
4. El HTML, los filtros, exportaciones, totales, lenguaje Point y permisos permanecerán iguales. No se agregará caché de datos mensuales que pueda ocultar sincronizaciones recientes.

## Contratos y riesgos

- `get_total_cost_map` tiene otros consumidores; sus resultados se compararán contra el comportamiento anterior mediante pruebas de paridad y límite de consultas.
- La fuente de verdad sigue siendo Point. No se crearán ventas, producciones, mermas, conversiones o snapshots.
- Los periodos históricos continúan accesibles por `?periodo=YYYY-MM`; el periodo elegido siempre se incluye en el selector aunque no tenga evidencia.
- Los avisos de autoridad de las fuentes no se convertirán artificialmente en verificados. La composición de jobs rodantes requiere un contrato de cobertura independiente y no es necesaria para corregir el bloqueo de la pantalla.

## Aceptación

- Abrir septiembre no emite ninguna consulta a `pos_bridge_pointinventorysnapshot` para construir el selector de meses.
- Las recetas sin merma no se envían al calculador de costos.
- Los resultados numéricos y las exportaciones conservan su contrato.
- La prueba del reporte y del balance Point pasa en PostgreSQL.
- En producción, el HTML autenticado carga con información del mes y un tiempo materialmente menor al diagnóstico de 34.4 segundos.
