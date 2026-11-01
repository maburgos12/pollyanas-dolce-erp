# Fuentes: cierre principal de Rentabilidad

## Necesidad
Enero–agosto 2026, sucursal/mes: mostrar el mismo resultado gerencial conciliado en resumen, detalle e informe CSV/JSON. No reemplazarlo por contribución fiscal de productos ni presentar una cobertura por categorías como porcentaje monetario.

## Fuentes y evidencia
- PointDailySale: ventas brutas menos descuentos por sucursal histórica. Identidad externa 2: Crucero hasta junio; Bamoa desde julio. Reutilizar ventas_con_identidad_historica.
- LineaPresupuestoMensual ORIGINAL y reglas existentes: gastos directos y fondos de costos/producción/administración/logística. Reutilizar leer_estado_resultados y reparto_historico, incluidos controles de conciliación. Evidencia productiva previa: 77 filas y 539 importes, resultado $3,312,006.78.
- ProductoReventaCosto MANUAL: 14 decisiones DG de extras, tarifas, regalos y referencias de compra vigentes desde septiembre. Corrección histórica autorizada en este hilo; crear nuevas decisiones desde enero o la fecha de compra posterior, conservando originales y AuditLog. Identidad exacta external_id y nombre, sin nueva equivalencia.
- GastoOperativoMensual y real_breakdown: lector existente puede reconocer el reparto consolidado validado; habilitarlo enero–agosto conservando sus guardas de importe, fuente y vigencia.
- Nómina y horas extra: lectores existentes, informativas en reporte; nunca descontarlas otra vez del cierre que ya incluye los fondos.

## Decisión y consumidores
Reutilizar cierre en dashboard, detalle, reporte mensual, comando y CSV/JSON. Preservar análisis unitario y cobertura documental bajo enlaces explícitos. Ninguna tabla o migración nueva, ventas/snapshots/nómina sin cambios. No ejecutar IA.
El margen mostrado es ventas menos costos y producción asignados; no un margen variable para calcular punto de equilibrio. El resultado no prueba costos evitables de cierre ni cobertura documental total. No calcular YoY entre bases fiscales y gerenciales diferentes.

## Validación
Inventario de fuentes rentabilidad/gasto en PostgreSQL local. Contrastar 77 filas contra cierre original y exportaciones; verificar pantalla móvil, identidad histórica, períodos fuera de alcance y rechazo de cierre inconsistente. Mantener comprobantes y resultados fuera de Git en Documents/Codex/2026-11-01/rentabilidad-cierre-principal.
