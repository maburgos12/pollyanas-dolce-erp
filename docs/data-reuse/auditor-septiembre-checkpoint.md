# Ficha de fuentes — checkpoint nativo de septiembre

Fecha y ambiente: 5oct2026 UTC; evidencia original de producción aceptada hasta
PR1477, edición y pruebas en PostgreSQL16 aislado. No nueva captura ni modelo.

## Necesidad y unidad de análisis

Actualizar el conocimiento obligatorio del agente por hecho, procedencia y límite:
publicación de lectores, clasificación comercial frente a identidad transaccional,
compra por descripción y causas técnicas que todavía impiden el cierre.

## Fuentes candidatas

| Concepto | Modelo / fuente | Escritor e identidad | Evidencia | Consumidor |
| --- | --- | --- | --- | --- |
| Conocimiento nativo | SKILL.md, procedimiento.md, septiembre-2026.md | Git; archivo y commit publicado | Contexto obligatorio13archivos, reviews5015/5016 | reconciliation_guard, observe_review |
| Clasificación documental | PointWasteLine1492, PointConversionLine193 y reglas existentes | Point original y lectores compartidos; movimiento/domain/regla | PR1477,267mermas/26conversiones, dos lecturas idénticas | Balance mensual y agente |
| Compra CakeTopper | Costes5797/5799, versiones5351/5353, históricos391/393 | PointPurchaseResaleCostSyncService; compra1668859/folioA16242, FK coste derivada | Raw compra por descripción,100PZA cada variante; representaciones duplicadas no compras adicionales | Costeo; investigación, no curación automática |
| Guard mensual | ProductMonthClosureService y fuentes originales | Servicios oficiales; mes/par/corte | Preview íntegro post1477 SHA8e25d3f6415052d43e0baec7d3557ae99afbbc7a328e05a2a7d4fb311d4a35ac | Cierre documental |

## Alias y equivalencias

La descripción comercial de una compra y la FK derivada del coste no equivalen
a FK producto original. Las dos descripciones de la misma compra no equivalen a
dos recepciones. REVENTA/ACCESORIO aprobado no acredita ejecución de conversión.
Los ceros documentales y snapshots no equivalen a historia COMPLETE ni conteo físico.

## Decisión de diseño

Extender sólo tres archivos existentes del contexto nativo; ninguna segunda tabla,
captura, scheduler ni binding. Se sustituye el estado obsoleto «en implementación»
por aceptación1477, se conserva el antecedente fechado y se incorporan límites
reproducibles de compra y guard. No cambia ningún contrato lector en esta tarea.

Procedimiento: comparar los originales íntegros y hashes con el checkpoint,
probar lectura nativa sin HTTP/operaciones y revisar los mismos escenarios de
presión antes/después. No ejecutar inventario_fuentes_datos: esta tarea no amplía
un cálculo o captura, únicamente reutiliza su documentación y contexto obligatorio.

Riesgos: conocimiento desactualizado, confundir FK derivada con transaccional o
reportar551fronteras como decisiones humanas. El contrato de522ceros requiere
aprobación distinta; las membresías duplicadas conservan fail-closed. La entrega
de este checkpoint no satisface los guards ni cierra septiembre.
