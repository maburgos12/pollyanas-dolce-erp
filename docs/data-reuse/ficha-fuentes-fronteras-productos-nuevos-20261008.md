# Ficha de fuentes — fronteras de productos nuevos

Fecha y ambiente: 8oct2026 local PG16 y originales de producción ya verificados.

## Necesidad y unidad de análisis

Saldo inicial/final por mes, FK producto y FK sucursal. Dot Cake comenzó en septiembre: falta línea de agosto, no historial original. Un cargo comercial Extra10 no es conversión de pastel.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Fronteras | PointProductHistoryImport / pos_bridge_product_history_imports; PointProductHistoryRow | Ingreso oficial de originales Point | FK producto/sucursal, archivo SHA, membership, corte UTC | 18 nuevos Dot y 2 Bamoa conservados, COMPLETE/unknown0/cadena; segunda sin cambios | Traza, balance mensual, pantalla, cierre, runtime |
| Manifiesto | PointHistoricalInventoryClosing/Line | Captura histórica original | closing6/7 y FK exacta | Dot ausente de agosto; no modificar documento ni fingerprint | Traza y balance |
| Cargo | PointConversionLine; PointRecipeNode | Sync original, no nuevo mapeo | 189/195, nodos2229/2469, código0227 | Helper existente APPROVED_EXTRA10_EXACT_NON_PRODUCED_CHARGE_V1 acredita clasificación, no ejecución de pastel | Traza, balance mensual |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Dot4358/8734 recetas507/509 | Confirmada por identidad exacta | PR1524 publicado; no convertir vasos de reventa en fabricados | Autorización existente |
| Import ID / closingline ID | Distinta | Tablas diferentes; prueba tipada independiente, jamás insertar import como referencia de closing | No nueva equivalencia |
| Extra10 / conversión fabricada | Distinta | Helper existente valida original íntegro y nodo no producido; no usar sólo nombre | Autorización ya aprobada |

## Decisión de diseño

Reutilizar reconcile_many y su validación original. Suplementar exclusivamente pares ausentes del manifiesto, candidatos del manifiesto opuesto, con COMPLETE real, original íntegro/membership/ecuación/cortes sin unknown. No sustituir línea existente no probada. Mantener cobertura/manifiesto originales y evidencia independiente tipada separada, sin físico ni nueva captura. Reutilizar exclusión comercial exacta existente antes de resolver destino de conversión.

Consumidores: servicios pos_bridge, materializador por prueba ya tipada, reporte, cierre documental, investigación, evidencia de vista y runtime. No modelos, migraciones, maestros ni escrituras Point.

Procedimiento: inventario_fuentes_datos --term historial --term cierre --term conversion en PostgreSQL aislado; candidatos léxicos no identidad. Pruebas reales de ingreso original, corrupción de membresía, veto de línea existente, consumidores y no duplicación.

Riesgos: no resolver saltos Zanahoria ni custodia; originales alterados deben fallar cerrado. Validación de producción pendiente del ciclo oficial.
