# Ficha de fuentes — costo documental por producto fabricado sin receta ERP, 2025

Fecha/ambiente: 6nov2026, producción PostgreSQL, consultas de sólo lectura. Grafo consultado primero; cobertura de clases incompleta, completada por metadatos Django y lecturas acotadas. `inventario_fuentes_datos --term production --term costo --term producto --presence --limit 50` ejecutado con conexión PostgreSQL comprobada; salida en inventario-fuentes-produccion-producto.json.

## Necesidad y unidad de análisis

20 pares producto/mes (enero–octubre2025), Galleta Oreo y Galleta Dulce de Leche: costeados por Point al producirse, pero el lector ERP depende de Receta y estos dos productos no tienen una. Deben tener costo nativo ponderado por unidades producidas, aparecer Fabricado y conservarse como evidencia provisional, sin inventar BOM, receta maestra o adquisición de reventa. Una línea nativa es un detalle de orden de producción, no una venta ni un consumo de insumo.

## Fuentes candidatas

| Concepto | Modelo/tabla | Creador/actualizador | Identidad/ámbito | Evidencia | Consumidores |
|---|---|---|---|---|---|
| Producto canónico | pos_bridge.PointProduct | sincronización Point | PK ERP918/920; native external_id875/876; SKU01GO01/01GDL01 | Identidad exacta nombre+SKU+nativeID | ventas, rentabilidad |
| Producción nativa | pos_bridge.PointProductionLine / pos_bridge_production_lines | integración movimientos Point | source_hash único; production_external_id+detail_external_id; fecha+sucursal | **0 filas2025**,14,835filas2026. Receta/insumo/sync_job opcionales; raw_payload preserva nativo | servicios de comparación, balance, cierre, trazabilidad e inventario diario |
| Documentos históricos nativos | point-crucero2025-candidatos.json + movimientos originales | reportes Point recuperados |216pares orden/detalle únicos,20producto-mes; branchPoint10/PK13 ProduccionCrucero |216/216 líneas positivas cotejadas individualmente con movimiento: cantidad, unidad, costo unitario exactos, total≤.00005; corroboración cabecera≤.01 | evidencia auditada del importador |
| Receta histórico | reportes.RecetaCostoHistoricoMensual | importadores/costeo | receta+mes | no hay Receta para875/876; no cabe sin crear maestro falso | lector común actual |
| Histórico reventa | reportes.ProductoReventaCostoHistoricoMensual | builder/adquisición | producto+mes | técnicamente tiene FK PointProduct, pero semántica adquisición/reventa; no es producción | costeo etapas/logística/tareas/builder además del lector |

## Identidades/unidades

- Confirmadas, sin alias nuevo: native875 → PointProduct918 → SKU01GO01 → Galleta Oreo; native876 → PointProduct920 → SKU01GDL01 → Galleta Dulce de Leche. Nunca interpretar875/876 como PK ERP.
- Confirmada unidad **U**:216/216producciones y1929renglones de reducción (1014Oreo,915Dulce) usan U. No convertir a PZA/KG/paquete; no hace falta factor de equivalencia.
- Branch nativa10 corresponde a PointBranch13 ProduccionCrucero, `erp_branch_id=None`; conservarlo. No vincular a sucursal ventaCrucero/Bamoa por nombre.
- Recetas inexistentes: no rellenar con GalletaNutella/Lotus ni con una receta actual parecida.

## Decisión propuesta

**Reutilizar PointProductionLine como staging nativo y extender sólo el lector común de costos.** No crear modelos/migraciones ni repurponer tabla de reventa. Importar216líneas históricas con receta/insumo/sync_job nulos, source_hash determinista y raw_payload nativo completo+referencia documental revisada. Importación idempotente limitada a identidades/fechas del manifiesto; respaldo y hashes de otrasfilas/2026antes/después.

**No llamar `MovementSyncService.persist_production_lines`:** además de staging resuelve matches, genera pendientes y puede crear movimientos de inventario/CEDIS. El lote documental debe escribir exclusivamente PointProductionLine y AuditLog, sin invocar esas derivaciones. Usar bulk_create en el importador auditado: post_save en reportes/signals.py encola auditoría de inventario y analytics; el lector directo no necesita esa reconstrucción.

Añadir helper acotado en `rentabilidad/services_costos.py` para costos mensuales de producción documental2025, sólo como fallback de filas sinReceta y sin costo de adquisición/decisión DG vigente. Cost=SUM(produced_quantity*unit_cost)/SUM(produced_quantity), seis decimales. Fuente estrictamente marcada como producción nativa revisada2025. El costo calculado llega a `cost_evidence`, `cost_is_resale=False`; mantener identidad resuelta por servicios existentes. No modificar campos de venta.

## Contrato de lectura

- Sólo año2025 y mes exacto de las ventas, disponible únicamente al corte de fin de mes (through); nunca arrastrar este lote a2026 ni usar las14,835líneas2026 automáticamente.
- Exigir marca documental de lote revisado, hash de soporte, fecha/periodo coherentes, grupos completos esperados en manifiesto (número de detalles y suma de cantidades/costo), y pares orden/detalle únicos.
- Exigir noinsumo, cantidades/costos positivos, unidadU, orden activa/statusfinal3, SKU+nombre+nativoPK_Producto iguales al producto canónico. Distinguir artículoID de PKERP.
- Una línea incompleta/duplicada/conflictiva no permite promediar silenciosamente el resto: dejar ese producto/mes sin costo.
- Consistencia entre columnas del staging y raw nativo, datos/hash del manifiesto. No confiar únicamente en un booleano `reviewed`.
- Mantener precedencia de fuentes ya costeadas, adquisiciones válidas y decisionesDG (incluidos ceros autorizados). No reemplazar histórico receta o costeos vigentes de otro alcance.
- Preservar `financial_close=False`; un costo de producción ponderado no equivale a valuación certificada de las existencias vendidas.

## Alcance de archivos/consumidores

Código: `rentabilidad/services_costos.py` y helper `services_produccion_documental.py`. Manifiesto inmutable revisable `rentabilidad/data/produccion_documental_2025.json`: 20 grupos y los hashes SHA256 de 216 payloads completos (cabecera nativa, detalle nativo, referencias documentales). No contiene información personal de responsables; sólo hashes y las identidades necesarias. Tests existentes: `rentabilidad/tests_costos_fuente.py`, `rentabilidad/tests_resultado_integral.py` y `reportes/tests_sales_confidence.py` para consumidores relevantes. Ficha final en docs/data-reuse dentro del worktree.

Consumidores inmediatos ya comparten `asignar_costos_filas`: `rentabilidad/views_rentabilidad.py` (etiquetaFabricado), `rentabilidad/services_resultado.py` (costo_materia_prima, no costo_reventa) y `reportes/sales_confidence.py` (cobertura). Se prueban, no se duplican helpers en cadauno.

Consumidores staging: monthly_finished_goods_comparison_service sólo considera receta noNULL y no cambiará cobertura completa; monthly_product_balance_service clasifica las nuevas filas recetaNULL como producción pendiente de identidadERP. Es correcto mantener explícito ese pendiente. Revisar product_month_closure_service, branch_inventory_traceability_service, daily_inventory_break_service e import_historical_product_closure_excel: no convertir216filas parciales en garantía de cobertura anual completa.

No usar tabla reventa: otros consumidores la contabilizan como adquisición/fuera de fabricación, y HistoricalCostingService puede sobreescribir o borrar filas sin fuente adquisitiva. Reutilizarla introduciría más cambios y etiquetas falsas.

## Pruebas y aceptación

1. Mes completo con dos costos/qtydistintas → promedio ponderado correcto, Fabricado, sinReceta nueva.
2. Identidadnativa875 vsPKERP918, nombre oSKU incorrectos → rechazar; unidadesincorrectas → rechazar.
3. Registro sinmarca/documento/hash, cantidad/costo cero, duplicado de orden/detalle o faltauna línea esperada → grupo no costeado.
4. Fuente del mes siguiente/anterior y2026 → no rellenar2025ni costear2026.
5. Adquisición/histórico receta/decisiónDG existente mantiene precedencia y etiqueta.
6. Consumidor producto, resultado integral y cobertura concuerdan; no duplicar costo enreventa/MP; utilidad mantiene estado provisional.
7. Importación216 exactas y repetición idempotente; tablas inventario/movimientos/Receta no mutadas, restoPointProductionLine y2026 hashesintactos.
8. Producción real:20SKU-mes resultantes,216líneasdocumentadas, ventas netas anuales43,864,167.99sin cambios; recuperación estimada119,380.00 de cobertura que debe comprobarse con lector instalado, no darse por hecha.

## Evidencia reproducible

- cookies216-pares-verificados.json:216pares ySHA movimiento.
- cookies-unidades-venta.json:1929renglonesU, muestras originales.
- crucero-produccion-candidatos-validacion.json:20promedios ydetalleorden/movimiento.
- produccion-cookies-tabla-existente.json:0filas2025/14,8352026.
- fuentes-modelos-costo-producto.json/inventario-fuentes-produccion-producto.json:fuentes ybranch nativa.

La implementación del lector usa el manifiesto versionado para rechazar cualquier payload no revisado, incluso si lleva la misma etiqueta. Calcula con Decimal desde el original y compara columnas redondeadas a su precisión declarada (ROUND_HALF_UP de PostgreSQL), sin perder los seis decimales nativos. No se ha importado producción. Los216pares y originales están conservados. La materialización requiere worktree/PGaislado, Factory, pruebas, revisión,CI,merge,deploy,importación autorizada,UIy limpieza propias.
