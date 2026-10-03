# Ficha de fuentes — Ciruela Mediano, caso 3963

Fecha y ambiente: 3 de octubre de 2026, VPS de producción y código main 087cbec2. Investigación de solo lectura. No llamadas Point, importaciones, modificaciones de casos ni cambios de código en esta investigación.

## Necesidad y unidad de análisis

Explicar por qué el caso 3963 (Guamúchil, Ciruela Mediano) tiene salida histórica de una pieza, pero `source_trace.transfers=[]`. Distinguir identidad del artículo del documento, identidad del evento histórico y custodia física. La ecuación existente es 1 inicial − 1 salida = 0; no autoriza cierre físico.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Documento de transferencia | PointTransferLine / pos_bridge_transfer_lines | Sincronizador Point, job 41274 SUCCESS | PK 44672; transferencia/detalle 38598/539340; source_hash bc896066e60ce0ac67f2 | Producto, 1 enviada/1 recibida, finalizada, vigente, no cancelada | Trazabilidad por sucursal, materializador, auditor |
| Identidad Point del artículo | PointTransferLine.raw_payload.detail | Respuesta conservada /Transfer/GetTransfer | FK_articulo 112, isInsumo false | Código 0112; Pastel de Ciruela Mediano; PZA | El lector actual no usa esta FK |
| Producto | PointProduct | Catálogo e informes Point | PK 541, external_id 112 | SKU 0112; nombre y normalized_name Ciruela Mediano | Índices compartidos de trazabilidad |
| Receta | Receta | Catálogo ERP existente | PK 199 | Nombre Ciruela Mediano, codigo_point 0112 | Línea 44672 ya tiene receta_id 199 |
| Histórico de existencias | Importación canónica 707 | AuditStockHistoryService | Movimiento 1662087, caso 3963 | COMPLETE; unknown 0; remanente 0; salida 1 | Conciliación aritmética/materializador |
| Carga logística | RutaCargaChecklistLinea | Logística | FK point_transfer_line_id 44672 | Consulta exacta devuelve cero filas | Evidencia logística, no equivalencias por horario/nombre |
| Expediente | ProductInventoryAuditCase | InventoryAuditMaterializer / InventoryAuditAgent | PK 3963, branch 23, product 541 | Diferencia 0, BALANCED; transfers y transfer_evidence vacíos | Pantalla de investigación |

## Alias y equivalencias

| Identificadores | Estado | Evidencia y límite | Revisión requerida |
| --- | --- | --- | --- |
| raw FK_articulo 112, dominio producto → PointProduct.external_id 112 (PK 541) | Confirmada en el documento | Clave explícita conservada, is_insumo false / raw isInsumo false; no inferencia por SKU | Incorporación al contrato lector con validaciones y pruebas antes de aplicar |
| Origen branch 23 / Point 13 → ERP 6 Guamúchil; destino branch 6 / Point 12 → ERP 11 Devoluciones | Confirmada | Alias existentes activos; no crear otro alias | Ninguna equivalencia nueva |
| SKU 0112 → producto 541 exclusivamente | No confirmada; ambigua | También productos 427 (external official:0112) y 965 (external 948), nombres navideños | No fusionar ni cambiar catálogo |
| normalized_name de 427 → Ciruela | Inconsistencia conservada | Nombre actual navideño pero normalized_name `pastel de ciruela mediano`; duplica el de 541 | No corregir dato maestro sin autorización |
| Movimiento 1662087 → transferencia 38598/539340 | Candidata | 03/09 20:03:33.747 vs .750, cantidades y producto compatibles; diferencia 3 ms NO es FK | No afirmar identidad de evento ni prueba física |
| FK_articulo 112 de dominio insumo → producto 541 | Distinta / prohibida | Septiembre contiene 31 líneas is_insumo true con el mismo identificador, frente a 15 líneas de producto | Guardar separación de dominios |

## Documento y horarios conservados

Línea 44672: enviada 03/09/2026 20:03:33.750 Mazatlán, recibida 03/09/2026 22:44:02.767 Mazatlán. Point declara `is_received=true`, `is_finalized=true`, `is_cancelled=false`, `is_current_snapshot=true`; 1 PZA enviada y recibida. Esto acredita registro documental Point, no conteo físico ni persona ERP que apruebe el cierre. Las ocho unidades de la cabecera corresponden al total de la transferencia, no a Ciruela (una pieza).

## Causa reproducida y consumidores

`BranchInventoryTraceabilityService._resolve_product(line44672, indexes)` devuelve `(None, 'AMBIGUOUS_PRODUCT')`: consulta `product_id`, external_id por item_code, SKU y normalized_name, pero no la FK del artículo del detalle conservado. SKU 0112 es múltiple y la rutina termina antes del nombre; el nombre tampoco sería único por la inconsistencia de 427.

La rutina tiene cinco llamadores: ventas, producción, mermas, transferencias y destino de conversiones. El origen de conversión usa otra rutina, `_resolve_product_identity`. No extender interpretación de raw de una familia a todas sin comprobar su contrato.

`_apply_transfers` usa la rutina compartida y construye las listas de fuentes. `InventoryAuditMaterializer` conserva esas listas; `InventoryAuditAgent` obtiene sus transferencias desde `source_trace.transfers`. Su `_transfer_evidence([44672])` devuelve `{}` deliberadamente porque omite transferencias finalizadas con recibido igual a enviado: **no es un inventario completo de documentos**. No concluir ausencia del documento a partir de esa lista vacía.

El queryset mensual `.only(...)` de transferencias no carga `raw_payload`. Una eventual lectura por FK debe incluirlo para evitar consultas N+1. `PointOpenTransferSnapshotMember` es inmutable y no tiene raw_payload/FK_articulo: preservar evidencia congelada; no enriquecerla desde datos mutables actuales por aproximación.

## Decisión

Reutilizar el documento y las identidades explícitas existentes; no nueva tabla, captura, alias, cambio de categorías ni consulta Point. Registrar el defecto lector antes de una corrección mínima. No asignar manualmente la línea al expediente ni forzar saldos.

La autoridad mensual de mermas sigue bloqueada por la omisión/eliminación de 1683114 (autorización concreta solicitada y aún pendiente). Un arreglo de identidad no elimina ese bloqueo ni habilita un rebuild ignorándolo. No modificar importador/mermas/ventas/inventario, no cerrar caso ni mes. Tampoco repetir la solicitud humana ya realizada.

## Procedimiento reproducible y límites

- Inventario de fuentes con PostgreSQL de producción: `inventario_fuentes_datos --term transferencia --term ciruela --limit 12 --presence` no encuentra modelos por términos léxicos; no invalida fuentes verificadas por modelo/FK. La repetición `--term transfer --term PointProduct --limit 8` encuentra 14 modelos candidatos (8 mostrados), incluyendo PointProduct con external_id único y snapshots/historiales. Es metadato, no prueba semántica.
- QuerySet acotado PointTransferLine: PK 44672; raw detalle FK_articulo 112, sent_at dentro de septiembre Mazatlán, separado por is_insumo. Resultado: 15 líneas de producto y 31 de insumo. No sumar cantidades entre dominios.
- PointProduct: SKU 0112 o normalized_name de Ciruela; tres candidatos SKU, dos normalized_name. No escrituras.
- RutaCargaChecklistLinea: FK 44672, cero resultados. `_transfer_evidence([44672])` bajo requests.Session.request prohibido: `{}` sin HTTP.
- Grafo de código no devolvió los símbolos; se verificaron los llamadores en código. No se indexó ni modificó el repositorio.
- Auditoría de workspaces: main limpio/sincronizado 087cbec2, 33 worktrees, 0 detached, 0 sin registro; prune dry-run vacío. No nueva tarea de implementación.

## Riesgos y próximo corte verificable

Una corrección lectora futura debe probar SKU duplicado + FK explícita de producto; no resolver insumos por ese ID; conservar error ante FK desconocida/contradictoria; preservar fallback cuando no existe FK, snapshots y fuentes no transfer; evitar N+1. Aplicar flujo oficial worktree/TDD/CI/PR/deploy y validar las fuentes del expediente en pantalla solo después de recuperar autoridad mensual. Ninguna etiqueta COMPLETE/BALANCED constituye por sí sola aprobación o conteo físico.
