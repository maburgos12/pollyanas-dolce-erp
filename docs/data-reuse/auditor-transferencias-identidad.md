# Ficha de fuentes — identidad documental de transferencias

Fecha: 3 octubre2026. Ambiente: fuentes VPS read-only ya documentadas; PostgreSQL16 local aislado5470. Autorización humana explícita para lector de transferencias, sin cambios operativos.

## Necesidad y unidad de análisis

Una línea documental Point de transferencia, con dominio producto/insumo separado. Resolver el artículo explícito del documento, no inventar equivalencias entre maestros ni vincular eventos por horario.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creador / consumidor | Identificador y evidencia |
| --- | --- | --- | --- |
| Documento mutable | PointTransferLine / pos_bridge_transfer_lines | Sincronizador existente; BranchInventoryTraceabilityService, auditor/materializador | PK44672, folio38598/539340; raw.detail.FK_articulo112/isInsumoFalse |
| Producto | PointProduct | Catálogo existente; índices de trazabilidad | external_id único112→PK541; SKU0112 ambiguo entre541/427/965 |
| Documento Lotus | PointTransferLine | Mismo flujo | Payán11 líneas32PZA; Matriz26 líneas129PZA; FK818producto, SKU0160 compartido con external160 |
| Snapshot histórico | PointOpenTransferSnapshotMember | Captura inmutable; balance histórico | Sin rawFK; no enriquecer desde línea mutable |
| Fuentes restantes | ventas, producción, merma, conversiones | Resolver genérico compartido | Su contrato no cambia; no interpretar raw de transferencia en otra familia |

## Alias y equivalencias

FK112 en detalle declarado producto→external112 es identidad documental confirmada. FK112 en dominio insumo es distinto y jamás identifica producto541. SKU0112 o0160 son ambiguos, no maestros que deban fusionarse. Movimiento1662087 y transferencia38598 separados3ms siguen candidatos, no FK de evento. Recibido/finalizado acredita documento, no existencia física.

## Decisión de diseño

Extender exclusivamente _apply_transfers con resolver específico. FK presente debe ser entero positivo estricto, existir en índice external y tener dominio coherente; valores inválidos/desconocidos/contradictorios conservan issue y no fallback. SKU único contradictorio conserva conflicto. FK ausente conserva fallback anterior. Snapshot inmutable conserva su contrato y no obtiene raw desde otra tabla. Queryset incluye raw_payload para evitar N+1. Sin nueva tabla, captura, categoría, cambio de ventas ni stock.

## Consultas reproducibles / consumidores

inventario_fuentes_datos --term transfer --term PointProduct --limit8 sobre PostgreSQL local. Registros VPS ya acreditados en auditoria-ciruela-identidad-20261003.md, auditoria-bollo-lotus-payan-3432-20261003.md y auditoria-identidad-transferencias-lote-20261003.md del hilo; no repetir Point. Grafo devolvió símbolos con rutas incorrectas; módulo fuente verificado directamente.

Lectores afectados: balance por sucursal y proyección mensual que lo consume. Los guards de autoridad mensual, ventas comerciales discrepantes y snapshots permanecen. La fuente merma aún no autoritativa impide materialización; resolver129entradas no explica una venta comercial restante ni autoriza cierre.

## Aceptación

TDD de FK explícita/SKU ambiguo, dominios, IDs inválidos, unknown/conflicto, ausencia/fallback, snapshot congelado y consultas constantes. Suite consumidores sin normalización ni autoridad falsa. CI exactSHA completo, deploy oficial, comprobación read-only de documentos/huellas y pantalla autenticada sin aprobación ficticia.
