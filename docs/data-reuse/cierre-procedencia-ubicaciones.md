# Ficha de fuentes — Cierre: procedencia y ubicaciones de existencias

Fecha y ambiente:19/11/2026. Código basecf29c900; PostgreSQL16 local propio55634 solo esquemas/fixtures. Evidencia productiva REPEATABLE READ READ ONLY existente de Point19nov18:09:50UTC, snapshot69833311, corte18novMazatlán; no segunda consulta ni captura.

## Necesidad y unidad de análisis

La alerta cuenta registros ExistenciaInsumo únicos por(insumo,almacén), mientras su tabla elige una sola ubicación por insumo. Se necesitan procedencia legada legible y todas las ubicaciones contadas, sin confundir filas con insumos faltantes ni origen del registro con apertura/saldo físico acreditado. Los registros actuales pueden actualizarse19nov aunque el fact global sea del18nov: mostrar actualizado_en por registro y advertir ese alcance.

## Fuentes candidatas

|Concepto|Modelo/tabla|Creador/actualizador|Identificador y ámbito|Evidencia|Consumidor|
|---|---|---|---|---|---|
|Registro actual por ubicación|inventario.ExistenciaInsumo|Aplicaciones de deltas,consumoBOM,sync/movimientos existentes|PK y unique(insumo_id,almacen)|140 filas para66insumos del fact:65sourceMANUAL_SYNC y75pendientes|Cierre inventario y referenciasHTML|
|Origen automático legado|trazabilidad_stock.fuente+creado_automaticamente|inventario.services_consumo_bom._apply_stock_delta|Misma fila exacta; fuenteCONSUMO_BOM y booleanTrue|73 de75 con ambos marcadores;2metadata{}|Nuevo rótulo de lectura parcial, sin escribirsource/metadata|
|Procedencia estándar|trazabilidad_stock.source/label/effective_at|inventario.stock_trace y escritores vigentes|Misma fila exacta|65MANUAL_SYNC;75sourcevacío|Regla existente de conteo intacta|
|Nombre de ubicación|ExistenciaInsumo.almacen/ALMACEN_CHOICES|get_almacen_display existente|Código exacto del almacén; noFK/nombre como identidad|66CuartoFrío+6Casa1+2Casa2+1Limpieza entre75|DetalleHTML por ubicación|
|Saldo global al cierre|reportes.FactInventarioDiario|Materializador existente|fecha18nov,insumo,sucursalNULL|66insumos; cantidades/unidades se preservan|Tabla existente|
|Stock vivo Point ALMACÉN|CanonicalPointInventoryService.read_many|Snapshots existentes, sin refresh|FK insumo,locationALMACEN,fecha propia19nov|Lecturas existentes de otra fecha/ubicación|Tabla existente, sin restar contra global|
|Movimientos/expedientes|MovimientoInventario/InsumoAuditSnapshot|Flujos actuales|insumo+almacén+vigencia yFKdocumental|74/75 con movimiento misma clave;1no, sin saldos certificados|Solo diagnóstico archivado, NO nuevas consultas/inferencias de este parche|

## Alias y equivalencias

CONSUMO_BOM con creado_automaticamente booleanTrue es el contrato del escritor para creación/uso automático del registro; no demuestra que todo el saldo ni su última escritura provenga deBOM. No se convierte en source estándar ni en autorización de saldo. Un string'true',otrofuente o metadatos ambiguos no se promueven al grupoBOM. Fuente estándar presente mantiene prioridad y sus etiquetas/reglas actuales; UNTRACED conserva advertencia. Ubicaciones del mismo insumo son distintas filas: no se fusionan ni se suman sus stocks para producir un nuevo saldo.

## Consumidores y decisión

Búsqueda de grafo intentada:search_graph fallóTransportclosed; fallback de símbolos acotado. build_daily_operational_closure solo es llamado por reportes.views.cierre_operativo y pruebas; _inventory_domain/_inventory_references solo desde ese constructor y pruebas. La vista renderiza cierre_operativo_diario.html, sin APIJSON/export compartido encontrado. Aun así no se eliminan/renombran campos existentes del contexto ni se cambian firmas: añadir clasificación/ubicaciones para HTML y mantener campos/cálculos previos.

Reutilizar los mismos modelos/consultas. Helper puro de lectura para procedencia: canónica existente, automáticaBOMlegada, sinmetadatos uotroorigenpendiente. Mantener75pendientes,65source y140registros bajo la misma revisión de fuentes, estadoRequiererevisión y reglas del cierre. Añadir resumen de filas porinsumo+ubicación/subgrupo y detalle de todas las ubicaciones/actualizado_en; no seleccionar una procedencia como si cubriera el insumo entero. Preservar tabla globalERP/Point/unidad/fechas sin sumar stock_actual.

No tablas, maestros, campos persistidos, scripts de backfill, nuevas capturas ni infer_stock_trace/reconcile_inventory_traceability. Sin cambios a escritores, API pública, corte de ventas o certificación financiera. Solo servicio de lectura, template, regresiones, esta ficha y tres versionesSW17 después de1760verificado. Autorización de alcance ratificada por raíz19nov antes de editar código.

## Procedimiento y límites

Inventario automatizado local:manage.py inventario_fuentes_datos --term existencia --term inventario (29 candidatos léxicos,15mostrados; no prueba identidad). migrate--check/check0 antes de editar. Producción reutilizada de ~/.codex-task-artifacts/cierre-existencias-traza-20261119/production-readonly.json; SHA del archivo original4cf818aed28ca68c1498e63fc077087d0f5b419c3f893f20b0c09ba5f43913ed, según clasificación del autor.73/2 y75/65/140 son evidencia de ese corte, no constantes en código ni promesa para otro día.

Pruebas:marcadorBOMestricto, metadata vacía/otroorigen/sourceUNTRACED/canónico, múltiples ubicaciones y fecha actual distinta del día, cero sinocultarlo, cifras/alertas/metadata sinmutar y sin N+1; render de todas las ubicaciones y flujo1440/390. RevisiónPoint independiente; rebase/CI únicos después de1760publicado/validado y coordinación de main estable. La existencia116/insumo190/Casa1 sigue pendiente de evidencia exacta fuera de este cambio de presentación.
